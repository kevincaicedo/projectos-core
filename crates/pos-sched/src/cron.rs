//! Tz-aware cron: expression parsing, next-fire search, the preview query,
//! and the tick driver that turns schedules into jobs (m0-s14).
//!
//! ## Why a time-zone library
//!
//! A schedule is written in *civil* time ("02:30 every day, Europe/Berlin").
//! Twice a year that civil time either does not exist or exists twice, and the
//! difference between a job that runs once, twice, or never is a correctness
//! bug the user only discovers after it has already happened. `jiff` resolves
//! civil→instant with an explicit disambiguation policy over the IANA
//! database, which is exactly the decision this module must not improvise.
//!
//! ## The two DST policies, stated
//!
//! - **Spring forward (the civil time does not exist).** The firing shifts
//!   forward by the gap: a 02:30 job runs at 03:30 local on that day, once.
//!   The alternative — firing at the first instant after the gap — bunches
//!   every skipped schedule onto the same second.
//! - **Fall back (the civil time exists twice).** The firing takes the first
//!   (earlier) occurrence, once. The search walks civil minutes, so the
//!   repeated hour is simply never visited a second time.
//!
//! Both are `jiff`'s `Disambiguation::Compatible`, which is also what the
//! RFC 5545 calendaring rules choose.

use crate::SchedError;
use crate::job::{JobKind, JobSpec};
use crate::metrics::SchedulerMetrics;
use crate::queue::JobQueue;
use jiff::civil::DateTime;
use jiff::tz::TimeZone;
use jiff::{Span, Timestamp};
use pos_domain::{
    CronOverlapPolicy, CronRecord, CronRegisteredBody, DomainEvent, JobClass, JobCronOrigin,
    JobDurableState, JobListFilter, JobPriority, list_crons, list_jobs, read_cron,
};
use pos_foundation::{CronId, JobId, ProjectId, WallClock};
use pos_log::{Actor, ProjectLog};
use pos_store::blake3;
use std::fmt;
use std::sync::Arc;

/// Runs one `cron.preview` answers (L8: the bound is part of the contract).
/// Ten is the master-plan figure; the cap leaves room for an editor that
/// wants a fuller window without letting a caller ask for a year.
pub const CRON_PREVIEW_COUNT_MAX: u32 = 32;

/// Search steps before the next-fire scan gives up. The worst legitimate
/// schedule (`0 0 29 2 *`, a leap-day job) is under four years of day steps
/// plus one day of hour/minute steps; anything beyond that is a typo, and a
/// typo must surface as a typed error rather than as a hung tick.
pub const CRON_SEARCH_STEP_COUNT_MAX: u32 = 4 * 366 + 24 + 60 + 64;

/// Missed firings a single tick will scan before it stops counting. A laptop
/// asleep for a month must not turn one tick into thousands of scans.
const CRON_CATCHUP_SCAN_COUNT_MAX: u32 = 512;

const CRON_FIELD_COUNT: usize = 5;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CronParseError {
    pub field: &'static str,
    pub reason: String,
}

impl fmt::Display for CronParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "cron {} field is invalid: {}",
            self.field, self.reason
        )
    }
}

impl std::error::Error for CronParseError {}

/// A parsed five-field cron expression (`minute hour day-of-month month
/// day-of-week`), stored as bitsets so matching is a shift and a mask.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CronExpr {
    minutes: u64,
    hours: u32,
    days_of_month: u32,
    months: u16,
    days_of_week: u8,
    /// Vixie-cron rule: when *both* day fields are restricted the schedule
    /// fires when **either** matches. Recording restriction separately is the
    /// only way to tell `*` from an explicit full range.
    day_of_month_restricted: bool,
    day_of_week_restricted: bool,
}

impl CronExpr {
    pub fn parse(text: &str) -> Result<Self, CronParseError> {
        let fields: Vec<&str> = text.split_whitespace().collect();
        if fields.len() != CRON_FIELD_COUNT {
            return Err(CronParseError {
                field: "expression",
                reason: format!(
                    "{CRON_FIELD_COUNT} fields required (minute hour day-of-month month day-of-week), got {}",
                    fields.len()
                ),
            });
        }
        let minutes = parse_field(fields[0], "minute", 0, 59)?;
        let hours = parse_field(fields[1], "hour", 0, 23)?;
        let days_of_month = parse_field(fields[2], "day-of-month", 1, 31)?;
        let months = parse_field(fields[3], "month", 1, 12)?;
        let days_of_week = parse_day_of_week(fields[4])?;
        Ok(Self {
            minutes,
            hours: mask_u32(hours),
            days_of_month: mask_u32(days_of_month),
            months: mask_u16(months),
            days_of_week: mask_u8(days_of_week),
            day_of_month_restricted: fields[2] != "*",
            day_of_week_restricted: fields[4] != "*",
        })
    }

    fn matches_minute(self, minute: i8) -> bool {
        bit_set(self.minutes, minute)
    }

    fn matches_hour(self, hour: i8) -> bool {
        bit_set(u64::from(self.hours), hour)
    }

    fn matches_date(self, datetime: DateTime) -> bool {
        if !bit_set(u64::from(self.months), datetime.month()) {
            return false;
        }
        let day_of_month = bit_set(u64::from(self.days_of_month), datetime.day());
        let day_of_week = bit_set(
            u64::from(self.days_of_week),
            datetime.weekday().to_sunday_zero_offset(),
        );
        match (self.day_of_month_restricted, self.day_of_week_restricted) {
            (true, true) => day_of_month || day_of_week,
            _ => day_of_month && day_of_week,
        }
    }
}

/// An expression bound to a zone: the pair is what actually names an instant.
#[derive(Clone, Debug)]
pub struct CronSchedule {
    expr: CronExpr,
    tz: TimeZone,
    tz_name: String,
}

impl CronSchedule {
    /// Parses the expression and resolves the zone against the tz database.
    /// An unknown zone is a typed error here, not a silent UTC fallback — a
    /// schedule that quietly runs in the wrong zone is the exact bug this
    /// module exists to prevent.
    pub fn new(expr: &str, tz_name: &str) -> Result<Self, CronParseError> {
        let expr_parsed = CronExpr::parse(expr)?;
        let tz = TimeZone::get(tz_name).map_err(|error| CronParseError {
            field: "timezone",
            reason: format!("{tz_name:?} is not an IANA zone: {error}"),
        })?;
        Ok(Self {
            expr: expr_parsed,
            tz,
            tz_name: tz_name.to_owned(),
        })
    }

    #[must_use]
    pub fn tz_name(&self) -> &str {
        &self.tz_name
    }

    /// The first firing strictly after `after_ts_ms`, or `None` when the
    /// expression has no firing inside the search bound.
    #[must_use]
    pub fn next_fire_after(&self, after_ts_ms: i64) -> Option<i64> {
        let after = Timestamp::from_millisecond(after_ts_ms).ok()?;
        let civil = after.to_zoned(self.tz.clone()).datetime();
        // Cron granularity is one minute, so the search starts at the next
        // whole minute; sub-minute precision would only re-fire the current one.
        let mut candidate = start_of_minute(civil)?
            .checked_add(Span::new().minutes(1))
            .ok()?;
        for _ in 0..CRON_SEARCH_STEP_COUNT_MAX {
            if !self.expr.matches_date(candidate) {
                candidate = start_of_next_day(candidate)?;
                continue;
            }
            if !self.expr.matches_hour(candidate.hour()) {
                candidate = start_of_next_hour(candidate)?;
                continue;
            }
            if !self.expr.matches_minute(candidate.minute()) {
                candidate = candidate.checked_add(Span::new().minutes(1)).ok()?;
                continue;
            }
            // `compatible()` is the documented DST policy above: gaps shift
            // forward, ambiguity takes the earlier instant.
            if let Ok(zoned) = self.tz.to_ambiguous_zoned(candidate).compatible() {
                let fire_ts_ms = zoned.timestamp().as_millisecond();
                if fire_ts_ms > after_ts_ms {
                    return Some(fire_ts_ms);
                }
            }
            candidate = candidate.checked_add(Span::new().minutes(1)).ok()?;
        }
        None
    }

    /// The next `count` firings — the `cron.preview` answer. Clamped to
    /// [`CRON_PREVIEW_COUNT_MAX`]; the caller sees the clamp in the result.
    #[must_use]
    pub fn preview(&self, after_ts_ms: i64, count: u32) -> Vec<i64> {
        let count = count.min(CRON_PREVIEW_COUNT_MAX);
        let mut runs = Vec::with_capacity(count as usize);
        let mut cursor = after_ts_ms;
        for _ in 0..count {
            let Some(next) = self.next_fire_after(cursor) else {
                break;
            };
            runs.push(next);
            cursor = next;
        }
        runs
    }
}

/// What a caller registers. `cron_id` is the caller's stable name for the
/// schedule (see [`derive_cron_id`]) so editing the expression updates the
/// schedule instead of orphaning it.
#[derive(Clone, Debug)]
pub struct CronSpec {
    pub cron_id: CronId,
    pub job_kind: JobKind,
    pub expr: String,
    pub tz: String,
    pub overlap_policy: CronOverlapPolicy,
    pub enabled: bool,
    pub priority: JobPriority,
    pub class: JobClass,
    pub payload: Vec<u8>,
}

const CRON_ID_DOMAIN: &[u8] = b"projectos/cron-id/v1";

/// A stable id for a named schedule inside a project.
#[must_use]
pub fn derive_cron_id(project_id: ProjectId, name: &str) -> CronId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(CRON_ID_DOMAIN);
    hasher.update(&project_id.into_bytes());
    hasher.update(name.as_bytes());
    let digest = hasher.finalize();
    let mut id = [0_u8; 16];
    id.copy_from_slice(&digest.as_bytes()[..16]);
    CronId::from_bytes(id)
}

/// What one tick did. Every number is reported, including the ones that mean
/// "nothing happened, on purpose" — a skipped overlap and a swallowed missed
/// tick are decisions the user is entitled to see (L8).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CronTickReport {
    pub fired: Vec<JobId>,
    pub skipped_overlap_count: u32,
    pub superseded_count: u32,
    /// Firings that elapsed while nothing was running and were collapsed into
    /// the single catch-up job.
    pub missed_tick_count: u32,
}

/// Turns registered schedules into jobs. Stateless: everything it needs is in
/// the projections, so a restarted process resumes mid-schedule correctly.
pub struct CronDriver {
    queue: Arc<JobQueue>,
    metrics: Arc<SchedulerMetrics>,
}

impl CronDriver {
    #[must_use]
    pub fn new(queue: Arc<JobQueue>) -> Self {
        let metrics = Arc::clone(queue.metrics());
        Self { queue, metrics }
    }

    /// Records a schedule as a durable fact. Re-registering the same id
    /// updates it in place; the watermark is preserved by the projection.
    pub fn register(
        &self,
        log: &ProjectLog,
        project_id: ProjectId,
        spec: &CronSpec,
        clock: &dyn WallClock,
    ) -> Result<(), SchedError> {
        // Parsed before it is durable: an unparseable schedule in the log
        // would be a permanent tick failure nobody asked for.
        CronSchedule::new(&spec.expr, &spec.tz)?;
        let event = DomainEvent::CronRegistered(CronRegisteredBody::V1 {
            cron_id: spec.cron_id,
            project_id,
            job_kind: spec.job_kind.as_str().to_owned(),
            expr: spec.expr.clone(),
            tz: spec.tz.clone(),
            overlap_policy: spec.overlap_policy,
            enabled: spec.enabled,
            priority: spec.priority,
            class: spec.class,
            payload: spec.payload.clone(),
        });
        let request = event.into_request(
            self.queue.config().device,
            Actor::System(JobId::from_bytes(spec.cron_id.into_bytes())),
        )?;
        log.append(request, clock)?;
        Ok(())
    }

    /// Fires every schedule whose time has come. One job per schedule per
    /// tick (I6), keyed by the nominal fire instant so a catch-up firing and
    /// the on-time firing it replaces are the same job.
    pub fn tick(
        &self,
        log: &ProjectLog,
        project_id: ProjectId,
        clock: &dyn WallClock,
    ) -> Result<CronTickReport, SchedError> {
        let now_ms = clock.now_ms();
        let mut report = CronTickReport::default();
        for record in list_crons(log)? {
            if !record.enabled {
                continue;
            }
            self.tick_one(log, project_id, &record, now_ms, clock, &mut report)?;
        }
        self.metrics.record_cron_tick(
            report.fired.len() as u64,
            u64::from(report.skipped_overlap_count),
            u64::from(report.missed_tick_count),
        );
        Ok(report)
    }

    fn tick_one(
        &self,
        log: &ProjectLog,
        project_id: ProjectId,
        record: &CronRecord,
        now_ms: u64,
        clock: &dyn WallClock,
        report: &mut CronTickReport,
    ) -> Result<(), SchedError> {
        let schedule = CronSchedule::new(&record.expr, &record.tz)?;
        let Some(due) = due_firing(&schedule, record.watermark_ts_ms(), now_ms) else {
            return Ok(());
        };
        report.missed_tick_count += due.missed_count;
        let live = self.live_jobs_of(log, record)?;
        match record.overlap_policy {
            CronOverlapPolicy::Skip if !live.is_empty() => {
                report.skipped_overlap_count += 1;
                return Ok(());
            }
            CronOverlapPolicy::CancelPrevious => {
                for job_id in live {
                    self.queue.supersede(log, job_id, record.cron_id, clock)?;
                    report.superseded_count += 1;
                }
            }
            CronOverlapPolicy::Skip | CronOverlapPolicy::Queue => {}
        }
        let spec = JobSpec::new(
            JobKind::new(record.job_kind.clone()).map_err(|error| SchedError::InvalidSpec {
                field: "job_kind",
                reason: error.to_string(),
            })?,
            format!("cron:{}:{}", record.cron_id.to_hex(), due.scheduled_ts_ms),
        )
        .with_class(record.class)
        .with_priority(record.priority)
        .with_payload(record.payload.clone())
        .with_run_at_ts_ms(due.scheduled_ts_ms);
        let spec = JobSpec {
            cron: Some(JobCronOrigin {
                cron_id: record.cron_id,
                scheduled_ts_ms: due.scheduled_ts_ms,
            }),
            ..spec
        };
        let outcome = self.queue.enqueue(log, project_id, &spec, clock)?;
        if !outcome.is_duplicate() {
            report.fired.push(outcome.job_id());
        }
        Ok(())
    }

    /// Jobs of this schedule that have not reached a terminal state.
    fn live_jobs_of(
        &self,
        log: &ProjectLog,
        record: &CronRecord,
    ) -> Result<Vec<JobId>, SchedError> {
        let filter = JobListFilter {
            state: Some(JobDurableState::Queued),
            cron_id: Some(record.cron_id),
            ..JobListFilter::default()
        };
        Ok(list_jobs(log, filter)?
            .into_iter()
            .map(|job| job.job_id)
            .collect())
    }
}

struct DueFiring {
    scheduled_ts_ms: u64,
    missed_count: u32,
}

/// The firing a tick owes: the most recent one at or before `now`, plus how
/// many earlier ones were collapsed into it.
///
/// Collapsing rather than replaying is deliberate — a device that slept for a
/// week owes one digest, not a week of them — and the count makes the choice
/// visible instead of silent.
fn due_firing(schedule: &CronSchedule, watermark_ts_ms: u64, now_ms: u64) -> Option<DueFiring> {
    let now = i64::try_from(now_ms).unwrap_or(i64::MAX);
    let mut cursor = i64::try_from(watermark_ts_ms).unwrap_or(i64::MAX);
    let mut latest = None;
    let mut count = 0_u32;
    for _ in 0..CRON_CATCHUP_SCAN_COUNT_MAX {
        let Some(next) = schedule.next_fire_after(cursor) else {
            break;
        };
        if next > now {
            break;
        }
        latest = Some(next);
        cursor = next;
        count += 1;
    }
    latest.map(|scheduled| DueFiring {
        scheduled_ts_ms: u64::try_from(scheduled).unwrap_or(0),
        missed_count: count.saturating_sub(1),
    })
}

/// Reads a registered schedule and answers its preview — the read path
/// `cron.preview` uses when the caller names a stored schedule.
pub fn preview_registered(
    log: &ProjectLog,
    cron_id: CronId,
    after_ts_ms: i64,
    count: u32,
) -> Result<Option<Vec<i64>>, SchedError> {
    let Some(record) = read_cron(log, cron_id)? else {
        return Ok(None);
    };
    let schedule = CronSchedule::new(&record.expr, &record.tz)?;
    Ok(Some(schedule.preview(after_ts_ms, count)))
}

fn start_of_minute(datetime: DateTime) -> Option<DateTime> {
    DateTime::new(
        datetime.year(),
        datetime.month(),
        datetime.day(),
        datetime.hour(),
        datetime.minute(),
        0,
        0,
    )
    .ok()
}

fn start_of_next_hour(datetime: DateTime) -> Option<DateTime> {
    if datetime.hour() >= 23 {
        return start_of_next_day(datetime);
    }
    DateTime::new(
        datetime.year(),
        datetime.month(),
        datetime.day(),
        datetime.hour() + 1,
        0,
        0,
        0,
    )
    .ok()
}

fn start_of_next_day(datetime: DateTime) -> Option<DateTime> {
    let tomorrow = datetime.date().checked_add(Span::new().days(1)).ok()?;
    Some(tomorrow.at(0, 0, 0, 0))
}

fn bit_set(mask: u64, value: i8) -> bool {
    u32::try_from(value).is_ok_and(|value| value < 64 && (mask >> value) & 1 == 1)
}

fn mask_u32(mask: u64) -> u32 {
    u32::try_from(mask & u64::from(u32::MAX)).unwrap_or(0)
}

fn mask_u16(mask: u64) -> u16 {
    u16::try_from(mask & u64::from(u16::MAX)).unwrap_or(0)
}

fn mask_u8(mask: u64) -> u8 {
    u8::try_from(mask & u64::from(u8::MAX)).unwrap_or(0)
}

/// `*`, `a`, `a-b`, `a-b/s`, `*/s`, and comma-separated lists of those.
/// Anything else is a typed error naming the field — cron expressions are
/// user input, and a mis-parsed one silently runs at the wrong time.
fn parse_field(text: &str, field: &'static str, min: u8, max: u8) -> Result<u64, CronParseError> {
    let invalid = |reason: String| CronParseError { field, reason };
    let mut mask = 0_u64;
    for part in text.split(',') {
        if part.is_empty() {
            return Err(invalid(format!("empty item in {text:?}")));
        }
        let (range_text, step) = match part.split_once('/') {
            Some((range_text, step_text)) => {
                let step: u8 = step_text
                    .parse()
                    .map_err(|_| invalid(format!("step {step_text:?} is not a number")))?;
                if step == 0 {
                    return Err(invalid("step 0 would never advance".to_owned()));
                }
                (range_text, step)
            }
            None => (part, 1),
        };
        let (first, last) = parse_range(range_text, field, min, max)?;
        let mut value = first;
        while value <= last {
            mask |= 1_u64 << value;
            value = value.saturating_add(step);
            if step == 0 {
                break;
            }
        }
    }
    if mask == 0 {
        return Err(invalid(format!("{text:?} selects no values")));
    }
    Ok(mask)
}

fn parse_range(
    text: &str,
    field: &'static str,
    min: u8,
    max: u8,
) -> Result<(u8, u8), CronParseError> {
    let invalid = |reason: String| CronParseError { field, reason };
    if text == "*" {
        return Ok((min, max));
    }
    let (first_text, last_text) = match text.split_once('-') {
        Some((first, last)) => (first, last),
        None => (text, text),
    };
    let first: u8 = first_text
        .parse()
        .map_err(|_| invalid(format!("{first_text:?} is not a number")))?;
    let last: u8 = last_text
        .parse()
        .map_err(|_| invalid(format!("{last_text:?} is not a number")))?;
    if first < min || last > max || first > last {
        return Err(invalid(format!(
            "{text:?} is outside the {min}..={max} range"
        )));
    }
    Ok((first, last))
}

/// Day-of-week accepts `7` as Sunday, the widely-implemented cron extension;
/// the bitset is normalized to 0..=6 with 0 = Sunday.
fn parse_day_of_week(text: &str) -> Result<u64, CronParseError> {
    let mask = parse_field(text, "day-of-week", 0, 7)?;
    let sunday_from_seven = (mask >> 7) & 1;
    Ok(((mask & 0x7f) | sunday_from_seven) & 0x7f)
}

#[cfg(test)]
mod tests {
    use super::{CronExpr, CronSchedule, due_firing};

    #[test]
    fn expressions_parse_ranges_steps_and_lists() {
        let every_minute = CronExpr::parse("* * * * *").expect("valid");
        assert!(every_minute.matches_minute(0) && every_minute.matches_minute(59));
        let quarter = CronExpr::parse("0,15,30,45 * * * *").expect("valid");
        assert!(quarter.matches_minute(15));
        assert!(!quarter.matches_minute(16));
        let stepped = CronExpr::parse("*/20 9-17 * * 1-5").expect("valid");
        assert!(stepped.matches_minute(40));
        assert!(!stepped.matches_minute(41));
        assert!(stepped.matches_hour(9) && stepped.matches_hour(17));
        assert!(!stepped.matches_hour(18));
    }

    #[test]
    fn malformed_expressions_are_typed_errors_naming_the_field() {
        assert_eq!(
            CronExpr::parse("* * * *")
                .expect_err("too few fields")
                .field,
            "expression"
        );
        assert_eq!(
            CronExpr::parse("60 * * * *")
                .expect_err("minute out of range")
                .field,
            "minute"
        );
        assert_eq!(
            CronExpr::parse("* */0 * * *").expect_err("zero step").field,
            "hour"
        );
        assert_eq!(
            CronExpr::parse("* * * 13 *").expect_err("month 13").field,
            "month"
        );
    }

    #[test]
    fn an_unknown_zone_is_refused_rather_than_defaulted_to_utc() {
        let error = CronSchedule::new("0 3 * * *", "Mars/Olympus").expect_err("no such zone");
        assert_eq!(error.field, "timezone");
    }

    #[test]
    fn a_missed_window_collapses_to_one_firing_and_reports_the_rest() {
        let schedule = CronSchedule::new("0 * * * *", "UTC").expect("valid");
        // 2026-03-01T00:00:00Z, then five hours of sleep.
        let watermark = 1_772_323_200_000_u64;
        let now = watermark + 5 * 3_600_000;
        let due = due_firing(&schedule, watermark, now).expect("firings elapsed");
        assert_eq!(due.scheduled_ts_ms, now);
        assert_eq!(due.missed_count, 4);
    }
}
