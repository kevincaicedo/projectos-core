//! Foundation roster registry (m0-s12).
//!
//! The eleven future product charters and the disposable M0 Echo charter are
//! data over one harness, not separate control loops. Their ids are fixed and
//! iteration order is stable for registry checks and generated surfaces.

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RosterCharter {
    Navigator,
    Analyst,
    Archivist,
    Planner,
    Foreman,
    Scout,
    Sentinel,
    IncidentCommander,
    Investigator,
    Verifier,
    Scribe,
    /// M0 walking-skeleton worker. It is deleted when Navigator lands (M2),
    /// rather than pretending the scaffold is a product specialist.
    Echo,
}

impl RosterCharter {
    pub const FOUNDATION_COUNT: usize = 11;
    pub const FOUNDATION: [Self; Self::FOUNDATION_COUNT] = [
        Self::Navigator,
        Self::Analyst,
        Self::Archivist,
        Self::Planner,
        Self::Foreman,
        Self::Scout,
        Self::Sentinel,
        Self::IncidentCommander,
        Self::Investigator,
        Self::Verifier,
        Self::Scribe,
    ];
    pub const COUNT: usize = Self::FOUNDATION_COUNT + 1;
    pub const ALL: [Self; Self::COUNT] = [
        Self::Navigator,
        Self::Analyst,
        Self::Archivist,
        Self::Planner,
        Self::Foreman,
        Self::Scout,
        Self::Sentinel,
        Self::IncidentCommander,
        Self::Investigator,
        Self::Verifier,
        Self::Scribe,
        Self::Echo,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Navigator => "Navigator",
            Self::Analyst => "Analyst",
            Self::Archivist => "Archivist",
            Self::Planner => "Planner",
            Self::Foreman => "Foreman",
            Self::Scout => "Scout",
            Self::Sentinel => "Sentinel",
            Self::IncidentCommander => "Incident Commander",
            Self::Investigator => "Investigator",
            Self::Verifier => "Verifier",
            Self::Scribe => "Scribe",
            Self::Echo => "Echo",
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RosterRegistry;

impl RosterRegistry {
    #[must_use]
    pub const fn contains(&self, charter: RosterCharter) -> bool {
        let _ = self;
        matches!(
            charter,
            RosterCharter::Navigator
                | RosterCharter::Analyst
                | RosterCharter::Archivist
                | RosterCharter::Planner
                | RosterCharter::Foreman
                | RosterCharter::Scout
                | RosterCharter::Sentinel
                | RosterCharter::IncidentCommander
                | RosterCharter::Investigator
                | RosterCharter::Verifier
                | RosterCharter::Scribe
                | RosterCharter::Echo
        )
    }

    #[must_use]
    pub const fn charters(&self) -> &'static [RosterCharter; RosterCharter::COUNT] {
        let _ = self;
        &RosterCharter::ALL
    }

    #[must_use]
    pub const fn foundation_charters(
        &self,
    ) -> &'static [RosterCharter; RosterCharter::FOUNDATION_COUNT] {
        let _ = self;
        &RosterCharter::FOUNDATION
    }
}

#[cfg(test)]
mod tests {
    use super::{RosterCharter, RosterRegistry};
    use std::collections::BTreeSet;

    #[test]
    fn all_eleven_charters_are_unique_and_registered() {
        let registry = RosterRegistry;
        let names = registry
            .foundation_charters()
            .iter()
            .map(|charter| charter.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(names.len(), RosterCharter::FOUNDATION_COUNT);
        assert!(
            registry
                .foundation_charters()
                .iter()
                .all(|charter| registry.contains(*charter))
        );
        assert!(registry.contains(RosterCharter::Echo));
        assert!(!names.contains(RosterCharter::Echo.as_str()));
    }
}
