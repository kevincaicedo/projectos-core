//! The m0-s08 acceptance suite over the real served shell: cross-tenant
//! isolation on EVERY registered route (the suite iterates the registry, so
//! a new name is covered — or failed — automatically), the viewer RBAC
//! matrix, and the audit-log + zero-secret-material oracles.

#![forbid(unsafe_code)]

use pos_server::control::Role;
use pos_server::web::{ServerConfig, ServerState, serve};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;

use pos_api::{CommandName, QueryName, StreamName};

/// A served shell on an ephemeral loopback port with its own data root.
struct ServedShell {
    addr: SocketAddr,
    state: Arc<ServerState>,
    #[allow(
        dead_code,
        reason = "held for its Drop: the tempdir outlives the server"
    )]
    data_root: tempfile::TempDir,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

/// One authenticated browser, as far as the server can tell.
struct Client {
    cookie: String,
    account_id: String,
    workspace_id: String,
    projects_root: String,
}

impl ServedShell {
    fn start() -> Self {
        let data_root = tempfile::tempdir().expect("tempdir");
        let state = ServerState::initialize(&ServerConfig {
            data_root: data_root.path().to_path_buf(),
            ui_dist: None,
        })
        .expect("server state initializes");
        let served = Arc::clone(&state);
        let (shutdown, on_shutdown) = tokio::sync::oneshot::channel::<()>();
        let (addr_tx, addr_rx) = std::sync::mpsc::channel::<SocketAddr>();
        let thread = std::thread::spawn(move || {
            let tokio_runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test tokio runtime builds");
            tokio_runtime.block_on(async move {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                    .await
                    .expect("ephemeral loopback bind succeeds");
                addr_tx
                    .send(listener.local_addr().expect("bound socket has an addr"))
                    .expect("test main thread is waiting for the addr");
                serve(listener, served, async {
                    let _ = on_shutdown.await;
                })
                .await
                .expect("serve runs until shutdown");
            });
        });
        let addr = addr_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("server reports its address within 10s");
        Self {
            addr,
            state,
            data_root,
            shutdown: Some(shutdown),
            thread: Some(thread),
        }
    }

    fn request(
        &self,
        method: &str,
        target: &str,
        cookie: &str,
        body: &str,
    ) -> (u16, Vec<(String, String)>, String) {
        let mut stream = TcpStream::connect(self.addr).expect("connect to the served shell");
        let mut request = format!(
            "{method} {target} HTTP/1.1\r\nhost: {}\r\nconnection: close\r\ncontent-length: {}\r\n",
            self.addr,
            body.len()
        );
        if !cookie.is_empty() {
            request.push_str(&format!("cookie: {cookie}\r\n"));
        }
        request.push_str("\r\n");
        request.push_str(body);
        stream
            .write_all(request.as_bytes())
            .expect("request writes");
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).expect("response reads");
        let text = String::from_utf8_lossy(&raw);
        let (head, body) = text
            .split_once("\r\n\r\n")
            .expect("response has a header/body separator");
        let status: u16 = head
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|code| code.parse().ok())
            .expect("status line parses");
        let headers = head
            .lines()
            .skip(1)
            .filter_map(|line| {
                let (name, value) = line.split_once(':')?;
                Some((name.trim().to_ascii_lowercase(), value.trim().to_owned()))
            })
            .collect();
        (status, headers, body.to_owned())
    }

    fn signup(&self, email: &str, password: &str) -> Client {
        let body = format!("{{\"email\":\"{email}\",\"password\":\"{password}\"}}");
        let (status, headers, response) = self.request("POST", "/auth/signup", "", &body);
        assert_eq!(status, 200, "signup failed: {response}");
        let set_cookie = headers
            .iter()
            .find(|(name, _)| name == "set-cookie")
            .map(|(_, value)| value.clone())
            .expect("signup sets the session cookie");
        for attribute in ["HttpOnly", "SameSite=Lax", "Secure", "Path=/"] {
            assert!(
                set_cookie.contains(attribute),
                "session cookie is missing {attribute}: {set_cookie}"
            );
        }
        let cookie = set_cookie
            .split_once(';')
            .map(|(pair, _)| pair.to_owned())
            .expect("cookie has a name=value pair");
        Client {
            cookie,
            account_id: json_field(&response, "accountId"),
            workspace_id: json_field(&response, "workspaceId"),
            projects_root: json_field(&response, "projectsRoot"),
        }
    }
}

impl Drop for ServedShell {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Minimal happy-path field extraction from the tiny auth bodies this suite
/// controls.
fn json_field(body: &str, field: &str) -> String {
    let value: serde_json::Value = serde_json::from_str(body).expect("auth body parses");
    value
        .get(field)
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| panic!("{field} missing from {body}"))
        .to_owned()
}

fn percent_encode(text: &str) -> String {
    let mut encoded = String::new();
    for byte in text.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            other => encoded.push_str(&format!("%{other:02X}")),
        }
    }
    encoded
}

fn project_path(client: &Client, name: &str) -> String {
    format!("{}/{name}.pos", client.projects_root)
}

fn path_input(path: &str) -> String {
    format!(
        "{{\"path\":{}}}",
        serde_json::to_string(path).expect("serializes")
    )
}

/// Dispatches `(name, input)` through the authenticated API surface.
fn api(shell: &ServedShell, client: &Client, kind: &str, name: &str, input: &str) -> (u16, String) {
    let (status, _, body) = match kind {
        "cmd" => shell.request("POST", &format!("/api/cmd/{name}"), &client.cookie, input),
        "query" => shell.request(
            "GET",
            &format!("/api/query/{name}?input={}", percent_encode(input)),
            &client.cookie,
            "",
        ),
        "stream" => shell.request(
            "GET",
            &format!("/api/stream/{name}?input={}", percent_encode(input)),
            &client.cookie,
            "",
        ),
        other => panic!("unknown dispatch kind {other}"),
    };
    (status, body)
}

/// Every registered route refuses an unauthenticated request with the typed
/// envelope — deny-by-default proven registry-wide.
#[test]
fn every_registered_route_requires_a_session() {
    let shell = ServedShell::start();
    for query in QueryName::ALL {
        let (status, _, body) =
            shell.request("GET", &format!("/api/query/{}", query.as_str()), "", "");
        assert_eq!(status, 401, "{} answered without a session", query.as_str());
        assert!(body.contains("\"code\":\"unauthenticated\""));
    }
    for command in CommandName::ALL {
        let (status, _, body) =
            shell.request("POST", &format!("/api/cmd/{}", command.as_str()), "", "{}");
        assert_eq!(
            status,
            401,
            "{} answered without a session",
            command.as_str()
        );
        assert!(body.contains("\"code\":\"unauthenticated\""));
    }
    for stream in StreamName::ALL {
        let (status, _, body) =
            shell.request("GET", &format!("/api/stream/{}", stream.as_str()), "", "");
        assert_eq!(
            status,
            401,
            "{} answered without a session",
            stream.as_str()
        );
        assert!(body.contains("\"code\":\"unauthenticated\""));
    }
    // A garbage cookie is unauthenticated, not an error.
    let (status, _, _) = shell.request("GET", "/api/query/health", "pos_session=deadbeef", "");
    assert_eq!(status, 401);
}

/// The m0-s08 cross-tenant AC: user B, enumerating ids, cannot read or
/// mutate user A's projects through ANY registered API route. The suite
/// iterates the registry: every name is dispatched by B against A's
/// resources, and no response may carry A's data.
#[test]
fn cross_tenant_isolation_holds_on_every_registered_route() {
    let shell = ServedShell::start();
    let a = shell.signup("a@example.com", "alpha password 1");
    let b = shell.signup("b@example.com", "bravo password 1");

    // A builds real state: a created, seeded, opened project.
    let alpha = project_path(&a, "alpha");
    let (status, body) = api(
        &shell,
        &a,
        "cmd",
        "project.create",
        &format!(
            "{{\"path\":{},\"name\":\"Secret Alpha\"}}",
            serde_json::to_string(&alpha).expect("serializes")
        ),
    );
    assert_eq!(status, 200, "A cannot create: {body}");
    let a_project_id = json_field(&body, "projectId");
    let (status, _) = api(
        &shell,
        &a,
        "cmd",
        "project.seed-synthetic",
        &format!(
            "{{\"path\":{},\"eventCount\":16}}",
            serde_json::to_string(&alpha).expect("serializes")
        ),
    );
    assert_eq!(status, 200);
    let (status, _) = api(&shell, &a, "cmd", "project.open", &path_input(&alpha));
    assert_eq!(status, 200);

    // B enumerates A's ids: workspace hex is known, project path is known.
    // Every registered name gets a row; path-bearing ops target A's project.
    let a_export = format!("{}/exfil.pos", a.projects_root);
    let mut rows: Vec<(&str, String, String)> = Vec::new();
    for query in QueryName::ALL {
        let input = match query {
            QueryName::ProjectInspect | QueryName::ProjectVerify => path_input(&alpha),
            _ => "{}".to_owned(),
        };
        rows.push(("query", query.as_str().to_owned(), input));
    }
    for command in CommandName::ALL {
        let input = match command {
            CommandName::ProjectCreate => format!(
                "{{\"path\":{}}}",
                serde_json::to_string(&format!("{}/implant.pos", a.projects_root))
                    .expect("serializes")
            ),
            CommandName::ProjectSeedSynthetic => format!(
                "{{\"path\":{},\"eventCount\":1}}",
                serde_json::to_string(&alpha).expect("serializes")
            ),
            CommandName::ProjectExport => format!(
                "{{\"path\":{},\"out\":{}}}",
                serde_json::to_string(&alpha).expect("serializes"),
                serde_json::to_string(&a_export).expect("serializes")
            ),
            CommandName::ProjectOpen => path_input(&alpha),
            _ => "{}".to_owned(),
        };
        rows.push(("cmd", command.as_str().to_owned(), input));
    }
    for stream in StreamName::ALL {
        rows.push(("stream", stream.as_str().to_owned(), "{}".to_owned()));
    }

    let b_client = &b;
    for (kind, name, input) in &rows {
        let (status, body) = api(&shell, b_client, kind, name, input);
        // The isolation property: nothing of A's ever reaches B.
        assert!(
            !body.contains(&a_project_id),
            "{name}: A's project id leaked to B: {body}"
        );
        assert!(
            !body.contains("Secret Alpha"),
            "{name}: A's project name leaked to B: {body}"
        );
        // Path-bearing rows must be refused outright.
        let targets_a = input.contains(&a.projects_root);
        if targets_a {
            assert_eq!(
                status, 403,
                "{name}: B reached A's workspace (status {status}): {body}"
            );
            assert!(body.contains("\"code\":\"forbidden\""), "{name}: {body}");
        }
    }

    // B's own session state saw none of it.
    let (_, list) = api(&shell, &b, "query", "project.list", "{}");
    assert!(
        list.contains("\"projects\":[]"),
        "B's list is not empty: {list}"
    );
    let (_, health) = api(&shell, &b, "query", "health", "{}");
    assert!(health.contains("\"openProjectCount\":0"));

    // A, the positive control, still sees exactly A's state.
    let (_, list) = api(&shell, &a, "query", "project.list", "{}");
    assert!(list.contains(&a_project_id));

    // Paths outside the placement grammar never reach the filesystem.
    for smuggled in ["/etc/passwd", "../../escape.pos", "relative.pos"] {
        let (status, body) = api(
            &shell,
            &b,
            "query",
            "project.inspect",
            &path_input(smuggled),
        );
        assert_eq!(status, 400, "{smuggled} was not refused: {body}");
        assert!(body.contains("\"code\":\"invalid_input\""));
    }
}

/// The m0-s08 RBAC matrix AC: a viewer-role account can read but cannot
/// invoke any mutating command, across the whole v0 surface.
#[test]
fn a_viewer_cannot_invoke_mutating_commands() {
    let shell = ServedShell::start();
    let a = shell.signup("owner@example.com", "owner password 1");
    let alpha = project_path(&a, "alpha");
    let (status, _) = api(&shell, &a, "cmd", "project.create", &path_input(&alpha));
    assert_eq!(status, 200);

    // C becomes a pure viewer: viewer in A's workspace, and their own
    // personal membership downgraded through the operator seam (the v0
    // registry deliberately has no invite/role routes — auth surface stays
    // tiny; the seam is the documented path until a later milestone).
    let c = shell.signup("viewer@example.com", "viewer password 1");
    let a_ws: [u8; 16] = hex_bytes(&a.workspace_id);
    let c_ws: [u8; 16] = hex_bytes(&c.workspace_id);
    let c_id: [u8; 16] = hex_bytes(&c.account_id);
    shell
        .state
        .control()
        .grant_membership(a_ws, c_id, Role::Viewer)
        .expect("grant viewer in A's workspace");
    shell
        .state
        .control()
        .grant_membership(c_ws, c_id, Role::Viewer)
        .expect("downgrade personal membership");

    // Reads on A's project: allowed for the viewer.
    for read in ["project.inspect", "project.verify"] {
        let (status, body) = api(&shell, &c, "query", read, &path_input(&alpha));
        assert_eq!(status, 200, "viewer read {read} refused: {body}");
    }
    let (status, body) = api(&shell, &c, "cmd", "project.open", &path_input(&alpha));
    assert_eq!(status, 200, "viewer open refused: {body}");

    // Every mutating command in the registry: refused with `forbidden`.
    let viewer_target = format!("{}/viewer-write.pos", a.projects_root);
    for command in CommandName::ALL {
        let input = match command {
            CommandName::ProjectOpen => continue, // read-shaped, proven above
            CommandName::ProjectCreate => path_input(&viewer_target),
            CommandName::ProjectSeedSynthetic => format!(
                "{{\"path\":{},\"eventCount\":1}}",
                serde_json::to_string(&alpha).expect("serializes")
            ),
            CommandName::ProjectExport => format!(
                "{{\"path\":{},\"out\":{}}}",
                serde_json::to_string(&alpha).expect("serializes"),
                serde_json::to_string(&viewer_target).expect("serializes")
            ),
            // Any future command is presumed mutating until this suite gets
            // a deliberate row — deny-by-default extends to the test.
            _ => "{}".to_owned(),
        };
        let (status, body) = api(&shell, &c, "cmd", command.as_str(), &input);
        assert_eq!(status, 403, "viewer invoked {}: {body}", command.as_str());
        assert!(body.contains("\"code\":\"forbidden\""));
    }
}

/// The m0-s08 audit AC: every audited action lands a queryable row, and no
/// password or session-token material exists anywhere in the control
/// database or its WAL.
#[test]
fn audit_rows_exist_and_no_secret_material_is_stored() {
    let shell = ServedShell::start();
    let password = "hunter2 but actually long";
    let a = shell.signup("audit@example.com", password);

    // Login again (a second audited action), then the project actions.
    let (status, _, login_body) = shell.request(
        "POST",
        "/auth/login",
        "",
        &format!("{{\"email\":\"audit@example.com\",\"password\":\"{password}\"}}"),
    );
    assert_eq!(status, 200, "login failed: {login_body}");
    let alpha = project_path(&a, "audited");
    let (status, _) = api(&shell, &a, "cmd", "project.create", &path_input(&alpha));
    assert_eq!(status, 200);
    let (status, _) = api(&shell, &a, "cmd", "project.open", &path_input(&alpha));
    assert_eq!(status, 200);
    // run.start / run.cancel: audited attempts even while the engine answers
    // not_yet_supported (the audit records who tried to act).
    let (status, _) = api(&shell, &a, "cmd", "run.start", "{}");
    assert_eq!(status, 501);
    let (status, _) = api(&shell, &a, "cmd", "run.cancel", "{}");
    assert_eq!(status, 501);

    let (status, _, audit) = shell.request("GET", "/auth/audit", &a.cookie, "");
    assert_eq!(status, 200, "audit query failed: {audit}");
    for action in [
        "auth.signup",
        "auth.login",
        "project.create",
        "project.open",
        "run.start",
        "run.cancel",
    ] {
        assert!(
            audit.contains(&format!("\"action\":\"{action}\"")),
            "audit trail is missing {action}: {audit}"
        );
    }
    // The project actions record their target path.
    assert!(
        audit.contains("audited.pos"),
        "audit rows lost their target"
    );

    // Zero secret material at rest: the raw password and the raw session
    // token appear nowhere in control.db or its WAL (only argon2id and
    // BLAKE3 hashes do). This is the grep half of the AC.
    let token = a
        .cookie
        .split_once('=')
        .map(|(_, value)| value.to_owned())
        .expect("cookie has a value");
    assert_eq!(token.len(), 64, "session token is the 64-hex cookie value");
    let mut stored = Vec::new();
    for file in ["control.db", "control.db-wal"] {
        let path = shell.data_root.path().join(file);
        if path.exists() {
            stored.extend(std::fs::read(&path).expect("control database file reads"));
        }
    }
    assert!(!stored.is_empty(), "control.db was never written");
    let stored_text = String::from_utf8_lossy(&stored);
    assert!(
        !stored_text.contains(password),
        "the raw password is stored somewhere in control.db"
    );
    assert!(
        !stored_text.contains(&token),
        "the raw session token is stored somewhere in control.db"
    );
}

/// The served UI bundle: hashed assets are immutable, the app shell is
/// revalidated, SPA routes fall back to it, and none of it requires a
/// session (the login page must load logged-out).
#[test]
fn the_ui_bundle_is_served_with_cache_discipline() {
    let dist = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dist.path().join("index.html"),
        "<!doctype html><title>pos</title>",
    )
    .expect("writes");
    std::fs::create_dir_all(dist.path().join("assets")).expect("mkdir");
    std::fs::write(dist.path().join("assets/app-abc123.js"), "console.log(1)").expect("writes");

    let data_root = tempfile::tempdir().expect("tempdir");
    let state = ServerState::initialize(&ServerConfig {
        data_root: data_root.path().to_path_buf(),
        ui_dist: Some(dist.path().to_path_buf()),
    })
    .expect("server state initializes");
    let served = Arc::clone(&state);
    let (shutdown, on_shutdown) = tokio::sync::oneshot::channel::<()>();
    let (addr_tx, addr_rx) = std::sync::mpsc::channel::<SocketAddr>();
    let thread = std::thread::spawn(move || {
        let tokio_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test tokio runtime builds");
        tokio_runtime.block_on(async move {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("ephemeral loopback bind succeeds");
            addr_tx
                .send(listener.local_addr().expect("bound socket has an addr"))
                .expect("addr sends");
            serve(listener, served, async {
                let _ = on_shutdown.await;
            })
            .await
            .expect("serve runs until shutdown");
        });
    });
    let addr = addr_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("addr within 10s");
    let request = |target: &str| {
        let mut stream = TcpStream::connect(addr).expect("connects");
        stream
            .write_all(
                format!("GET {target} HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n").as_bytes(),
            )
            .expect("writes");
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).expect("reads");
        String::from_utf8_lossy(&raw).into_owned()
    };

    let index = request("/");
    assert!(index.contains("200 OK") && index.contains("<!doctype html>"));
    assert!(index.contains("no-cache"));
    let asset = request("/assets/app-abc123.js");
    assert!(asset.contains("200 OK") && asset.contains("immutable"));
    assert!(asset.contains("text/javascript"));
    let spa = request("/projects/some-route");
    assert!(spa.contains("200 OK") && spa.contains("<!doctype html>"));
    let missing = request("/missing.map");
    assert!(missing.contains("404"));

    let _ = shutdown.send(());
    let _ = thread.join();
}

fn hex_bytes(text: &str) -> [u8; 16] {
    let mut bytes = [0_u8; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[2 * index..2 * index + 2], 16).expect("hex id");
    }
    bytes
}
