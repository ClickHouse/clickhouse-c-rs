//! Query option tests using a temporary ClickHouse server.

mod common;

use std::net::TcpStream;

use clickhouse_c::{
    Allocator, Client, ClientOpts, ColumnLayout, ErrorKind, Event, PosixIo, QueryOpts, QueryParam,
    QuerySetting,
};
use common::{ChServer, TestResult, clickhouse_on_path};

fn connect(server: &ChServer) -> TestResult<Client<'static>> {
    let sock = TcpStream::connect(("127.0.0.1", server.tcp_port))?;
    Ok(Client::init(
        &ClientOpts::new()
            .client_name("clickhouse-c-rs-tests")
            .client_version(9, 8, 7),
        Allocator::stdlib(),
        PosixIo::new_owned(sock),
        None,
    )?)
}

/// Reads first string column from all Data blocks through EndOfStream.
fn collect_strings(client: &mut Client<'_>) -> TestResult<Vec<String>> {
    let mut rows = vec![];
    loop {
        match client.recv_event()? {
            Event::EndOfStream => return Ok(rows),
            Event::Exception(e) => return Err(Box::new(e)),
            Event::Data(block) => {
                let Some(column) = block.column(0) else {
                    continue;
                };
                assert!(matches!(column.layout(), Some(ColumnLayout::String)));
                let Some((offsets, data)) = column.string() else {
                    continue;
                };
                let mut start = 0usize;
                for &end in offsets {
                    let end = end as usize;
                    rows.push(String::from_utf8_lossy(&data[start..end]).into_owned());
                    start = end;
                }
            }
            _ => {}
        }
    }
}

fn select_strings(server: &ChServer, sql: &str, opts: &QueryOpts<'_>) -> TestResult<Vec<String>> {
    let mut client = connect(server)?;
    client.send_query_with(sql, opts)?;
    client.send_data(None)?;
    collect_strings(&mut client)
}

#[test]
fn settings_reach_the_server() -> TestResult {
    if !clickhouse_on_path() {
        eprintln!("skipping: clickhouse not on PATH");
        return Ok(());
    }
    let server = ChServer::spawn()?;

    let settings = [
        QuerySetting::TEXT_TYPE_NAMES,
        QuerySetting::new("max_block_size", "4321"),
    ];
    let rows = select_strings(
        &server,
        "SELECT value FROM system.settings WHERE name = 'max_block_size'",
        &QueryOpts::new().settings(&settings),
    )?;
    assert_eq!(rows, vec!["4321".to_string()]);
    Ok(())
}

#[test]
fn custom_settings_reach_the_server() -> TestResult {
    if !clickhouse_on_path() {
        eprintln!("skipping: clickhouse not on PATH");
        return Ok(());
    }
    // Server requires declared prefix for custom settings
    let server = ChServer::spawn_with(&["--custom_settings_prefixes=custom_"])?;

    let settings = [
        QuerySetting::TEXT_TYPE_NAMES,
        QuerySetting::new("custom_tag", "'shipped'").custom(),
    ];
    let rows = select_strings(
        &server,
        "SELECT toString(getSetting('custom_tag'))",
        &QueryOpts::new().settings(&settings),
    )?;
    assert_eq!(rows, vec!["shipped".to_string()]);
    Ok(())
}

/// Verifies important unknown setting returns server exception.
#[test]
fn important_flag_makes_an_unknown_setting_fatal() -> TestResult {
    if !clickhouse_on_path() {
        eprintln!("skipping: clickhouse not on PATH");
        return Ok(());
    }
    let server = ChServer::spawn()?;

    let lenient = [
        QuerySetting::TEXT_TYPE_NAMES,
        QuerySetting::new("no_such_setting_here", "1"),
    ];
    let rows = select_strings(&server, "SELECT 'ok'", &QueryOpts::new().settings(&lenient))?;
    assert_eq!(rows, vec!["ok".to_string()]);

    let strict = [
        QuerySetting::TEXT_TYPE_NAMES,
        QuerySetting::new("no_such_setting_here", "1").important(),
    ];
    let err = select_strings(&server, "SELECT 'ok'", &QueryOpts::new().settings(&strict))
        .expect_err("important unknown setting must fail the query");
    assert!(
        err.to_string().contains("no_such_setting_here"),
        "unexpected error: {err}"
    );
    Ok(())
}

#[test]
fn parameters_substitute_into_placeholders() -> TestResult {
    if !clickhouse_on_path() {
        eprintln!("skipping: clickhouse not on PATH");
        return Ok(());
    }
    let server = ChServer::spawn()?;

    let settings = [QuerySetting::TEXT_TYPE_NAMES];
    let params = [
        QueryParam::new("greeting", "'hello'"),
        QueryParam::new("count", "'3'"),
    ];
    let rows = select_strings(
        &server,
        "SELECT concat({greeting:String}, toString({count:UInt8}))",
        &QueryOpts::new().settings(&settings).params(&params),
    )?;
    assert_eq!(rows, vec!["hello3".to_string()]);
    Ok(())
}

#[test]
fn query_id_is_the_one_the_server_reports() -> TestResult {
    if !clickhouse_on_path() {
        eprintln!("skipping: clickhouse not on PATH");
        return Ok(());
    }
    let server = ChServer::spawn()?;

    let settings = [QuerySetting::TEXT_TYPE_NAMES];
    let rows = select_strings(
        &server,
        "SELECT queryID()",
        &QueryOpts::new()
            .query_id("chc-rs-fixed-id")
            .settings(&settings),
    )?;
    assert_eq!(rows, vec!["chc-rs-fixed-id".to_string()]);
    Ok(())
}

#[test]
fn empty_opts_behave_like_a_bare_query() -> TestResult {
    if !clickhouse_on_path() {
        eprintln!("skipping: clickhouse not on PATH");
        return Ok(());
    }
    let server = ChServer::spawn()?;

    let rows = select_strings(&server, "SELECT 'bare'", &QueryOpts::new())?;
    assert_eq!(rows, vec!["bare".to_string()]);
    Ok(())
}

/// Verifies interior null byte is rejected before I/O.
#[test]
fn interior_nul_in_client_opts_is_a_usage_error() {
    let opts = ClientOpts::new().user("def\u{0}ault");
    let sock = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("bind");
    let Err(err) = Client::init(
        &opts,
        Allocator::stdlib(),
        PosixIo::new_owned(
            std::net::TcpStream::connect(sock.local_addr().expect("addr")).expect("connect"),
        ),
        None,
    ) else {
        panic!("interior NUL must not reach the server");
    };
    assert_eq!(err.kind, ErrorKind::Usage);
    assert!(err.message.contains("user"), "{err}");
}
