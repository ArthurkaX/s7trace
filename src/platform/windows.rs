//! Native Windows operator panel for `s7trace server`.
//!
//! This window is embedded in the server process. It does not connect back to
//! the local server over HTTP; it reads the shared SQLite connection and writes
//! control requests into the same durable command queue used by the CLI.

#![cfg(windows)]

use crate::db::command_queue;
use crate::types::CommandKind;
use anyhow::Result;
use parking_lot::Mutex;
use rusqlite::Connection;
use std::ffi::OsStr;
use std::os::windows::ffi::OsStrExt;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicIsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use windows_sys::Win32::Foundation::*;
use windows_sys::Win32::Graphics::Gdi::*;
use windows_sys::Win32::System::LibraryLoader::GetModuleHandleW;
use windows_sys::Win32::UI::Controls::*;
use windows_sys::Win32::UI::WindowsAndMessaging::*;

const CLASS_NAME: &str = "s7traceOperatorPanel";
const WINDOW_W: i32 = 980;
const WINDOW_H: i32 = 460;
const IDT_REFRESH: usize = 1;
const REFRESH_MS: u32 = 1000;

const ID_REFRESH: u32 = 101;
const ID_ENABLE: u32 = 102;
const ID_DISABLE: u32 = 103;
const ID_MINUS_50: u32 = 104;
const ID_PLUS_50: u32 = 105;
const ID_EXIT: u32 = 106;
const ID_LIST: u32 = 200;
const WM_STATUS_READY: u32 = WM_APP + 1;
const REFRESH_MIN_US: i64 = REFRESH_MS as i64 * 1000;

static REFRESHING: AtomicBool = AtomicBool::new(false);
static LAST_REFRESH_START_US: AtomicI64 = AtomicI64::new(0);
static PENDING_SNAPSHOT: Mutex<Option<StatusSnapshot>> = Mutex::new(None);

struct Col;
impl Col {
    const PLC: i32 = 0;
    const SET: i32 = 1;
    const ON: i32 = 2;
    const TAGS: i32 = 3;
    const BAD: i32 = 4;
    const PERIOD: i32 = 5;
    const RATE: i32 = 6;
    const AGE: i32 = 7;
    const STATE: i32 = 8;
}

#[derive(Clone, Debug, Default)]
struct PanelRow {
    plc_name: String,
    metric_set_name: Option<String>,
    metric_set_enabled: bool,
    tags_active: i64,
    tags_total: i64,
    tags_bad: i64,
    poll_mode: String,
    poll_group_id: Option<i64>,
    period_ms: Option<i64>,
    rate_hz: Option<f64>,
    last_poll_cycle_finished_ts: Option<i64>,
}

#[derive(Clone, Debug, Default)]
struct StatusSnapshot {
    plc_count: i64,
    active_dataset_count: i64,
    command_queue_pending: i64,
    command_queue_running: i64,
    config_apply_pending: i64,
    rows: Vec<PanelRow>,
    last_message: Option<String>,
}

struct AppState {
    db: Arc<Mutex<Connection>>,
    hwnd: HWND,
    list: HWND,
    exit_requested: AtomicBool,
    rows: Mutex<Vec<PanelRow>>,
    selected_key: Mutex<Option<(String, String)>>,
    last_message: Mutex<Option<String>>,
}

static APP_STATE: AtomicIsize = AtomicIsize::new(0);

fn state() -> &'static mut AppState {
    let p = APP_STATE.load(Ordering::SeqCst);
    assert!(p != 0, "operator panel state not initialized");
    unsafe { &mut *(p as *mut AppState) }
}

fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}

fn wstr(s: &str) -> Vec<u16> {
    OsStr::new(s).encode_wide().chain(Some(0)).collect()
}

/// Run the embedded server panel. Returns when the operator chooses Exit.
pub fn run_control_window(db: Arc<Mutex<Connection>>) -> Result<()> {
    unsafe {
        let hinstance = GetModuleHandleW(std::ptr::null());
        if hinstance.is_null() {
            anyhow::bail!("GetModuleHandleW failed");
        }

        let mut icc: INITCOMMONCONTROLSEX = std::mem::zeroed();
        icc.dwSize = std::mem::size_of::<INITCOMMONCONTROLSEX>() as u32;
        icc.dwICC = ICC_LISTVIEW_CLASSES;
        InitCommonControlsEx(&icc);

        let class = wstr(CLASS_NAME);
        let wc = WNDCLASSEXW {
            cbSize: std::mem::size_of::<WNDCLASSEXW>() as u32,
            style: CS_HREDRAW | CS_VREDRAW,
            lpfnWndProc: Some(wndproc),
            cbClsExtra: 0,
            cbWndExtra: 0,
            hInstance: hinstance,
            hIcon: LoadIconW(std::ptr::null_mut(), IDI_APPLICATION),
            hCursor: LoadCursorW(std::ptr::null_mut(), IDC_ARROW),
            hbrBackground: GetSysColorBrush(COLOR_BTNFACE),
            lpszMenuName: std::ptr::null(),
            lpszClassName: class.as_ptr(),
            hIconSm: std::ptr::null_mut(),
        };
        if RegisterClassExW(&wc) == 0 {
            anyhow::bail!("RegisterClassExW failed");
        }

        let app_state = Box::new(AppState {
            db,
            hwnd: std::ptr::null_mut(),
            list: std::ptr::null_mut(),
            exit_requested: AtomicBool::new(false),
            rows: Mutex::new(Vec::new()),
            selected_key: Mutex::new(None),
            last_message: Mutex::new(None),
        });
        let state_ptr = Box::into_raw(app_state) as isize;
        APP_STATE.store(state_ptr, Ordering::SeqCst);

        let title = wstr("s7trace server");
        let hwnd = CreateWindowExW(
            0,
            class.as_ptr(),
            title.as_ptr(),
            WS_OVERLAPPEDWINDOW | WS_VISIBLE,
            CW_USEDEFAULT,
            CW_USEDEFAULT,
            WINDOW_W,
            WINDOW_H,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            hinstance,
            std::ptr::null(),
        );
        if hwnd.is_null() {
            APP_STATE.store(0, Ordering::SeqCst);
            let _ = Box::from_raw(state_ptr as *mut AppState);
            anyhow::bail!("CreateWindowExW failed");
        }
        state().hwnd = hwnd;

        start_refresh(hwnd);

        let mut msg: MSG = std::mem::zeroed();
        loop {
            let rv = GetMessageW(&mut msg, std::ptr::null_mut(), 0, 0);
            if rv <= 0 {
                break;
            }
            TranslateMessage(&msg);
            DispatchMessageW(&msg);
            if state().exit_requested.load(Ordering::SeqCst) {
                PostQuitMessage(0);
            }
        }

        DestroyWindow(hwnd);
        APP_STATE.store(0, Ordering::SeqCst);
        let _ = Box::from_raw(state_ptr as *mut AppState);
        Ok(())
    }
}

unsafe extern "system" fn wndproc(hwnd: HWND, msg: u32, wparam: usize, lparam: isize) -> isize {
    match msg {
        WM_CREATE => {
            create_controls(hwnd);
            SetTimer(hwnd, IDT_REFRESH, REFRESH_MS, None);
            0
        }
        WM_SIZE => {
            let w = lparam as i32 & 0xFFFF;
            let h = (lparam as i32 >> 16) & 0xFFFF;
            let list = state().list;
            if !list.is_null() {
                MoveWindow(list, 0, 34, w, h - 34, 1);
            }
            0
        }
        WM_TIMER => {
            if wparam == IDT_REFRESH {
                start_refresh(hwnd);
            }
            0
        }
        WM_STATUS_READY => {
            finish_refresh(hwnd);
            0
        }
        WM_COMMAND => {
            handle_command(hwnd, (wparam & 0xFFFF) as u32);
            0
        }
        WM_NOTIFY => {
            let nmhdr = lparam as *const NMHDR;
            if !nmhdr.is_null() && (*nmhdr).hwndFrom == state().list {
                remember_current_selection();
            }
            0
        }
        WM_CLOSE => {
            ShowWindow(hwnd, SW_MINIMIZE);
            0
        }
        WM_DESTROY => {
            KillTimer(hwnd, IDT_REFRESH);
            PostQuitMessage(0);
            0
        }
        _ => DefWindowProcW(hwnd, msg, wparam, lparam),
    }
}

unsafe fn create_controls(hwnd: HWND) {
    let hinstance = GetModuleHandleW(std::ptr::null());

    let button = |text: &str, x: i32, w: i32, id: u32| {
        CreateWindowExW(
            0,
            WC_BUTTONW,
            wstr(text).as_ptr(),
            WS_CHILD | WS_VISIBLE | BS_PUSHBUTTON as u32,
            x,
            5,
            w,
            24,
            hwnd,
            id as HMENU,
            hinstance,
            std::ptr::null(),
        )
    };

    button("Refresh", 6, 72, ID_REFRESH);
    button("Enable", 84, 72, ID_ENABLE);
    button("Disable", 162, 78, ID_DISABLE);
    button("-50 ms", 246, 74, ID_MINUS_50);
    button("+50 ms", 326, 74, ID_PLUS_50);
    button("Exit", 406, 58, ID_EXIT);

    let list = CreateWindowExW(
        0,
        WC_LISTVIEWW,
        std::ptr::null(),
        WS_CHILD | WS_VISIBLE | WS_BORDER | LVS_REPORT | LVS_SINGLESEL | LVS_SHOWSELALWAYS,
        0,
        34,
        WINDOW_W,
        WINDOW_H - 34,
        hwnd,
        ID_LIST as HMENU,
        hinstance,
        std::ptr::null(),
    );
    state().list = list;

    SendMessageW(
        list,
        LVM_SETEXTENDEDLISTVIEWSTYLE,
        0,
        (LVS_EX_FULLROWSELECT | LVS_EX_GRIDLINES | LVS_EX_DOUBLEBUFFER) as isize,
    );

    add_column(list, Col::PLC, "PLC", 90);
    add_column(list, Col::SET, "Set", 190);
    add_column(list, Col::ON, "On", 44);
    add_column(list, Col::TAGS, "Tags", 78);
    add_column(list, Col::BAD, "Bad", 44);
    add_column(list, Col::PERIOD, "Period", 78);
    add_column(list, Col::RATE, "Rate", 72);
    add_column(list, Col::AGE, "Age", 90);
    add_column(list, Col::STATE, "State", 90);
}

fn add_column(list: HWND, index: i32, title: &str, width: i32) {
    let mut title_w = wstr(title);
    let mut col = LVCOLUMNW {
        mask: LVCF_WIDTH | LVCF_TEXT | LVCF_SUBITEM | LVCF_FMT,
        fmt: LVCFMT_LEFT,
        cx: width,
        pszText: title_w.as_mut_ptr(),
        cchTextMax: title_w.len() as i32,
        iSubItem: index,
        ..unsafe { std::mem::zeroed() }
    };
    unsafe {
        SendMessageW(
            list,
            LVM_INSERTCOLUMNW,
            index as usize,
            &mut col as *mut _ as isize,
        );
    }
}

fn start_refresh(hwnd: HWND) {
    if REFRESHING
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return;
    }

    let now = now_us();
    let last = LAST_REFRESH_START_US.load(Ordering::SeqCst);
    if last != 0 && now.saturating_sub(last) < REFRESH_MIN_US {
        REFRESHING.store(false, Ordering::SeqCst);
        return;
    }
    LAST_REFRESH_START_US.store(now, Ordering::SeqCst);

    let db = state().db.clone();
    let hwnd_val = hwnd as isize;
    std::thread::spawn(move || {
        let snapshot = read_status(&db);
        *PENDING_SNAPSHOT.lock() = Some(snapshot);
        unsafe {
            PostMessageW(hwnd_val as HWND, WM_STATUS_READY, 0, 0);
        }
    });
}

fn finish_refresh(hwnd: HWND) {
    let snapshot = PENDING_SNAPSHOT.lock().take();
    REFRESHING.store(false, Ordering::SeqCst);
    let Some(snapshot) = snapshot else {
        return;
    };

    let st = state();
    *st.rows.lock() = snapshot.rows.clone();
    render_rows(st.list, &snapshot.rows);

    let msg = st.last_message.lock().clone().or(snapshot.last_message);
    let suffix = msg.unwrap_or_else(|| {
        format!(
            "plc={} datasets={} queue={}+{} cfg={}",
            snapshot.plc_count,
            snapshot.active_dataset_count,
            snapshot.command_queue_pending,
            snapshot.command_queue_running,
            snapshot.config_apply_pending
        )
    });
    unsafe {
        SetWindowTextW(hwnd, wstr(&format!("s7trace server - {suffix}")).as_ptr());
    }
}

fn render_rows(list: HWND, rows: &[PanelRow]) {
    if list.is_null() {
        return;
    }
    let selected_key = current_selection_key().or_else(|| state().selected_key.lock().clone());
    unsafe {
        SendMessageW(list, LVM_DELETEALLITEMS, 0, 0);
    }
    for (i, row) in rows.iter().enumerate() {
        insert_item(list, i as i32, &row.plc_name, i as isize);
        set_subitem(
            list,
            i as i32,
            Col::SET,
            row.metric_set_name.as_deref().unwrap_or("---"),
        );
        set_subitem(
            list,
            i as i32,
            Col::ON,
            if row.metric_set_enabled { "yes" } else { "no" },
        );
        set_subitem(
            list,
            i as i32,
            Col::TAGS,
            &format!("{}/{}", row.tags_active, row.tags_total),
        );
        set_subitem(list, i as i32, Col::BAD, &row.tags_bad.to_string());
        set_subitem(list, i as i32, Col::PERIOD, &period_text(row));
        set_subitem(list, i as i32, Col::RATE, &rate_text(row));
        set_subitem(list, i as i32, Col::AGE, &age_text(row));
        set_subitem(list, i as i32, Col::STATE, &state_text(row));
    }
    if let Some(key) = selected_key {
        select_row_by_key(list, rows, &key);
    }
}

fn insert_item(list: HWND, index: i32, text: &str, param: isize) {
    let mut text_w = wstr(text);
    let mut item = LVITEMW {
        mask: LVIF_TEXT | LVIF_PARAM,
        iItem: index,
        iSubItem: 0,
        pszText: text_w.as_mut_ptr(),
        cchTextMax: text_w.len() as i32,
        lParam: param,
        ..unsafe { std::mem::zeroed() }
    };
    unsafe {
        SendMessageW(list, LVM_INSERTITEMW, 0, &mut item as *mut _ as isize);
    }
}

fn set_subitem(list: HWND, index: i32, subitem: i32, text: &str) {
    let mut text_w = wstr(text);
    let mut item = LVITEMW {
        mask: LVIF_TEXT,
        iItem: index,
        iSubItem: subitem,
        pszText: text_w.as_mut_ptr(),
        cchTextMax: text_w.len() as i32,
        ..unsafe { std::mem::zeroed() }
    };
    unsafe {
        SendMessageW(list, LVM_SETITEMW, 0, &mut item as *mut _ as isize);
    }
}

fn selected_row() -> Option<PanelRow> {
    let st = state();
    if let Some(sel) = selected_index(st.list) {
        let row = st.rows.lock().get(sel as usize).cloned();
        if let Some(row) = &row {
            if let Some(key) = row_key(row) {
                *st.selected_key.lock() = Some(key);
            }
        }
        return row;
    }
    let key = st.selected_key.lock().clone()?;
    st.rows
        .lock()
        .iter()
        .find(|row| row_key(row).as_ref() == Some(&key))
        .cloned()
}

fn selected_index(list: HWND) -> Option<i32> {
    let sel = unsafe { SendMessageW(list, LVM_GETNEXTITEM, usize::MAX, LVNI_SELECTED as isize) };
    if sel < 0 {
        None
    } else {
        Some(sel as i32)
    }
}

fn row_key(row: &PanelRow) -> Option<(String, String)> {
    row.metric_set_name
        .as_ref()
        .map(|set| (row.plc_name.clone(), set.clone()))
}

fn current_selection_key() -> Option<(String, String)> {
    let st = state();
    let sel = selected_index(st.list)?;
    st.rows.lock().get(sel as usize).and_then(row_key)
}

fn remember_current_selection() {
    if let Some(key) = current_selection_key() {
        *state().selected_key.lock() = Some(key);
    }
}

fn select_row_by_key(list: HWND, rows: &[PanelRow], key: &(String, String)) {
    if let Some(idx) = rows
        .iter()
        .position(|row| row_key(row).as_ref() == Some(key))
    {
        let mut item = LVITEMW {
            mask: LVIF_STATE,
            iItem: idx as i32,
            state: LVIS_SELECTED | LVIS_FOCUSED,
            stateMask: LVIS_SELECTED | LVIS_FOCUSED,
            ..unsafe { std::mem::zeroed() }
        };
        unsafe {
            SendMessageW(list, LVM_SETITEMSTATE, idx, &mut item as *mut _ as isize);
            SendMessageW(list, LVM_ENSUREVISIBLE, idx, 0);
        }
    }
}

fn handle_command(hwnd: HWND, id: u32) {
    match id {
        ID_REFRESH => start_refresh(hwnd),
        ID_ENABLE | ID_DISABLE => {
            let Some(row) = selected_row() else {
                set_message("select a metric set first");
                start_refresh(hwnd);
                return;
            };
            let Some(set_name) = row.metric_set_name else {
                set_message("selected PLC has no metric set");
                start_refresh(hwnd);
                return;
            };
            let action = if id == ID_ENABLE {
                "set_enable"
            } else {
                "set_disable"
            };
            enqueue_control(serde_json::json!({
                "action": action,
                "set_name": set_name,
            }));
            start_refresh(hwnd);
        }
        ID_MINUS_50 | ID_PLUS_50 => {
            let Some(row) = selected_row() else {
                set_message("select a metric set first");
                start_refresh(hwnd);
                return;
            };
            if row.poll_mode != "single" {
                set_message("period adjustment requires a single poll group");
                start_refresh(hwnd);
                return;
            }
            let Some(poll_group_id) = row.poll_group_id else {
                set_message("selected set has no poll group");
                start_refresh(hwnd);
                return;
            };
            let current = row.period_ms.unwrap_or(1000);
            let new_period = adjust_period(current, if id == ID_MINUS_50 { -50 } else { 50 });
            enqueue_control(serde_json::json!({
                "action": "poll_group_update_period",
                "poll_group_id": poll_group_id,
                "period_ms": new_period,
            }));
            start_refresh(hwnd);
        }
        ID_EXIT => {
            enqueue_control(serde_json::json!({"action": "server_exit"}));
            state().exit_requested.store(true, Ordering::SeqCst);
        }
        _ => {}
    }
}

fn set_message(msg: &str) {
    *state().last_message.lock() = Some(msg.to_string());
}

fn enqueue_control(request: serde_json::Value) {
    let st = state();
    let conn = st.db.lock();
    match command_queue::enqueue(
        &conn,
        CommandKind::Control,
        &request.to_string(),
        request["plc_id"].as_i64(),
        Some("control-window"),
        None,
    ) {
        Ok(id) => set_message(&format!("queued command {id}")),
        Err(e) => set_message(&format!("enqueue failed: {e}")),
    }
}

fn read_status(db: &Arc<Mutex<Connection>>) -> StatusSnapshot {
    let conn = db.lock();

    let plc_count = scalar_i64(&conn, "SELECT COUNT(*) FROM plc WHERE enabled = 1");
    let active_dataset_count =
        scalar_i64(&conn, "SELECT COUNT(*) FROM dataset WHERE state = 'active'");
    let command_queue_pending = scalar_i64(
        &conn,
        "SELECT COUNT(*) FROM command_request WHERE state = 'pending'",
    );
    let command_queue_running = scalar_i64(
        &conn,
        "SELECT COUNT(*) FROM command_request WHERE state = 'running'",
    );
    let config_apply_pending = scalar_i64(
        &conn,
        "SELECT COUNT(*) FROM config_apply WHERE state IN ('pending','debouncing','applying')",
    );

    let plcs: Vec<(i64, String)> = conn
        .prepare("SELECT id, name FROM plc WHERE enabled = 1 ORDER BY id")
        .ok()
        .and_then(|mut stmt| {
            stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))
                .ok()
                .map(|it| it.filter_map(|r| r.ok()).collect())
        })
        .unwrap_or_default();

    let mut rows = Vec::new();
    for (plc_id, plc_name) in plcs {
        let sets: Vec<(i64, String, bool)> = conn
            .prepare("SELECT id, name, enabled FROM metric_set WHERE plc_id = ?1 ORDER BY id")
            .ok()
            .and_then(|mut stmt| {
                stmt.query_map(rusqlite::params![plc_id], |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, bool>(2)?,
                    ))
                })
                .ok()
                .map(|it| it.filter_map(|r| r.ok()).collect())
            })
            .unwrap_or_default();

        if sets.is_empty() {
            rows.push(PanelRow {
                plc_name,
                poll_mode: "none".to_string(),
                ..PanelRow::default()
            });
            continue;
        }

        for (set_id, set_name, set_enabled) in sets {
            let tags_total: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM metric_set_tag WHERE metric_set_id = ?1",
                    rusqlite::params![set_id],
                    |r| r.get(0),
                )
                .unwrap_or(0);
            let tags_active: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM metric_set_tag mst
                     JOIN tag t ON t.id = mst.tag_id
                     WHERE mst.metric_set_id = ?1 AND t.enabled = 1 AND t.bad = 0",
                    rusqlite::params![set_id],
                    |r| r.get(0),
                )
                .unwrap_or(0);
            let tags_bad: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM metric_set_tag mst
                     JOIN tag t ON t.id = mst.tag_id
                     WHERE mst.metric_set_id = ?1 AND t.bad = 1",
                    rusqlite::params![set_id],
                    |r| r.get(0),
                )
                .unwrap_or(0);

            let groups: Vec<(i64, String, i64)> = conn
                .prepare(
                    "SELECT DISTINCT pg.id, pg.name, pg.period_ms
                     FROM metric_set_tag mst
                     JOIN tag t ON t.id = mst.tag_id
                     JOIN poll_group pg ON pg.id = t.poll_group_id
                     WHERE mst.metric_set_id = ?1 AND t.enabled = 1
                     ORDER BY pg.id",
                )
                .ok()
                .and_then(|mut stmt| {
                    stmt.query_map(rusqlite::params![set_id], |r| {
                        Ok((
                            r.get::<_, i64>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, i64>(2)?,
                        ))
                    })
                    .ok()
                    .map(|it| it.filter_map(|r| r.ok()).collect())
                })
                .unwrap_or_default();

            let (poll_mode, poll_group_id, period_ms, rate_hz) = match groups.as_slice() {
                [] => ("none".to_string(), None, None, None),
                [(id, _name, period)] => (
                    "single".to_string(),
                    Some(*id),
                    Some(*period),
                    Some(1000.0 / *period as f64),
                ),
                _ => ("mixed".to_string(), None, None, None),
            };

            let last_poll_cycle_finished_ts = match poll_group_id {
                Some(pg_id) => conn
                    .query_row(
                        "SELECT MAX(finished_ts) FROM poll_cycle
                         WHERE plc_id = ?1 AND poll_group_id = ?2",
                        rusqlite::params![plc_id, pg_id],
                        |r| r.get::<_, Option<i64>>(0),
                    )
                    .ok()
                    .flatten(),
                None => conn
                    .query_row(
                        "SELECT MAX(finished_ts) FROM poll_cycle WHERE plc_id = ?1",
                        rusqlite::params![plc_id],
                        |r| r.get::<_, Option<i64>>(0),
                    )
                    .ok()
                    .flatten(),
            };

            rows.push(PanelRow {
                plc_name: plc_name.clone(),
                metric_set_name: Some(set_name),
                metric_set_enabled: set_enabled,
                tags_active,
                tags_total,
                tags_bad,
                poll_mode,
                poll_group_id,
                period_ms,
                rate_hz,
                last_poll_cycle_finished_ts,
            });
        }
    }

    StatusSnapshot {
        plc_count,
        active_dataset_count,
        command_queue_pending,
        command_queue_running,
        config_apply_pending,
        rows,
        last_message: None,
    }
}

fn scalar_i64(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |r| r.get(0)).unwrap_or(0)
}

fn adjust_period(current: i64, delta: i64) -> i64 {
    std::cmp::max(current + delta, 50)
}

fn age_ms(row: &PanelRow) -> Option<i64> {
    row.last_poll_cycle_finished_ts
        .map(|ts| (now_us() - ts) / 1000)
}

fn stale(row: &PanelRow) -> bool {
    let Some(age) = age_ms(row) else {
        return row.metric_set_enabled;
    };
    let threshold = row
        .period_ms
        .map(|p| std::cmp::max(p * 5, 5000))
        .unwrap_or(5000);
    age > threshold
}

fn period_text(row: &PanelRow) -> String {
    match row.poll_mode.as_str() {
        "single" => format!("{}ms", row.period_ms.unwrap_or(0)),
        "mixed" => "mixed".to_string(),
        _ => "none".to_string(),
    }
}

fn rate_text(row: &PanelRow) -> String {
    row.rate_hz
        .map(|hz| format!("{hz:.1}Hz"))
        .unwrap_or_else(|| "-".to_string())
}

fn age_text(row: &PanelRow) -> String {
    match age_ms(row) {
        Some(ms) if stale(row) => format!("STALE {:.1}s", ms as f64 / 1000.0),
        Some(ms) => format!("{:.1}s", ms as f64 / 1000.0),
        None => "-".to_string(),
    }
}

fn state_text(row: &PanelRow) -> String {
    if row.metric_set_name.is_none() {
        return "no sets".to_string();
    }
    if !row.metric_set_enabled {
        return "off".to_string();
    }
    if row.poll_mode == "mixed" {
        return "MIXED".to_string();
    }
    if stale(row) {
        return "STALE".to_string();
    }
    "RUN".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::schema;

    fn temp_db() -> Arc<Mutex<Connection>> {
        let path = std::env::temp_dir().join(format!(
            "s7trace_panel_{}_{}.db",
            std::process::id(),
            now_us()
        ));
        let _ = std::fs::remove_file(&path);
        Arc::new(Mutex::new(schema::open(path.to_str().unwrap()).unwrap()))
    }

    #[test]
    fn empty_db_has_no_rows() {
        let db = temp_db();
        let snap = read_status(&db);
        assert_eq!(snap.rows.len(), 0);
    }

    #[test]
    fn status_single_poll_group() {
        let db = temp_db();
        seed_single(&db);
        let snap = read_status(&db);
        assert_eq!(snap.rows.len(), 1);
        let row = &snap.rows[0];
        assert_eq!(row.poll_mode, "single");
        assert_eq!(row.period_ms, Some(150));
        assert_eq!(row.poll_group_id, Some(1));
        assert!((row.rate_hz.unwrap() - 6.666).abs() < 0.01);
    }

    #[test]
    fn status_mixed_poll_group() {
        let db = temp_db();
        seed_mixed(&db);
        let snap = read_status(&db);
        assert_eq!(snap.rows[0].poll_mode, "mixed");
        assert_eq!(snap.rows[0].period_ms, None);
    }

    #[test]
    fn status_none_poll_group() {
        let db = temp_db();
        let conn = db.lock();
        conn.execute_batch(
            "INSERT INTO plc (id, name, host) VALUES (1, 'plc1', '127.0.0.1');
             INSERT INTO metric_set (id, plc_id, name, enabled, created_ts, updated_ts)
             VALUES (1, 1, 'empty', 1, 1, 1);",
        )
        .unwrap();
        drop(conn);
        let snap = read_status(&db);
        assert_eq!(snap.rows[0].poll_mode, "none");
    }

    #[test]
    fn period_adjustment_clamps_to_50() {
        assert_eq!(adjust_period(50, -50), 50);
        assert_eq!(adjust_period(80, -50), 50);
        assert_eq!(adjust_period(150, 50), 200);
    }

    fn seed_single(db: &Arc<Mutex<Connection>>) {
        let conn = db.lock();
        conn.execute_batch(
            "INSERT INTO plc (id, name, host) VALUES (1, 'plc1', '127.0.0.1');
             INSERT INTO poll_group (id, name, period_ms) VALUES (1, 'fast', 150);
             INSERT INTO metric_set (id, plc_id, name, enabled, created_ts, updated_ts)
             VALUES (1, 1, 'diag', 1, 1, 1);
             INSERT INTO tag (id, plc_id, name, address, db_num, byte_offset, byte_size,
                              data_type, poll_group_id,
                              store_mode, storage_hook, enabled, bad, created_ts, updated_ts)
             VALUES (1, 1, 't1', 'DB1.DBW0', 1, 0, 2, 'i16', 1,
                     'on_change', 'on_change', 1, 0, 1, 1);
             INSERT INTO metric_set_tag (metric_set_id, tag_id, plc_id, created_ts)
             VALUES (1, 1, 1, 1);",
        )
        .unwrap();
    }

    fn seed_mixed(db: &Arc<Mutex<Connection>>) {
        let conn = db.lock();
        conn.execute_batch(
            "INSERT INTO plc (id, name, host) VALUES (1, 'plc1', '127.0.0.1');
             INSERT INTO poll_group (id, name, period_ms) VALUES (1, 'fast', 150);
             INSERT INTO poll_group (id, name, period_ms) VALUES (2, 'slow', 1000);
             INSERT INTO metric_set (id, plc_id, name, enabled, created_ts, updated_ts)
             VALUES (1, 1, 'diag', 1, 1, 1);
             INSERT INTO tag (id, plc_id, name, address, db_num, byte_offset, byte_size,
                              data_type, poll_group_id,
                              store_mode, storage_hook, enabled, bad, created_ts, updated_ts)
             VALUES
             (1, 1, 't1', 'DB1.DBW0', 1, 0, 2, 'i16', 1, 'on_change', 'on_change', 1, 0, 1, 1),
             (2, 1, 't2', 'DB1.DBW2', 1, 2, 2, 'i16', 2, 'on_change', 'on_change', 1, 0, 1, 1);
             INSERT INTO metric_set_tag (metric_set_id, tag_id, plc_id, created_ts)
             VALUES (1, 1, 1, 1), (1, 2, 1, 1);",
        )
        .unwrap();
    }
}
