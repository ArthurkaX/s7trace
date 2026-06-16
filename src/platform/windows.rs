//! Windows control window and tray icon for s7trace server.
//!
//! Implements the "server control window" described in
//! `docs/server-control-window.md`:
//!
//! - A small native Win32 window showing runtime status (uptime, DB state,
//!   PLCs, datasets, command queue depth, last events).
//! - A tray icon with menu (Open, Start/Stop collection, Exit).
//! - Buttons that submit typed commands through the same `command_request`
//!   queue the CLI uses, so ordering stays consistent.
//!
//! The window is intentionally NOT a database browser, SQL editor, or
//! tag editor. Those operations stay on the CLI per the doc.
//!
//! Start with [`run_control_window`]; it returns when the user picks
//! "Exit Server" from the tray menu (which sends a `server_exit` command
//! and then posts `WM_CLOSE`).

#![cfg(windows)]

use crate::db::command_queue;
use crate::types::CommandKind;
use anyhow::Result;
use parking_lot::Mutex;
use rusqlite::Connection;
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Instant;
use windows_sys::Win32::Foundation::{HWND, LPARAM, LRESULT, POINT, WPARAM};
use windows_sys::Win32::Graphics::Gdi::{
    BeginPaint, BitBlt, CreateCompatibleBitmap, CreateCompatibleDC, DeleteDC, DeleteObject,
    DrawTextW, EndPaint, FillRect, GetSysColorBrush, InvalidateRect, SelectObject, SetBkMode,
    DT_LEFT, DT_NOPREFIX, PAINTSTRUCT, SRCCOPY,
};
use windows_sys::Win32::System::LibraryLoader::GetModuleHandleW;
use windows_sys::Win32::UI::Shell::{
    Shell_NotifyIconW, NIF_ICON, NIF_MESSAGE, NIF_TIP, NIM_ADD, NIM_DELETE, NOTIFYICONDATAW,
};
use windows_sys::Win32::UI::WindowsAndMessaging::{
    AppendMenuW, CreatePopupMenu, CreateWindowExW, DefWindowProcW, DestroyWindow, DispatchMessageW,
    GetCursorPos, GetMessageW, GetSystemMetrics, LoadIconW, PostQuitMessage, RegisterClassExW,
    SetForegroundWindow, SetTimer, SetWindowTextW, ShowWindow, TrackPopupMenu, TranslateMessage,
    CS_HREDRAW, CS_VREDRAW, HMENU, IDI_APPLICATION, MF_SEPARATOR, MF_STRING, MSG, SM_CXSCREEN,
    SM_CYSCREEN, SW_HIDE, SW_SHOW, TPM_RIGHTBUTTON, WM_CLOSE, WM_COMMAND, WM_CREATE, WM_DESTROY,
    WM_ERASEBKGND, WM_LBUTTONDBLCLK, WM_LBUTTONDOWN, WM_PAINT, WM_RBUTTONUP, WM_TIMER, WM_USER,
    WNDCLASSEXW, WS_CAPTION, WS_OVERLAPPED, WS_SYSMENU, WS_VISIBLE,
};

const WM_TRAYICON: u32 = WM_USER + 1;
const ID_TRAYICON: u32 = 1;

const IDM_OPEN: u16 = 1001;
const IDM_START: u16 = 1002;
const IDM_STOP: u16 = 1003;
const IDM_EXIT: u16 = 1004;

const IDB_START: u16 = 2001;
const IDB_STOP: u16 = 2002;
const IDB_MINIMIZE: u16 = 2003;
const IDB_EXIT: u16 = 2004;

const IDT_REFRESH: usize = 1;

static APP_STARTED: OnceLock<Instant> = OnceLock::new();

fn app_started() -> Instant {
    *APP_STARTED.get_or_init(Instant::now)
}
static CLASS_NAME: &[u16] = &[
    b's' as u16,
    b'7' as u16,
    b't' as u16,
    b'r' as u16,
    b'a' as u16,
    b'c' as u16,
    b'e' as u16,
    b'C' as u16,
    b't' as u16,
    b'r' as u16,
    b'l' as u16,
    0,
];

struct AppState {
    db: Arc<Mutex<Connection>>,
    hwnd: HWND,
    hwnd_button_start: HWND,
    hwnd_button_stop: HWND,
    hwnd_button_min: HWND,
    hwnd_button_exit: HWND,
    hmenu_tray: HMENU,
    /// Cached status rendered on the next paint. Updated by the timer.
    cached_status: Mutex<StatusSnapshot>,
    /// Flag set when the user picked Exit so the run loop returns.
    exit_requested: AtomicBool,
    /// Index of the selected row (-1 = nothing selected).
    selected_row: Mutex<isize>,
}

#[derive(Default, Clone)]
struct StatusSnapshot {
    server_running: bool,
    plc_count: i64,
    active_dataset_count: i64,
    command_queue_pending: i64,
    command_queue_running: i64,
    config_apply_pending: i64,
    rows: Vec<MetricSetRow>,
    last_events: Vec<String>,
    last_command_result: Option<String>,
}

/// A row in the dashboard table — one per (PLC, metric-set) pair.
/// PLCs with no metric sets get one row with metric_set_name = None.
#[derive(Clone)]
struct MetricSetRow {
    plc_name: String,
    plc_connectivity: String,
    metric_set_name: Option<String>, // None = PLC has no metric sets
    dataset_name: Option<String>,    // active dataset, if any
    tags_active: i64,
    tags_bad: i64,
    has_active_poller: bool,
}

/// Run the control window. Blocks until the user exits the server.
///
/// `db` is the same database connection pool the server uses. The window
/// only reads from it and writes new rows into `command_request`.
pub fn run_control_window(db: Arc<Mutex<Connection>>) -> Result<()> {
    unsafe {
        let hinstance = GetModuleHandleW(std::ptr::null());
        if hinstance.is_null() {
            anyhow::bail!("GetModuleHandleW failed");
        }

        let wc = WNDCLASSEXW {
            cbSize: std::mem::size_of::<WNDCLASSEXW>() as u32,
            style: CS_HREDRAW | CS_VREDRAW,
            lpfnWndProc: Some(wndproc),
            hInstance: hinstance,
            hIcon: LoadIconW(std::ptr::null_mut(), IDI_APPLICATION as *const u16),
            hCursor: std::ptr::null_mut(),
            hbrBackground: 16 as isize as _, // COLOR_BTNFACE+1
            lpszMenuName: std::ptr::null(),
            lpszClassName: CLASS_NAME.as_ptr(),
            cbClsExtra: 0,
            cbWndExtra: 0,
            hIconSm: std::ptr::null_mut(),
        };
        if RegisterClassExW(&wc) == 0 {
            anyhow::bail!("RegisterClassExW failed");
        }

        // Center the window
        let screen_w = GetSystemMetrics(SM_CXSCREEN);
        let screen_h = GetSystemMetrics(SM_CYSCREEN);
        let win_w = 640;
        let win_h = 480;
        let x = (screen_w - win_w) / 2;
        let y = (screen_h - win_h) / 2;

        let title = win_string("s7trace server");

        // Allocate AppState and store in global BEFORE CreateWindowExW,
        // since WM_CREATE/WM_PAINT fire synchronously during CreateWindowExW
        // and need access to state_from_ptr().
        let state = Box::new(AppState {
            db,
            hwnd: std::ptr::null_mut(), // filled in after window creation
            hwnd_button_start: std::ptr::null_mut(),
            hwnd_button_stop: std::ptr::null_mut(),
            hwnd_button_min: std::ptr::null_mut(),
            hwnd_button_exit: std::ptr::null_mut(),
            hmenu_tray: std::ptr::null_mut(),
            cached_status: Mutex::new(StatusSnapshot::default()),
            exit_requested: AtomicBool::new(false),
            selected_row: Mutex::new(-1),
        });
        let state_ptr = Box::into_raw(state) as isize;
        APP_STATE.store(state_ptr, Ordering::SeqCst);

        let hwnd = CreateWindowExW(
            0,
            CLASS_NAME.as_ptr(),
            title.as_ptr(),
            WS_OVERLAPPED | WS_CAPTION | WS_SYSMENU | WS_VISIBLE,
            x,
            y,
            win_w,
            win_h,
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
        // Update the real hwnd in the state (WM_CREATE handler uses the
        // function parameter hwnd directly, but other code reads from
        // the AppState field).
        state_from_ptr().hwnd = hwnd;

        SetWindowTextW(hwnd, title.as_ptr());
        ShowWindow(hwnd, SW_SHOW);

        // Pump messages until WM_QUIT
        let mut msg: MSG = std::mem::zeroed();
        loop {
            let rv = GetMessageW(&mut msg, std::ptr::null_mut(), 0, 0);
            if rv <= 0 {
                break;
            }
            TranslateMessage(&msg);
            DispatchMessageW(&msg);
            if state_from_ptr().exit_requested.load(Ordering::SeqCst) {
                // Submit a final WM_QUIT so GetMessageW returns 0 next iteration
                PostQuitMessage(0);
            }
        }

        // Cleanup tray
        let st = state_from_ptr();
        let mut nid: NOTIFYICONDATAW = std::mem::zeroed();
        nid.cbSize = std::mem::size_of::<NOTIFYICONDATAW>() as u32;
        nid.hWnd = st.hwnd;
        nid.uID = ID_TRAYICON;
        Shell_NotifyIconW(NIM_DELETE, &mut nid);
        DestroyWindow(st.hwnd);
        let _ = Box::from_raw(state_ptr as *mut AppState);
        APP_STATE.store(0, Ordering::SeqCst);
        Ok(())
    }
}

static APP_STATE: AtomicIsize = AtomicIsize::new(0);

fn state_from_ptr() -> &'static mut AppState {
    let p = APP_STATE.load(Ordering::SeqCst);
    assert!(p != 0, "control window state not initialized");
    unsafe { &mut *(p as *mut AppState) }
}

/// Convert a Rust string to a null-terminated UTF-16 vector suitable for
/// Win32 APIs that take `*const u16`.
fn win_string(s: &str) -> Vec<u16> {
    s.encode_utf16().chain(std::iter::once(0)).collect()
}

unsafe extern "system" fn wndproc(hwnd: HWND, msg: u32, wparam: WPARAM, lparam: LPARAM) -> LRESULT {
    match msg {
        WM_CREATE => {
            let st = state_from_ptr();
            // Buttons
            let hinstance = GetModuleHandleW(std::ptr::null());
            let text = |s: &str| win_string(s);
            let bw = 140;
            let bh = 28;
            let pad = 8;
            let base_y = 420;
            let total_w = bw * 4 + pad * 3;
            let win_w = 640;
            let start_x = (win_w - total_w) / 2;
            st.hwnd_button_start = CreateWindowExW(
                0,
                text("BUTTON").as_ptr(),
                text("Start Collection").as_ptr(),
                WS_VISIBLE | WS_OVERLAPPED | WS_CAPTION,
                start_x,
                base_y,
                bw,
                bh,
                hwnd,
                IDB_START as HMENU,
                hinstance,
                std::ptr::null(),
            );
            st.hwnd_button_stop = CreateWindowExW(
                0,
                text("BUTTON").as_ptr(),
                text("Stop Collection").as_ptr(),
                WS_VISIBLE | WS_OVERLAPPED,
                start_x + (bw + pad),
                base_y,
                bw,
                bh,
                hwnd,
                IDB_STOP as HMENU,
                hinstance,
                std::ptr::null(),
            );
            st.hwnd_button_min = CreateWindowExW(
                0,
                text("BUTTON").as_ptr(),
                text("Minimize to Tray").as_ptr(),
                WS_VISIBLE | WS_OVERLAPPED,
                start_x + (bw + pad) * 2,
                base_y,
                bw,
                bh,
                hwnd,
                IDB_MINIMIZE as HMENU,
                hinstance,
                std::ptr::null(),
            );
            st.hwnd_button_exit = CreateWindowExW(
                0,
                text("BUTTON").as_ptr(),
                text("Exit Server").as_ptr(),
                WS_VISIBLE | WS_OVERLAPPED,
                start_x + (bw + pad) * 3,
                base_y,
                bw,
                bh,
                hwnd,
                IDB_EXIT as HMENU,
                hinstance,
                std::ptr::null(),
            );
            // Tray menu
            st.hmenu_tray = CreatePopupMenu();
            AppendMenuW(
                st.hmenu_tray,
                MF_STRING,
                IDM_OPEN as usize,
                text("Open window").as_ptr(),
            );
            AppendMenuW(
                st.hmenu_tray,
                MF_STRING,
                IDM_START as usize,
                text("Start collection").as_ptr(),
            );
            AppendMenuW(
                st.hmenu_tray,
                MF_STRING,
                IDM_STOP as usize,
                text("Stop collection").as_ptr(),
            );
            AppendMenuW(st.hmenu_tray, MF_SEPARATOR, 0, std::ptr::null());
            AppendMenuW(
                st.hmenu_tray,
                MF_STRING,
                IDM_EXIT as usize,
                text("Exit server").as_ptr(),
            );
            // Tray icon
            let mut nid: NOTIFYICONDATAW = std::mem::zeroed();
            nid.cbSize = std::mem::size_of::<NOTIFYICONDATAW>() as u32;
            nid.hWnd = hwnd;
            nid.uID = ID_TRAYICON;
            nid.uFlags = NIF_ICON | NIF_MESSAGE | NIF_TIP;
            nid.uCallbackMessage = WM_TRAYICON;
            nid.hIcon = LoadIconW(std::ptr::null_mut(), IDI_APPLICATION as *const u16);
            let tip = text("s7trace server");
            for (dst, src) in nid.szTip.iter_mut().zip(tip.iter()) {
                *dst = *src;
            }
            Shell_NotifyIconW(NIM_ADD, &mut nid);

            // Refresh timer (1s)
            SetTimer(hwnd, IDT_REFRESH, 1000, None);
            0
        }
        WM_TIMER => {
            if wparam == IDT_REFRESH as WPARAM {
                refresh_status(hwnd);
            }
            0
        }
        WM_LBUTTONDOWN => {
            // Extract cursor Y coordinate from lparam (low word = x, high word = y)
            let y = ((lparam >> 16) & 0xFFFF) as i32;
            // If Y is negative, treat as signed i16
            let y_signed = if y > 32767 { y - 65536 } else { y };
            let st = state_from_ptr();
            let snap = st.cached_status.lock();
            let row_top = 56;
            let row_height = 15;
            let rel = y_signed - row_top;
            let idx = if rel >= 0 && rel < snap.rows.len() as i32 * row_height {
                (rel / row_height) as isize
            } else {
                -1_isize
            };
            drop(snap);
            *st.selected_row.lock() = idx;
            unsafe {
                InvalidateRect(hwnd, std::ptr::null(), 0);
            }
            0
        }
        WM_ERASEBKGND => {
            // Prevent background erasure — we fully overwrite the
            // client area in WM_PAINT, so the default erase-then-draw
            // cycle causes visible flicker.
            1
        }
        WM_PAINT => {
            paint(hwnd);
            0
        }
        WM_TRAYICON => {
            let lo = (lparam & 0xFFFF) as u32;
            if lo == WM_RBUTTONUP {
                show_tray_menu(hwnd);
            } else if lo == WM_LBUTTONDBLCLK {
                ShowWindow(hwnd, SW_SHOW);
            }
            0
        }
        WM_COMMAND => {
            let id = (wparam & 0xFFFF) as u16;
            handle_command(hwnd, id);
            0
        }
        WM_CLOSE => {
            // Close button minimizes to tray
            ShowWindow(hwnd, SW_HIDE);
            0
        }
        WM_DESTROY => {
            PostQuitMessage(0);
            0
        }
        _ => {
            // Pass through to default — do NOT invalidate here.
            // The timer-based refresh_status drives redraws, and
            // extra InvalidateRect calls cause flicker.
            DefWindowProcW(hwnd, msg, wparam, lparam)
        }
    }
}

fn handle_command(hwnd: HWND, id: u16) {
    let st = state_from_ptr();
    let db = st.db.clone();
    let status = st.cached_status.lock();
    unsafe {
        match id {
            IDB_START | IDM_START => {
                drop(status);
                submit_start(&db);
            }
            IDB_STOP | IDM_STOP => {
                drop(status);
                submit_stop(&db);
            }
            IDB_MINIMIZE => {
                ShowWindow(hwnd, SW_HIDE);
            }
            IDB_EXIT | IDM_EXIT => {
                drop(status);
                submit_exit(&db);
                // Server will process `server_exit` and stop. We then post WM_QUIT
                // from the message loop once exit_requested flips.
                st.exit_requested.store(true, Ordering::SeqCst);
            }
            _ => {}
        }
    }
}

fn show_tray_menu(hwnd: HWND) {
    unsafe {
        let st = state_from_ptr();
        let mut pt: POINT = std::mem::zeroed();
        GetCursorPos(&mut pt);
        SetForegroundWindow(hwnd);
        TrackPopupMenu(
            st.hmenu_tray,
            TPM_RIGHTBUTTON,
            pt.x,
            pt.y,
            0,
            hwnd,
            std::ptr::null(),
        );
    }
}

fn refresh_status(hwnd: HWND) {
    let st = state_from_ptr();
    let snap = read_status(&st.db);
    *st.cached_status.lock() = snap;
    unsafe {
        InvalidateRect(hwnd, std::ptr::null(), 0);
    }
}

fn read_status(db: &Arc<Mutex<Connection>>) -> StatusSnapshot {
    let conn = db.lock();

    let plc_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM plc WHERE enabled = 1", [], |r| {
            r.get(0)
        })
        .unwrap_or(0);
    let active_dataset_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM dataset WHERE state = 'active'",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);
    let command_queue_pending: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM command_request WHERE state = 'pending'",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);
    let command_queue_running: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM command_request WHERE state = 'running'",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);
    let config_apply_pending: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM config_apply WHERE state IN ('pending','debouncing','applying')",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);

    // Build enriched per-PLC metric set rows
    let mut rows: Vec<MetricSetRow> = Vec::new();

    // 1. Get all enabled PLCs
    let plc_ids: Vec<(i64, String, String)> = {
        let mut stmt = match conn
            .prepare("SELECT id, name, connectivity FROM plc WHERE enabled = 1 ORDER BY id")
        {
            Ok(s) => s,
            Err(_) => return StatusSnapshot::default(),
        };
        stmt.query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
            ))
        })
        .ok()
        .map(|it| it.filter_map(|r| r.ok()).collect())
        .unwrap_or_default()
    };

    for (plc_id, plc_name, plc_connectivity) in &plc_ids {
        // 2. Get active dataset name for this PLC
        let dataset_name: Option<String> = conn
            .query_row(
                "SELECT name FROM dataset WHERE plc_id = ?1 AND state = 'active' LIMIT 1",
                rusqlite::params![plc_id],
                |r| r.get(0),
            )
            .ok();

        // 3. Get metric sets for this PLC
        let metric_sets: Vec<(String, bool, i64, i64)> = {
            let mut stmt = match conn.prepare(
                "SELECT ms.name, ms.enabled,
                    (SELECT COUNT(*) FROM metric_set_tag mst
                     JOIN tag t ON t.id = mst.tag_id
                     WHERE mst.metric_set_id = ms.id AND t.bad = 0 AND t.enabled = 1) as tags_active,
                    (SELECT COUNT(*) FROM metric_set_tag mst
                     JOIN tag t ON t.id = mst.tag_id
                     WHERE mst.metric_set_id = ms.id AND t.bad = 1) as tags_bad
                 FROM metric_set ms
                 WHERE ms.plc_id = ?1
                 ORDER BY ms.id",
            ) {
                Ok(s) => s,
                Err(_) => {
                    // PLC has no metric sets — add a single fallback row
                    rows.push(MetricSetRow {
                        plc_name: plc_name.clone(),
                        plc_connectivity: plc_connectivity.clone(),
                        metric_set_name: None,
                        dataset_name: dataset_name.clone(),
                        tags_active: 0,
                        tags_bad: 0,
                        has_active_poller: dataset_name.is_some(),
                    });
                    continue;
                }
            };
            stmt.query_map(rusqlite::params![plc_id], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, bool>(1)?,
                    r.get::<_, i64>(2)?,
                    r.get::<_, i64>(3)?,
                ))
            })
            .ok()
            .map(|it| it.filter_map(|r| r.ok()).collect())
            .unwrap_or_default()
        };

        if metric_sets.is_empty() {
            // PLC has no metric sets — add a single fallback row
            rows.push(MetricSetRow {
                plc_name: plc_name.clone(),
                plc_connectivity: plc_connectivity.clone(),
                metric_set_name: None,
                dataset_name: dataset_name.clone(),
                tags_active: 0,
                tags_bad: 0,
                has_active_poller: dataset_name.is_some(),
            });
        } else {
            for (ms_name, _ms_enabled, tags_active, tags_bad) in &metric_sets {
                rows.push(MetricSetRow {
                    plc_name: plc_name.clone(),
                    plc_connectivity: plc_connectivity.clone(),
                    metric_set_name: Some(ms_name.clone()),
                    dataset_name: dataset_name.clone(),
                    tags_active: *tags_active,
                    tags_bad: *tags_bad,
                    has_active_poller: dataset_name.is_some(),
                });
            }
        }
    }

    // Last 5 events (newest first)
    let mut last_events: Vec<String> = Vec::new();
    if let Ok(mut stmt) = conn.prepare(
        "SELECT ts, kind, COALESCE(details_json, '') FROM event_marker
         ORDER BY id DESC LIMIT 5",
    ) {
        if let Ok(ev_rows) = stmt.query_map([], |r| {
            let ts: i64 = r.get(0)?;
            let kind: String = r.get(1)?;
            let details: String = r.get(2)?;
            Ok(format_event(ts, &kind, &details))
        }) {
            for r in ev_rows.flatten() {
                last_events.push(r);
            }
        }
    }

    // Last finished command result
    let last_command_result: Option<String> = conn
        .query_row(
            "SELECT cr.ok || '|' || COALESCE(cr.result_json, '') || '|' || COALESCE(cr.error, '')
             FROM command_result cr
             JOIN command_request cmd ON cmd.id = cr.command_id
             ORDER BY cr.command_id DESC LIMIT 1",
            [],
            |r| r.get(0),
        )
        .ok();

    StatusSnapshot {
        server_running: plc_count > 0,
        plc_count,
        active_dataset_count,
        command_queue_pending,
        command_queue_running,
        config_apply_pending,
        rows,
        last_events,
        last_command_result,
    }
}

fn format_event(ts: i64, kind: &str, details: &str) -> String {
    let secs = ts / 1_000_000;
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, 0);
    let timestr = match dt {
        Some(d) => d.format("%H:%M:%S").to_string(),
        None => "??:??:??".to_string(),
    };
    let det = if details.is_empty() {
        String::new()
    } else {
        format!(" {details}")
    };
    format!("{timestr}  {kind}{det}")
}

fn paint(hwnd: HWND) {
    let st = state_from_ptr();
    let snap = st.cached_status.lock().clone();
    let sel = *st.selected_row.lock();
    // st borrow ends here; values are owned clones
    let _ = st;

    unsafe {
        let mut ps: PAINTSTRUCT = std::mem::zeroed();
        let hdc = BeginPaint(hwnd, &mut ps);
        let (cx, cy) = (
            ps.rcPaint.right - ps.rcPaint.left,
            ps.rcPaint.bottom - ps.rcPaint.top,
        );
        if cx <= 0 || cy <= 0 {
            EndPaint(hwnd, &ps);
            return;
        }

        // Double-buffer: draw to memory DC, then BitBlt to screen.
        // This eliminates flicker from repeated GDI text drawing.
        let mem_dc = CreateCompatibleDC(hdc);
        let bmp = CreateCompatibleBitmap(hdc, cx, cy);
        let old_bmp = SelectObject(mem_dc, bmp);

        // Fill background with button face color
        let bg_brush = GetSysColorBrush(15); // COLOR_BTNFACE
        let mut bg_rect = windows_sys::Win32::Foundation::RECT {
            left: 0,
            top: 0,
            right: cx,
            bottom: cy,
        };
        FillRect(mem_dc, &mut bg_rect, bg_brush);

        // Transparent background for all text
        SetBkMode(mem_dc, 1); // TRANSPARENT

        let uptime = app_started().elapsed();
        let uptime_str = format!(
            "{:02}:{:02}:{:02}",
            uptime.as_secs() / 3600,
            (uptime.as_secs() / 60) % 60,
            uptime.as_secs() % 60,
        );

        // Top status line
        let header = win_string(&format!(
            "s7trace  PLCs:{}  Datasets:{}  Status: {}  Uptime {}",
            snap.plc_count,
            snap.active_dataset_count,
            if snap.server_running {
                "RUNNING"
            } else {
                "IDLE"
            },
            uptime_str,
        ));
        let mut header_rect = windows_sys::Win32::Foundation::RECT {
            left: 10,
            top: 8,
            right: 620,
            bottom: 30,
        };
        DrawTextW(
            mem_dc,
            header.as_ptr(),
            -1,
            &mut header_rect,
            DT_LEFT | DT_NOPREFIX,
        );

        // Counts line
        let counts = win_string(&format!(
            "Queue: pending={} running={}  config-apply pending={}",
            snap.command_queue_pending, snap.command_queue_running, snap.config_apply_pending,
        ));
        let mut counts_rect = windows_sys::Win32::Foundation::RECT {
            left: 10,
            top: 34,
            right: 620,
            bottom: 50,
        };
        DrawTextW(
            mem_dc,
            counts.as_ptr(),
            -1,
            &mut counts_rect,
            DT_LEFT | DT_NOPREFIX,
        );

        // Dashboard table
        let mut table_text =
            String::from("PLC            Task (metric set)     Dataset            State    Tags\n");
        table_text
            .push_str("----------------------------------------------------------------------\n");

        let row_top = 56;
        let row_height = 15;
        let table_bottom = row_top + snap.rows.len() as i32 * row_height;

        for (i, row) in snap.rows.iter().enumerate() {
            let state = row_state(row);
            let tags_str = format!("{}/{}", row.tags_active, row.tags_bad);
            let ds_str = row.dataset_name.as_deref().unwrap_or("---");
            let set_str = row.metric_set_name.as_deref().unwrap_or("---");
            let prefix = if sel == i as isize { "> " } else { "  " };
            table_text.push_str(&format!(
                "{}{:<13} {:<20} {:<18} {:<7} {}\n",
                prefix, row.plc_name, set_str, ds_str, state, tags_str
            ));
        }

        let mut table_rect = windows_sys::Win32::Foundation::RECT {
            left: 10,
            top: row_top,
            right: 620,
            bottom: table_bottom + 4,
        };
        let table_w = win_string(&table_text);
        DrawTextW(
            mem_dc,
            table_w.as_ptr(),
            -1,
            &mut table_rect,
            DT_LEFT | DT_NOPREFIX,
        );

        // Selection info line
        let info_text = if sel >= 0 && (sel as usize) < snap.rows.len() {
            let r = &snap.rows[sel as usize];
            let set_str = r.metric_set_name.as_deref().unwrap_or("(no sets)");
            format!("Selected: {} / {}", r.plc_name, set_str)
        } else {
            String::from("Click a row to select")
        };
        let info_y = table_bottom + 6;
        let mut info_rect = windows_sys::Win32::Foundation::RECT {
            left: 10,
            top: info_y,
            right: 620,
            bottom: info_y + 18,
        };
        let info_w = win_string(&info_text);
        DrawTextW(
            mem_dc,
            info_w.as_ptr(),
            -1,
            &mut info_rect,
            DT_LEFT | DT_NOPREFIX,
        );

        // Last events
        let mut ev_text = String::from("Last events:\n");
        for e in &snap.last_events {
            ev_text.push_str(&format!("  {e}\n"));
        }
        if let Some(r) = &snap.last_command_result {
            ev_text.push_str(&format!("  [last cmd] {r}\n"));
        }
        let ev_y = info_y + 20;
        let mut ev_rect = windows_sys::Win32::Foundation::RECT {
            left: 10,
            top: ev_y,
            right: 620,
            bottom: ev_y + 120,
        };
        let ev_w = win_string(&ev_text);
        DrawTextW(
            mem_dc,
            ev_w.as_ptr(),
            -1,
            &mut ev_rect,
            DT_LEFT | DT_NOPREFIX,
        );

        // Blit the memory DC to the screen DC in one shot
        BitBlt(hdc, 0, 0, cx, cy, mem_dc, 0, 0, SRCCOPY);

        // Cleanup
        SelectObject(mem_dc, old_bmp);
        DeleteObject(bmp);
        DeleteDC(mem_dc);

        EndPaint(hwnd, &ps);
    }
}

/// Compute a short state string for a dashboard row.
fn row_state(row: &MetricSetRow) -> String {
    if row.plc_connectivity == "offline" || row.plc_connectivity == "disconnected" {
        return "DISCONN".to_string();
    }
    if row.has_active_poller {
        return format!("{} RUN", "\u{25CF}"); // filled circle
    }
    if row.metric_set_name.is_some() {
        return format!("{} off", "\u{25CB}"); // empty circle
    }
    // No metric sets for this PLC
    format!("{} idle", "\u{25CB}")
}

// ---------------------------------------------------------------------------
// Command submission helpers
// ---------------------------------------------------------------------------

fn submit_start(db: &Arc<Mutex<Connection>>) {
    let conn = db.lock();

    // Try to read selection from AppState (may not be set in tests)
    let selection: Option<(String, Option<String>)> = {
        let ptr = APP_STATE.load(Ordering::SeqCst);
        if ptr != 0 {
            unsafe {
                let st = &mut *(ptr as *mut AppState);
                let sel = *st.selected_row.lock();
                let rows = st.cached_status.lock().rows.clone();
                if sel >= 0 && (sel as usize) < rows.len() {
                    Some((
                        rows[sel as usize].plc_name.clone(),
                        rows[sel as usize].metric_set_name.clone(),
                    ))
                } else {
                    None
                }
            }
        } else {
            None
        }
    };

    // Determine which PLC + metric set pair to use
    let (plc_id, plc_name, set_name): (i64, String, Option<String>) =
        if let Some((ref pname, ref msname)) = selection {
            let pid: i64 = match conn.query_row(
                "SELECT id FROM plc WHERE name = ?1",
                rusqlite::params![pname],
                |row| row.get(0),
            ) {
                Ok(id) => id,
                Err(_) => {
                    log::warn!("control window: selected PLC '{}' not found", pname);
                    return;
                }
            };
            (pid, pname.clone(), msname.clone())
        } else {
            // Fallback: first enabled PLC with enabled metric sets, no active dataset
            let fallback: Option<(i64, String, String)> = conn
                .query_row(
                    "SELECT p.id, p.name, ms.name FROM plc p
                     JOIN metric_set ms ON ms.plc_id = p.id
                     WHERE p.enabled = 1 AND ms.enabled = 1
                       AND NOT EXISTS (SELECT 1 FROM dataset d
                                       WHERE d.plc_id = p.id AND d.state = 'active')
                     ORDER BY p.id, ms.id LIMIT 1",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .ok();
            match fallback {
                Some((pid, pname, msname)) => (pid, pname, Some(msname)),
                None => {
                    // Try any enabled PLC with no active dataset (even without sets)
                    let any_plc: Option<(i64, String)> = conn
                        .query_row(
                            "SELECT p.id, p.name FROM plc p
                             WHERE p.enabled = 1
                               AND NOT EXISTS (SELECT 1 FROM dataset d
                                               WHERE d.plc_id = p.id AND d.state = 'active')
                             ORDER BY p.id LIMIT 1",
                            [],
                            |r| Ok((r.get(0)?, r.get(1)?)),
                        )
                        .ok();
                    match any_plc {
                        Some((pid, pname)) => (pid, pname, None),
                        None => {
                            log::warn!("control window: no PLC available to start collection");
                            return;
                        }
                    }
                }
            }
        };

    // Check no active dataset for this PLC
    let active: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM dataset WHERE plc_id = ?1 AND state = 'active'",
            rusqlite::params![plc_id],
            |r| r.get(0),
        )
        .unwrap_or(0);
    if active > 0 {
        log::warn!(
            "control window: PLC '{}' already has an active dataset",
            plc_name
        );
        return;
    }

    // Auto-name the dataset
    let now = chrono::Utc::now();
    let ts = now.format("%Y_%m_%d_%H%M").to_string();
    let set_part = set_name.as_deref().unwrap_or(&plc_name);
    let dataset_name = format!("{}_{}", set_part, ts);

    let sets: Vec<String> = set_name.clone().into_iter().collect();

    let request = serde_json::json!({
        "action": "collection_start",
        "plc_id": plc_id,
        "plc": plc_name,
        "dataset": dataset_name,
        "sets": sets,
    });
    match command_queue::enqueue(
        &conn,
        CommandKind::Control,
        &request.to_string(),
        Some(plc_id),
        Some("control-window"),
        None,
    ) {
        Ok(id) => log::info!("control window: enqueued collection_start cmd id={id}"),
        Err(e) => log::error!("control window: enqueue failed: {e}"),
    }
}

fn submit_stop(db: &Arc<Mutex<Connection>>) {
    let conn = db.lock();

    // Try to read selection from AppState (may not be set in tests)
    let sel_plc_name: Option<String> = {
        let ptr = APP_STATE.load(Ordering::SeqCst);
        if ptr != 0 {
            unsafe {
                let st = &mut *(ptr as *mut AppState);
                let sel = *st.selected_row.lock();
                let rows = st.cached_status.lock().rows.clone();
                if sel >= 0 && (sel as usize) < rows.len() {
                    Some(rows[sel as usize].plc_name.clone())
                } else {
                    None
                }
            }
        } else {
            None
        }
    };

    // Determine which PLC to stop
    let (plc_id, plc_name): (i64, String) = if let Some(ref pname) = sel_plc_name {
        match conn.query_row(
            "SELECT id FROM plc WHERE name = ?1",
            rusqlite::params![pname],
            |row| row.get(0),
        ) {
            Ok(pid) => (pid, pname.clone()),
            Err(_) => {
                log::warn!("control window: selected PLC '{}' not found", pname);
                // Fallback to first active dataset
                conn.query_row(
                    "SELECT plc_id, name FROM dataset WHERE state = 'active' ORDER BY id LIMIT 1",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .unwrap_or_else(|_| {
                    log::info!("control window: no active dataset to stop");
                    return (0, String::new());
                })
            }
        }
    } else {
        // Fallback: first active dataset
        conn.query_row(
            "SELECT plc_id, name FROM dataset WHERE state = 'active' ORDER BY id LIMIT 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap_or_else(|_| {
            log::info!("control window: no active dataset to stop");
            return (0, String::new());
        })
    };

    if plc_id == 0 {
        return;
    }

    let request = serde_json::json!({
        "action": "collection_stop",
        "plc_id": plc_id,
        "plc": plc_name,
    });
    match command_queue::enqueue(
        &conn,
        CommandKind::Control,
        &request.to_string(),
        Some(plc_id),
        Some("control-window"),
        None,
    ) {
        Ok(id) => log::info!("control window: enqueued collection_stop cmd id={id}"),
        Err(e) => log::error!("control window: enqueue failed: {e}"),
    }
}

fn submit_exit(db: &Arc<Mutex<Connection>>) {
    let conn = db.lock();
    // Reuse a Control-kind request with action=server_exit. The server's
    // command worker dispatches it; the window itself doesn't kill the
    // process directly.
    let request = serde_json::json!({"action": "server_exit"});
    match command_queue::enqueue(
        &conn,
        CommandKind::Control,
        &request.to_string(),
        None,
        Some("control-window"),
        None,
    ) {
        Ok(id) => log::info!("control window: enqueued server_exit cmd id={id}"),
        Err(e) => log::error!("control window: enqueue failed: {e}"),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::schema;
    use std::sync::Arc;

    fn temp_db() -> Arc<Mutex<Connection>> {
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "s7trace_ctrl_{}_{:?}.db",
            std::process::id(),
            std::thread::current().id()
        ));
        let s = path.to_string_lossy().into_owned();
        let _ = std::fs::remove_file(&s);
        Arc::new(Mutex::new(schema::open(&s).unwrap()))
    }

    #[test]
    fn test_status_snapshot_for_empty_db() {
        let db = temp_db();
        let snap = read_status(&db);
        assert_eq!(snap.plc_count, 0);
        assert!(snap.rows.is_empty());
        assert_eq!(snap.command_queue_pending, 0);
    }

    #[test]
    fn test_status_snapshot_for_seeded_db() {
        let db = temp_db();
        let conn = db.lock();
        conn.execute_batch(
            "INSERT INTO plc (name, host, connectivity) VALUES
                 ('plc1', '127.0.0.1:102', 'online'),
                 ('plc2', '127.0.0.1:103', 'offline');
             INSERT INTO dataset (plc_id, name, state, started_ts, created_ts, updated_ts)
                 VALUES (1, 'ds1', 'active', 1000000, 1000000, 1000000);",
        )
        .unwrap();
        drop(conn);
        let snap = read_status(&db);
        assert_eq!(snap.plc_count, 2);
        assert_eq!(snap.active_dataset_count, 1);
        // PLCs without metric sets get one fallback row each
        assert_eq!(snap.rows.len(), 2);
        assert_eq!(snap.rows[0].plc_connectivity, "online");
        assert_eq!(snap.rows[1].plc_connectivity, "offline");
        assert!(snap.rows[0].dataset_name.is_some());
        assert_eq!(snap.rows[0].dataset_name.as_deref(), Some("ds1"));
    }

    #[test]
    fn test_submit_start_no_plc() {
        let db = temp_db();
        // No PLCs: should be a no-op + log, not a panic.
        submit_start(&db);
    }
}
