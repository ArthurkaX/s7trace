//! s7panel — Native Win32 operator panel for s7trace.
//!
//! Displays metric set status from a remote s7trace server and
//! allows enabling/disabling sets and adjusting poll periods.
//!
//! ```text
//! cargo run --bin s7panel -- --server http://127.0.0.1:7707
//! ```
//!
//! No web UI, no browser — plain Win32 controls only.

#![cfg(windows)]

use std::ffi::OsStr;
use std::os::windows::ffi::OsStrExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use windows_sys::Win32::Foundation::*;
use windows_sys::Win32::Graphics::Gdi::*;
use windows_sys::Win32::System::LibraryLoader::GetModuleHandleW;
use windows_sys::Win32::UI::Controls::*;
use windows_sys::Win32::UI::WindowsAndMessaging::*;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const WINDOW_CLASS: &str = "s7panel_window";
const WINDOW_WIDTH: i32 = 860;
const WINDOW_HEIGHT: i32 = 400;
const REFRESH_TIMER_ID: usize = 1;
const REFRESH_MS: u32 = 2000;
const HTTP_TIMEOUT: Duration = Duration::from_secs(2);

// Button control IDs
const ID_REFRESH: u32 = 101;
const ID_ENABLE: u32 = 102;
const ID_DISABLE: u32 = 103;
const ID_MINUS_50: u32 = 104;
const ID_PLUS_50: u32 = 105;
const ID_LISTVIEW: u32 = 200;

// ListView columns
struct Col;
#[allow(non_upper_case_globals)]
impl Col {
    const PLC: i32 = 0;
    const SET: i32 = 1;
    const ENABLED: i32 = 2;
    const TAGS: i32 = 3;
    const BAD: i32 = 4;
    const PERIOD: i32 = 5;
    const RATE: i32 = 6;
    const AGE: i32 = 7;
}

// ListView column count
#[allow(dead_code)]
const COL_COUNT: i32 = 8;

// ---------------------------------------------------------------------------
// Row data
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct RowData {
    plc: String,
    set_name: String,
    enabled: bool,
    tags_active: i64,
    tags_total: i64,
    tags_bad: i64,
    poll_mode: String,
    poll_group_id: Option<i64>,
    poll_group_name: String,
    period_ms: Option<i64>,
    rate_hz: Option<f64>,
    last_poll_ts: Option<i64>,
}

// ---------------------------------------------------------------------------
// Global state
// ---------------------------------------------------------------------------

static ROW_DATA: Mutex<Vec<RowData>> = Mutex::new(Vec::new());
static REFRESHING: AtomicBool = AtomicBool::new(false);
static mut MAIN_HWND: HWND = std::ptr::null_mut();
static mut LISTVIEW_HWND: HWND = std::ptr::null_mut();
static SERVER_URL: Mutex<String> = Mutex::new(String::new());

// ---------------------------------------------------------------------------
// UTF-16 conversion helper
// ---------------------------------------------------------------------------

fn to_utf16(s: &str) -> Vec<u16> {
    OsStr::new(s).encode_wide().chain(Some(0)).collect()
}

// ---------------------------------------------------------------------------
// Time helper
// ---------------------------------------------------------------------------

fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}

// ---------------------------------------------------------------------------
// Minimal HTTP client
// ---------------------------------------------------------------------------

use std::io::{Read, Write};
use std::net::TcpStream;

fn http_get_body(base_url: &str, path: &str, timeout: Duration) -> Result<String, String> {
    let host_port = base_url
        .trim_end_matches('/')
        .strip_prefix("http://")
        .or_else(|| base_url.strip_prefix("https://"))
        .ok_or_else(|| format!("bad URL: {base_url}"))?;
    let stream = TcpStream::connect_timeout(
        &host_port.parse().map_err(|e| format!("addr: {e}"))?,
        timeout,
    )
    .map_err(|e| format!("connect: {e}"))?;
    stream.set_read_timeout(Some(timeout)).ok();
    stream.set_write_timeout(Some(timeout)).ok();

    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: {host_port}\r\nConnection: close\r\nAccept: application/json\r\n\r\n"
    );
    let mut s = stream;
    s.write_all(req.as_bytes())
        .map_err(|e| format!("write: {e}"))?;
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).map_err(|e| format!("read: {e}"))?;
    let text = std::str::from_utf8(&buf).map_err(|e| format!("utf8: {e}"))?;
    let mut parts = text.split("\r\n\r\n");
    let head = parts.next().unwrap_or("");
    let body = parts.next().unwrap_or("");
    if !head.contains(" 200 ") && !head.contains(" 201 ") {
        return Err(format!("HTTP {}", head.lines().next().unwrap_or("?")));
    }
    Ok(body.to_string())
}

fn http_post_json(
    base_url: &str,
    path: &str,
    json_body: &str,
    timeout: Duration,
) -> Result<String, String> {
    let host_port = base_url
        .trim_end_matches('/')
        .strip_prefix("http://")
        .or_else(|| base_url.strip_prefix("https://"))
        .ok_or_else(|| format!("bad URL: {base_url}"))?;
    let stream = TcpStream::connect_timeout(
        &host_port.parse().map_err(|e| format!("addr: {e}"))?,
        timeout,
    )
    .map_err(|e| format!("connect: {e}"))?;
    stream.set_read_timeout(Some(timeout)).ok();
    stream.set_write_timeout(Some(timeout)).ok();

    let req = format!(
        "POST {path} HTTP/1.1\r\n\
         Host: {host_port}\r\n\
         Connection: close\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {len}\r\n\
         Accept: application/json\r\n\
         \r\n{json_body}",
        len = json_body.len()
    );
    let mut s = stream;
    s.write_all(req.as_bytes())
        .map_err(|e| format!("write: {e}"))?;
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).map_err(|e| format!("read: {e}"))?;
    let text = std::str::from_utf8(&buf).map_err(|e| format!("utf8: {e}"))?;
    let mut parts = text.split("\r\n\r\n");
    let head = parts.next().unwrap_or("");
    let body = parts.next().unwrap_or("");
    if !head.contains(" 200 ") && !head.contains(" 201 ") && !head.contains(" 204 ") {
        return Err(format!("HTTP {}", head.lines().next().unwrap_or("?")));
    }
    Ok(body.to_string())
}

// ---------------------------------------------------------------------------
// ListView helpers
// ---------------------------------------------------------------------------

fn lv_insert_column(listview: HWND, index: i32, text: &str, width: i32) {
    let text_u16 = to_utf16(text);
    let mut col = LVCOLUMNW {
        mask: LVCF_WIDTH | LVCF_TEXT | LVCF_SUBITEM | LVCF_FMT,
        fmt: LVCFMT_LEFT,
        cx: width,
        pszText: text_u16.as_ptr() as *mut u16,
        cchTextMax: text_u16.len() as i32,
        ..unsafe { std::mem::zeroed() }
    };
    unsafe {
        SendMessageW(
            listview,
            LVM_INSERTCOLUMNW as u32,
            index as usize,
            &mut col as *mut _ as isize,
        );
    }
}

fn lv_insert_item(listview: HWND, index: i32, text: &str, row_index: isize) {
    let text_u16 = to_utf16(text);
    let mut item = LVITEMW {
        mask: LVIF_TEXT | LVIF_PARAM,
        iItem: index,
        iSubItem: 0,
        state: 0,
        stateMask: 0,
        pszText: text_u16.as_ptr() as *mut u16,
        cchTextMax: text_u16.len() as i32,
        iImage: 0,
        lParam: row_index,
        iIndent: 0,
        iGroupId: 0,
        cColumns: 0,
        puColumns: std::ptr::null_mut(),
        piColFmt: std::ptr::null_mut(),
        iGroup: 0,
    };
    unsafe {
        SendMessageW(
            listview,
            LVM_INSERTITEMW as u32,
            0,
            &mut item as *mut _ as isize,
        );
    }
}

fn lv_set_subitem(listview: HWND, index: i32, sub: i32, text: &str) {
    let text_u16 = to_utf16(text);
    let mut item = LVITEMW {
        mask: LVIF_TEXT,
        iItem: index,
        iSubItem: sub,
        state: 0,
        stateMask: 0,
        pszText: text_u16.as_ptr() as *mut u16,
        cchTextMax: text_u16.len() as i32,
        iImage: 0,
        lParam: 0,
        iIndent: 0,
        iGroupId: 0,
        cColumns: 0,
        puColumns: std::ptr::null_mut(),
        piColFmt: std::ptr::null_mut(),
        iGroup: 0,
    };
    unsafe {
        SendMessageW(
            listview,
            LVM_SETITEMW as u32,
            0,
            &mut item as *mut _ as isize,
        );
    }
}

fn lv_delete_all(listview: HWND) {
    unsafe {
        SendMessageW(listview, LVM_DELETEALLITEMS as u32, 0, 0);
    }
}

// ---------------------------------------------------------------------------
// Stale detection
// ---------------------------------------------------------------------------

fn is_stale(last_poll_ts: Option<i64>, period_ms: Option<i64>) -> bool {
    let threshold = match (last_poll_ts, period_ms) {
        (Some(_), Some(period)) => std::cmp::max(period * 5, 5000),
        (Some(_), None) => 5000,
        (None, _) => return true,
    };
    if let Some(ts) = last_poll_ts {
        let age_ms = (now_us() - ts) / 1000;
        return age_ms > threshold;
    }
    true
}

// ---------------------------------------------------------------------------
// Update ListView from status JSON
// ---------------------------------------------------------------------------

fn update_listview_from_status(json_text: &str) {
    let v: serde_json::Value = match serde_json::from_str(json_text) {
        Ok(v) => v,
        Err(_) => return,
    };

    let plcs = match v["plcs"].as_array() {
        Some(p) => p,
        None => return,
    };

    let mut rows: Vec<RowData> = Vec::new();
    for plc in plcs {
        let plc_name = plc["name"].as_str().unwrap_or("?");
        let last_poll_ts = plc["last_poll_cycle_finished_ts"].as_i64();
        let metric_sets = match plc["metric_sets"].as_array() {
            Some(ms) => ms,
            None => continue,
        };
        for ms in metric_sets {
            let ep = &ms["effective_poll"];
            let mode = ep["mode"].as_str().unwrap_or("none").to_string();
            let pg_id = ep["poll_group_id"].as_i64();
            let pg_name = ep["poll_group_name"].as_str().unwrap_or("").to_string();
            let period = ep["period_ms"].as_i64();
            let rate = ep["rate_hz"].as_f64();
            rows.push(RowData {
                plc: plc_name.to_string(),
                set_name: ms["name"].as_str().unwrap_or("?").to_string(),
                enabled: ms["enabled"].as_bool().unwrap_or(false),
                tags_active: ms["tags_active"].as_i64().unwrap_or(0),
                tags_total: ms["tags_total"].as_i64().unwrap_or(0),
                tags_bad: ms["tags_bad"].as_i64().unwrap_or(0),
                poll_mode: mode,
                poll_group_id: pg_id,
                poll_group_name: pg_name,
                period_ms: period,
                rate_hz: rate,
                last_poll_ts,
            });
        }
    }

    *ROW_DATA.lock().unwrap() = rows;

    let listview = unsafe { LISTVIEW_HWND };
    if listview.is_null() {
        return;
    }
    lv_delete_all(listview);

    let data = ROW_DATA.lock().unwrap();
    for (i, row) in data.iter().enumerate() {
        let enabled_str = if row.enabled { "\u{2713}" } else { "\u{2717}" };
        let tags_str = format!("{}/{}", row.tags_active, row.tags_total);

        let period_str = match row.poll_mode.as_str() {
            "single" => format!("{}ms", row.period_ms.unwrap_or(0)),
            "mixed" => "mixed".to_string(),
            _ => "none".to_string(),
        };
        let rate_str = match row.rate_hz {
            Some(hz) => format!("{:.1}Hz", hz),
            None => String::new(),
        };
        let age_str = match row.last_poll_ts {
            Some(ts) => {
                let age_ms = (now_us() - ts) / 1000;
                let stale = is_stale(row.last_poll_ts, row.period_ms);
                if stale {
                    format!("STALE! {:.1}s", age_ms as f64 / 1000.0)
                } else {
                    format!("{:.1}s", age_ms as f64 / 1000.0)
                }
            }
            None => "-".to_string(),
        };

        lv_insert_item(listview, i as i32, &row.plc, i as isize);
        lv_set_subitem(listview, i as i32, Col::SET, &row.set_name);
        lv_set_subitem(listview, i as i32, Col::ENABLED, enabled_str);
        lv_set_subitem(listview, i as i32, Col::TAGS, &tags_str);
        lv_set_subitem(listview, i as i32, Col::BAD, &row.tags_bad.to_string());
        lv_set_subitem(listview, i as i32, Col::PERIOD, &period_str);
        lv_set_subitem(listview, i as i32, Col::RATE, &rate_str);
        lv_set_subitem(listview, i as i32, Col::AGE, &age_str);
    }
}

// ---------------------------------------------------------------------------
// Refresh from server
// ---------------------------------------------------------------------------

fn do_refresh() {
    if REFRESHING.load(Ordering::SeqCst) {
        return;
    }
    REFRESHING.store(true, Ordering::SeqCst);

    let url = SERVER_URL.lock().unwrap().clone();
    drop(url); // release lock immediately

    // Re-acquire to get the owned value
    let url_owned = SERVER_URL.lock().unwrap().clone();

    match http_get_body(&url_owned, "/v1/status", HTTP_TIMEOUT) {
        Ok(body) => {
            update_listview_from_status(&body);
            unsafe {
                SetWindowTextW(
                    MAIN_HWND,
                    to_utf16(&format!("s7panel \u{2014} connected")).as_ptr(),
                );
            }
        }
        Err(e) => unsafe {
            SetWindowTextW(
                MAIN_HWND,
                to_utf16(&format!("s7panel \u{2014} {e}")).as_ptr(),
            );
        },
    }
    REFRESHING.store(false, Ordering::SeqCst);
}

// ---------------------------------------------------------------------------
// Send a control command
// ---------------------------------------------------------------------------

fn send_control(action_json: &str, timeout: Duration) -> Result<String, String> {
    let url = SERVER_URL.lock().unwrap().clone();
    let body = format!(
        r#"{{"kind":"control","request":{},"actor":"panel"}}"#,
        action_json
    );
    http_post_json(&url, "/v1/command", &body, timeout)
}

// ---------------------------------------------------------------------------
// Helper: safe LVM_GETNEXTITEM wrapper
// ---------------------------------------------------------------------------

fn listview_selection(listview: HWND) -> Option<i32> {
    // LVM_GETNEXTITEM: wParam = search start (-1 = start from beginning),
    // lParam = flags (LVNI_SELECTED)
    // We pass wParam as -1i64 cast to isize
    let sel = unsafe {
        SendMessageW(
            listview,
            LVM_GETNEXTITEM as u32,
            usize::MAX, // -1 as unsigned is usize::MAX
            LVNI_SELECTED as isize,
        )
    };
    if sel == -1 {
        None
    } else {
        Some(sel as i32)
    }
}

fn trigger_delayed_refresh() {
    let hwnd_val = unsafe { MAIN_HWND as isize };
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(500));
        unsafe {
            PostMessageW(hwnd_val as HWND, WM_TIMER, REFRESH_TIMER_ID, 0);
        }
    });
}

// ---------------------------------------------------------------------------
// Window procedure
// ---------------------------------------------------------------------------

unsafe extern "system" fn wnd_proc(hwnd: HWND, msg: u32, wparam: usize, lparam: isize) -> isize {
    match msg {
        WM_CREATE => {
            MAIN_HWND = hwnd;

            let instance = GetModuleHandleW(std::ptr::null());

            // Create ListView
            let listview = CreateWindowExW(
                0,
                WC_LISTVIEWW,
                std::ptr::null(),
                (WS_CHILD | WS_VISIBLE | WS_BORDER | LVS_REPORT | LVS_SINGLESEL) as u32,
                0,
                32,
                WINDOW_WIDTH,
                WINDOW_HEIGHT - 32,
                hwnd,
                ID_LISTVIEW as *mut std::ffi::c_void,
                instance,
                std::ptr::null(),
            );
            LISTVIEW_HWND = listview;

            // Set extended styles
            let ext_style = LVS_EX_FULLROWSELECT | LVS_EX_GRIDLINES | LVS_EX_DOUBLEBUFFER;
            SendMessageW(
                listview,
                LVM_SETEXTENDEDLISTVIEWSTYLE as u32,
                0,
                ext_style as isize,
            );

            // Add columns
            lv_insert_column(listview, Col::PLC, "PLC", 70);
            lv_insert_column(listview, Col::SET, "Set", 130);
            lv_insert_column(listview, Col::ENABLED, "On", 40);
            lv_insert_column(listview, Col::TAGS, "Active/Total", 80);
            lv_insert_column(listview, Col::BAD, "Bad", 35);
            lv_insert_column(listview, Col::PERIOD, "Period", 70);
            lv_insert_column(listview, Col::RATE, "Rate", 70);
            lv_insert_column(listview, Col::AGE, "Age", 90);

            // Button bar
            let b = |text: &str, x: i32, w: i32, id: u32| -> HWND {
                CreateWindowExW(
                    0,
                    WC_BUTTONW,
                    to_utf16(text).as_ptr(),
                    (WS_CHILD | WS_VISIBLE) as u32 | BS_PUSHBUTTON as u32,
                    x,
                    4,
                    w,
                    24,
                    hwnd,
                    id as *mut std::ffi::c_void,
                    instance,
                    std::ptr::null(),
                )
            };

            b("Refresh", 4, 60, ID_REFRESH);
            b("Enable", 68, 60, ID_ENABLE);
            b("Disable", 132, 60, ID_DISABLE);
            b("-50 ms", 196, 60, ID_MINUS_50);
            b("+50 ms", 260, 60, ID_PLUS_50);

            // Start refresh timer
            SetTimer(hwnd, REFRESH_TIMER_ID, REFRESH_MS, None);

            0
        }

        WM_SIZE => {
            let w = lparam as i32 & 0xFFFF;
            let h = (lparam as i32 >> 16) & 0xFFFF;
            let lv = LISTVIEW_HWND;
            if !lv.is_null() {
                MoveWindow(lv, 0, 32, w, h - 32, 1);
            }
            0
        }

        WM_TIMER => {
            if wparam == REFRESH_TIMER_ID {
                do_refresh();
            }
            0
        }

        WM_COMMAND => {
            let id = (wparam & 0xFFFF) as u32;
            match id {
                ID_REFRESH => do_refresh(),

                ID_ENABLE | ID_DISABLE => {
                    let lv = LISTVIEW_HWND;
                    if let Some(sel) = listview_selection(lv) {
                        let data = ROW_DATA.lock().unwrap();
                        let idx = sel as usize;
                        if idx < data.len() {
                            let row = &data[idx];
                            let action = if id == ID_ENABLE {
                                "set_enable"
                            } else {
                                "set_disable"
                            };
                            let action_json = format!(
                                r#"{{"action":"{}","set_name":"{}"}}"#,
                                action,
                                serde_json::json!(row.set_name)
                                    .as_str()
                                    .unwrap_or(&row.set_name)
                            );
                            drop(data);

                            match send_control(&action_json, Duration::from_secs(5)) {
                                Ok(_) => {
                                    SetWindowTextW(
                                        hwnd,
                                        to_utf16("s7panel \u{2014} command queued").as_ptr(),
                                    );
                                    trigger_delayed_refresh();
                                }
                                Err(e) => {
                                    SetWindowTextW(
                                        hwnd,
                                        to_utf16(&format!("s7panel \u{2014} failed: {e}")).as_ptr(),
                                    );
                                }
                            }
                        } else {
                            drop(data);
                        }
                    } else {
                        SetWindowTextW(
                            hwnd,
                            to_utf16("s7panel \u{2014} select a set first").as_ptr(),
                        );
                    }
                }

                ID_MINUS_50 | ID_PLUS_50 => {
                    let lv = LISTVIEW_HWND;
                    if let Some(sel) = listview_selection(lv) {
                        let data = ROW_DATA.lock().unwrap();
                        let idx = sel as usize;
                        if idx < data.len() {
                            let row = &data[idx];
                            if row.poll_mode != "single" {
                                SetWindowTextW(
                                    hwnd,
                                    to_utf16(
                                        "s7panel \u{2014} period adj. requires single poll group",
                                    )
                                    .as_ptr(),
                                );
                                drop(data);
                                return 0;
                            }
                            let current = row.period_ms.unwrap_or(1000);
                            let pg = if let Some(pgid) = row.poll_group_id {
                                format!("poll_group_id\":{pgid}")
                            } else {
                                format!(
                                    "poll_group\":\"{}\"",
                                    serde_json::json!(row.poll_group_name)
                                        .as_str()
                                        .unwrap_or(&row.poll_group_name)
                                )
                            };
                            let new_period = if id == ID_MINUS_50 {
                                std::cmp::max(current - 50, 50)
                            } else {
                                current + 50
                            };
                            drop(data);

                            let action_json = format!(
                                r#"{{"action":"poll_group_update_period",{},"period_ms":{}}}"#,
                                pg, new_period
                            );
                            match send_control(&action_json, Duration::from_secs(5)) {
                                Ok(_) => {
                                    SetWindowTextW(
                                        hwnd,
                                        to_utf16(&format!(
                                            "s7panel \u{2014} period set to {}ms",
                                            new_period
                                        ))
                                        .as_ptr(),
                                    );
                                    trigger_delayed_refresh();
                                }
                                Err(e) => {
                                    SetWindowTextW(
                                        hwnd,
                                        to_utf16(&format!("s7panel \u{2014} failed: {e}")).as_ptr(),
                                    );
                                }
                            }
                        } else {
                            drop(data);
                        }
                    } else {
                        SetWindowTextW(
                            hwnd,
                            to_utf16("s7panel \u{2014} select a set first").as_ptr(),
                        );
                    }
                }

                _ => {}
            }
            0
        }

        WM_NOTIFY => {
            let nmhdr = lparam as *const NMHDR;
            if !LISTVIEW_HWND.is_null()
                && (*nmhdr).hwndFrom == LISTVIEW_HWND
                && (*nmhdr).code == NM_CUSTOMDRAW
            {
                let nmlvcd = lparam as *mut NMLVCUSTOMDRAW;
                let stage = (*nmlvcd).nmcd.dwDrawStage;
                if stage == CDDS_PREPAINT {
                    return CDRF_NOTIFYITEMDRAW as isize;
                }
                if stage == CDDS_ITEMPREPAINT {
                    let item_idx = (*nmlvcd).nmcd.dwItemSpec;
                    let data = ROW_DATA.lock().unwrap();
                    if item_idx < data.len() {
                        let row = &data[item_idx];
                        if is_stale(row.last_poll_ts, row.period_ms) {
                            (*nmlvcd).clrText = 0x0032_32FF; // RGB(255, 50, 50)
                        }
                    }
                    return CDRF_DODEFAULT as isize;
                }
                return CDRF_DODEFAULT as isize;
            }
            0
        }

        WM_DESTROY => {
            KillTimer(hwnd, REFRESH_TIMER_ID);
            PostQuitMessage(0);
            0
        }

        _ => DefWindowProcW(hwnd, msg, wparam, lparam),
    }
}

// ---------------------------------------------------------------------------
// URL normalization (mirrors cli::remote::resolve_url)
// ---------------------------------------------------------------------------

fn resolve_url(host_or_url: &str, default_port: u16) -> String {
    let s = host_or_url.trim();
    if s.contains("://") {
        return s.trim_end_matches('/').to_string();
    }
    if let Some((host, port_str)) = s.split_once(':') {
        let port: u16 = port_str.parse().unwrap_or(default_port);
        return format!("http://{}:{}", host, port);
    }
    format!("http://{}:{}", s, default_port)
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let server_raw = if args.len() > 2 && args[1] == "--server" {
        args[2].clone()
    } else {
        "http://127.0.0.1:7707".to_string()
    };

    let server_url = resolve_url(&server_raw, 7707);
    *SERVER_URL.lock().unwrap() = server_url.clone();

    // Initialize common controls for ListView
    unsafe {
        let mut icc: INITCOMMONCONTROLSEX = std::mem::zeroed();
        icc.dwSize = std::mem::size_of::<INITCOMMONCONTROLSEX>() as u32;
        icc.dwICC = ICC_LISTVIEW_CLASSES;
        InitCommonControlsEx(&icc);
    }

    let instance = unsafe { GetModuleHandleW(std::ptr::null()) };

    // Register window class
    let class_utf16 = to_utf16(WINDOW_CLASS);
    let wc = WNDCLASSEXW {
        cbSize: std::mem::size_of::<WNDCLASSEXW>() as u32,
        style: CS_HREDRAW | CS_VREDRAW,
        lpfnWndProc: Some(wnd_proc),
        cbClsExtra: 0,
        cbWndExtra: 0,
        hInstance: instance,
        hIcon: unsafe { LoadIconW(instance, IDI_APPLICATION) },
        hCursor: unsafe { LoadCursorW(std::ptr::null_mut(), IDC_ARROW) },
        hbrBackground: unsafe { GetStockObject(WHITE_BRUSH) } as HBRUSH,
        lpszMenuName: std::ptr::null(),
        lpszClassName: class_utf16.as_ptr(),
        hIconSm: std::ptr::null_mut(),
    };

    if unsafe { RegisterClassExW(&wc) } == 0 {
        eprintln!("s7panel: failed to register window class");
        std::process::exit(1);
    }

    // Create main window
    let hwnd = unsafe {
        CreateWindowExW(
            0,
            to_utf16(WINDOW_CLASS).as_ptr(),
            to_utf16(&format!("s7panel \u{2014} {server_url}")).as_ptr(),
            WS_OVERLAPPEDWINDOW | WS_VISIBLE,
            CW_USEDEFAULT,
            CW_USEDEFAULT,
            WINDOW_WIDTH,
            WINDOW_HEIGHT,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            instance,
            std::ptr::null(),
        )
    };

    if hwnd.is_null() {
        eprintln!("s7panel: failed to create window");
        std::process::exit(1);
    }

    // Message loop
    let mut msg = MSG {
        hwnd: std::ptr::null_mut(),
        message: 0,
        wParam: 0,
        lParam: 0,
        time: 0,
        pt: POINT { x: 0, y: 0 },
    };
    unsafe {
        while GetMessageW(&mut msg, std::ptr::null_mut(), 0, 0) != 0 {
            TranslateMessage(&msg);
            DispatchMessageW(&msg);
        }
    }
}
