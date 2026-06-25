use crate::Error;
use crate::ReadItem;
use crate::ReadResult;
use crate::WriteItem;

pub const S7_WL_BIT: u8 = 0x01;
pub const S7_WL_BYTE: u8 = 0x02;
const TS_RES_BIT: u8 = 0x03;
const TS_RES_BYTE: u8 = 0x04;

// ---------------------------------------------------------------
//  Одиночные запросы
// ---------------------------------------------------------------

pub fn build_read_request(area: u8, db_number: u16, start: u32, size: u16, wordlen: u8) -> Vec<u8> {
    let mut s7_pdu = vec![
        0x32,                       // S7 ID
        0x01,                       // Job Request
        0x00, 0x00,                 // Reserved
        0x00, 0x01,                 // PDU Reference
        0x00, 0x0E,                 // Parameter Length = 14
        0x00, 0x00,                 // Data Length = 0
        0x04,                       // Function: Read Var
        0x01,                       // Item count = 1
        0x12,                       // Variable spec
        0x0A,                       // Length of address spec
        0x10,                       // Syntax ID
        wordlen,                    // Transport size
        (size >> 8) as u8,          // Data length HI
        (size & 0xFF) as u8,        // Data length LO
        (db_number >> 8) as u8,     // DB number HI
        (db_number & 0xFF) as u8,   // DB number LO
        area,                       // Area
        0x00, 0x00, 0x00,           // Start Address (24-bit)
    ];

    // Для байтового доступа адрес умножается на 8, для битового – передаётся как есть
    let address = if wordlen == S7_WL_BYTE {
        start * 8
    } else {
        start
    };

    let len = s7_pdu.len();
    s7_pdu[len - 3] = ((address >> 16) & 0xFF) as u8;
    s7_pdu[len - 2] = ((address >> 8) & 0xFF) as u8;
    s7_pdu[len - 1] = (address & 0xFF) as u8;

    wrap_tpkt_cotp(&s7_pdu)
}

pub fn build_write_request(
    area: u8,
    db_number: u16,
    start: u32,
    data: &[u8],
    wordlen: u8,
) -> Vec<u8> {
    let data_len = data.len() as u16;
    let mut s7_pdu = Vec::new();

    s7_pdu.push(0x32);
    s7_pdu.push(0x01);
    s7_pdu.extend_from_slice(&[0x00, 0x00]);
    s7_pdu.extend_from_slice(&[0x00, 0x01]);
    let param_len_pos = s7_pdu.len();
    s7_pdu.extend_from_slice(&[0x00, 0x00]);      // param len placeholder
    let data_len_pos = s7_pdu.len();
    s7_pdu.extend_from_slice(&[0x00, 0x00]);      // data len placeholder

    s7_pdu.push(0x05);
    s7_pdu.push(0x01);

    s7_pdu.push(0x12);
    s7_pdu.push(0x0A);
    s7_pdu.push(0x10);
    s7_pdu.push(wordlen);
    s7_pdu.extend_from_slice(&data_len.to_be_bytes());
    s7_pdu.extend_from_slice(&db_number.to_be_bytes());
    s7_pdu.push(area);

    let address = if wordlen == S7_WL_BYTE {
        start * 8
    } else {
        start
    };
    s7_pdu.push(((address >> 16) & 0xFF) as u8);
    s7_pdu.push(((address >> 8) & 0xFF) as u8);
    s7_pdu.push((address & 0xFF) as u8);

    let param_len = (s7_pdu.len() - param_len_pos - 2) as u16;
    s7_pdu[param_len_pos] = (param_len >> 8) as u8;
    s7_pdu[param_len_pos + 1] = param_len as u8;

    s7_pdu.push(0x00);
    s7_pdu.push(if wordlen == S7_WL_BIT { TS_RES_BIT } else { TS_RES_BYTE });
    s7_pdu.extend_from_slice(&data_len.to_be_bytes());
    s7_pdu.extend_from_slice(data);

    let total_data_len = (data_len + 4) as u16;
    s7_pdu[data_len_pos] = (total_data_len >> 8) as u8;
    s7_pdu[data_len_pos + 1] = total_data_len as u8;

    wrap_tpkt_cotp(&s7_pdu)
}

// ---------------------------------------------------------------
//  Множественные запросы
// ---------------------------------------------------------------

pub fn build_read_multi_request(items: &[ReadItem]) -> Vec<u8> {
    if items.is_empty() {
        return Vec::new();
    }

    // Для одного элемента возвращаем точно такой же пакет, как build_read_request
    if items.len() == 1 {
        let item = &items[0];
        return build_read_request(item.area, item.db_number, item.start, item.size, S7_WL_BYTE);
    }

    // Общий случай: несколько элементов
    let mut s7_pdu = vec![
        0x32, 0x01, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x00,                         // param length (placeholder)
        0x00, 0x00,                         // data length = 0
    ];

    let param_start = s7_pdu.len();
    s7_pdu.push(0x04);             // Function: Read Var
    s7_pdu.push(items.len() as u8);

    for item in items {
        s7_pdu.push(0x12);
        s7_pdu.push(0x0A);
        s7_pdu.push(0x10);
        s7_pdu.push(S7_WL_BYTE);                     // Transport size
        s7_pdu.push((item.size >> 8) as u8);         // Length HI
        s7_pdu.push(item.size as u8);                // Length LO
        s7_pdu.push((item.db_number >> 8) as u8);    // DB number HI
        s7_pdu.push(item.db_number as u8);           // DB number LO
        s7_pdu.push(item.area);
        // Для DB адрес в байтах умножается на 8 (как в байтовом доступе)
        let addr_bits = if item.area == 0x84 { item.start * 8 } else { item.start };
        s7_pdu.push(((addr_bits >> 16) & 0xFF) as u8);
        s7_pdu.push(((addr_bits >> 8) & 0xFF) as u8);
        s7_pdu.push((addr_bits & 0xFF) as u8);
    }

    let param_len = (s7_pdu.len() - param_start) as u16;
    s7_pdu[6] = (param_len >> 8) as u8;
    s7_pdu[7] = param_len as u8;

    wrap_tpkt_cotp(&s7_pdu)
}

pub fn build_write_multi_request(items: &[WriteItem]) -> Vec<u8> {
    let mut s7_pdu = vec![
        0x32, 0x01, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x00, 0x00,
    ];
    let param_len_pos = 6;
    s7_pdu.push(0x05);
    s7_pdu.push(items.len() as u8);

    let mut data_section = Vec::new();
    for item in items {
        s7_pdu.push(0x12);
        s7_pdu.push(0x0A);
        s7_pdu.push(0x10);
        s7_pdu.push(S7_WL_BYTE);
        let len = item.data.len() as u16;
        s7_pdu.extend_from_slice(&len.to_be_bytes());
        s7_pdu.extend_from_slice(&item.db_number.to_be_bytes());
        s7_pdu.push(item.area);
        let addr = if item.area == 0x84 { item.start * 8 } else { item.start };
        s7_pdu.push(((addr >> 16) & 0xFF) as u8);
        s7_pdu.push(((addr >> 8) & 0xFF) as u8);
        s7_pdu.push((addr & 0xFF) as u8);

        data_section.push(0x00);
        data_section.push(0x04);
        data_section.extend_from_slice(&len.to_be_bytes());
        data_section.extend_from_slice(&item.data);
    }

    let param_len = (s7_pdu.len() - param_len_pos - 2) as u16;
    s7_pdu[param_len_pos] = (param_len >> 8) as u8;
    s7_pdu[param_len_pos + 1] = param_len as u8;

    let data_len = data_section.len() as u16;
    s7_pdu[param_len_pos + 2] = (data_len >> 8) as u8;
    s7_pdu[param_len_pos + 3] = data_len as u8;

    s7_pdu.extend_from_slice(&data_section);
    wrap_tpkt_cotp(&s7_pdu)
}

// ---------------------------------------------------------------
//  Парсеры
// ---------------------------------------------------------------

pub fn parse_read_response(response: &[u8]) -> Result<Vec<u8>, Error> {
    if response.len() < 12 {
        return Err(Error::Protocol(format!("Response too short: {} bytes", response.len())));
    }
    if response[0] != 0x32 || response[1] != 0x03 {
        return Err(Error::Protocol("Invalid S7 response header".to_string()));
    }
    let error_code = u16::from_be_bytes([response[10], response[11]]);
    if error_code != 0 {
        return Err(Error::PlcError {
            code: error_code,
            message: format!("0x{:04X}", error_code),
        });
    }
    let param_length = u16::from_be_bytes([response[6], response[7]]) as usize;
    let data_length = u16::from_be_bytes([response[8], response[9]]) as usize;

    if data_length < 4 {
        return Ok(vec![]);
    }

    let data_start = 12 + param_length;

    if response.len() < data_start + data_length {
        return Err(Error::Protocol("Data payload truncated".to_string()));
    }

    let return_code = response[data_start];
    let _transport_size = response[data_start + 1];
    let item_data_len = u16::from_be_bytes([response[data_start + 2], response[data_start + 3]]) as usize;

    let actual_payload_len = data_length.saturating_sub(4);

    if return_code != 0xFF {
        return Err(Error::PlcError {
            code: return_code as u16,
            message: format!("Item error: 0x{:02X}", return_code),
        });
    }

    let payload_len = actual_payload_len.min(item_data_len);
    let payload_offset = data_start + 4;

    if payload_len == 0 {
        return Ok(vec![]);
    }

    Ok(response[payload_offset..payload_offset + payload_len].to_vec())
}

pub fn parse_write_response(response: &[u8]) -> Result<(), Error> {
    if response.len() < 12 {
        return Err(Error::Protocol("Write response too short".into()));
    }
    if response[0] != 0x32 || (response[1] != 0x02 && response[1] != 0x03) {
        return Err(Error::Protocol("Invalid S7 write response header".into()));
    }
    let error_code = u16::from_be_bytes([response[10], response[11]]);
    if error_code != 0 {
        return Err(Error::PlcError {
            code: error_code,
            message: format!("PLC write error: 0x{:04X}", error_code),
        });
    }
    Ok(())
}

pub fn parse_read_multi_response(response: &[u8]) -> Result<Vec<ReadResult>, String> {
    if response.len() < 14 {
        return Err("Response too short".into());
    }
    if response[0] != 0x32 || response[1] != 0x03 {
        return Err("Invalid S7 response header".into());
    }

    let global_error = u16::from_be_bytes([response[10], response[11]]);
    if global_error != 0 {
        return Err(format!("Global S7 error: 0x{:04X}", global_error));
    }

    let param_length = u16::from_be_bytes([response[6], response[7]]) as usize;
    let data_length = u16::from_be_bytes([response[8], response[9]]) as usize;

    let mut pos = 12 + param_length;
    let end_data = pos + data_length;
    let mut results = Vec::new();

    while pos + 4 <= response.len() && pos < end_data {
        let return_code = response[pos];
        let _transport_size = response[pos + 1];
        let length = u16::from_be_bytes([response[pos + 2], response[pos + 3]]) as usize;
        pos += 4;

        let success = return_code == 0xFF;
        let data = if success && pos + length <= response.len() {
            let d = response[pos..pos + length].to_vec();
            pos += length;
            d
        } else {
            if success {
                pos += length;
            }
            vec![]
        };

        results.push(ReadResult {
            data,
            success,
            error_code: if success { None } else { Some(return_code) },
        });
    }

    Ok(results)
}

// ---------------------------------------------------------------
//  Вспомогательная функция
// ---------------------------------------------------------------

fn wrap_tpkt_cotp(s7_pdu: &[u8]) -> Vec<u8> {
    let tpkt_len = 4 + 3 + s7_pdu.len();
    let mut full = vec![
        0x03, 0x00,
        (tpkt_len >> 8) as u8,
        tpkt_len as u8,
        0x02, 0xF0, 0x80,
    ];
    full.extend_from_slice(s7_pdu);
    full
}