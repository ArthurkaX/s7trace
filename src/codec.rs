//! Wire codec for S7 values.
//!
//! Pure conversions between raw big-endian PLC bytes and typed values. This is
//! a leaf module: it depends only on `types`, so both the `db` layer (tag
//! validation) and the `engine` layer (poller, server reads) can use it
//! without `db` having to reach up into `engine`.

use crate::types::DataType;

/// Decode raw bytes into a typed value.
///
/// `bit_offset` is used for `DataType::Bool` to extract a single bit.
/// When `None`, any non‑zero byte value is treated as `true`.
pub fn decode_value(
    data: &[u8],
    data_type: DataType,
    bit_offset: Option<u8>,
) -> (Option<i64>, Option<f64>) {
    match data_type {
        DataType::Bool => {
            if data.is_empty() {
                (None, None)
            } else {
                let val = match bit_offset {
                    Some(bit) if bit < 8 => ((data[0] >> bit) & 1) as i64,
                    _ => (data[0] != 0) as i64,
                };
                (Some(val), None)
            }
        }
        DataType::I16 => {
            if data.len() < 2 {
                (None, None)
            } else {
                let val = i16::from_be_bytes([data[0], data[1]]);
                (Some(val as i64), None)
            }
        }
        DataType::U16 => {
            if data.len() < 2 {
                (None, None)
            } else {
                let val = u16::from_be_bytes([data[0], data[1]]);
                (Some(val as i64), None)
            }
        }
        DataType::I32 => {
            if data.len() < 4 {
                (None, None)
            } else {
                let val = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                (Some(val as i64), None)
            }
        }
        DataType::U32 => {
            if data.len() < 4 {
                (None, None)
            } else {
                let val = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                (Some(val as i64), None)
            }
        }
        DataType::F32 => {
            if data.len() < 4 {
                (None, None)
            } else {
                let val = f32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                (None, Some(val as f64))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_i16() {
        let (vi, vf) = decode_value(&[0x00, 0x2A], DataType::I16, None);
        assert_eq!(vi, Some(42));
        assert!(vf.is_none());
    }

    #[test]
    fn test_decode_negative_i16() {
        let (vi, vf) = decode_value(&[0xFF, 0xCE], DataType::I16, None);
        assert_eq!(vi, Some(-50));
        assert!(vf.is_none());
    }

    #[test]
    fn test_decode_f32() {
        let (vi, vf) = decode_value(&[0x42, 0x48, 0x00, 0x00], DataType::F32, None);
        assert!(vi.is_none());
        assert!(vf.is_some());
        let v = vf.unwrap();
        assert!((v - 50.0).abs() < 0.01);
    }

    #[test]
    fn test_decode_u16() {
        let (vi, vf) = decode_value(&[0xEA, 0x60], DataType::U16, None);
        assert_eq!(vi, Some(60000));
        assert!(vf.is_none());
    }

    #[test]
    fn test_decode_bool_true() {
        let (vi, _vf) = decode_value(&[0x01], DataType::Bool, None);
        assert_eq!(vi, Some(1));
    }

    #[test]
    fn test_decode_bool_false() {
        let (vi, _vf) = decode_value(&[0x00], DataType::Bool, None);
        assert_eq!(vi, Some(0));
    }

    #[test]
    fn test_decode_bool_bit() {
        // Byte 0b01000000, bit 6 -> true, bit 5 -> false
        let (vi, _vf) = decode_value(&[0b01000000], DataType::Bool, Some(6));
        assert_eq!(vi, Some(1));
        let (vi, _vf) = decode_value(&[0b01000000], DataType::Bool, Some(5));
        assert_eq!(vi, Some(0));
    }
}
