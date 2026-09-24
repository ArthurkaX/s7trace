use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{timeout, Duration};
use crate::Result;

#[derive(Debug, Clone)]
pub struct SessionParams {
    pub pdu_size: u16,
}

pub struct Connection {
    stream: TcpStream,
    pub params: SessionParams,
}

impl Connection {
    pub async fn connect(
        address: &str,
        local_tsap: u16,
        remote_tsap: u16,
        connect_timeout: Option<Duration>,
    ) -> Result<Self> {
        let stream = if let Some(t) = connect_timeout {
            timeout(t, TcpStream::connect(address))
                .await
                .map_err(|_| crate::Error::Timeout)??
        } else {
            TcpStream::connect(address).await?
        };
        stream.set_nodelay(true)?;

        let mut conn = Connection {
            stream,
            params: SessionParams { pdu_size: 480 },
        };

        conn.cotp_handshake(local_tsap, remote_tsap, connect_timeout).await?;
        conn.s7_setup_communication(connect_timeout).await?;

        Ok(conn)
    }

    pub async fn connect_rack_slot(
        address: &str,
        rack: u16,
        slot: u16,
        conn_type: u16,
        connect_timeout: Option<Duration>,
    ) -> Result<Self> {
        let local_tsap: u16 = 0x0100;
        let remote_tsap: u16 = (conn_type << 8) + (rack * 0x20) + slot;
        Self::connect(address, local_tsap, remote_tsap, connect_timeout).await
    }

    async fn cotp_handshake(
        &mut self,
        local_tsap: u16,
        remote_tsap: u16,
        timeout_dur: Option<Duration>,
    ) -> Result<()> {
        let cr = vec![
            0x03, 0x00, 0x00, 0x16,
            0x11, 0xe0, 0x00, 0x00, 0x00, 0x01, 0x00,
            0xc0, 0x01, 0x0a,
            0xc1, 0x02, (local_tsap >> 8) as u8, local_tsap as u8,
            0xc2, 0x02, (remote_tsap >> 8) as u8, remote_tsap as u8,
        ];

        write_with_timeout(&mut self.stream, &cr, timeout_dur).await?;

        let mut buf = [0u8; 256];
        let n = read_with_timeout(&mut self.stream, &mut buf, timeout_dur).await?;

        if n == 0 || n < 7 || buf[0] != 0x03 || buf[5] != 0xd0 {
            return Err(crate::Error::Connection("COTP handshake failed".into()));
        }
        Ok(())
    }

    async fn s7_setup_communication(&mut self, timeout_dur: Option<Duration>) -> Result<()> {
        let setup = vec![
            0x03, 0x00, 0x00, 0x19,
            0x02, 0xf0, 0x80,
            0x32, 0x01, 0x00, 0x00, 0x00, 0x01,
            0x00, 0x08, 0x00, 0x00,
            0xf0, 0x00, 0x00, 0x01, 0x00, 0x01,
            0x01, 0xe0,
        ];

        write_with_timeout(&mut self.stream, &setup, timeout_dur).await?;

        let mut resp = [0u8; 27];
        read_exact_with_timeout(&mut self.stream, &mut resp, timeout_dur).await?;

        if resp[0] != 0x03 || resp[7] != 0x32 || resp[17] != 0x00 {
            return Err(crate::Error::Protocol("S7 setup response malformed".into()));
        }

        let pdu_size = u16::from_be_bytes([resp[25], resp[26]]);
        if pdu_size == 0 {
            return Err(crate::Error::Protocol("Negotiated PDU size is zero".into()));
        }
        self.params.pdu_size = pdu_size;
        Ok(())
    }

    /// Отправить данные (с таймаутом, если задан)
    pub async fn write(&mut self, data: &[u8], timeout_dur: Option<Duration>) -> Result<()> {
        write_with_timeout(&mut self.stream, data, timeout_dur).await
    }

    /// Прочитать TPKT-пакет и вернуть S7 PDU (с таймаутом)
    pub async fn read_tpkt(&mut self, timeout_dur: Option<Duration>) -> Result<Vec<u8>> {
        let mut header = [0u8; 4];
        read_exact_with_timeout(&mut self.stream, &mut header, timeout_dur).await?;

        let tpkt_len = u16::from_be_bytes([header[2], header[3]]) as usize;
        if tpkt_len < 7 {
            return Err(crate::Error::Protocol("TPKT too short".into()));
        }

        let payload_len = tpkt_len - 4;
        let mut full = vec![0u8; payload_len];
        read_exact_with_timeout(&mut self.stream, &mut full, timeout_dur).await?;

        if full.len() < 3 || full[0] != 0x02 || full[1] != 0xf0 || full[2] != 0x80 {
            return Err(crate::Error::Protocol("Invalid COTP DT".into()));
        }
        Ok(full[3..].to_vec())
    }
}

// ---- Вспомогательные функции для таймаутов ----
async fn write_with_timeout(
    stream: &mut (impl AsyncWriteExt + Unpin),
    data: &[u8],
    dur: Option<Duration>,
) -> Result<()> {
    if let Some(d) = dur {
        timeout(d, stream.write_all(data))
            .await
            .map_err(|_| crate::Error::Timeout)??;
    } else {
        stream.write_all(data).await?;
    }
    Ok(())
}

async fn read_with_timeout(
    stream: &mut (impl AsyncReadExt + Unpin),
    buf: &mut [u8],
    dur: Option<Duration>,
) -> Result<usize> {
    if let Some(d) = dur {
        let n = timeout(d, stream.read(buf))
            .await
            .map_err(|_| crate::Error::Timeout)?
            .map_err(crate::Error::from)?;
        Ok(n)
    } else {
        Ok(stream.read(buf).await?)
    }
}

async fn read_exact_with_timeout(
    stream: &mut (impl AsyncReadExt + Unpin),
    buf: &mut [u8],
    dur: Option<Duration>,
) -> Result<()> {
    if let Some(d) = dur {
        timeout(d, stream.read_exact(buf))
            .await
            .map_err(|_| crate::Error::Timeout)?
            .map_err(crate::Error::from)?;
    } else {
        stream.read_exact(buf).await?;
    }
    Ok(())
}