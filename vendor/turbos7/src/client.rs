use crate::connection::Connection;
use crate::protocol::s7;
use crate::Result;
use crate::ReadItem;
use crate::ReadResult;
use crate::WriteItem;
use std::time::Duration;
use crate::Error;

pub struct S7Client {
    connection: Connection,
    /// Таймаут на чтение ответа (по умолчанию 5 с)
    read_timeout: Option<Duration>,
    /// Таймаут на отправку (по умолчанию 5 с)
    write_timeout: Option<Duration>,
}

impl S7Client {
    // ---------------------------------------------------------------
    //  Конструкторы
    // ---------------------------------------------------------------

    /// Подключение с явным указанием TSAP и таймаутами
    pub async fn connect_tsap(
        address: &str,
        local_tsap: u16,
        remote_tsap: u16,
        connect_timeout: Option<Duration>,
        read_timeout: Option<Duration>,
        write_timeout: Option<Duration>,
    ) -> Result<Self> {
        let connection =
            Connection::connect(address, local_tsap, remote_tsap, connect_timeout).await?;
        Ok(S7Client {
            connection,
            read_timeout,
            write_timeout,
        })
    }

    /// Подключение по Rack/Slot с выбором типа соединения (PG/OP/S7)
    pub async fn connect_rack_slot(
        address: &str,
        rack: u16,
        slot: u16,
        conn_type: u16,
        connect_timeout: Option<Duration>,
        read_timeout: Option<Duration>,
        write_timeout: Option<Duration>,
    ) -> Result<Self> {
        let connection =
            Connection::connect_rack_slot(address, rack, slot, conn_type, connect_timeout).await?;
        Ok(S7Client {
            connection,
            read_timeout,
            write_timeout,
        })
    }

    /// Стандартное подключение к S7-300 (Rack 0, Slot 2, тип PG)
    pub async fn connect(address: &str) -> Result<Self> {
        Self::connect_rack_slot(address, 0, 2, 0x0001, Some(Duration::from_secs(5)), Some(Duration::from_secs(5)), Some(Duration::from_secs(5))).await
    }

    /// Хелпер: S7-300 (Rack 0, Slot 2)
    pub async fn connect_s7300(address: &str, conn_type: u16) -> Result<Self> {
        Self::connect_rack_slot(address, 0, 2, conn_type, Some(Duration::from_secs(5)), Some(Duration::from_secs(5)), Some(Duration::from_secs(5))).await
    }

    /// Хелпер: S7-1200/1500 (Rack 0, Slot 0)
    pub async fn connect_s71200_1500(address: &str, conn_type: u16) -> Result<Self> {
        Self::connect_rack_slot(address, 0, 0, conn_type, Some(Duration::from_secs(5)), Some(Duration::from_secs(5)), Some(Duration::from_secs(5))).await
    }

    /// Получить согласованный PDU
    pub fn pdu_size(&self) -> u16 {
        self.connection.params.pdu_size
    }

    // ---------------------------------------------------------------
    //  Низкоуровневые методы отправки/приёма
    // ---------------------------------------------------------------

    async fn send(&mut self, data: &[u8]) -> Result<()> {
        self.connection.write(data, self.write_timeout).await
    }

    async fn receive_s7(&mut self) -> Result<Vec<u8>> {
        self.connection.read_tpkt(self.read_timeout).await
    }

    // ---------------------------------------------------------------
    //  Чтение байтовых областей (с чанкингом)
    // ---------------------------------------------------------------

    pub async fn read_area(
        &mut self,
        area: u8,
        db_number: u16,
        start: u32,
        size: u16,
    ) -> Result<Vec<u8>> {
        self.read_area_wordlen(area, db_number, start, size, s7::S7_WL_BYTE).await
    }

    async fn read_area_wordlen(
        &mut self,
        area: u8,
        db_number: u16,
        start: u32,
        mut size: u16,
        wordlen: u8,
    ) -> Result<Vec<u8>> {
        let pdu = self.pdu_size();
        let max_chunk = if wordlen == s7::S7_WL_BYTE {
            pdu.saturating_sub(18) // 18 – оверхед ответа
        } else {
            1 // битовый запрос всегда 1 элемент
        } as u16;

        let mut full_data = Vec::new();
        let mut offset = 0u32;

        while size > 0 {
            let chunk = size.min(max_chunk);
            let request = s7::build_read_request(area, db_number, start + offset, chunk, wordlen);
            self.send(&request).await?;
            let pdu = self.receive_s7().await?;
            let mut data = s7::parse_read_response(&pdu)?;
            full_data.append(&mut data);

            size -= chunk;
            offset += chunk as u32;
        }

        Ok(full_data)
    }

    // ---------------------------------------------------------------
    //  Запись байтовых областей (с чанкингом)
    // ---------------------------------------------------------------

    pub async fn write_area(
        &mut self,
        area: u8,
        db_number: u16,
        start: u32,
        data: &[u8],
    ) -> Result<()> {
        self.write_area_wordlen(area, db_number, start, data, s7::S7_WL_BYTE).await
    }

    async fn write_area_wordlen(
        &mut self,
        area: u8,
        db_number: u16,
        start: u32,
        data: &[u8],
        wordlen: u8,
    ) -> Result<()> {
        let pdu = self.pdu_size();
        let max_chunk = if wordlen == s7::S7_WL_BYTE {
            pdu.saturating_sub(28) // 28 – оверхед запроса
        } else {
            1
        } as usize;

        let mut offset = 0;
        while offset < data.len() {
            let chunk_end = (offset + max_chunk).min(data.len());
            let chunk = &data[offset..chunk_end];
            let request = s7::build_write_request(
                area,
                db_number,
                start + offset as u32,
                chunk,
                wordlen,
            );
            self.send(&request).await?;
            let pdu = self.receive_s7().await?;
            s7::parse_write_response(&pdu)?;
            offset = chunk_end;
        }
        Ok(())
    }

    // ---------------------------------------------------------------
    //  Типизированные методы (байтовые)
    // ---------------------------------------------------------------

    pub async fn read_u16(&mut self, area: u8, db: u16, start: u32) -> Result<u16> {
        let data = self.read_area(area, db, start, 2).await?;
        Ok(u16::from_be_bytes([data[0], data[1]]))
    }

    pub async fn read_i16(&mut self, area: u8, db: u16, start: u32) -> Result<i16> {
        let data = self.read_area(area, db, start, 2).await?;
        Ok(i16::from_be_bytes([data[0], data[1]]))
    }

    pub async fn read_u32(&mut self, area: u8, db: u16, start: u32) -> Result<u32> {
        let data = self.read_area(area, db, start, 4).await?;
        Ok(u32::from_be_bytes([data[0], data[1], data[2], data[3]]))
    }

    pub async fn read_f32(&mut self, area: u8, db: u16, start: u32) -> Result<f32> {
        let data = self.read_area(area, db, start, 4).await?;
        Ok(f32::from_be_bytes([data[0], data[1], data[2], data[3]]))
    }

    pub async fn read_u8(&mut self, area: u8, db: u16, start: u32) -> Result<u8> {
        let data = self.read_area(area, db, start, 1).await?;
        Ok(data[0])
    }

    pub async fn read_i8(&mut self, area: u8, db: u16, start: u32) -> Result<i8> {
        let data = self.read_area(area, db, start, 1).await?;
        Ok(data[0] as i8)
    }

    pub async fn read_u64(&mut self, area: u8, db: u16, start: u32) -> Result<u64> {
        let data = self.read_area(area, db, start, 8).await?;
        Ok(u64::from_be_bytes([data[0],data[1],data[2],data[3],data[4],data[5],data[6],data[7]]))
    }

    pub async fn read_i64(&mut self, area: u8, db: u16, start: u32) -> Result<i64> {
        let data = self.read_area(area, db, start, 8).await?;
        Ok(i64::from_be_bytes([data[0],data[1],data[2],data[3],data[4],data[5],data[6],data[7]]))
    }

    pub async fn read_f64(&mut self, area: u8, db: u16, start: u32) -> Result<f64> {
        let data = self.read_area(area, db, start, 8).await?;
        Ok(f64::from_be_bytes([data[0],data[1],data[2],data[3],data[4],data[5],data[6],data[7]]))
    }

    // ---------------------------------------------------------------
    //  Битовый доступ (нативный S7-бит)
    // ---------------------------------------------------------------

    pub async fn read_bit(
        &mut self,
        area: u8,
        db_number: u16,
        byte_num: u16,
        bit_idx: u8,
    ) -> Result<bool> {
        if bit_idx > 7 {
            return Err(crate::Error::InvalidData);
        }
        let start_bit = byte_num as u32 * 8 + bit_idx as u32;
        let data = self
            .read_area_wordlen(area, db_number, start_bit, 1, s7::S7_WL_BIT)
            .await?;
        Ok(data[0] != 0)
    }

    pub async fn write_bit(
        &mut self,
        area: u8,
        db_number: u16,
        byte_num: u16,
        bit_idx: u8,
        value: bool,
    ) -> Result<()> {
        if bit_idx > 7 {
            return Err(crate::Error::InvalidData);
        }
        let mut byte = self.read_u8(area, db_number, byte_num as u32).await?;
        if value {
            byte |= 1 << bit_idx;
        } else {
            byte &= !(1 << bit_idx);
        }
        self.write_u8(area, db_number, byte_num as u32, byte).await
    }

    // ---------------------------------------------------------------
    //  Запись байтовых типов
    // ---------------------------------------------------------------

    pub async fn write_u8(&mut self, area: u8, db: u16, start: u32, value: u8) -> Result<()> {
        self.write_area(area, db, start, &[value]).await
    }

    pub async fn write_u16(&mut self, area: u8, db: u16, start: u32, value: u16) -> Result<()> {
        self.write_area(area, db, start, &value.to_be_bytes()).await
    }

    pub async fn write_i16(&mut self, area: u8, db: u16, start: u32, value: i16) -> Result<()> {
        self.write_area(area, db, start, &value.to_be_bytes()).await
    }

    pub async fn write_u32(&mut self, area: u8, db: u16, start: u32, value: u32) -> Result<()> {
        self.write_area(area, db, start, &value.to_be_bytes()).await
    }

    pub async fn write_f32(&mut self, area: u8, db: u16, start: u32, value: f32) -> Result<()> {
        self.write_area(area, db, start, &value.to_be_bytes()).await
    }

    // ---------------------------------------------------------------
    //  Массовые операции (пока без чанкинга, но можно добавить)
    // ---------------------------------------------------------------

    pub async fn read_multiple(&mut self, items: &[ReadItem]) -> Result<Vec<ReadResult>> {
        if items.is_empty() {
            return Ok(vec![]);
        }
        let request = s7::build_read_multi_request(items);
        self.send(&request).await?;
        let pdu = self.receive_s7().await?;

        // Для одного элемента ответ такой же, как у read_area – используем одиночный парсер
        if items.len() == 1 {
            let data = s7::parse_read_response(&pdu)?;
            return Ok(vec![ReadResult {
                data,
                success: true,
                error_code: None,
            }]);
        }

        s7::parse_read_multi_response(&pdu).map_err(Error::Protocol)
    }

    pub async fn write_multiple(&mut self, items: &[WriteItem]) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let request = s7::build_write_multi_request(items);
        self.send(&request).await?;
        let pdu = self.receive_s7().await?;
        s7::parse_write_response(&pdu)
    }

    /// Отправляет несколько запросов подряд, а затем читает ответы в том же порядке.
    /// Возвращает результаты в порядке запросов.
    pub async fn read_multiple_pipelined(
        &mut self,
        requests: &[Vec<u8>],
    ) -> Result<Vec<Result<Vec<u8>>>> {
        println!("[PIPE] Sending {} requests", requests.len());
        for (i, req) in requests.iter().enumerate() {
            println!("[PIPE] Sending request {}", i);
            if let Err(e) = self.send(req).await {
                println!("[PIPE] Error sending request {}: {:?}", i, e);
                return Err(e);
            }
        }
        println!("[PIPE] All requests sent, reading responses...");

        let mut results = Vec::with_capacity(requests.len());
        for i in 0..requests.len() {
            println!("[PIPE] Reading response {}", i);
            match self.receive_s7().await {
                Ok(pdu) => {
                    println!("[PIPE] Response {} received, parsing...", i);
                    results.push(s7::parse_read_response(&pdu));
                }
                Err(e) => {
                    println!("[PIPE] Error reading response {}: {:?}", i, e);
                    return Err(e);
                }
            }
        }
        Ok(results)
    }

}