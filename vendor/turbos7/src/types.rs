/// Описание одной переменной для batch-чтения
#[derive(Clone, Debug)]
pub struct ReadItem {
    pub area: u8,
    pub db_number: u16,
    pub start: u32,
    pub size: u16,
}

/// Результат чтения одной переменной при batch-чтении
#[derive(Clone, Debug)]
pub struct ReadResult {
    pub data: Vec<u8>,
    pub success: bool,
    pub error_code: Option<u8>,
}

/// Описание одной переменной для batch-записи
#[derive(Clone, Debug)]
pub struct WriteItem {
    pub area: u8,
    pub db_number: u16,
    pub start: u32,
    pub data: Vec<u8>,
}