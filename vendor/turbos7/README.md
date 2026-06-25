# turbos7
#### v0.2.1

Асинхронная библиотека на Rust для взаимодействия с Siemens S7‑PLC (S7‑300, S7‑400, S7‑1200, S7‑1500) по протоколу ISO‑on‑TCP. Основана на идеях и протокольной части `rust7`, но с полным переходом на `tokio` для использования в современных асинхронных приложениях.

**Возможности:**

- Полностью асинхронный ввод‑вывод на базе `tokio`
- Автоматическое разбиение больших запросов на чанки с учётом согласованного PDU
- Настраиваемые таймауты подключения, чтения и записи
- Поддержка всех типов данных S7: `BOOL`, `BYTE`, `WORD`, `INT`, `DWORD`, `REAL` и др.
- Нативное чтение отдельных битов (запись – безопасным read‑modify‑write)
- Простой API для массовых операций `read_multiple` / `write_multiple`
- read_multiple объединяет до 19 независимых переменных в один запрос
- Дружелюбная обработка ошибок с кодами ПЛК
- Подходит для использования в `spawn_blocking`‑совместимых сервисах (веб‑сервера, SCADA, IoT)

**Ограничения:**

- Требует включённой опции "Permit access with PUT/GET communication" в настройках CPU
- Не поддерживает оптимизированные блоки данных (DB с атрибутом "Optimized block access")
- Для работы с S7‑1200/1500 рекомендуется использовать `connect_s71200_1500()`

---

## Установка

Добавьте в `Cargo.toml`:

```toml
[dependencies]
turbos7 = { git = "https://github.com/ghouldevourer/turbos7" }
tokio = { version = "1", features = ["full"] }
```
## Быстрый старт

```rust
use turbos7::S7Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Подключение к S7-300 (Rack 0, Slot 2, тип PG)
    let mut client = S7Client::connect_s7300("172.20.0.100:102", 0x0001).await?;

    // Чтение DB7001.DBW0
    let value = client.read_u16(0x84, 7001, 0).await?;
    println!("DB7001.DBW0 = {}", value);

    // Чтение бита DB7001.DBX0.0
    let bit = client.read_bit(0x84, 7001, 0, 0).await?;
    println!("DB7001.DBX0.0 = {}", bit);

    Ok(())
}
```

## Подключение
Библиотека предоставляет несколько методов для разных конфигураций:

```rust
// S7-300 (Rack 0, Slot 2) – классический вариант
let client = S7Client::connect_s7300("192.168.1.10:102", 0x0001).await?;

// S7-1200/1500 (Rack 0, Slot 0)
let client = S7Client::connect_s71200_1500("192.168.1.20:102", 0x0001).await?;

// Полный контроль TSAP (для LOGO!, S7-200, нестандартных конфигураций)
let client = S7Client::connect_tsap(
    "10.0.0.5:102",
    0x0100,   // локальный TSAP
    0x0302,   // удалённый TSAP (например, Rack 0, Slot 2 для OP)
    Some(Duration::from_secs(3)), // таймаут подключения
    Some(Duration::from_secs(1)), // таймаут чтения
    Some(Duration::from_secs(1)), // таймаут записи
).await?;

// Универсальный connect_rack_slot
let client = S7Client::connect_rack_slot(
    "172.20.0.100:102",
    0,     // rack
    2,     // slot
    0x0002,// тип соединения: 0x0001=PG, 0x0002=OP, 0x0003=S7_BASIC
    Some(Duration::from_secs(5)),
    Some(Duration::from_secs(5)),
    Some(Duration::from_secs(5)),
).await?;
```
### Чтение и запись
#### Байтовые типы
```rust
let w: u16 = client.read_u16(0x84, 10, 4).await?;   // DB10.DBW4
let f: f32 = client.read_f32(0x84, 20, 8).await?;   // DB20.DBD8
client.write_u8(0x84, 10, 0, 42).await?;            // DB10.DBB0 ← 42
```
#### Битовый доступ
```rust
let b = client.read_bit(0x84, 5, 10, 3).await?;     // DB5.DBX10.3
client.write_bit(0x84, 5, 10, 3, true).await?;      // установить бит
```
Запись бита реализована как read‑modify‑write, поэтому безопасна для соседних битов.

#### Массовые операции (до 19 переменных в одном запросе)
```rust
use turbos7::{ReadItem, WriteItem};

let items = vec![
    ReadItem { area: 0x84, db_number: 1, start: 0, size: 2 },
    ReadItem { area: 0x84, db_number: 2, start: 10, size: 4 },
];

let results = client.read_multiple(&items).await?;
for r in results {
    println!("success={}, data={:?}", r.success, r.data);
}
```

### Обработка ошибок
#### Все методы возвращают Result<T, turbos7::Error>, который содержит:

    Io – ошибка ввода‑вывода
    Connection – ошибка на уровне TCP или ISO‑рукопожатия
    Protocol – некорректный ответ ПЛК
    PlcError { code, message } – ошибка, возвращённая самим ПЛК (например, неверный адрес)
    Timeout – таймаут операции
    InvalidData – неверные параметры вызова

#### Пример обработки:

```rust
match client.read_u16(0x84, 99, 0).await {
    Ok(v) => println!("OK: {}", v),
    Err(turbos7::Error::PlcError { code, .. }) if code == 0x05 => {
        eprintln!("Адрес не существует или блок оптимизирован");
    }
    Err(e) => eprintln!("Другая ошибка: {}", e),
}
```
### Принципы работы
    - Подключение начинается с TCP‑соединения и сразу отключает алгоритм Нейгла (TCP_NODELAY).
    - Выполняется ISO‑on‑TCP рукопожатие (COTP CR/CC) с указанием TSAP.
    - Согласование размера PDU (S7 Setup Communication), после чего все запросы и ответы укладываются в согласованный лимит.
    - Чанкинг больших блоков выполняется автоматически, прозрачно для пользователя.
    - read_multiple формирует один S7‑запрос с несколькими переменными, сокращая число сетевых обменов.

### Лицензия
    Данный проект распространяется под двойной лицензией MIT и Apache-2.0.
    Вы можете выбрать любую из них.

### Благодарности
    
    rust7 – оригинальная синхронная библиотека, на основе которой создавался turbos7.

    Сообществу Rust и Siemens за документацию по S7‑протоколу.