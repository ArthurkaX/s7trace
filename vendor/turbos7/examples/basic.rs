use turbos7::S7Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Подключение как PG (0x0001) к S7-300 (Rack 0, Slot 2)
    let mut client = S7Client::connect_s7300("172.20.0.100:102", 0x0001).await?;
    println!("✅ Connected, PDU = {}", client.pdu_size());

    // Чтение WORD'ов
    let tests = vec![
        (7001, 0, "DB7001.DBW0"),
        (7001, 100, "DB7001.DBW100"),
        (9, 0, "DB9.DBW0"),
    ];

    for (db, offset, name) in &tests {
        match client.read_u16(0x84, *db, *offset as u32).await {
            Ok(val) => println!("✅ {} = {}", name, val),
            Err(e) => println!("❌ {} = {:?}", name, e),
        }
    }

    // Чтение бита (например, DB7001.DBX0.0)
    match client.read_bit(0x84, 7001, 0, 0).await {
        Ok(bit) => println!("✅ DB7001.DBX0.0 = {}", bit),
        Err(e) => println!("❌ Bit read error: {:?}", e),
    }

    // Запись бита (например, установить DB7001.DBX120.0 в true)
    //client.write_bit(0x84, 7001, 120, 0, true).await?;
    //println!("✅ Written DB7001.DBX120.0 = true");

    // Проверка
    let bit = client.read_bit(0x84, 7001, 120, 0).await?;
    println!("✅ Read back: DB7001.DBX120.0 = {}", bit);

    Ok(())
}