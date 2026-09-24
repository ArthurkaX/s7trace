use turbos7::{S7Client, ReadItem};
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = S7Client::connect_s7300("172.20.0.100:102", 0x0001).await?;
    println!("✅ Connected, PDU = {}", client.pdu_size());

    // Только один элемент
    let items = vec![
        ReadItem { area: 0x84, db_number: 7001, start: 0, size: 2 },
    ];

    let start = Instant::now();
    let results = client.read_multiple(&items).await?;
    let elapsed = start.elapsed();

    println!("Read {} items in {:?}", results.len(), elapsed);
    for (i, res) in results.iter().enumerate() {
        if res.success {
            println!("  Item {}: success, data = {:02X?}", i, res.data);
        } else {
            println!("  Item {}: FAILED, error_code = {:02X?}", i, res.error_code);
        }
    }

    Ok(())
}