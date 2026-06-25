use turbos7::{S7Client, s7, ReadItem};
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = S7Client::connect_s7300("172.20.0.100:102", 0x0001).await?;
    println!("✅ Connected, PDU = {}", client.pdu_size());

    let items = vec![ReadItem { area: 0x84, db_number: 7001, start: 0, size: 2 }];
    let request = s7::build_read_multi_request(&items);
    let requests = vec![request];

    let start = Instant::now();
    let results = client.read_multiple_pipelined(&requests).await?;
    let elapsed = start.elapsed();

    println!("Single request via pipelined in {:?}", elapsed);
    for (i, res) in results.iter().enumerate() {
        match res {
            Ok(data) => println!("  Success, data = {:02X?}", data),
            Err(e) => println!("  FAILED: {:?}", e),
        }
    }
    Ok(())
}