use turbos7::{S7Client, s7, ReadItem};
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = S7Client::connect_s7300("172.20.0.100:102", 0x0001).await?;
    println!("✅ Connected, PDU = {}", client.pdu_size());

    let items1 = vec![ReadItem { area: 0x84, db_number: 7001, start: 0, size: 2 }];
    let items2 = vec![ReadItem { area: 0x84, db_number: 9, start: 0, size: 2 }];
    let req1 = s7::build_read_multi_request(&items1);
    let req2 = s7::build_read_multi_request(&items2);
    let requests = vec![req1, req2];

    let start = Instant::now();
    let results = client.read_multiple_pipelined(&requests).await?;
    let elapsed = start.elapsed();

    println!("Two requests via pipelined in {:?}", elapsed);
    for (i, res) in results.iter().enumerate() {
        match res {
            Ok(data) => println!("  Request {}: success, data = {:02X?}", i, data),
            Err(e) => println!("  Request {}: FAILED: {:?}", i, e),
        }
    }
    Ok(())
}