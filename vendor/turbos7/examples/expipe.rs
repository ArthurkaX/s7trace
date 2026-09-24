use turbos7::{S7Client, ReadItem};
use turbos7::s7;   // теперь это работает
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = S7Client::connect_s7300("172.20.0.100:102", 0x0001).await?;
    println!("✅ Connected, PDU = {}", client.pdu_size());

    // Подготавливаем несколько одиночных запросов
    let items1 = vec![ReadItem { area: 0x84, db_number: 7001, start: 0, size: 2 }];
    let items2 = vec![ReadItem { area: 0x84, db_number: 7001, start: 100, size: 2 }];
    let items3 = vec![ReadItem { area: 0x84, db_number: 9, start: 0, size: 2 }];

    let req1 = s7::build_read_multi_request(&items1);
    let req2 = s7::build_read_multi_request(&items2);
    let req3 = s7::build_read_multi_request(&items3);

    let requests = vec![req1, req2, req3];

    let start = Instant::now();
    let results = client.read_multiple_pipelined(&requests).await?;
    let elapsed = start.elapsed();

    println!("Pipelined {} requests in {:?}", results.len(), elapsed);
    for (i, res) in results.iter().enumerate() {
        match res {
            Ok(data) => println!("  Request {}: success, data = {:02X?}", i, data),
            Err(e) => println!("  Request {}: FAILED, error = {:?}", i, e),
        }
    }

    // Для сравнения – выполним те же запросы последовательно
    let mut sequential_results = Vec::new();
    let start_seq = Instant::now();
    for items in [&items1, &items2, &items3] {
        sequential_results.push(client.read_multiple(items).await?);
    }
    let seq_elapsed = start_seq.elapsed();

    println!("\nSequential read_multiple: {:?}", seq_elapsed);
    for (i, res) in sequential_results.iter().enumerate() {
        println!("  Request {}: success, data = {:02X?}", i, res[0].data);
    }

    Ok(())
}