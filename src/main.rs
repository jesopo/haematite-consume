use clap::Parser;
use futures_lite::stream::StreamExt as _;
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions},
    types::FieldTable,
    Connection, ConnectionProperties,
};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct CliArgs {
    #[clap(index = 1)]
    host: String,
    #[clap(index = 2)]
    queue: String,
    #[clap(index = 3)]
    routing_key: String,
}

#[tokio::main]
async fn main() {
    let args = CliArgs::parse();

    let conn = Connection::connect(&args.host, ConnectionProperties::default())
        .await
        .unwrap();
    let channel = conn.create_channel().await.unwrap();
    let mut consumer = channel
        .basic_consume(
            &args.queue,
            &args.routing_key,
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    while let Some(Ok(delivery)) = consumer.next().await {
        println!(
            "{} {}",
            delivery.routing_key,
            std::str::from_utf8(&delivery.data).unwrap()
        );
        delivery.ack(BasicAckOptions::default()).await.unwrap();
    }
}
