use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    alloyport_worker::application::run_from_args().await
}
