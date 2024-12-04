
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    sia_vbd::run("localhost:5112").await?;
    Ok(())
}