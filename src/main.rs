
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    sia_vbd::run("0.0.0.0:5112").await?;
    Ok(())
}