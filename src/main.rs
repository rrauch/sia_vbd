use sia_vbd::nbd::handler::DummyHandler;
use sia_vbd::Builder;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let runner = Builder::new("0.0.0.0:5112")
        .with_export(
            "sia_vbd",
            DummyHandler::new(
                Some("Sia Virtual Block Device"),
                1024 * 1024 * 1024 * 10,
                false,
            ),
            false,
        )
        .build();
    runner.run().await?;
    Ok(())
}
