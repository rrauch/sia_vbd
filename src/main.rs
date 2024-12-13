use sia_vbd::nbd::block_device::DummyBlockDevice;
use sia_vbd::Builder;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let runner = Builder::tcp("0.0.0.0", 5112)
        //let runner = Builder::unix("/tmp/sia_vbd.sock")
        .with_export(
            "sia_vbd",
            DummyBlockDevice::new(
                Some("Sia Virtual Block Device"),
                1024 * 1024 * 1024 * 10,
                false,
            ),
            false,
        )?
        .build();
    runner.run().await?;
    Ok(())
}
