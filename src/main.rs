use sia_vbd::nbd::vbd::dummy::DummyBlockDevice;
use sia_vbd::nbd::vbd::mem::MemDevice;
use sia_vbd::Builder;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let runner = Builder::tcp("0.0.0.0", 5112)
        //let runner = Builder::unix("/tmp/sia_vbd.sock")
        .with_export(
            "sia_vbd",
            /*DummyBlockDevice::new(
                Some("Sia Virtual Block Device"),
                1024 * 1024 * 1024 * 10,
                false,
            ),*/
            //MemDevice::new(4096*32, 262144/32, Some("Virtual Memory Block Device")),
            MemDevice::new(4096, 262144, Some("Virtual Memory Block Device")),
            false,
        )?
        .build();
    runner.run().await?;
    Ok(())
}
