use sia_vbd::hash;
use sia_vbd::hash::HashAlgorithm;
use sia_vbd::nbd::vbd::dedup::DedupDevice;
use sia_vbd::nbd::vbd::dummy::DummyBlockDevice;
use sia_vbd::nbd::vbd::mem::MemDevice;
use sia_vbd::nbd::Builder;
use sia_vbd::vbd::nbd_device::NbdDevice;
use sia_vbd::vbd::{BlockSize, ClusterSize, Structure};
use std::str::FromStr;
use std::time::Duration;
use uuid::Uuid;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    //sia_vbd::vbd::foo();

    //console_subscriber::init();
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
            //MemDevice::new(4096, 262144, Some("Virtual Memory Block Device")),
            //MemDevice::new(1024 * 64, 16384, Some("Virtual Memory Block Device")),
            //MemDevice::new(4096, 262144, Some("Virtual Memory Block Device")),
            /*DedupDevice::new(
                1024 * 64,
                16384,
                HashAlgorithm::Blake3,
                Some("Virtual Deduplicating Block Device"),
            ),*/
            NbdDevice::new(Structure::new(
                Uuid::from_str("019408c6-9ad0-7e31-b272-042c3d01c68c")?,
                ClusterSize::Cs256,
                64,
                HashAlgorithm::Blake3,
                BlockSize::Bs64k,
                HashAlgorithm::Blake3,
                HashAlgorithm::Blake3,
            )?),
            false,
        )?
        .build();
    runner.run().await?;
    Ok(())
}
