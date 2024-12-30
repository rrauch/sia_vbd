use sia_vbd::hash::HashAlgorithm;
use sia_vbd::nbd::Builder;
use sia_vbd::vbd::nbd_device::NbdDevice;
use sia_vbd::vbd::{BlockSize, ClusterSize, VirtualBlockDevice};
use std::str::FromStr;
use uuid::Uuid;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let runner = Builder::tcp("0.0.0.0", 5112)
        //let runner = Builder::unix("/tmp/sia_vbd.sock")
        .with_export(
            "sia_vbd",
            NbdDevice::new(VirtualBlockDevice::new(
                Uuid::from_str("019408c6-9ad0-7e31-b272-042c3d01c68c")?,
                ClusterSize::Cs256,
                64,
                BlockSize::Bs64k,
                HashAlgorithm::Blake3,
                HashAlgorithm::Blake3,
                1024 * 1024 * 100,
            )?),
            false,
        )?
        .build();
    runner.run().await?;
    Ok(())
}
