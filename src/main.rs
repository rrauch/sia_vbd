use anyhow::anyhow;
use bytesize::ByteSize;
use clap::Parser;
use sia_vbd::hash::HashAlgorithm;
use sia_vbd::nbd::Builder;
use sia_vbd::vbd::nbd_device::NbdDevice;
use sia_vbd::vbd::{BlockSize, ClusterSize, VirtualBlockDevice};
use url::Url;
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(version)]
/// Exports a Virtual Block Device via NBD.
struct Arguments {
    /// Block size in KiB.
    /// Possible values are: 16, 64, 256.
    #[arg(long, short = 'b')]
    #[clap(default_value = "64")]
    block_size: BlockSize,
    /// Cluster size, in number of Blocks.
    /// Possible values are: 256.
    #[arg(long, short = 'c')]
    #[clap(default_value = "256")]
    cluster_size: ClusterSize,
    /// Hash Algorithm to use for Block Content Hashing.
    /// Possible values are: blake3, xxh128, tent0_4.
    #[arg(long, short = 'a')]
    #[clap(default_value = "blake3")]
    content_hash: HashAlgorithm,
    /// Hash Algorithm to use for Metadata Hashing.
    /// Possible values are: blake3, xxh128, tent0_4.
    #[arg(long, short = 'm')]
    #[clap(default_value = "blake3")]
    meta_hash: HashAlgorithm,
    /// Maximum write buffer size.
    #[arg(long, short = 'x')]
    #[clap(default_value = "128 MiB")]
    max_buffer_size: ByteSize,
    /// URL of address to listen on.
    /// For tcp, the format is 'tcp://host:port'.
    /// On unix platforms, unix domain sockets are also supported: 'unix:///path/to/socket'.
    #[arg(long, short = 'l')]
    #[clap(default_value = "tcp://localhost:5112")]
    listen_address: Url,
    /// Desired size of the virtual block device
    /// e.g. `10 GiB`
    #[arg(long, short = 's')]
    size: ByteSize,
    /// NBD export name
    #[arg(long, short = 'e')]
    #[clap(default_value = "sia_vbd")]
    export_name: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let arguments = Arguments::parse();
    let builder = match arguments
        .listen_address
        .scheme()
        .to_ascii_lowercase()
        .as_str()
    {
        "tcp" => {
            let host = arguments
                .listen_address
                .host()
                .ok_or(anyhow!("host missing in listen address"))?;
            let port = arguments
                .listen_address
                .port()
                .ok_or(anyhow!("port missing in listen address"))?;
            Builder::tcp(host, port)
        }
        "unix" => {
            if !uds_supported() {
                anyhow::bail!("Unix Domain Sockets are only not available on this platform");
            }
            Builder::unix(arguments.listen_address.path())
        }
        _ => anyhow::bail!(
            "invalid listen address specified: {}",
            arguments.listen_address
        ),
    };

    let cluster_size_bytes = *arguments.block_size * *arguments.cluster_size;

    if arguments.size.as_u64() < cluster_size_bytes as u64 {
        anyhow::bail!(
            "'size' must be equivalent to at least {} bytes",
            cluster_size_bytes
        );
    }

    let num_clusters =
        (arguments.size.as_u64() as usize + cluster_size_bytes - 1) / cluster_size_bytes;

    let runner = builder
        .with_export(
            arguments.export_name,
            NbdDevice::new(VirtualBlockDevice::new(
                Uuid::now_v7(),
                arguments.cluster_size,
                num_clusters,
                arguments.block_size,
                arguments.content_hash,
                arguments.meta_hash,
                arguments.max_buffer_size.as_u64() as usize,
            )?),
            false,
        )?
        .build();
    runner.run().await?;
    Ok(())
}

#[cfg(unix)]
fn uds_supported() -> bool {
    true
}

#[cfg(not(unix))]
fn uds_supported() -> bool {
    false
}
