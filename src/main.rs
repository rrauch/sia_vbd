use anyhow::anyhow;
use bytesize::ByteSize;
use clap::Parser;
use futures::TryStreamExt;
use sia_vbd::hash::HashAlgorithm;
use sia_vbd::nbd::Builder;
use sia_vbd::repository::fs::FsRepository;
use sia_vbd::repository::{Repository, RepositoryHandler, VolumeHandler};
use sia_vbd::vbd::nbd_device::NbdDevice;
use sia_vbd::vbd::{BlockSize, ClusterSize, VirtualBlockDevice};
use std::path::PathBuf;
use tracing::Level;
use tracing_subscriber::EnvFilter;
use url::Url;

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
    #[clap(default_value = "4 MiB")]
    max_write_buffer: ByteSize,
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
    /// WAL directory
    #[arg(long, short = 'w')]
    wal_dir: PathBuf,
    /// Maximum WAL file size
    #[arg(long, short = 'y')]
    #[clap(default_value = "128 MiB")]
    max_wal_size: ByteSize,
    /// Maximum Transaction Size
    /// Must be smaller than maximum WAL file size
    #[arg(long, short = 'z')]
    #[clap(default_value = "16 MiB")]
    max_tx_size: ByteSize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        //.without_time()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(Level::INFO.into())
                .from_env_lossy(),
        )
        .init();

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

    let db_dir = PathBuf::from("/tmp/foo/db/");
    let db_file = db_dir.join("sia_vbd.sqlite");
    let max_db_connections = 25;
    let branch = "main".try_into()?;

    let repository: RepositoryHandler = FsRepository::new("/tmp/foo/repo").await?.into();

    let volume = if let Some(vbd_id) = repository.list_volumes().await?.try_next().await? {
        repository.open_volume(&vbd_id, &branch).await?
    } else {
        let vbd_id = repository
            .create_volume(
                None,
                &branch,
                arguments.size.as_u64() as usize,
                arguments.cluster_size,
                arguments.block_size,
                arguments.content_hash,
                arguments.meta_hash,
            )
            .await?;

        eprintln!("new vbd created: vbd_id: {}", &vbd_id,);

        repository.open_volume(&vbd_id, &branch).await?
    };

    let runner = builder
        .with_export(
            arguments.export_name,
            NbdDevice::new(
                VirtualBlockDevice::new(
                    arguments.max_write_buffer.as_u64() as usize,
                    arguments.wal_dir,
                    arguments.max_wal_size.as_u64(),
                    arguments.max_tx_size.as_u64(),
                    &db_file,
                    max_db_connections,
                    branch,
                    volume,
                )
                .await?,
            ),
            false,
        )
        .await?
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
