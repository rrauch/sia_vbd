use anyhow::{anyhow, bail};
use bytesize::ByteSize;
use clap::{Parser, Subcommand};
use duration_str::deserialize_duration;
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use indicatif::ProgressBar;
use serde::Deserialize;
use sia_vbd::hash::{Hash, HashAlgorithm};
use sia_vbd::nbd::{Builder, RunGuard};
use sia_vbd::repository::fs::FsRepository;
use sia_vbd::repository::renterd::RenterdRepository;
use sia_vbd::repository::{CommitInfo, CommitType, RepositoryHandler};
use sia_vbd::vbd::nbd_device::NbdDevice;
use sia_vbd::vbd::{
    BlockSize, BranchName, ClusterSize, CommitId, TagName, VbdId, VirtualBlockDevice,
};
use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tracing::Level;
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use url::Url;

#[derive(Debug, Parser)]
#[command(version)]
/// Exports a Virtual Block Device via NBD.
///
/// If no commands are specified,
/// the server process will run and
/// export volumes as configured.
struct Arguments {
    /// Path to Config File
    #[arg(long, short = 'c', env)]
    config: PathBuf,
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// List all configured repositories
    Repos,
    /// Volume related actions
    #[command(subcommand)]
    Volume(VolumeCommands),
    /// Branch related actions
    #[command(subcommand)]
    Branch(BranchCommands),
    /// Tag related actions
    #[command(subcommand)]
    Tag(TagCommands),
}

#[derive(Debug, Subcommand)]
enum VolumeCommands {
    /// List all volumes in a given repository
    List {
        /// Name of repository
        repo: String,
    },
    /// Create a new volume
    Create {
        /// Name of repository
        repo: String,
        /// Size of Volume
        size: ByteSize,
        /// Optional descriptive name
        #[arg(short = 'n')]
        name: Option<String>,
        /// Name of the default branch
        #[arg(short = 'd')]
        #[clap(default_value = "main")]
        branch: String,
        /// Block size in KiB.
        /// Possible values are: 16, 64, 256, 1024.
        #[arg(short = 'b')]
        #[clap(default_value = "64")]
        block_size: BlockSize,
        /// Cluster size, in number of Blocks.
        /// Possible values are: 64, 128, 256.
        #[arg(short = 'c')]
        #[clap(default_value = "256")]
        cluster_size: ClusterSize,
        /// Hash Algorithm to use for Block Content Hashing.
        /// Possible values are: blake3, tent, xxh128
        #[arg(short = 'o')]
        #[clap(default_value = "blake3")]
        content_hash: HashAlgorithm,
        /// Hash Algorithm to use for Metadata Hashing.
        /// Possible values are: blake3, tent, xxh128
        #[arg(short = 'm')]
        #[clap(default_value = "blake3")]
        meta_hash: HashAlgorithm,
    },
    /// Resize a specific volume
    Resize {
        /// Branch to resize
        #[arg(short = 'b')]
        #[clap(default_value = "main")]
        branch: String,
        /// New size of Volume
        size: ByteSize,
        /// Name of repository
        repo: String,
        /// Id of the volume to resize
        volume_id: String,
    },
    /// Delete a specific volume
    Delete {
        /// Name of repository
        repo: String,
        /// Id of the volume to delete
        volume_id: String,
    },
}

#[derive(Debug, Subcommand)]
enum BranchCommands {
    /// Create a new Branch from a given Branch, Tag or Commit Id
    Create {
        /// Name of new Branch
        name: String,
        /// Existing Branch, Tag or Commit Id the new Branch should be based on
        source: String,
        /// Name of repository
        repo: String,
        /// Id of the volume
        volume_id: String,
    },
    /// Delete a specific Branch
    Delete {
        /// Name of Branch to delete
        name: String,
        /// Name of repository
        repo: String,
        /// Id of the volume
        volume_id: String,
    },
}

#[derive(Debug, Subcommand)]
enum TagCommands {
    /// Create a new Tag from a given Branch, Tag or Commit Id
    Create {
        /// Name of new Tag
        name: String,
        /// Existing Branch, Tag or Commit Id the new Tag should be based on
        source: String,
        /// Name of repository
        repo: String,
        /// Id of the volume
        volume_id: String,
    },
    /// Delete a specific Tag
    Delete {
        /// Name of Tag to delete
        name: String,
        /// Name of repository
        repo: String,
        /// Id of the volume
        volume_id: String,
    },
}

#[derive(Deserialize)]
struct Config {
    server: Option<BTreeMap<String, ServerConfig>>,
    repository: Option<BTreeMap<String, RepoConfig>>,
    volume: Option<Vec<VolumeConfig>>,
}

fn default_max_write_buffer() -> ByteSize {
    ByteSize::mib(4)
}

fn default_max_wal_size() -> ByteSize {
    ByteSize::mib(128)
}

fn default_max_tx_size() -> ByteSize {
    ByteSize::mib(16)
}

fn default_max_chunk_size() -> ByteSize {
    ByteSize::mib(40)
}

fn default_max_db_connections() -> u8 {
    25
}

fn default_branch() -> String {
    "main".to_string()
}

fn default_initial_sync_delay() -> Duration {
    Duration::from_secs(60)
}

fn default_sync_interval() -> Duration {
    Duration::from_secs(300)
}

fn default_read_only() -> bool {
    false
}

fn default_cache_max_memory() -> ByteSize {
    ByteSize::mib(64)
}

fn default_cache_max_disk() -> ByteSize {
    ByteSize::gib(4)
}

#[derive(Deserialize)]
struct VolumeConfig {
    repository: String,
    volume_id: String,
    export_server: String,
    export_name: String,
    #[serde(default = "default_max_write_buffer")]
    max_write_buffer: ByteSize,
    wal: PathBuf,
    #[serde(default = "default_max_wal_size")]
    max_wal_size: ByteSize,
    #[serde(default = "default_max_tx_size")]
    max_tx_size: ByteSize,
    #[serde(default = "default_max_chunk_size")]
    max_chunk_size: ByteSize,
    inventory: PathBuf,
    cache: PathBuf,
    #[serde(default = "default_cache_max_memory")]
    cache_max_memory: ByteSize,
    #[serde(default = "default_cache_max_disk")]
    cache_max_disk: ByteSize,
    #[serde(default = "default_max_db_connections")]
    max_db_connections: u8,
    #[serde(default = "default_branch")]
    branch: String,
    #[serde(default = "default_initial_sync_delay")]
    #[serde(deserialize_with = "deserialize_duration")]
    initial_sync_delay: Duration,
    #[serde(default = "default_sync_interval")]
    #[serde(deserialize_with = "deserialize_duration")]
    sync_interval: Duration,
    #[serde(default = "default_read_only")]
    read_only: bool,
}

#[derive(Deserialize)]
struct TcpListenConfig {
    host: Option<String>,
    port: u16,
}

#[derive(Deserialize)]
struct ServerConfig {
    r#type: ServerType,
    unix: Option<PathBuf>,
    tcp: Option<TcpListenConfig>,
    max_connections: Option<u32>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum ServerType {
    Nbd,
}

impl TryFrom<ServerConfig> for Builder {
    type Error = anyhow::Error;

    fn try_from(value: ServerConfig) -> Result<Self, Self::Error> {
        let mut builder = None;

        if let Some(path) = value.unix {
            builder = Some(Builder::unix(path));
        }

        if builder.is_none() {
            if let Some(config) = value.tcp {
                let host = config.host.unwrap_or("localhost".to_string());
                let port = config.port;

                builder = Some(Builder::tcp(host, port));
            }
        }

        let mut builder = match builder {
            Some(builder) => builder,
            None => {
                bail!("either 'unix' or 'tcp' needs to be set");
            }
        };

        if let Some(max_connections) = value.max_connections {
            builder = builder.max_connections(max_connections);
        }

        Ok(builder)
    }
}

#[derive(Deserialize)]
struct RepoConfig {
    renterd_endpoint: Option<Url>,
    api_password: Option<String>,
    bucket: Option<String>,
    path: String,
}

impl TryFrom<RepoConfig> for RepositoryHandler {
    type Error = anyhow::Error;

    fn try_from(value: RepoConfig) -> Result<Self, Self::Error> {
        let path = value.path.trim();
        if path.is_empty() {
            bail!("invalid 'path'");
        }
        let path = if !path.ends_with('/') {
            format!("{}/", path)
        } else {
            path.to_string()
        };

        if value.renterd_endpoint.is_none()
            && value.api_password.is_none()
            && value.bucket.is_none()
        {
            Ok(RepositoryHandler::FsRepo(FsRepository::new(&path)))
        } else {
            let endpoint = value
                .renterd_endpoint
                .ok_or(anyhow!("'renterd_endpoint' setting is missing"))?;
            let password = value.api_password.unwrap_or_else(|| "".to_string());
            let renterd = renterd_client::ClientBuilder::new()
                .api_endpoint_url(endpoint.clone())
                .api_password(password)
                .build()?;
            let bucket = value.bucket.ok_or(anyhow!("'bucket' setting is missing"))?;
            Ok(RepositoryHandler::RenterdRepo(RenterdRepository::new(
                renterd,
                endpoint,
                bucket.into(),
                path.into(),
            )))
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::builder()
        .with_default_directive(Level::INFO.into())
        .from_env_lossy();
    let (filter, reload_handle) = tracing_subscriber::reload::Layer::new(filter);

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::Layer::default())
        .init();

    let arguments = Arguments::parse();
    let config = arguments.config;
    match tokio::fs::metadata(&config).await {
        Ok(metadata) => {
            if !metadata.is_file() {
                bail!("config file [{}] not a regular file", config.display());
            }
            if metadata.len() > 1024 * 1024 {
                bail!("config file [{}] too large", config.display());
            }
        }
        Err(err) => {
            bail!(
                "error accessing config file [{}]: {}",
                config.display(),
                err
            );
        }
    }
    let config = tokio::fs::read_to_string(&config).await?;
    let config: Config =
        toml::from_str(&config).map_err(|err| anyhow!("error parsing config file: {}", err))?;

    let mut servers = config
        .server
        .unwrap_or_default()
        .into_iter()
        .map(|(name, conf)| Builder::try_from(conf).map(|b| (name, b)))
        .collect::<Result<BTreeMap<String, Builder>, anyhow::Error>>()
        .map_err(|e| anyhow!("error in server configuration: {}", e))?;

    let repositories = config
        .repository
        .unwrap_or_default()
        .into_iter()
        .map(|(name, conf)| RepositoryHandler::try_from(conf).map(|h| (name, h)))
        .collect::<Result<BTreeMap<String, RepositoryHandler>, anyhow::Error>>()
        .map_err(|e| anyhow!("error in repository configuration: {}", e))?;

    match arguments.command {
        Some(Commands::Repos) => {
            list_repos(repositories.iter());
            return Ok(());
        }
        Some(Commands::Volume(cmd)) => match cmd {
            VolumeCommands::List { repo } => {
                let repo_name = repo;
                let repo = repositories
                    .get(&repo_name)
                    .ok_or(anyhow!("repository [{}] not found", repo_name))?;
                list_volumes(&repo, repo_name.as_str()).await?;
                return Ok(());
            }
            VolumeCommands::Create {
                repo,
                size,
                name,
                branch,
                block_size,
                cluster_size,
                content_hash,
                meta_hash,
            } => {
                let repo_name = repo;
                let repo = repositories
                    .get(&repo_name)
                    .ok_or(anyhow!("repository [{}] not found", repo_name))?;

                create_volume(
                    &repo,
                    repo_name.as_str(),
                    servers.iter().map(|(name, _)| name.as_str()),
                    name,
                    branch,
                    size,
                    cluster_size,
                    block_size,
                    content_hash,
                    meta_hash,
                )
                .await?;
                return Ok(());
            }
            VolumeCommands::Resize {
                repo,
                volume_id,
                branch,
                size,
            } => {
                let repo_name = repo;
                let repo = repositories
                    .get(&repo_name)
                    .ok_or(anyhow!("repository [{}] not found", repo_name))?;

                let volume_config = config
                    .volume
                    .as_ref()
                    .ok_or(anyhow!("no volumes configured"))?
                    .iter()
                    .find(|v| &v.repository == &repo_name && &v.volume_id == &volume_id)
                    .ok_or(anyhow!("no config found for volume {}", volume_id))?;

                reload_handle.modify(|layer| {
                    *layer = EnvFilter::builder()
                        .with_default_directive(Level::ERROR.into())
                        .with_env_var("__DISABLE_RUST_LOG__")
                        .from_env_lossy()
                })?;

                resize_volume(
                    &repo,
                    repo_name.as_str(),
                    volume_id,
                    volume_config,
                    branch,
                    size.as_u64(),
                )
                .await?;
                return Ok(());
            }
            VolumeCommands::Delete { repo, volume_id } => {
                let repo_name = repo;
                let repo = repositories
                    .get(&repo_name)
                    .ok_or(anyhow!("repository [{}] not found", repo_name))?;

                delete_volume(&repo, repo_name.as_str(), volume_id).await?;
                return Ok(());
            }
        },
        Some(Commands::Branch(cmd)) => match cmd {
            BranchCommands::Create {
                name,
                source,
                repo,
                volume_id,
            } => {
                let repo_name = repo;
                let repo = repositories
                    .get(&repo_name)
                    .ok_or(anyhow!("repository [{}] not found", repo_name))?;
                let branch_name = BranchName::try_from(name.as_str())
                    .map_err(|e| anyhow!("invalid branch name: {}", e))?;
                create_commit(
                    &repo,
                    repo_name.as_str(),
                    volume_id,
                    CommitType::Branch(branch_name),
                    source,
                )
                .await?;
                return Ok(());
            }
            BranchCommands::Delete {
                repo,
                volume_id,
                name,
            } => {
                let repo_name = repo;
                let repo = repositories
                    .get(&repo_name)
                    .ok_or(anyhow!("repository [{}] not found", repo_name))?;
                let branch_name = BranchName::try_from(name.as_str())
                    .map_err(|e| anyhow!("invalid branch name: {}", e))?;
                delete_commit(
                    &repo,
                    repo_name.as_str(),
                    volume_id,
                    CommitType::Branch(branch_name),
                )
                .await?;
                return Ok(());
            }
        },
        Some(Commands::Tag(cmd)) => match cmd {
            TagCommands::Create {
                name,
                source,
                repo,
                volume_id,
            } => {
                let repo_name = repo;
                let repo = repositories
                    .get(&repo_name)
                    .ok_or(anyhow!("repository [{}] not found", repo_name))?;
                let tag_name = TagName::try_from(name.as_str())
                    .map_err(|e| anyhow!("invalid tag name: {}", e))?;
                create_commit(
                    &repo,
                    repo_name.as_str(),
                    volume_id,
                    CommitType::Tag(tag_name),
                    source,
                )
                .await?;
                return Ok(());
            }
            TagCommands::Delete {
                repo,
                volume_id,
                name,
            } => {
                let repo_name = repo;
                let repo = repositories
                    .get(&repo_name)
                    .ok_or(anyhow!("repository [{}] not found", repo_name))?;
                let tag_name = TagName::try_from(name.as_str())
                    .map_err(|e| anyhow!("invalid tag name: {}", e))?;
                delete_commit(
                    &repo,
                    repo_name.as_str(),
                    volume_id,
                    CommitType::Tag(tag_name),
                )
                .await?;
                return Ok(());
            }
        },
        None => {}
    }

    if servers.is_empty() {
        bail!("Invalid Configuration: No [server]'s found. Please configure at least one server.");
    }

    if repositories.is_empty() {
        bail!("Invalid Configuration: No [repository]'s found. Please configure at least one repository.");
    }

    let volumes = config.volume.unwrap_or_else(|| vec![]);
    if volumes.is_empty() {
        bail!(
            "Invalid Configuration: No [[volume]]'s found. Please configure at least one volume."
        );
    }

    let mut vol_set = HashSet::with_capacity(volumes.len());
    for config in volumes.iter() {
        let key = (config.repository.as_str(), config.volume_id.as_str());
        if vol_set.contains(&key) {
            bail!(
                "configuration error: volume(id=[{}], repository=[{}]) is exported multiple times",
                config.volume_id.as_str(),
                config.repository.as_str()
            );
        }
        vol_set.insert(key);
    }

    for config in volumes {
        let repo = repositories.get(&config.repository).ok_or(anyhow!(
            "configuration error: repository [{}] not found",
            &config.repository
        ))?;
        let branch = BranchName::try_from(&config.branch).map_err(|_| {
            anyhow!(
                "configuration error: invalid branch name [{}]",
                config.branch
            )
        })?;
        let vbd_id = VbdId::try_from(config.volume_id.as_str()).map_err(|_| {
            anyhow!(
                "configuration error: invalid volume_id [{}]",
                config.volume_id
            )
        })?;
        let volume_handler = repo
            .open_volume(&vbd_id, &branch)
            .await
            .map_err(|e| anyhow!("unable to open volume [{}]: {}", vbd_id, e))?;

        let vbd = VirtualBlockDevice::new(
            config.max_write_buffer.as_u64() as usize,
            config.wal,
            config.max_wal_size.as_u64(),
            config.max_tx_size.as_u64(),
            config.max_chunk_size.as_u64(),
            config.inventory.join("sia_vbd_inventory.sqlite"),
            config.max_db_connections,
            config.cache,
            config.cache_max_memory.as_u64() as usize,
            config.cache_max_disk.as_u64(),
            config.branch.try_into()?,
            volume_handler,
            config.initial_sync_delay,
            config.sync_interval,
        )
        .await
        .map_err(|e| anyhow!("unable to configure volume [{}]: {}", vbd_id, e))?;

        let builder = servers.remove(&config.export_server).ok_or(anyhow!(
            "configuration error: server [{}] not found",
            &config.export_server
        ))?;

        let builder = builder
            .with_export(config.export_name, NbdDevice::new(vbd), config.read_only)
            .await
            .map_err(|e| anyhow!("unable to export volume [{}]: {}", vbd_id, e))?;
        servers.insert(config.export_server, builder);
    }

    let mut run_guards = vec![];
    let mut runners = vec![];

    for (name, builder) in servers {
        tracing::info!(name, "starting server");
        let (runner, run_guard) = builder.build();
        run_guards.push(run_guard);
        runners.push((runner, name));
    }

    let shutdown = tokio::spawn(shutdown_listener(run_guards.into_iter()));
    let mut errors = 0;
    let mut runner_futs = FuturesUnordered::new();
    for (runner, name) in runners.as_mut_slice() {
        runner_futs.push(async move {
            let res = runner.run().await;
            (res, name)
        });
    }

    notify_ready().await;

    while let Some((res, name)) = runner_futs.next().await {
        if let Err(err) = res {
            tracing::error!(error = %err, name, "server error");
            errors += 1;
        }
    }

    shutdown.abort();

    if errors == 0 {
        tracing::info!("sia_vbd shut down cleanly");
    } else {
        bail!("sia_vbd experienced errors during shutdown");
    }

    Ok(())
}

fn list_repos<'a>(repos: impl Iterator<Item = (&'a String, &'a RepositoryHandler)>) {
    println!("The following repositories are currently configured:");
    println!();
    for (name, repo) in repos {
        println!("{}: {}", name, repo);
    }
    println!();
}

async fn list_volumes(repo: &RepositoryHandler, repo_name: &str) -> anyhow::Result<()> {
    println!("Selected Repository: {} - {}", repo_name, repo);
    println!("The following volumes are currently available:");
    println!();

    let pb = ProgressBar::new_spinner();
    pb.enable_steady_tick(Duration::from_millis(50));
    pb.set_message("retrieving volume list");

    let mut stream = repo.list_volumes().await?;

    pb.finish_and_clear();

    while let Some(vbd_id) = stream.try_next().await? {
        volume_details(repo, &vbd_id).await?;
    }

    println!();
    Ok(())
}

async fn volume_details(repo: &RepositoryHandler, vbd_id: &VbdId) -> anyhow::Result<()> {
    println!("{}", vbd_id);

    let pb = ProgressBar::new_spinner();
    pb.enable_steady_tick(Duration::from_millis(50));
    pb.set_message("retrieving volume details");

    let (volume_info, mut commits) = repo.volume_details(&vbd_id).await?;

    pb.finish_and_clear();

    let cluster_size_bytes = *volume_info.specs.block_size() * *volume_info.specs.cluster_size();
    let cluster_size_display = ByteSize::b(cluster_size_bytes as u64);

    if let Some(name) = volume_info.name.as_ref() {
        println!("    Name:            {}", name)
    }
    println!("    Created at:      {}", &volume_info.created);
    println!("    Block Size:      {}", volume_info.specs.block_size());
    println!("    Cluster Size:    {}", volume_info.specs.cluster_size());
    println!("    Content Hash:    {}", volume_info.specs.content_hash());
    println!("    Metadata Hash:   {}", volume_info.specs.meta_hash());
    println!("    Branches & Tags: ");

    while let Some(commit_info) = commits.try_next().await? {
        let size = ByteSize::b((commit_info.commit().num_clusters() * cluster_size_bytes) as u64);
        match &commit_info {
            CommitInfo::Branch(branch_name, _) => {
                println!("            Branch Name: {}", branch_name);
            }
            CommitInfo::Tag(tag_name, _) => {
                println!("               Tag Name: {}", tag_name);
            }
        }
        println!(
            "          Latest Commit: {}",
            commit_info.commit().content_id()
        );
        println!(
            "           Committed at: {}",
            commit_info.commit().committed()
        );
        println!(
            "                   Size: {} ({} clusters @ {})",
            size.to_string_as(true),
            commit_info.commit().num_clusters(),
            cluster_size_display.to_string_as(true)
        );
        println!("          ------------------------------------------");
    }
    println!();
    Ok(())
}

async fn create_volume(
    repo: &RepositoryHandler,
    repo_name: &str,
    servers: impl Iterator<Item = &str>,
    name: Option<String>,
    default_branch_name: String,
    size: ByteSize,
    cluster_size: ClusterSize,
    block_size: BlockSize,
    content_hash: HashAlgorithm,
    meta_hash: HashAlgorithm,
) -> anyhow::Result<()> {
    let branch: BranchName = default_branch_name
        .try_into()
        .map_err(|e| anyhow!("invalid branch name: {}", e))?;

    let cluster_size_bytes = *block_size * *cluster_size;

    if size.as_u64() < cluster_size_bytes as u64 {
        bail!(
            "'size' must be equivalent to at least {} bytes",
            cluster_size_bytes
        );
    }

    println!("Selected Repository: {} - {}", repo_name, repo);
    println!();
    println!("Creation of new Volume with the following specifications:");
    if let Some(name) = name.as_ref() {
        println!("Name: {}", name);
    }
    println!("Size: {}", size.to_string_as(true));
    println!("Default Branch Name: {}", branch);
    println!("Block Size: {}", block_size);
    println!("Cluster Size: {}", cluster_size);
    println!("Content Hash Algorithm: {}", content_hash);
    println!("Metadata Hash Algorithm: {}", meta_hash);
    println!();
    if !ask_confirmation("Do you want to continue (y/n)?").await {
        println!("Aborting");
        return Ok(());
    }

    let pb = ProgressBar::new_spinner();
    pb.enable_steady_tick(Duration::from_millis(50));
    pb.set_message("Volume creation in progress");

    let vbd_id = repo
        .create_volume(
            name.as_deref(),
            &branch,
            size.as_u64() as usize,
            cluster_size,
            block_size,
            content_hash,
            meta_hash,
        )
        .await?;

    pb.finish_with_message("Volume created successfully");
    println!();
    println!();
    volume_details(repo, &vbd_id).await?;
    println!();
    println!("You can now add this volume to your configuration:");
    println!();

    let servers = servers.into_iter().collect::<Vec<_>>();
    let server: String = servers.join(" or ");

    println!(
        r###"[[volume]]
repository = "{}"
volume_id = "{}"
export_server = "{}"
export_name = "<export as>"
wal = "<path to wal directory>"
inventory = "<path to inventory directory>"
cache = "<path to cache directory>"
"###,
        repo_name, &vbd_id, server
    );
    println!();
    Ok(())
}

async fn resize_volume(
    repo: &RepositoryHandler,
    repo_name: &str,
    volume_id: String,
    config: &VolumeConfig,
    branch_name: String,
    size: u64,
) -> anyhow::Result<()> {
    let vbd_id =
        VbdId::try_from(volume_id.as_str()).map_err(|e| anyhow!("volume id is invalid: {}", e))?;
    let branch_name =
        BranchName::try_from(branch_name).map_err(|e| anyhow!("invalid branch name: {}", e))?;

    println!("Selected Repository: {} - {}", repo_name, repo);
    println!();
    println!("Resize the following Volume:");
    println!();
    volume_details(repo, &vbd_id).await?;
    println!();

    let pb = ProgressBar::new_spinner();
    pb.enable_steady_tick(Duration::from_millis(50));
    pb.set_message("retrieving volume details");
    let mut branch_info = None;
    let (volume_info, mut commits) = repo.volume_details(&vbd_id).await?;
    let specs = volume_info.specs;
    while let Some(commit_info) = commits.try_next().await? {
        if let CommitInfo::Branch(name, info) = commit_info {
            if &name == &branch_name {
                branch_info = Some(info);
                break;
            }
        }
    }
    pb.finish_and_clear();
    let branch_info = branch_info.ok_or(anyhow!("branch {} not found", branch_name))?;
    let cluster_size_bytes = *specs.block_size() * *specs.cluster_size();
    let cluster_size_display = ByteSize::b(cluster_size_bytes as u64);

    if size < cluster_size_bytes as u64 {
        bail!(
            "'size' must be equivalent to at least {} bytes",
            cluster_size_bytes
        );
    }

    let old_size = ByteSize::b((branch_info.commit.num_clusters() * cluster_size_bytes) as u64);
    let num_clusters = (size as usize + cluster_size_bytes - 1) / cluster_size_bytes;

    println!(
        "Current Size: {} ({} clusters @ {})",
        old_size.to_string_as(true),
        branch_info.commit.num_clusters(),
        cluster_size_display.to_string_as(true)
    );

    println!(
        "New Size: {} ({} clusters @ {})",
        ByteSize::b(size).to_string_as(true),
        num_clusters,
        cluster_size_display.to_string_as(true)
    );

    println!();

    if num_clusters == branch_info.commit.num_clusters() {
        println!("Size is already as requested. Nothing needs to be done.");
        println!();
        return Ok(());
    }

    if num_clusters < branch_info.commit.num_clusters() {
        println!("WARNING: You are SHRINKING the volume. Any existing data beyond the new size WILL BE LOST!");
    }
    println!("Only proceed if you are sure you want to PERMANENTLY RESIZE the above volume!");
    println!();
    if !ask_confirmation("Are you sure you want to resize this volume (y/n)?").await {
        println!("Aborting");
        return Ok(());
    }
    let pb = ProgressBar::new_spinner();
    pb.enable_steady_tick(Duration::from_millis(50));
    pb.set_message("Volume resize in progress");

    let volume_handler = repo
        .open_volume(&vbd_id, &branch_name)
        .await
        .map_err(|e| anyhow!("unable to open volume [{}]: {}", vbd_id, e))?;

    let mut vbd = VirtualBlockDevice::new(
        config.max_write_buffer.as_u64() as usize,
        &config.wal,
        config.max_wal_size.as_u64(),
        config.max_tx_size.as_u64(),
        config.max_chunk_size.as_u64(),
        config.inventory.join("sia_vbd_inventory.sqlite"),
        config.max_db_connections,
        &config.cache,
        config.cache_max_memory.as_u64() as usize,
        config.cache_max_disk.as_u64(),
        branch_name,
        volume_handler,
        config.initial_sync_delay,
        config.sync_interval,
    )
    .await
    .map_err(|e| anyhow!("unable to instantiate volume [{}]: {}", vbd_id, e))?;
    vbd.resize(num_clusters).await?;
    vbd.close().await?;

    pb.finish_with_message("Volume resized successfully");
    println!();
    println!();
    Ok(())
}

async fn delete_volume(
    repo: &RepositoryHandler,
    repo_name: &str,
    volume_id: String,
) -> anyhow::Result<()> {
    let vbd_id =
        VbdId::try_from(volume_id.as_str()).map_err(|e| anyhow!("volume id is invalid: {}", e))?;

    println!("Selected Repository: {} - {}", repo_name, repo);
    println!();
    println!("Deletion of the following Volume:");
    println!();
    volume_details(repo, &vbd_id).await?;
    println!();
    println!("WARNING: This operation can NOT be undone. DATA LOSS IMMINENT!");
    println!("Only proceed if you are sure you want to PERMANENTLY DELETE the above volume!");
    println!();
    if !ask_confirmation("Are you sure you want to delete this volume (y/n)?").await {
        println!("Aborting");
        return Ok(());
    }
    let pb = ProgressBar::new_spinner();
    pb.enable_steady_tick(Duration::from_millis(50));
    pb.set_message("Volume deletion in progress");

    repo.delete_volume(&vbd_id).await?;
    pb.finish_with_message("Volume deleted successfully");
    println!();
    println!();
    Ok(())
}

async fn create_commit(
    repo: &RepositoryHandler,
    repo_name: &str,
    volume_id: String,
    commit_type: CommitType,
    source: String,
) -> anyhow::Result<()> {
    println!("Selected Repository: {} - {}", repo_name, repo);
    println!();

    let vbd_id =
        VbdId::try_from(volume_id.as_str()).map_err(|e| anyhow!("volume id is invalid: {}", e))?;

    let pb = ProgressBar::new_spinner();
    pb.enable_steady_tick(Duration::from_millis(50));
    pb.set_message("retrieving volume details");

    let (volume_info, mut commits) = repo.volume_details(&vbd_id).await?;

    pb.finish_and_clear();

    println!(
        "Selected Volume: {}",
        volume_info
            .name
            .as_ref()
            .map(|n| format!("{} ({})", n, &vbd_id))
            .unwrap_or(vbd_id.to_string())
    );
    println!();

    let (source_type, source_id) = match source.as_str().split_once(':') {
        Some((source_type, source_id)) => (source_type, source_id),
        None => ("commit", source.as_str()),
    };

    let commit_id = if source_type == "commit" {
        CommitId::try_from(
            Hash::try_from((source_id, volume_info.specs.meta_hash()))
                .map_err(|e| anyhow!("commit id in invalid: {}", e))?,
        )
        .ok()
    } else {
        let mut commit_id = None;
        while let Some(commit_info) = commits.try_next().await? {
            match (source_type, commit_info) {
                ("branch", CommitInfo::Branch(branch_name, branch_info))
                    if branch_name.as_ref() == source_id =>
                {
                    commit_id = Some(branch_info.commit.content_id().clone());
                    break;
                }
                ("tag", CommitInfo::Tag(tag_name, tag_info)) if tag_name.as_ref() == source_id => {
                    commit_id = Some(tag_info.commit.content_id().clone());
                    break;
                }
                _ => {}
            }
        }
        commit_id
    };

    let commit_id = commit_id.ok_or(anyhow!(
        "{} not found or not a valid source identifier",
        source
    ))?;

    let pb = ProgressBar::new_spinner();
    pb.enable_steady_tick(Duration::from_millis(50));
    pb.set_message("Branch creation in progress");

    let commit_info = repo
        .create_commit(&commit_type, &vbd_id, &commit_id)
        .await?;

    let cluster_size_bytes = *volume_info.specs.block_size() * *volume_info.specs.cluster_size();
    let cluster_size_display = ByteSize::b(cluster_size_bytes as u64);
    let size = ByteSize::b((commit_info.commit().num_clusters() * cluster_size_bytes) as u64);

    match &commit_info {
        CommitInfo::Branch(branch_name, _) => {
            pb.finish_with_message("Branch created successfully");
            println!();
            println!();
            println!("Branch Name:   {}", branch_name);
        }
        CommitInfo::Tag(tag_name, _) => {
            pb.finish_with_message("Tag created successfully");
            println!();
            println!();
            println!("Tag Name:   {}", tag_name);
        }
    }

    println!("Latest Commit: {}", commit_info.commit().content_id());
    println!("Committed at:  {}", commit_info.commit().committed());
    println!(
        "Size:          {} ({} clusters @ {})",
        size.to_string_as(true),
        commit_info.commit().num_clusters(),
        cluster_size_display.to_string_as(true)
    );
    println!();
    Ok(())
}

async fn delete_commit(
    repo: &RepositoryHandler,
    repo_name: &str,
    volume_id: String,
    commit_type: CommitType,
) -> anyhow::Result<()> {
    println!("Selected Repository: {} - {}", repo_name, repo);
    println!();

    let vbd_id =
        VbdId::try_from(volume_id.as_str()).map_err(|e| anyhow!("volume id is invalid: {}", e))?;

    let pb = ProgressBar::new_spinner();
    pb.enable_steady_tick(Duration::from_millis(50));
    pb.set_message("retrieving volume details");

    let (volume_info, mut commits) = repo.volume_details(&vbd_id).await?;
    let mut commit_info = None;

    while let Some(c) = commits.try_next().await? {
        match (&c, &commit_type) {
            (CommitInfo::Branch(existing, info), (CommitType::Branch(branch_name))) => {
                if branch_name == existing {
                    commit_info = Some(c);
                    break;
                }
            }
            (CommitInfo::Tag(existing, info), (CommitType::Tag(tag_name))) => {
                if tag_name == existing {
                    commit_info = Some(c);
                    break;
                }
            }
            _ => {}
        }
    }

    pb.finish_and_clear();

    let commit_info = commit_info.ok_or(anyhow!("branch / tag not found"))?;

    println!(
        "Selected Volume: {}",
        volume_info
            .name
            .as_ref()
            .map(|n| format!("{} ({})", n, &vbd_id))
            .unwrap_or(vbd_id.to_string())
    );
    println!();

    let cluster_size_bytes = *volume_info.specs.block_size() * *volume_info.specs.cluster_size();
    let cluster_size_display = ByteSize::b(cluster_size_bytes as u64);
    let size = ByteSize::b((commit_info.commit().num_clusters() * cluster_size_bytes) as u64);

    match &commit_info {
        CommitInfo::Branch(branch_name, _) => {
            println!("Deletion of the following Branch:");
            println!("Branch Name:   {}", branch_name);
        }
        CommitInfo::Tag(tag_name, _) => {
            println!("Deletion of the following Tag:");
            println!("Tag Name:   {}", tag_name);
        }
    }

    println!("Latest Commit: {}", commit_info.commit().content_id());
    println!("Committed at:  {}", commit_info.commit().committed());
    println!(
        "Size:          {} ({} clusters @ {})",
        size.to_string_as(true),
        commit_info.commit().num_clusters(),
        cluster_size_display.to_string_as(true)
    );
    println!();
    println!("WARNING: This operation can NOT be undone. DATA LOSS IMMINENT!");
    println!("Only proceed if you are sure you want to PERMANENTLY DELETE the above branch / tag!");
    println!();
    if !ask_confirmation("Are you sure you want to delete this (y/n)?").await {
        println!("Aborting");
        return Ok(());
    }
    let pb = ProgressBar::new_spinner();
    pb.enable_steady_tick(Duration::from_millis(50));
    pb.set_message("Deletion in progress");

    repo.delete_commit(&vbd_id, &commit_type).await?;
    pb.finish_with_message("Deletion successful");
    println!();
    println!();
    Ok(())
}

async fn ask_confirmation(question: &str) -> bool {
    let mut stdin = tokio::io::stdin();
    let mut buf: [u8; 1] = [0x00];
    loop {
        println!("{}", question);
        match stdin.read_exact(buf.as_mut_slice()).await {
            Ok(_) => {
                let resp = std::str::from_utf8(buf.as_slice()).ok().unwrap_or("");
                if resp.eq_ignore_ascii_case("y") {
                    return true;
                }
                if resp.eq_ignore_ascii_case("n") {
                    return false;
                }
                println!("only y/n accepted, please try again");
            }
            Err(_) => return false,
        }
    }
}

#[cfg(unix)]
fn shutdown_listener(guards: impl Iterator<Item = RunGuard>) -> impl Future<Output = ()> {
    use tokio::signal::unix::{signal, SignalKind};

    let mut sigint = signal(SignalKind::interrupt()).unwrap();
    let mut sigterm = signal(SignalKind::terminate()).unwrap();

    async move {
        let _guards = guards.into_iter().collect::<Vec<_>>();
        tokio::select! {
            _ = sigint.recv() => {
                tracing::info!("SIGINT received, shutting down")
            }
            _ = sigterm.recv() => {
                tracing::info!("SIGTERM received, shutting down")
            }
        }
        notify_stopping().await;
    }
}

#[cfg(windows)]
fn shutdown_listener(guards: impl Iterator<Item = RunGuard>) -> impl Future<Output = ()> {
    use tokio::signal::windows::ctrl_break;
    use tokio::signal::windows::ctrl_c;
    use tokio::signal::windows::ctrl_close;

    let mut ctrl_c = ctrl_c()?;
    let mut ctrl_close = ctrl_close()?;
    let mut ctrl_break = ctrl_break()?;

    async move {
        let _guards = guards.into_iter().collect::<Vec<_>>();
        tokio::select! {
            _ = ctrl_c.recv() => {
                tracing::info!("CTRL_C received, shutting down")
            }
            _ = ctrl_close.recv() => {
                tracing::info!("CTRL_CLOSE received, shutting down")
            }
            _ = ctrl_break.recv() => {
                tracing::info!("CTRL_BREAK received, shutting down")
            }
        }
    }
}

#[cfg(unix)]
fn uds_supported() -> bool {
    true
}

#[cfg(not(unix))]
fn uds_supported() -> bool {
    false
}

#[cfg(target_os = "linux")]
async fn notify_ready() {
    let _ = tokio::task::spawn_blocking(|| {
        let _ = sd_notify::notify(false, &[sd_notify::NotifyState::Ready]);
    })
    .await;
}

#[cfg(target_os = "linux")]
async fn notify_stopping() {
    let _ = tokio::task::spawn_blocking(|| {
        let _ = sd_notify::notify(false, &[sd_notify::NotifyState::Stopping]);
    })
    .await;
}

#[cfg(not(target_os = "linux"))]
async fn notify_ready() {}

#[cfg(not(target_os = "linux"))]
async fn notify_stopping() {}
