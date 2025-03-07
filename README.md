# Sia Virtual Block Device (sia_vbd)

## Description

`sia_vbd` implements virtual block devices backed by [Sia Decentralized data storage](https://sia.tech).

Essentially, it provides virtual disks that are location-independent, can grow to almost any size, allow snapshots,
branching, and are deduplicated and compressed.

## Features

- **`NBD` *(Network Block Device)* support**
- **Cross-Platform:** Runs on every platform where [renterd](https://sia.tech/software/renterd) is available.
- **Immutability:** Writes never modify existing data; any change leads to a new overall state (`Snapshot`). Previously
  held data remains available (until eventual garbage collection).
- **Content-Addressed Storage:** All data is hashed and identified by its content ID for integrity and deduplication.
- **Content Compression:** Transparently compresses content (`Zstd`) before uploading.
- **Branching & Tagging:** Volumes support multiple active branches. Tagging and retaining specific states is also
  possible.
- **Transactional Writes:** Atomic writes with automatic rollback on failure.
- **Write-Ahead Logging:** Records transactions in a local, durable `WAL` before being committed to eventual storage.
- **Caching:** Recently accessed content is cached locally, improving read performance significantly. The cache is
  persistent and survives process restarts.
- **Crash Tolerant:** Detects when the local, WAL-recorded state is ahead of the committed backend state.
- **Background Synchronization:** Continuously uploads new data to the backend in the background, allowing fast writes
  and avoids blocking reads.
- **Automatic Garbage Collection:** Unreferenced data will be deleted eventually.
- **Resizing:** Volumes can be freely resized on a per-branch basis.
- **Multiple Block Devices and Backends:** Supports multiple block devices, across one or more `renterd` instances.
- **Single Binary, Single Process:** Delivered as a single, self-contained binary that runs as a single
  process, making deployment easy and straightforward.
- **Highly Configurable:** While coming with reasonable default settings, `sia_vbd` offers many additional options to
  configure and fine-tune.
- **CLI Interface:** Includes an easy-to-use CLI for common operations.
- **`Docker` and `systemd` support**,

## Possible Future Improvements

- **Latency mitigating `renterd` read strategies:** Reading data from `renterd` can be slow due to the highly variable
  access latency (`TTFB`). This is exacerbated by the fact that `sia_vbd` has to do many short, random reads. Together,
  this often leads to very low total throughput - at least for content that is not cached yet. This limits the
  usefulness of `sia_vbd`. Some approaches could help reduce this effect to a certain extend. That being said, there
  **are** limits due to the basic fact that access latency is orders of magnitudes higher compared to typical block
  devices.
- **Repacking:** Live data from `Chunks` with a high percentage of obsolete entries can be added to newly written
  `Chunks` whenever there is space to spare, allowing older `Chunks` to be deleted more eagerly.

## Status

**Version 0.4.0**: `sia_vbd` is fully functional at this point.

This release is a fully functional version of `sia_vbd`. Everything has been implemented in accordance with
its [initial proposal](https://forum.sia.tech/t/small-grant-sia-virtual-block-device-sia-vbd/743).

A modern, fully-featured `NBD` server has been implemented and tested against Linux's built-in client as well as
against [nbdublk](https://libguestfs.org/nbdublk.1.html) - a modern userland `nbd` client backed
by [ublk](https://docs.kernel.org/block/ublk.html) & [nbdkit](https://www.libguestfs.org/nbdkit.1.html).

`ext4` and `xfs` have been used during testing and both work well.

## Usage

### Prerequisites

- API access to a `renterd` instance
- An empty directory on an existing bucket
- an `NBD` client

The `sia_vbd` binary is both, the CLI and the server:

```
:~$ sia_vbd --help
Exports a Virtual Block Device via NBD.

If no commands are specified, the server process will run and export volumes as configured.

Usage: sia_vbd --config <CONFIG> [COMMAND]

Commands:
  repos   List all configured repositories
  volume  Volume related actions
  branch  Branch related actions
  tag     Tag related actions
  help    Print this message or the help of the given subcommand(s)

Options:
  -c, --config <CONFIG>
          Path to Config File
          
          [env: CONFIG=]

  -h, --help
          Print help (see a summary with '-h')

  -V, --version
          Print version
```

### Configuration

`sia_vbd` uses a single [TOML](https://en.wikipedia.org/wiki/TOML) file for its configuration.

Example:

```
# General server configuration
# At least one [server.<name>] entry is requried. 
# Multiple server processes can be configured simultaneously
# hence the mandatory .<name> part.
[server.nbd]
type = "nbd"
tcp = { host = "localhost", port = 5112 }
#unix = "/path/to/uds.socket"  # on Unix platforms sia_vbd can listen on a UDS
#max_connections = # number of maximum simultaneous client connections

# Repository configuration
# At least one [repository.<name>] entry is requried. 
# Multiple repositories can be configured
[repository.local-renterd]
renterd_endpoint = "http://localhost:9980/api/"
api_password = "<password>"
bucket = "<name of bucket>"
path = "<full path of directory in bucket>"
```

Something like this would be a basic configuration with a single `Repository`.
The configuration can be tested like this (assuming `sia_vbd.toml` is the name of the config file):

```
:~$ sia_vbd --config sia_vbd.toml repos
The following repositories are currently configured:

local-renterd: renterd [endpoint=http://localhost:9980/api/, bucket=<bucket>, path=<path>>]

```

The `repos` command lists all currently configured `Repositories`.

### Creating a Volume

Now the `volume` command can be used to create, list and delete volumes:

```
:~$ sia_vbd --config sia_vbd.toml volume --help
Volume related actions

Usage: sia_vbd --config <CONFIG> volume <COMMAND>

Commands:
  list    List all volumes in a given repository
  create  Create a new volume
  delete  Delete a specific volume
  help    Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
```

```
:~$ sia_vbd --config sia_vbd.toml volume create --help
Create a new volume

Usage: sia_vbd volume create [OPTIONS] <REPO> <SIZE>

Arguments:
  <REPO>  Name of repository
  <SIZE>  Size of Volume

Options:
  -n <NAME>              Optional descriptive name
  -d <BRANCH>            Name of the default branch [default: main]
  -b <BLOCK_SIZE>        Block size in KiB. Possible values are: 16, 64, 256, 1024 [default: 64]
  -c <CLUSTER_SIZE>      Cluster size, in number of Blocks. Possible values are: 64, 128, 256 [default: 256]
  -o <CONTENT_HASH>      Hash Algorithm to use for Block Content Hashing. Possible values are: blake3, tent, xxh128 [default: blake3]
  -m <META_HASH>         Hash Algorithm to use for Metadata Hashing. Possible values are: blake3, tent, xxh128 [default: blake3]
  -h, --help             Print help
```

Only `<REPO>` and `<SIZE>` are mandatory. However, specifying a name via `-n` is recommended.
Creating a volume looks like this:

```
:~$ sia_vbd --config sia_vbd.toml volume create -n MyVolume1 local-renterd 10GiB
Selected Repository: local-renterd: renterd [endpoint=http://localhost:9980/api/, bucket=<bucket>, path=<path>>]

Creation of new Volume with the following specifications:
Name: MyVolume1
Size: 10.0 GiB
Default Branch Name: main
Block Size: 64KiB
Cluster Size: 256
Content Hash Algorithm: BLAKE3
Metadata Hash Algorithm: BLAKE3

Do you want to continue (y/n)?
y
  Volume created successfully                                                                                                                                                       

01951f48-907c-7160-9f8e-42503e762e32
    Name:            MyVolume1
    Created at:      2025-02-19 17:36:44.158980 UTC
    Block Size:      64KiB
    Cluster Size:    256
    Content Hash:    BLAKE3
    Metadata Hash:   BLAKE3
    Branches & Tags:
            Branch Name: main
          Latest Commit: 0e327a1a93a1495eab55605431af0e3a739640e7f776810c3c5858904e976fed
           Committed at: 2025-02-19 17:36:44.163708 UTC
                   Size: 10.0 GiB (640 clusters @ 16.0 MiB)
          ------------------------------------------

You can now add this volume to your configuration:

[[volume]]
repository = "local-renterd"
volume_id = "01951f48-907c-7160-9f8e-42503e762e32"
export_server = "nbd"
export_name = "<export as>"
wal = "<path to wal directory>"
inventory = "<path to inventory directory>"
cache = "<path to cache directory>"
```

The `[[volume]]` section can now be added to the config file:

```
[[volume]]
repository = "local-renterd"
volume_id = "01951f48-907c-7160-9f8e-42503e762e32"
export_server = "nbd"
export_name = "myvolume1"
wal = "/path/to/durable/storage/"
inventory = "/path/to/fast/storage/"
cache = "/path/to/large/storage/"
```

Three directories need to be specified here: `wal`, `inventory` and `cache`.

- `wal`: Location of the `Write-Ahead Log`. This needs to be on **persistent, durable** storage. Losing the WAL can lead
  to **data loss** under certain circumstances!
- `inventory`: Path for storing runtime-related data. This **should** be on fast storage, e.g. an SSD.
  While generally recommended to keep it around, the data in this directory will be automatically rebuilt on startup
  if lost.
- `cache`: Directory holding the persistent content cache.

Now the server process can be started:

```
:~$ sia_vbd --config sia_vbd.toml
```

Connecting a client, e.g. Linux kernel client:

```
sudo nbd-client -N myvolume1 localhost 5112 /dev/nbd0
```

The fully Sia-backed virtual block device is now accessible at `/dev/nbd0`.

The client can be disconnected like this:

```
sudo nbd-client -d /dev/nbd0
```

`sia_vbd` can now be shut down cleanly. Any pending data will be uploaded to `renterd` automatically during shutdown, so
this may take some time. Make sure the process is not killed prematurely.

## Configuration Options

### Volume

- **repository:** Name of `Repository` this `Volume` is located in. *Required*
- **volume_id:** `UUID` of `Volume`, e.g. 01951f48-907c-7160-9f8e-42503e762e32. *Required*
- **export_server:** Name of `Server` this `Volume` should be exported with. *Required*
- **export_name:** Name the `Volume` should be accessible under. *Required*
- **wal:** Directory where `WAL` related data should be stored. *Required*
- **max_wal_size:** Maximum size of a single `WAL` file. *Optional, default: `128MiB`*
- **max_tx_size:** Maximum size of a single write transaction before it gets committed to the `WAL`, *Optional,
  default: `16MiB`*
- **max_chunk_size:** Maximum size of a single `Chunk`. *Optional, default `40MiB`*
- **inventory:** Directory where `Inventory` related data is stored. *Required*
- **cache:** Directory where `Cache` related data is stored. *Required*
- **cache_max_memory:** Maximum size of L1 (in-memory) content cache. *Optional, default: `64MiB`*
- **cache_max_disk:** Maximum size of L2 (on-disk) content cache. *Optional, default: `4GiB`*
- **max_db_connections:** Maximum number of simultaneous database connections. *Optional, default: `25`*
- **branch:** Name of the `Branch` to use. *Optional, default: `main`*
- **initial_sync_delay:** Delay after startup before running background synchronisation. *Optional, default: `60s`
- **sync_interval:** Interval at which to run background synchronisation. *Optional, default `300s`*
- **read_only:** `Volume` should be exported `read-only`. *Optional, default `false`*

### Repository

- **renterd_endpoint:** URL pointing to `renterd` API-endpoint. *Required*
- **api_password:** Password for API access. *Required*
- **bucket:** `Bucket` the `Repository` is stored in. *Required*
- **path:** Path pointing to the directory the `Repository` is stored in. *Required*

### Server

- **tcp:** Address the `Server` should listen on, specified via `host` and `port`.
- **unix:** Path of `Unix Domain Socket` the `Server` should listen on. **Not** available on Windows.
- **max_connections:** Maximum number of simultaneous client connections. *Optional, default: `no limit`*

*Please Note:* Either `tcp` or `unix` has to be specified. Listening on both simultaneously is **not** supported.

## Concepts & Terminology

### Block

`sia_vbd` stores data in `Blocks`. A `Block` consists of a fixed-size payload and its `BlockId`, which is essentially a
hash of the payload. The same content will therefore lead to the same `BlockId` - this is the basis of `sia_vbd`'s
deduplication capability.

### Cluster

A `Cluster` is an intermediate data structure that makes the block device more manageable. A `Cluster` consists of only
two elements: a fixed-length list of `BlockIds` and a `ClusterId`, which is a hash of all `BlockIds`.

### Snapshot

`Snapshots` represent the full state of the block device. Similar to `Clusters`, `Snapshots` contain a list of
`ClusterIds` as well as a `SnapshotId`, which - you guessed it - is essentially a hash of the contained `ClusterIds`.
The
same block device state will always lead to the same `SnapshotId`.

### Commit

`Commits` are *unique* and represent a block device's state at a given time. `CommitIds` are also derived from the
content they contain.

### Branch

A `Branch` is a named reference to a `Commit`. This reference is updated whenever a new `Commit` occurs on the selected
`Branch`.

### Tag

Similar to a `Branch`, a `Tag` is a reference to a specific `Commit`. However, unlike `Branches`, `Tags` are immutable
and cannot be modified or instantiated.

### Volume

All the above elements together form a `Volume`. `Volumes` have a fixed `UUID` and certain immutable properties, such as
block size and hash algorithms used.

### Repository

`Volume` data is stored in a `Repository`. `Repositories` can contain multiple `Volumes` and are stored
via `renterd` at a specified bucket and path.

### Chunk

`Blocks`, `Clusters`, and `Snapshots` are stored in compressed `Chunks`. New `Chunks` are uploaded to the `Repository`
whenever they reach a certain configurable size (default: 40 MiB) or during a clean shutdown. `Chunks` that contain no
relevant data are eventually deleted.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as
defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.

## Acknowledgements

This project has been made possible by the [Sia Foundation's Grant program](https://sia.tech/grants). 
