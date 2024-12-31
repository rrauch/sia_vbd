# Sia Virtual Block Device (sia_vbd)

## Description

`sia_vbd` implements virtual block devices backed by [Sia Decentralized data storage](https://sia.tech). 

Essentially, it provides virtual disks that are location-independent, can grow to almost any size, allow snapshots, branching, and are deduplicated and compressed.

## Features

- **`NBD` *(Network Block Device)* support**
- **Cross-Platform:** Runs on every platform where [renterd](https://sia.tech/software/renterd) is available.
- **Open Source:** Released under the Apache-2.0 and MIT licenses.
- ***More features soon***

## Status

**Milestone 1**: A working, testable virtual block.

This release incorporates the main design aspects as presented and discussed in its [initial proposal](https://forum.sia.tech/t/small-grant-sia-virtual-block-device-sia-vbd/743).
However, `blocks`, `chunks` & `states` are currently stored in-memory only and are not yet backed by the eventual `renterd` backend. Also, no `GC` is implemented at this point,
the block device will grow indefinitely.

That being said, this release of `sia_vbd` provides a fully functional and performant deduplicating & branching block device.

A modern, fully-featued `NBD` server has been implemented and tested against Linux's built-in client as well as 
agains [nbdublk](https://libguestfs.org/nbdublk.1.html) - a modern userland `nbd` client backed 
by [ublk](https://docs.kernel.org/block/ublk.html) & [nbdkit](https://www.libguestfs.org/nbdkit.1.html).

`ext4` and `xfs` have been used during testing and both work well.

*This release is to be considered an early preview and is **for testing purposes only**. Data is **NOT** persistent in this release!*
Anything stored on it **WILL BE GONE** once `sia_vbd` is shut down.

## Usage

```bash
:~$ sia_vbd --help
Exports a Virtual Block Device via NBD

Usage: sia_vbd [OPTIONS] --size <SIZE>

Options:
  -b, --block-size <BLOCK_SIZE>
          Block size in KiB. Possible values are: 16, 64, 256 [default: 64]
  -c, --cluster-size <CLUSTER_SIZE>
          Cluster size, in number of Blocks. Possible values are: 256 [default: 256]
  -a, --content-hash <CONTENT_HASH>
          Hash Algorithm to use for Block Content Hashing. Possible values are: blake3, xxh128, tent0_4 [default: blake3]
  -m, --meta-hash <META_HASH>
          Hash Algorithm to use for Metadata Hashing. Possible values are: blake3, xxh128, tent0_4 [default: blake3]
  -x, --max-buffer-size <MAX_BUFFER_SIZE>
          Maximum write buffer size [default: "128 MiB"]
  -l, --listen-address <LISTEN_ADDRESS>
          URL of address to listen on. For tcp, the format is 'tcp://host:port'. On unix platforms, unix domain sockets are also supported: 'unix:///path/to/socket' [default: tcp://localhost:5112]
  -s, --size <SIZE>
          Desired size of the virtual block device e.g. `10 GiB`
  -e, --export-name <EXPORT_NAME>
          NBD export name [default: sia_vbd]
  -h, --help
          Print help
  -V, --version
          Print version
```

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as
defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.

## Acknowledgements

This project has been made possible by the [Sia Foundation's Grant program](https://sia.tech/grants). 
