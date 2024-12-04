# Sia Virtual Block Device (sia_vbd)

## Description

`sia_vbd` implements virtual block devices backed by [Sia Decentralized data storage](https://sia.tech). 

Essentially, it provides virtual disks that are location-independent, can grow to almost any size, allow snapshots, branching, and are deduplicated and compressed.

## Features

- **NBD *(Network Block Device)* support**
- **Cross-Platform:** Runs on every platform where [renterd](https://sia.tech/software/renterd) is available.
- **Open Source:** Released under the Apache-2.0 and MIT licenses.
- ***More features soon***

## Status

**Early WIP**: Nothing much to see here currently.

A very basic `NBD` server has been implemented and tested against Linux's built-in client.

*Check back in a few weeks for something more substantial*

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as
defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.

## Acknowledgements

This project has been made possible by the [Sia Foundation's Grant program](https://sia.tech/grants). 
