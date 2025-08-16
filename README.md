# Bolt [![Package][package-img]][package-url] [![Documentation][documentation-img]][documentation-url] [![Build][build-img]][build-url]

The package provides a hierarchical read-write lock.

## Usage

Given a hierarchical path defined as a sequence of progressively nested segments
separated by slashes—as in `a/b/c` where `c` is nested in `a` and `b` and `b` in
`a`—the locking mechanism ensures that

* each writer is given access to a segment only when the segment is not
  currently being read from or written into and is not nested in a segment that
  is currently being written into, and that

* each reader is given access to a segment only when the segment is not
  currently being written into and is not nested in a segment that is currently
  being written into.

For instance, one can concurrently write into `a/b/c` and `a/b/d` and read from
`a` and `a/b`. However, reading from or writing into `a/b/c` or `a/b/d` would
have to wait for `a/b` if the last was acquired for writing, but one would be
able to read from `a`.

## Contribution

Your contribution is highly appreciated. Do not hesitate to open an issue or a
pull request. Note that any contribution submitted for inclusion in the project
will be licensed according to the terms given in [LICENSE.md](LICENSE.md).

[build-img]: https://github.com/stainless-steel/bolt/actions/workflows/build/badge.svg
[build-url]: https://github.com/stainless-steel/bolt/actions/workflows/build.yml
[documentation-img]: https://docs.rs/bolt/badge.svg
[documentation-url]: https://docs.rs/bolt
[package-img]: https://img.shields.io/crates/v/bolt.svg
[package-url]: https://crates.io/crates/bolt
