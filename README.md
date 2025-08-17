# Bolt [![Package][package-img]][package-url] [![Documentation][documentation-img]][documentation-url] [![Build][build-img]][build-url]

The package provides a hierarchical read-write lock.

Given a hierarchical path defined as a sequence of progressively nested segments
separated by slashes—as in `a/b/c` where `c` is nested in `b` and `a`, and `b`
is nested in `a`—the locking mechanism ensures that

* a writer is given access to the path only when the last segment is not
  currently being read from or written into and is not nested in any segment
  that is currently being written into, and that

* a reader is given access to the path only when the last segment is not
  currently being written into and is not nested in any segment that is
  currently being written into.

For instance, one can concurrently write into `a/b/c` and `a/b/d` and read from
`a` and `a/b`. However, reading from or writing into `a/b/c` or `a/b/d` would
have to wait for `a/b` if the last was acquired for writing, but one would be
able to read from `a`.

# Usage

```rust
const N: usize = 10;

let lock = std::sync::Arc::new(bolt::Lock::<N>::default());

{
    let lock = lock.clone();
    tokio::task::spawn(async move {
        let _guards = lock.write("a/b/c").await;
    });
}

{
    let lock = lock.clone();
    tokio::task::spawn(async move {
        let _guards = lock.write("a/b/d").await;
    });
}
```

## Contribution

Your contribution is highly appreciated. Do not hesitate to open an issue or a
pull request. Note that any contribution submitted for inclusion in the project
will be licensed according to the terms given in [LICENSE.md](LICENSE.md).

[build-img]: https://github.com/stainless-steel/bolt/actions/workflows/build.yml/badge.svg
[build-url]: https://github.com/stainless-steel/bolt/actions/workflows/build.yml
[documentation-img]: https://docs.rs/bolt/badge.svg
[documentation-url]: https://docs.rs/bolt
[package-img]: https://img.shields.io/crates/v/bolt.svg
[package-url]: https://crates.io/crates/bolt
