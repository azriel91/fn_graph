# Development

## Dependencies

```bash
rustup component add llvm-tools-preview
cargo install cargo-llvm-cov
cargo install cargo-nextest
```


## Running Tests

```bash
cargo nextest run
cargo nextest run --features "fn_meta"
cargo nextest run --features "resman"
cargo nextest run --features "resman fn_meta"
```


## Coverage

Collect coverage and output as `html`.

```bash
./coverage.sh && cargo coverage_open
```

Collect coverage and output as `lcov`.

```bash
./coverage.sh
```


## Releasing

Update crate versions, then push a tag to the repository. The [`publish`] GitHub workflow will automatically publish the crates to [`crates.io`].

[`publish`]: https://github.com/azriel91/rt_map/actions/workflows/publish.yml
[`crates.io`]:https://crates.io/
