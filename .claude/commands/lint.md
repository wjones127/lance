Check everything in the repo.

First, fix Rust:

1. Run `cargo clippy --all --tests --benches --examples --fix --allow-dirty`. This should fix
   lints that can be handled automatically.
1. Run `cargo clippy --all --tests --benches --examples` to see if there are remaining issues.
2. Fix any lints.
3. Run `cargo fmt --all` to format the code.

Then, do Python:

1. Go to `python/` directory
2. Run `make lint`
3. Fix any issues.
4. Run `make format`
