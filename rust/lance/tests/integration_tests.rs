// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

// NOTE: we only create one integration test binary, to keep compilation overhead down.

#[cfg(feature = "slow_tests")]
mod query;
#[cfg(feature = "slow_tests")]
mod utils;
#[cfg(feature = "slow_tests")]
mod vector_index;
