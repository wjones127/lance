// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

#[derive(Debug)]
pub struct InternalError {
    pub message: String,
    pub backtrace: std::backtrace::Backtrace,
}

impl std::error::Error for InternalError {}

impl std::fmt::Display for InternalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "InternalError: Please file a bug report at https://github.com/lance-format/lance/issues."
        )?;
        write!(f, "{}\nBacktrace:\n{}", self.message, self.backtrace)
    }
}
