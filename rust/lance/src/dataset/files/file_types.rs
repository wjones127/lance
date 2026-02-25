// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

#[derive(Debug, Clone, Copy)]
pub enum FileType {
    Manifest,
    DataFile,
    DeletionFile,
    TransactionFile,
    IndexFile,
}

impl std::fmt::Display for FileType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Manifest => "manifest",
            Self::DataFile => "data file",
            Self::DeletionFile => "deletion file",
            Self::TransactionFile => "transaction file",
            Self::IndexFile => "index file",
        };
        write!(f, "{s}")
    }
}

impl From<FileType> for i8 {
    fn from(file_type: FileType) -> Self {
        match file_type {
            FileType::Manifest => 0,
            FileType::DataFile => 1,
            FileType::DeletionFile => 2,
            FileType::TransactionFile => 3,
            FileType::IndexFile => 4,
        }
    }
}
