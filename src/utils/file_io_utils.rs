// Copyright 2023 Google LLC. All Rights Reserved.

//! This module contains sync functions for file read and write operations
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};

/// async function that creates and writes the contents to a file
pub async fn write_to_file(file_path: &PathBuf, contents: &[u8]) -> io::Result<()> {
    let mut file = File::create(file_path).await?;
    file.write_all(contents).await?;
    Ok(())
}

/// async function that reads the contents of a file into a binary vector
pub async fn read_from_file(file_path: &PathBuf) -> io::Result<Vec<u8>> {
    let mut file = File::open(file_path).await?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).await?;
    Ok(bytes)
}

/// async function that removes a file
pub async fn remove_file(file_path: &PathBuf) -> io::Result<()> {
    tokio::fs::remove_file(file_path).await?;
    Ok(())
}

/// async function that renames a file
pub async fn rename_file(from_path: &PathBuf, to_path: &PathBuf) -> io::Result<()> {
    tokio::fs::rename(from_path, to_path).await?;
    Ok(())
}

/// async function that creates a new directory
pub async fn create_dir(dir_path: &PathBuf) -> io::Result<()> {
    tokio::fs::create_dir_all(dir_path).await?;
    Ok(())
}

/// async function that removes a directory
pub async fn remove_dir(dir_path: &PathBuf) -> io::Result<()> {
    tokio::fs::remove_dir_all(dir_path).await?;
    Ok(())
}
