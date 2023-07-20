// Copyright 2023 Google LLC. All Rights Reserved.

use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};

pub async fn write_to_file(file_path: &PathBuf, contents: &[u8]) -> io::Result<()> {
    let mut file = File::create(file_path).await?;
    file.write_all(contents).await?;
    Ok(())
}

pub async fn read_from_file(file_path: &PathBuf) -> io::Result<Vec<u8>> {
    let mut file = File::open(file_path).await?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).await?;
    Ok(bytes)
}

pub async fn remove_file(file_path: &PathBuf) -> io::Result<()> {
    tokio::fs::remove_file(file_path).await?;
    Ok(())
}

pub async fn create_dir(dir_path: &PathBuf) -> io::Result<()> {
    tokio::fs::create_dir_all(dir_path).await?;
    Ok(())
}

#[allow(dead_code)]
pub async fn remove_dir(dir_path: &PathBuf) -> io::Result<()> {
    tokio::fs::remove_dir_all(dir_path).await?;
    Ok(())
}
