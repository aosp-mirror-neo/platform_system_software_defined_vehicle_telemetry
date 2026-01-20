// Copyright (C) 2026 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! This module contains async functions for file read and write operations
use std::path::Path;
use tokio::fs::File;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::{time, time::Duration};

const MAX_ATTEMPTS: u8 = 3;
const RETRY_DELAY: Duration = Duration::from_millis(500);

async fn run_file_io<Fut, T>(file_io_fn: impl Fn() -> Fut) -> io::Result<T>
where
    Fut: std::future::Future<Output = io::Result<T>>,
{
    let mut attempt = 1;

    loop {
        match file_io_fn().await {
            Ok(result) => return Ok(result),
            Err(err) if attempt >= MAX_ATTEMPTS => return Err(err),
            Err(_) => {
                attempt += 1;
                time::sleep(RETRY_DELAY).await;
            }
        }
    }
}

/// async function that creates and writes the contents to a file
pub async fn write_to_file(file_path: &Path, contents: &[u8]) -> io::Result<()> {
    run_file_io(|| async {
        let mut file = File::create(file_path).await?;
        file.write_all(contents).await?;
        file.flush().await?;
        Ok(())
    })
    .await
}

/// async function that reads the contents of a file into a binary vector
pub async fn read_from_file(file_path: &Path) -> io::Result<Vec<u8>> {
    run_file_io(|| async {
        let mut file = File::open(file_path).await?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).await?;
        Ok(bytes)
    })
    .await
}

/// async function that removes a file
pub async fn remove_file(file_path: &Path) -> io::Result<()> {
    run_file_io(|| tokio::fs::remove_file(file_path)).await
}

/// async function that renames a file
pub async fn rename_file(from_path: &Path, to_path: &Path) -> io::Result<()> {
    run_file_io(|| tokio::fs::rename(from_path, to_path)).await
}

/// async function that creates a new directory
pub async fn create_dir(dir_path: &Path) -> io::Result<()> {
    run_file_io(|| tokio::fs::create_dir_all(dir_path)).await
}

/// async function that removes a directory
pub async fn remove_dir(dir_path: &Path) -> io::Result<()> {
    run_file_io(|| tokio::fs::remove_dir_all(dir_path)).await
}
