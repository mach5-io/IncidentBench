// Copyright 2025 Mach5 Software, Inc.
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

use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tracing::{debug, warn};

const HEALTH_RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";

/// Start a minimal HTTP health server on the given port.
/// Responds 200 OK to any request on any path.
/// Runs until the task is cancelled.
pub async fn serve(port: u16) {
    let addr = format!("0.0.0.0:{}", port);
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            warn!("Failed to bind health server on {}: {}", addr, e);
            return;
        }
    };
    debug!("Health server listening on {}", addr);

    loop {
        match listener.accept().await {
            Ok((mut stream, _)) => {
                tokio::spawn(async move {
                    // Read the request (we don't care about the content).
                    let mut buf = [0u8; 512];
                    let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut buf).await;
                    let _ = stream.write_all(HEALTH_RESPONSE).await;
                    let _ = stream.shutdown().await;
                });
            }
            Err(e) => {
                warn!("Health server accept error: {}", e);
            }
        }
    }
}
