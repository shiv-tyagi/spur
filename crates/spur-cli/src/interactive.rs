// Copyright (c) 2026 Advanced Micro Devices, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result};
use spur_proto::proto::slurm_agent_client::SlurmAgentClient;
use spur_proto::proto::{interactive_input, interactive_output, InitSession, InteractiveInput};
use std::collections::HashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::signal::unix::{signal, SignalKind};

pub fn get_terminal_size() -> spur_proto::proto::WindowSize {
    let (cols, rows) = crossterm::terminal::size().unwrap_or((80, 24));
    spur_proto::proto::WindowSize {
        rows: rows as u32,
        cols: cols as u32,
        xpixel: 0,
        ypixel: 0,
    }
}

/// Run a full interactive PTY session over the InteractiveSession RPC.
/// Returns the remote exit code.
pub async fn run_interactive_session(
    agent: &mut SlurmAgentClient<tonic::transport::Channel>,
    job_id: u32,
    step_id: u32,
    argv: Vec<String>,
    winsize: spur_proto::proto::WindowSize,
    overlap: bool,
) -> Result<i32> {
    let init = InteractiveInput {
        msg: Some(interactive_input::Msg::Init(InitSession {
            job_id,
            step_id,
            overlap,
            pty: true,
            winsize: Some(winsize),
            argv,
            env: HashMap::new(),
        })),
    };

    let (in_tx, in_rx) = tokio::sync::mpsc::channel::<InteractiveInput>(64);
    in_tx.send(init).await.ok();

    let in_stream = tokio_stream::wrappers::ReceiverStream::new(in_rx);
    let response = agent
        .interactive_session(in_stream)
        .await
        .context("InteractiveSession RPC failed")?;

    let mut out_stream = response.into_inner();

    let _raw_guard = RawModeGuard::enter().ok();

    let mut sigwinch = signal(SignalKind::window_change())?;

    let mut stdout = tokio::io::stdout();
    let mut stdin = tokio::io::stdin();
    let mut stdin_buf = vec![0u8; 4096];

    let exit_code: i32 = loop {
        tokio::select! {
            msg = out_stream.message() => {
                match msg {
                    Ok(Some(output)) => {
                        match output.msg {
                            Some(interactive_output::Msg::Data(data)) => {
                                stdout.write_all(&data).await?;
                                stdout.flush().await?;
                            }
                            Some(interactive_output::Msg::ExitStatus(code)) => {
                                break code;
                            }
                            None => {}
                        }
                    }
                    Ok(None) => break 1,
                    Err(e) => {
                        eprintln!("\r\nstream error: {e}");
                        break 1;
                    }
                }
            }

            n = stdin.read(&mut stdin_buf) => {
                match n {
                    Ok(0) => break 0,
                    Ok(n) => {
                        let _ = in_tx.send(InteractiveInput {
                            msg: Some(interactive_input::Msg::Stdin(
                                stdin_buf[..n].to_vec(),
                            )),
                        }).await;
                    }
                    Err(_) => break 1,
                }
            }

            _ = sigwinch.recv() => {
                let ws = get_terminal_size();
                let _ = in_tx.send(InteractiveInput {
                    msg: Some(interactive_input::Msg::Resize(ws)),
                }).await;
            }
        }
    };

    drop(_raw_guard);

    Ok(exit_code)
}

/// RAII guard that puts the terminal into raw mode and restores it on drop.
pub struct RawModeGuard;

impl RawModeGuard {
    pub fn enter() -> Result<Self> {
        crossterm::terminal::enable_raw_mode().context("enable raw mode")?;
        Ok(Self)
    }
}

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        let _ = crossterm::terminal::disable_raw_mode();
    }
}
