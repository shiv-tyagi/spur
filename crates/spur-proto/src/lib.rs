// Copyright (c) 2026 Advanced Micro Devices, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

pub mod proto {
    tonic::include_proto!("slurm");
}

pub mod raft_proto {
    tonic::include_proto!("raft_internal");
}

pub use proto::*;

use tonic::transport::Channel;

/// Maximum size of a single client-facing gRPC message (request or response),
/// in bytes. tonic defaults both encode and decode limits to 4 MiB, which large
/// `GetJobs`/`GetNodes` responses and forwarded submissions can exceed on big
/// clusters. Construct clients and servers through the helpers below so this
/// limit is applied consistently.
pub const MAX_GRPC_MESSAGE_SIZE: usize = 32 * 1024 * 1024;

/// Controller client with message-size limits raised to `MAX_GRPC_MESSAGE_SIZE`.
pub fn controller_client(
    channel: Channel,
) -> proto::slurm_controller_client::SlurmControllerClient<Channel> {
    proto::slurm_controller_client::SlurmControllerClient::new(channel)
        .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE)
}

/// Accounting client with message-size limits raised to `MAX_GRPC_MESSAGE_SIZE`.
pub fn accounting_client(
    channel: Channel,
) -> proto::slurm_accounting_client::SlurmAccountingClient<Channel> {
    proto::slurm_accounting_client::SlurmAccountingClient::new(channel)
        .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE)
}

/// Controller server with message-size limits raised to `MAX_GRPC_MESSAGE_SIZE`.
pub fn controller_server<T: proto::slurm_controller_server::SlurmController>(
    service: T,
) -> proto::slurm_controller_server::SlurmControllerServer<T> {
    proto::slurm_controller_server::SlurmControllerServer::new(service)
        .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE)
}

/// Agent server with message-size limits raised to `MAX_GRPC_MESSAGE_SIZE`.
pub fn agent_server<T: proto::slurm_agent_server::SlurmAgent>(
    service: T,
) -> proto::slurm_agent_server::SlurmAgentServer<T> {
    proto::slurm_agent_server::SlurmAgentServer::new(service)
        .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE)
}

/// Accounting server with message-size limits raised to `MAX_GRPC_MESSAGE_SIZE`.
pub fn accounting_server<T: proto::slurm_accounting_server::SlurmAccounting>(
    service: T,
) -> proto::slurm_accounting_server::SlurmAccountingServer<T> {
    proto::slurm_accounting_server::SlurmAccountingServer::new(service)
        .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE)
}
