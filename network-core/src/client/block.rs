use chain_core::property::{Block, Deserialize};

use futures::prelude::*;

use std::{error, fmt};

/// Interface for the blockchain node service implementation responsible for
/// providing access to blocks.
pub trait BlockService<T: Block> {
    /// The type of asynchronous futures returned by method `tip`.
    ///
    /// The future resolves to the block identifier and the block date
    /// of the current chain tip as known by the serving node.
    type TipFuture: Future<Item = (T::Id, T::Date), Error = BlockError>;

    fn tip(&mut self) -> Self::TipFuture;
}

pub trait BlockServiceTodo<T: Block> {
    /// The type of an asynchronous stream that provides blocks in
    /// response to method `get_blocks`.
    type GetBlocksStream: Stream<Item = T, Error = BlockError>;

    /// The type of asynchronous futures returned by method `get_blocks`.
    ///
    /// The future resolves to a stream that will be used by the protocol
    /// implementation to produce a server-streamed response.
    type GetBlocksFuture: Future<Item = Self::GetBlocksStream, Error = BlockError>;

    /// The type of an asynchronous stream that provides block headers in
    /// response to method `get_headers`.
    type GetHeadersStream: Stream<Item = T::Header, Error = BlockError>;

    /// The type of asynchronous futures returned by method `get_headers`.
    ///
    /// The future resolves to a stream that will be used by the protocol
    /// implementation to produce a server-streamed response.
    type GetHeadersFuture: Future<Item = Self::GetHeadersStream, Error = BlockError>;

    /// The type of an asynchronous stream that provides blocks in
    /// response to method `stream_blocks_to_tip`.
    type StreamBlocksToTipStream: Stream<Item = T, Error = BlockError>;

    /// The type of asynchronous futures returned by method `stream_blocks_to_tip`.
    ///
    /// The future resolves to a stream that will be used by the protocol
    /// implementation to produce a server-streamed response.
    type StreamBlocksToTipFuture: Future<Item = Self::StreamBlocksToTipStream, Error = BlockError>;

    fn stream_blocks_to_tip(&mut self, from: &[T::Id]) -> Self::StreamBlocksToTipFuture;
}

/// Represents errors that can be returned by the node client implementation.
#[derive(Debug)]
pub enum BlockError {
    /// Error with protocol payload
    Format,
    /// An RPC error
    Rpc,
    // FIXME: add underlying error payload
}

impl error::Error for BlockError {}

impl fmt::Display for BlockError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BlockError::Format => write!(f, "malformed block received"),
            BlockError::Rpc => write!(f, "protocol error occurred"),
        }
    }
}
