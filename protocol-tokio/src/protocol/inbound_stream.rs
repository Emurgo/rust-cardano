use super::{
    nt, ConnectionState, KeepAlive, LightWeightConnectionState, Message, NodeId, Response,ChainMessage
};
use super::{BlockHeaders, GetBlockHeaders, GetBlocks};
use bytes::Bytes;
use futures::{stream::SplitStream, Async, Poll, Stream};
use std::{
    io,
    sync::{Arc, Mutex},
};
use tokio_io::AsyncRead;
use network_core::server;
use std::marker::PhantomData;

#[derive(Debug)]
pub enum InboundError {
    /// low level error from the low level protocol
    EventParsingError(nt::DecodeEventError),

    /// error from the I/O layer
    IoError(io::Error),

    /// the connection stopped
    ConnectionTerminated,

    /// this will happen if the remote peer creates a light
    /// that is still open.
    RemoteCreatedDuplicatedLightConnection(nt::LightWeightConnectionId),

    RemoteLightConnectionIdUnknown(nt::LightWeightConnectionId),

    RemoteLightConnectionIdNotLinkedToLocalClientId(nt::LightWeightConnectionId),

    RemoteLightConnectionIdNotLinkedToKnownLocalClientId(nt::LightWeightConnectionId, NodeId),
}
impl From<io::Error> for InboundError {
    fn from(e: io::Error) -> Self {
        InboundError::IoError(e)
    }
}
impl From<nt::DecodeEventError> for InboundError {
    fn from(e: nt::DecodeEventError) -> Self {
        InboundError::EventParsingError(e)
    }
}

#[derive(Debug)]
pub enum Inbound<B,T>
where
    B: server::block::BlockService,
    T: server::transaction::TransactionService,
{
    NothingExciting,
    NewConnection(nt::LightWeightConnectionId),

    // need to call Connection::ack_node_id(node_id)
    NewNode(nt::LightWeightConnectionId, NodeId),
    GetBlockHeaders(nt::LightWeightConnectionId, GetBlockHeaders<B::Header>),
    BlockHeaders(nt::LightWeightConnectionId, Response<BlockHeaders<B::Header>, String>),
    GetBlocks(nt::LightWeightConnectionId, GetBlocks<B::Block>),
    Block(nt::LightWeightConnectionId, Response<B::Block, String>),
    SendTransaction(nt::LightWeightConnectionId, T::TransactionId),
    TransactionReceived(nt::LightWeightConnectionId, Response<bool, String>),
    Subscribe(nt::LightWeightConnectionId, KeepAlive),
    Data(nt::LightWeightConnectionId, Bytes),
}

pub struct InboundStream<T,B,Q> {
    stream: SplitStream<nt::Connection<T>>,
    state: Arc<Mutex<ConnectionState>>,
    block: PhantomData<B>,
    transaction: PhantomData<Q>,
}
impl<T: AsyncRead, B: server::block::BlockService, Q: server::transaction::TransactionService> Stream for InboundStream<T, B, Q>
{
    type Item = Inbound<B,Q>;
    type Error = InboundError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match try_ready!(self.stream.poll()) {
            None => Ok(Async::Ready(None)),
            Some(event) => match self.process_event(event) {
                Err(err) => Err(err),
                Ok(inbound) => Ok(Async::Ready(Some(inbound))),
            },
        }
    }
}
impl<T,B: server::block::BlockService,Q: server::transaction::TransactionService> InboundStream<T,B,Q> {
    pub fn new(stream: SplitStream<nt::Connection<T>>, state: Arc<Mutex<ConnectionState>>) -> Self {
        InboundStream { stream, state, block:PhantomData, transaction:PhantomData }
    }

    /// this function will inbound events
    ///
    /// On success, the future will return the `Inbound` message and the connection
    /// so we can perform more actions later one with it.
    ///
    /// On Error, the future will return the error that happened and the connection
    /// so we can decide wether to stop the connection or to recover from it.
    ///
    fn process_event(&mut self, event: nt::Event) -> Result<Inbound<B,Q>, InboundError> 
    {
        match Message::from_nt_event(event) {
            Message::CreateLightWeightConnectionId(lwcid) => {
                self.process_new_light_connection(lwcid)
            }
            Message::CloseConnection(lwcid) => self.process_close_light_connection(lwcid),
            Message::CloseEndPoint(lwcid) => {
                unimplemented!("Close end point not implemented ({:?}", lwcid)
            }
            Message::CloseSocket(lwcid) => {
                unimplemented!("Close socket command not implemented {:?}", lwcid)
            }
            Message::ProbeSocket(lwcid) => {
                unimplemented!("Probe socket command not implemented {:?}", lwcid)
            }
            Message::ProbeSocketAck(lwcid) => {
                unimplemented!("Probe socket ack not implemented {:?}", lwcid)
            }
            Message::CreateNodeId(lwcid, node_id) => self.process_create_node_id(lwcid, node_id),
            Message::AckNodeId(lwcid, node_id) => self.process_ack_node_id(lwcid, node_id),
            Message::UserMessage(ChainMessage::GetBlockHeaders(lwcid, gbh)) => {
                self.forward_message(lwcid, Inbound::GetBlockHeaders, gbh)
            }
            Message::UserMessage(ChainMessage::BlockHeaders(lwcid, bh)) => {
                self.forward_message(lwcid, Inbound::BlockHeaders, bh)
            }
            Message::UserMessage(ChainMessage::SendTransaction(lwcid, st)) => {
                self.forward_message(lwcid, Inbound::SendTransaction, st)
            }
            Message::UserMessage(ChainMessage::TransactionReceived(lwcid, r)) => {
                self.forward_message(lwcid, Inbound::TransactionReceived, r)
            }
            Message::UserMessage(ChainMessage::GetBlocks(lwcid, gb)) => self.forward_message(lwcid, Inbound::GetBlocks, gb),
            Message::UserMessage(ChainMessage::Block(lwcid, b)) => self.forward_message(lwcid, Inbound::Block, b),
            Message::UserMessage(ChainMessage::Subscribe(lwcid, keep_alive)) => {
                self.forward_message(lwcid, Inbound::Subscribe, keep_alive)
            }
            Message::Bytes(lwcid, bytes) => self.forward_message(lwcid, Inbound::Data, bytes),
        }
    }

    fn process_new_light_connection(
        &mut self,
        lwcid: nt::LightWeightConnectionId,
    ) -> Result<Inbound<B,Q>, InboundError> {
        let mut state = self.state.lock().unwrap();
        if state.server_handles.contains_key(&lwcid) {
            Err(InboundError::RemoteCreatedDuplicatedLightConnection(lwcid))
        } else {
            let light_weight_connection_state =
                LightWeightConnectionState::new(lwcid).remote_initiated(true);

            state
                .server_handles
                .insert(lwcid, light_weight_connection_state);
            Ok(Inbound::NewConnection(lwcid))
        }
    }

    fn process_close_light_connection(
        &mut self,
        lwcid: nt::LightWeightConnectionId,
    ) -> Result<Inbound<B,Q>, InboundError> {
        let mut state = self.state.lock().unwrap();
        let result = state.server_handles.remove(&lwcid);
        match result {
            None => Ok(Inbound::NothingExciting),
            Some(light_state) => {
                if let Some(ref node_id) = &light_state.node {
                    if let Some(client_connection_id) = state.map_to_client.remove(node_id) {
                        if let Some(client_connection) =
                            state.client_handles.get_mut(&client_connection_id)
                        {
                            client_connection.remote_close = true;
                        }
                    }
                }
                Ok(Inbound::NothingExciting)
            }
        }
    }

    fn process_create_node_id(
        &mut self,
        lwcid: nt::LightWeightConnectionId,
        node_id: NodeId,
    ) -> Result<Inbound<B,Q>, InboundError> {
        let mut state = self.state.lock().unwrap();
        if let Some(light_connection) = state.server_handles.get_mut(&lwcid) {
            light_connection.node = Some(node_id);
            Ok(Inbound::NewNode(lwcid, node_id))
        } else {
            Err(InboundError::RemoteLightConnectionIdUnknown(lwcid))
        }
    }

    fn process_ack_node_id(
        &mut self,
        lwcid: nt::LightWeightConnectionId,
        node_id: NodeId,
    ) -> Result<Inbound<B,Q>, InboundError> {
        let mut state = self.state.lock().unwrap();
        match state.server_handles.get_mut(&lwcid) {
            None => {
                return Err(InboundError::RemoteLightConnectionIdUnknown(lwcid));
            }
            Some(ref mut light_state) => {
                light_state.node = Some(node_id);
            }
        }
        let client = state
            .client_handles
            .iter()
            .find(|&(_, v)| v.node == Some(node_id))
            .map(|(z, _)| *z);
        if let Some(z) = client {
            state.map_to_client.insert(node_id, z);
            Ok(Inbound::NothingExciting)
        } else {
            Err(InboundError::RemoteLightConnectionIdNotLinkedToKnownLocalClientId(lwcid, node_id))
        }
    }

    fn forward_message<A, F>(
        &mut self,
        lwcid: nt::LightWeightConnectionId,
        f: F,
        t: A,
    ) -> Result<Inbound<B,Q>, InboundError>
    where
        F: FnOnce(nt::LightWeightConnectionId, A) -> Inbound<B,Q>,
    {
        let state = self.state.lock().unwrap();
        let light_connection = state.server_handles.get(&lwcid).cloned();
        match light_connection {
            None => Err(InboundError::RemoteLightConnectionIdUnknown(lwcid)),
            Some(light_state) => {
                if light_state.remote_initiated {
                    Ok(f(lwcid, t))
                } else {
                    match light_state.node {
                        None => Err(
                            InboundError::RemoteLightConnectionIdNotLinkedToLocalClientId(lwcid),
                        ),
                        Some(node_id) => {
                            let client_id = { state.map_to_client.get(&node_id).cloned() };
                            match client_id {
                                None => { Err(InboundError::RemoteLightConnectionIdNotLinkedToKnownLocalClientId(lwcid, node_id)) },
                                Some(client_id) => {
                                    if state.client_handles.contains_key(&client_id) {
                                        Ok(f(lwcid, t))
                                    } else {
                                        unimplemented!()
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
