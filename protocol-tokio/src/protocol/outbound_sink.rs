use futures::{future, stream::SplitSink, Future, Poll, Sink, StartSend};
use std::{
    io,
    sync::{Arc, Mutex},
};
use tokio_io::AsyncWrite;

use super::{nt, ConnectionState, KeepAlive, LightWeightConnectionState, Message, NodeId};
use network_core::server;
use std::marker::PhantomData;

pub type Outbound<B,T> = Message<B,T>;

#[derive(Debug)]
pub enum OutboundError {
    IoError(io::Error),
    Unknown,
}
impl From<()> for OutboundError {
    fn from(_: ()) -> Self {
        OutboundError::Unknown
    }
}
impl From<io::Error> for OutboundError {
    fn from(e: io::Error) -> Self {
        OutboundError::IoError(e)
    }
}

pub struct OutboundSink<T, B, Q> {
    sink: SplitSink<nt::Connection<T>>,
    state: Arc<Mutex<ConnectionState>>,
    block: PhantomData<B>,
    transaction: PhantomData<Q>,
}
impl<T,B,Q> OutboundSink<T,B,Q> {
    fn get_next_light_id(&mut self) -> nt::LightWeightConnectionId {
        self.state.lock().unwrap().get_next_light_id()
    }

    fn get_next_node_id(&mut self) -> NodeId {
        self.state.lock().unwrap().get_next_node_id()
    }
}
impl <T: AsyncWrite, B: server::block::BlockService, Q: server::transaction::TransactionService> OutboundSink<T,B,Q>
where
    <B as server::block::BlockService>::BlockId: cbor_event::Deserialize,
    <B as server::block::BlockService>::BlockId: cbor_event::Serialize,
    <B as server::block::BlockService>::Block: cbor_event::Deserialize,
    <B as server::block::BlockService>::Block: cbor_event::Serialize,
    <B as server::block::BlockService>::Header: cbor_event::Deserialize,
    <B as server::block::BlockService>::Header: cbor_event::Serialize,
    <Q as server::transaction::TransactionService>::TransactionId: cbor_event::Serialize,
    <Q as server::transaction::TransactionService>::TransactionId: cbor_event::Deserialize,
{
    pub fn new(sink: SplitSink<nt::Connection<T>>, state: Arc<Mutex<ConnectionState>>) -> Self {
        OutboundSink { sink, state, block:PhantomData, transaction:PhantomData }
    }

    /// create a new light weight connection with the remote peer
    ///
    pub fn new_light_connection(
        mut self,
    ) -> impl Future<Item = (nt::LightWeightConnectionId, Self), Error = OutboundError> {
        let lwcid = self.get_next_light_id();
        let node_id = self.get_next_node_id();

        self.send(Message::CreateLightWeightConnectionId(lwcid))
            .and_then(move |connection| connection.send(Message::CreateNodeId(lwcid, node_id)))
            .and_then(move |connection| {
                let light_weight_connection_state = LightWeightConnectionState::new(lwcid)
                    .remote_initiated(false)
                    .with_node_id(node_id);

                connection
                    .state
                    .lock()
                    .unwrap()
                    .client_handles
                    .insert(lwcid, light_weight_connection_state);

                future::ok((lwcid, connection))
            })
    }

    /// initialize a subscription from the given outbound halve.
    pub fn subscribe(
        self,
        keep_alive: KeepAlive,
    ) -> impl Future<Item = (nt::LightWeightConnectionId, Self), Error = OutboundError> {
        self.new_light_connection()
            .and_then(move |(lwcid, connection)| {
                connection
                    .send(Message::Subscribe(lwcid, keep_alive))
                    .map(move |connection| (lwcid, connection))
            })
    }

    /// close a light connection that has been created with
    /// `new_light_connection`.
    ///
    pub fn close_light_connection(
        self,
        lwcid: nt::LightWeightConnectionId,
    ) -> impl Future<Item = Self, Error = OutboundError> {
        self.send(Message::CloseConnection(lwcid))
            .and_then(move |connection| {
                connection
                    .state
                    .lock()
                    .unwrap()
                    .client_handles
                    .remove(&lwcid);
                future::ok(connection)
            })
    }

    /// this function it to acknowledge the creation of the NodeId on the remote
    /// client side
    pub fn ack_node_id(
        mut self,
        node_id: NodeId,
    ) -> impl Future<Item = Self, Error = OutboundError> {
        let our_lwcid = self.get_next_light_id();

        self.send(Message::CreateLightWeightConnectionId(our_lwcid))
            .and_then(move |connection| connection.send(Message::AckNodeId(our_lwcid, node_id)))
            .map(move |connection| {
                // here we need to wire the acknowledged NodeId to our new created client LWCID
                connection
                    .state
                    .lock()
                    .unwrap()
                    .map_to_client
                    .insert(node_id, our_lwcid);
                connection
            })
    }
}

impl<T: AsyncWrite, B: server::block::BlockService, Q: server::transaction::TransactionService> Sink for OutboundSink<T, B, Q>
where
    <B as server::block::BlockService>::BlockId: cbor_event::Deserialize,
    <B as server::block::BlockService>::BlockId: cbor_event::Serialize,
    <B as server::block::BlockService>::Block: cbor_event::Deserialize,
    <B as server::block::BlockService>::Block: cbor_event::Serialize,
    <B as server::block::BlockService>::Header: cbor_event::Deserialize,
    <B as server::block::BlockService>::Header: cbor_event::Serialize,
    <Q as server::transaction::TransactionService>::TransactionId: cbor_event::Serialize,
    <Q as server::transaction::TransactionService>::TransactionId: cbor_event::Deserialize,
{
    type SinkItem = Outbound<B,Q>;
    type SinkError = OutboundError;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        self.sink
            .start_send(item.to_nt_event())
            .map_err(OutboundError::IoError)
            .map(|async| async.map(Message::from_nt_event))
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        self.sink.poll_complete().map_err(OutboundError::IoError)
    }

    fn close(&mut self) -> Poll<(), Self::SinkError> {
        self.sink.close().map_err(OutboundError::IoError)
    }
}
