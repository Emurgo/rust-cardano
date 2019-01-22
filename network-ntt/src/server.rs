//! Network transport node, implementes an interface to the
//! chain node, based on the network-transport protocol (NT later).
//! NT is a simplified protocol that mimic CCI, a protocol
//! that support differrent quality networks and provides a high
//! level interface for the mutiplexing uni-directional connections
//! between the nodes.
//!
//! For better understanding of the network transport concept
//! you may check [document]().
//!
//! Node may act both as a server and a client. Server listens
//! on the specified port and client connects to the remote service.
//! Once node node establised a connection to the other one, the other
//! once can build an opposite connection and nodes can talk to
//! each other.
//!
//!```text
//!                                      +---- server: sock1
//!                                      |      |
//!                                      |      |-- connection1
//!                                      |      |-- connection2
//!   +--------------+     +----------+  |
//!   | network-core | ----| NT state |--+---- server: sock2
//!   +--------------+     +----------+  |      |
//!                                      |      |-- connection3
//!                                      |
//!                                      +---- client: remote-sock1
//! ```
//!
//! As NT state is shared all the services could provide a uniform
//! access to the chain state. And one could build very flexible
//! topology.
use cbor_event;
use core::marker::PhantomData;
use futures::{future, prelude::*, stream::Stream, sync::mpsc};
use network_core::server::{self, block::BlockService, transaction::TransactionService};
use protocol::{
    network_transport::LightWeightConnectionId, protocol::ChainMessage, Inbound, Message,
};
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};

/// Internal structure of network transport node.
#[derive(Clone)]
pub struct Server<N> {
    node: N,
}

/// Sets up a listening TCP socket bound to the given address.
/// If successful, returns an asynchronous stream of `TcpStream` socket.
pub fn listen(
    sockaddr: SocketAddr,
) -> Result<impl Stream<Item = TcpStream, Error = tokio::io::Error>, tokio::io::Error> {
    let listener = TcpListener::bind(&sockaddr)?;
    let stream = listener.incoming();
    Ok(stream)
}

//pub struct Connection<N,F>(F,Server<N>)
pub struct Connection<F>(F)
where
    F: future::Future<Item = (), Error = ()>;

//impl<N,F> Future for Connection<N,F>
impl<F> Future for Connection<F>
where
    F: future::Future<Item = (), Error = ()>,
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        self.0.poll()
    }
}

trait SendMsg<M> {
    fn mk_ok_msg(id: LightWeightConnectionId, msg: Self) -> M;
    fn mk_err_msg(lwcid: LightWeightConnectionId, msg: String) -> M;
}

impl<B: server::block::BlockService, Q: server::transaction::TransactionService>
    SendMsg<ChainMessage<B, Q>> for Vec<B::Header>
{
    fn mk_ok_msg(lwcid: LightWeightConnectionId, hdr: Self) -> ChainMessage<B, Q> {
        ChainMessage::BlockHeaders(
            lwcid,
            protocol::protocol::Response::Ok(protocol::protocol::BlockHeaders(hdr)),
        )
    }

    fn mk_err_msg(lwcid: LightWeightConnectionId, msg: String) -> ChainMessage<B, Q> {
        ChainMessage::BlockHeaders(lwcid, protocol::protocol::Response::Err(msg))
    }
}

/// Run a server that will listen on a specific sockets
/// and accept all incomming connections.
/// Server maintains all of the incomming connection and
/// `run_connection` is spawned on each of those connections.
pub fn accept<N: 'static, F>(
    stream: TcpStream,
    node: Server<N>,
) -> impl future::Future<Item = Connection<impl futures::future::Future>, Error = ()>
where
    N: server::Node + Clone,
    F: future::Future<Item = (), Error = ()>,
    <<N as server::Node>::BlockService as server::block::BlockService>::Block : cbor_event::de::Deserialize,
    <<N as server::Node>::BlockService as server::block::BlockService>::Block : cbor_event::se::Serialize,
    <<N as server::Node>::BlockService as server::block::BlockService>::BlockId : cbor_event::de::Deserialize,
    <<N as server::Node>::BlockService as server::block::BlockService>::BlockId : cbor_event::se::Serialize,
    <<N as server::Node>::BlockService as server::block::BlockService>::Header : cbor_event::de::Deserialize,
    <<N as server::Node>::BlockService as server::block::BlockService>::Header : cbor_event::se::Serialize,
    <<N as server::Node>::TransactionService as server::transaction::TransactionService>::TransactionId : cbor_event::de::Deserialize,
    <<N as server::Node>::TransactionService as server::transaction::TransactionService>::TransactionId : cbor_event::se::Serialize,
{
    protocol::Connection::accept(stream)
        .map_err(|_| ())
        .and_then(move |connection| {
            let node = node.clone();
            Ok(Connection(run_connection(node, connection)))
        })
}

/// Connect to another client.
/// `run_connection` is spawned on the single heavyweight
/// connection.
pub fn connect<N: 'static, F>(
    sockaddr: SocketAddr,
    node: Server<N>,
) -> impl future::Future<Item = Connection<impl futures::future::Future>, Error = ()>
where
    N: server::Node + Clone,
    F: future::Future<Item = (), Error = ()>,
    <<N as server::Node>::BlockService as server::block::BlockService>::Block : cbor_event::de::Deserialize,
    <<N as server::Node>::BlockService as server::block::BlockService>::Block : cbor_event::se::Serialize,
    <<N as server::Node>::BlockService as server::block::BlockService>::BlockId : cbor_event::de::Deserialize,
    <<N as server::Node>::BlockService as server::block::BlockService>::BlockId : cbor_event::se::Serialize,
    <<N as server::Node>::BlockService as server::block::BlockService>::Header : cbor_event::de::Deserialize,
    <<N as server::Node>::BlockService as server::block::BlockService>::Header : cbor_event::se::Serialize,
    <<N as server::Node>::TransactionService as server::transaction::TransactionService>::TransactionId : cbor_event::de::Deserialize,
    <<N as server::Node>::TransactionService as server::transaction::TransactionService>::TransactionId : cbor_event::se::Serialize,
{
    TcpStream::connect(&sockaddr)
        .map_err(move |_err| ())
        .and_then(move |stream| {
            protocol::Connection::connect(stream)
                .map_err(move |_err| ())
                .and_then(move |connection| {
                    let node = node.clone();
                    Ok(Connection(run_connection(node, connection)))
                })
        })
}

/// Method defining communication over the heavyweight connection,
/// low-level work is hidden by inside the tokio-protocol crate.
/// So we see the high-level framed protocol, with the messages
/// types that has the semantics for our application.
pub fn run_connection<N, T>(
    server: Server<N>,
    connection: protocol::Connection<T, ChainMessage<N::BlockService,N::TransactionService>>,
) -> impl future::Future<Item = (), Error = ()>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite,
    N: server::Node,
    <<N as server::Node>::BlockService as server::block::BlockService>::Block : cbor_event::de::Deserialize,
    <<N as server::Node>::BlockService as server::block::BlockService>::Block : cbor_event::se::Serialize,
    <<N as server::Node>::BlockService as server::block::BlockService>::BlockId : cbor_event::de::Deserialize,
    <<N as server::Node>::BlockService as server::block::BlockService>::BlockId : cbor_event::se::Serialize,
    <<N as server::Node>::BlockService as server::block::BlockService>::Header : cbor_event::de::Deserialize,
    <<N as server::Node>::BlockService as server::block::BlockService>::Header : cbor_event::se::Serialize,
    <<N as server::Node>::TransactionService as server::transaction::TransactionService>::TransactionId : cbor_event::de::Deserialize,
    <<N as server::Node>::TransactionService as server::transaction::TransactionService>::TransactionId : cbor_event::se::Serialize,
{
    let (sink, stream): (
        protocol_tokio::OutboundSink<T, N::BlockService, N::TransactionService>,
        protocol_tokio::InboundStream<T, N::BlockService, N::TransactionService>,
    ) = connection.split();

    let (sink_tx, sink_rx) = mpsc::unbounded();

    // Processing of the incomming messages.
    let stream = stream
        .for_each(move |inbound| {
            match inbound {
                Inbound::NothingExciting => future::Either::A(future::ok(())),
                // New lightweight connection appeared.
                Inbound::NewConnection(_lwcid) => future::Either::A(future::ok(())),
                // New node has connected to the server.
                // We accept the node, we do that immediately without
                // running future.
                Inbound::NewNode(lwcid, node_id) => {
                    sink_tx
                        .unbounded_send(Message::AckNodeId(lwcid, node_id))
                        .unwrap();
                    future::Either::A(future::ok(()))
                }
                Inbound::Subscribe(_lwcid, _keep_alive) => {
                    // TODO: implement subscription mechanism.
                    //
                    //state.subscriptions.write().unwrap().insert(
                    //    SubscriptionId(state.connection.clone(), lwcid),
                    //    sink_tx.clone(),
                    //);
                    future::Either::A(future::ok(()))
                }
                Inbound::GetBlockHeaders(lwcid, get_block_header) => {
                    let handle: ReplyHandle<
                        T,
                        ChainMessage<N::BlockService, N::TransactionService>,
                    > = ReplyHandle::new(lwcid, &sink_tx);
                    future::Either::B(future::Either::A({
                        let mut service = server
                            .node
                            .block_service()
                            .expect("block service is not implemented");
                        match get_block_header.to {
                            Some(to) => service.block_headers(&get_block_header.from, &to),
                            None => service.block_headers_to_tip(&get_block_header.from),
                        }
                        .map_err(|err| err.to_string())
                        .and_then(move |headers| {
                            Stream::collect(headers)
                                .map_err(|err| err.to_string())
                                .and_then(move |hdrs| {
                                    let msg = ChainMessage::BlockHeaders(
                                        lwcid,
                                        protocol::Response::Ok(protocol::protocol::BlockHeaders(
                                            hdrs,
                                        )),
                                    );
                                    handle
                                        .send_one(Message::UserMessage(msg))
                                        .map_err(|err| err.to_string())
                                })
                        })
                        .map(|_| ())
                        .or_else(|_| {
                            //    let msg = ChainMessage::BlockHeaders(lwcid, protocol::Response::Err(msg));
                            //    handle2.send_one(Message::UserMessage(msg)).unwrap_or_else(|_| ());
                            future::ok(())
                        })
                    }))
                }
                Inbound::GetBlocks(lwcid, get_blocks) => {
                    let sink = sink_tx.clone();
                    future::Either::B(future::Either::B(future::Either::A(
                        server
                            .node
                            .block_service()
                            .expect("block service is not implemented")
                            .stream_blocks_to(&vec![get_blocks.from], &get_blocks.to)
                            .map_err(|_| ())
                            .and_then(move |blocks| {
                                let inner1 = sink.clone();
                                let inner2 = sink.clone();
                                blocks
                                    .map_err(|_| ())
                                    .for_each(move |blk| {
                                        inner1
                                            .unbounded_send(Message::UserMessage(
                                                ChainMessage::Block(
                                                    lwcid,
                                                    protocol::Response::Ok(blk),
                                                ),
                                            ))
                                            .map_err(|_| ())
                                    })
                                    .then(move |_| {
                                        inner2.unbounded_send(Message::CloseConnection(lwcid))
                                    })
                                    .or_else(|_| Ok(()))
                            })
                            .or_else(|_| Ok(())),
                    )))
                }
                Inbound::SendTransaction(_lwcid, tx) => {
                    future::Either::B(future::Either::B(future::Either::B(
                        server
                            .node
                            .transaction_service()
                            .unwrap()
                            .propose_transactions(&vec![tx])
                            .and_then(|_| Ok(()))
                            .or_else(|_| Ok(())),
                    )))
                }
                _x => future::Either::A(future::ok(())),
            }
        })
        .map_err(|_err| ())
        .map(|_| ());

    // Processing of the outgoing messages
    let sink = sink
        .subscribe(false)
        .map_err(|_err| ())
        .and_then(move |(_lwcid, sink)| {
            sink_rx
                .fold(sink, |sink, outbound| match outbound {
                    Message::AckNodeId(_lwcid, node_id) => {
                        future::Either::A(sink.ack_node_id(node_id).map_err(|_err| ()))
                    }
                    message => future::Either::B(sink.send(message).map_err(|_err| ())),
                })
                .map(|_| ())
        })
        .map_err(|_| ());

    stream.select(sink).then(|_| Ok(()))
}

#[derive(Clone)]
struct ReplyHandle<T, M> {
    id: LightWeightConnectionId,
    sink: mpsc::UnboundedSender<Message<M>>,
    closed: bool,
    phantom: PhantomData<T>,
}

impl<T, M> ReplyHandle<T, M> {
    pub fn new(id: LightWeightConnectionId, sink: &mpsc::UnboundedSender<Message<M>>) -> Self {
        Self {
            id: id,
            sink: sink.clone(),
            closed: false,
            phantom: PhantomData,
        }
    }

    pub fn send_one(mut self, msg: Message<M>) -> Result<(), mpsc::SendError<Message<M>>> {
        self.sink.unbounded_send(msg)?;
        self.sink
            .unbounded_send(Message::CloseConnection(self.id))?;
        self.closed = true;
        Ok(())
    }
}

impl<T, M> Drop for ReplyHandle<T, M> {
    fn drop(&mut self) {
        if !self.closed {
            self.sink
                .unbounded_send(Message::CloseConnection(self.id))
                .unwrap_or_default();
        }
    }
}
