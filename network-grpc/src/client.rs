use crate::gen::{self, node::client as gen_client};
use crate::peer::PeerAddr;

use chain_core::property::{Block, Deserialize};
use network_core::client::block::{BlockError, BlockService};

use futures::future::Executor;
use tokio::prelude::*;
use tokio::{
    io,
    net::tcp::{ConnectFuture, TcpStream},
};
use tower_grpc::{BoxBody, Request};
use tower_h2::client::{Background, Connect, ConnectError, Connection};
use tower_util::MakeService;

use std::{error, fmt, str::FromStr};

/// gRPC client for blockchain node.
///
/// This type encapsulates the gRPC protocol client that can
/// make connections and perform requests towards other blockchain nodes.
pub struct Client<S, E> {
    node: gen_client::Node<Connection<S, E, BoxBody>>,
}

impl<S, E> Client<S, E>
where
    S: AsyncRead + AsyncWrite,
    E: Executor<Background<S, BoxBody>> + Clone,
{
    pub fn connect<P>(peer: P, executor: E) -> impl Future<Item = Self, Error = Error>
    where
        P: tokio_connect::Connect<Connected = S, Error = io::Error> + 'static,
    {
        let mut make_client = Connect::new(peer, Default::default(), executor);
        make_client
            .make_service(())
            .map_err(|e| Error::Connect(e))
            .map(|conn| {
                // TODO: add origin URL with add_origin middleware from tower-http

                Client {
                    node: gen_client::Node::new(conn),
                }
            })
    }
}

fn deserialize_buf<T>(buf: &[u8]) -> Result<T, BlockError>
where
    T: Deserialize,
{
    T::deserialize(&mut buf).map_err(|e| BlockError::Format)
}

fn deserialize_str<T>(s: &str) -> Result<T, BlockError>
where
    T: FromStr,
{
    T::from_str(s).map_err(|e| BlockError::Format)
}

fn convert_tip_response<T>(res: gen::node::TipResponse) -> Result<(T::Id, T::Date), BlockError>
where
    T: Block,
    <T as Block>::Id: Deserialize,
    <T as Block>::Date: FromStr,
{
    let id = deserialize_buf(&res.id)?;
    let blockdate = deserialize_str(&res.blockdate)?;
    Ok((id, blockdate))
}

impl<T, S, E> BlockService<T> for Client<S, E>
where
    T: Block,
    S: AsyncRead + AsyncWrite,
    E: Executor<Background<S, BoxBody>> + Clone,
{
    type TipFuture = tower_grpc::client::unary::ResponseFuture<
        gen::node::TipResponse,
        tower_h2::client::ResponseFuture,
        tower_h2::RecvBody,
    >;

    fn tip(&mut self) -> Self::TipFuture {
        let req = gen::node::TipRequest {};
        self.node
            .tip(Request::new(req))
            .map_err(|e| BlockError::Rpc)
            .and_then(|res| convert_tip_response(res))
    }
}

impl tokio_connect::Connect for PeerAddr {
    type Connected = TcpStream;
    type Error = io::Error;
    type Future = ConnectFuture;

    fn connect(&self) -> Self::Future {
        match self {
            PeerAddr::Tcp(ref addr) => TcpStream::connect(addr),
            #[cfg(unix)]
            PeerAddr::Unix(_) => unimplemented!(), // FIXME: would need different Connected type
        }
    }
}

/// The error type for gRPC client operations.
#[derive(Debug)]
pub enum Error {
    Connect(ConnectError<io::Error>),
}

impl From<ConnectError<io::Error>> for Error {
    fn from(err: ConnectError<io::Error>) -> Self {
        Error::Connect(err)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Connect(e) => write!(f, "connection error: {}", e),
        }
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Error::Connect(e) => Some(e),
        }
    }
}
