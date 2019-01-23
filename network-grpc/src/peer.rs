use std::net::SocketAddr;

#[cfg(unix)]
use std::path::PathBuf;

/// Specifies the address of a remote peer.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum PeerAddr {
    /// IP address and TCP port
    Tcp(SocketAddr),
    /// Unix socket pathname
    #[cfg(unix)]
    Unix(PathBuf),
}
