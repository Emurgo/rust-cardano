use bytes::{BufMut, Bytes, BytesMut};
use std::{
    io::{BufRead, Cursor, Write},
    ops::Deref,
};

//use cardano::{
//    block::{self, HeaderHash},
//    tx::TxAux,
//};
use cbor_event::{
    self,
    de::{self, Deserializer},
    se::{self, Serializer},
};

use super::super::nt;
use super::NodeId;
use network_core::server;

pub type MessageCode = u32;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum MessageType {
    MsgGetHeaders = 4,
    MsgHeaders = 5,
    MsgGetBlocks = 6,
    MsgBlock = 7,
    MsgSubscribe1 = 13,
    MsgSubscribe = 14,
    MsgStream = 15,
    MsgStreamBlock = 16,
    MsgAnnounceTx = 37, // == InvOrData key TxMsgContents
    MsgTxMsgContents = 94,
}
impl MessageType {
    #[inline]
    pub fn encode_with<T>(&self, cbor: &T) -> Bytes
    where
        T: se::Serialize,
    {
        let mut bytes = se::Serializer::new_vec();
        bytes.serialize(self).unwrap().serialize(cbor).unwrap();
        let bytes = bytes.finalize();
        bytes.into()
    }
}
impl se::Serialize for MessageType {
    fn serialize<'se, W: Write>(
        &self,
        serializer: &'se mut Serializer<W>,
    ) -> cbor_event::Result<&'se mut Serializer<W>> {
        serializer.serialize(&(*self as u32))
    }
}
impl de::Deserialize for MessageType {
    fn deserialize<R: BufRead>(raw: &mut Deserializer<R>) -> cbor_event::Result<Self> {
        let v = raw.unsigned_integer()? as u32;
        match v {
            4 => Ok(MessageType::MsgGetHeaders),
            5 => Ok(MessageType::MsgHeaders),
            6 => Ok(MessageType::MsgGetBlocks),
            7 => Ok(MessageType::MsgBlock),
            13 => Ok(MessageType::MsgSubscribe1),
            14 => Ok(MessageType::MsgSubscribe),
            15 => Ok(MessageType::MsgStream),
            16 => Ok(MessageType::MsgStreamBlock),
            37 => Ok(MessageType::MsgAnnounceTx),
            93 => Ok(MessageType::MsgTxMsgContents),
            v => {
                return Err(cbor_event::Error::CustomError(format!(
                    "Unsupported message type: {:20x}",
                    v
                )));
            }
        }
    }
}

pub type KeepAlive = bool;

#[derive(Clone, Debug)]
pub enum Message<M>
{
    CreateLightWeightConnectionId(nt::LightWeightConnectionId),
    CloseConnection(nt::LightWeightConnectionId),
    CloseEndPoint(nt::LightWeightConnectionId),
    CloseSocket(nt::LightWeightConnectionId),
    ProbeSocket(nt::LightWeightConnectionId),
    ProbeSocketAck(nt::LightWeightConnectionId),
    CreateNodeId(nt::LightWeightConnectionId, NodeId),
    AckNodeId(nt::LightWeightConnectionId, NodeId),
    Bytes(nt::LightWeightConnectionId, Bytes),

    UserMessage(M),
}

pub enum ChainMessage<B,T>
where
    B: server::block::BlockService,
    T: server::transaction::TransactionService,
{
    GetBlockHeaders(nt::LightWeightConnectionId, GetBlockHeaders<B::BlockId>),
    BlockHeaders(nt::LightWeightConnectionId, Response<B::Header, String>),
    GetBlocks(nt::LightWeightConnectionId, GetBlocks<B::Header>),
    Block(nt::LightWeightConnectionId, Response<B::Block, String>),
    SendTransaction(nt::LightWeightConnectionId, T::TransactionId),
    TransactionReceived(nt::LightWeightConnectionId, Response<bool, String>),
    Subscribe(nt::LightWeightConnectionId, KeepAlive),
}

trait AsNtEvent
where
    Self: std::marker::Sized
{
    fn to_nt(self) -> nt::Event;
    fn try_from_nt(lwcid: nt::LightWeightConnectionId, bytes: &Bytes) -> Option<Self>; 
}

pub struct Any();

impl AsNtEvent for Any
{
    fn to_nt(self) -> nt::Event {
        unimplemented!()
    }

    fn try_from_nt(lwcid: nt::LightWeightConnectionId, bytes: &Bytes) -> Option<Self> {
        None
    }
}

impl<B: server::block::BlockService,T: server::transaction::TransactionService> AsNtEvent for ChainMessage<B,T>
where
    <B as server::block::BlockService>::BlockId: cbor_event::Deserialize,
    <B as server::block::BlockService>::BlockId: cbor_event::Serialize,
    <B as server::block::BlockService>::Block: cbor_event::Deserialize,
    <B as server::block::BlockService>::Block: cbor_event::Serialize,
    <B as server::block::BlockService>::Header: cbor_event::Deserialize,
    <B as server::block::BlockService>::Header: cbor_event::Serialize,
    <T as server::transaction::TransactionService>::TransactionId: cbor_event::Serialize,
    <T as server::transaction::TransactionService>::TransactionId: cbor_event::Deserialize,
{

    fn to_nt(self) -> nt::Event {
        use self::nt::{Event::*};
        match self {
            ChainMessage::GetBlockHeaders(lwcid, gbh) =>
                Data(lwcid, MessageType::MsgGetHeaders.encode_with(&gbh)),
            ChainMessage::BlockHeaders(lwcid, bh) =>
                Data(lwcid, cbor!(&bh).unwrap().into()),
            ChainMessage::GetBlocks(lwcid, gb) =>
                Data(lwcid, MessageType::MsgGetBlocks.encode_with(&gb)),
            ChainMessage::Block(lwcid, b) =>
                Data(lwcid, cbor!(&b).unwrap().into()),
            ChainMessage::SendTransaction(lwcid, tx) =>
                Data(lwcid, cbor!(&tx).unwrap().into()),
            ChainMessage::TransactionReceived(lwcid, rep) =>
                Data(lwcid, cbor!(&rep).unwrap().into()),
            ChainMessage::Subscribe(lwcid, keep_alive) => {
                let keep_alive: u64 = if keep_alive { 43 } else { 42 };
                Data(lwcid, MessageType::MsgSubscribe1.encode_with(&keep_alive))
            }
        }
    }

    fn try_from_nt(lwcid: nt::LightWeightConnectionId, bytes: &Bytes) -> Option<Self> {
        let mut cbor = Deserializer::from(Cursor::new(bytes.deref()));
        let msg_type: MessageType = cbor.deserialize().unwrap();
        match msg_type {
            MessageType::MsgGetHeaders => Some(ChainMessage::GetBlockHeaders(
                lwcid,
                cbor.deserialize_complete().unwrap(),
            )),
            MessageType::MsgHeaders => Some(ChainMessage::BlockHeaders(
                lwcid,
                cbor.deserialize_complete().unwrap(),
            )),
            MessageType::MsgGetBlocks => Some(ChainMessage::GetBlocks(
                lwcid,
                cbor.deserialize_complete().unwrap(),
            )),
            MessageType::MsgBlock =>
                Some(ChainMessage::Block(lwcid, cbor.deserialize_complete().unwrap())),
            MessageType::MsgSubscribe1 => {
                let v: u64 = cbor.deserialize_complete().unwrap();
                let keep_alive = v == 43;
                Some(ChainMessage::Subscribe(lwcid, keep_alive))
            },
            _ => unimplemented!(),
        }
    }
}


impl<M: AsNtEvent> Message<M> {
    pub fn to_nt_event(self) -> nt::Event
    {
        use self::nt::{ControlHeader::*, Event::*};
        match self {
            Message::CreateLightWeightConnectionId(lwcid) => Control(CreateNewConnection, lwcid),
            Message::CloseConnection(lwcid) => Control(CloseConnection, lwcid),
            Message::CloseEndPoint(lwcid) => Control(CloseEndPoint, lwcid),
            Message::CloseSocket(lwcid) => Control(CloseSocket, lwcid),
            Message::ProbeSocket(lwcid) => Control(ProbeSocket, lwcid),
            Message::ProbeSocketAck(lwcid) => Control(ProbeSocketAck, lwcid),
            Message::CreateNodeId(lwcid, node_id) => {
                let mut bytes = BytesMut::with_capacity(9);
                bytes.put_u8(0x53);
                bytes.put_u64_be(*node_id);
                Data(lwcid, bytes.freeze())
            }
            Message::AckNodeId(lwcid, node_id) => {
                let mut bytes = BytesMut::with_capacity(9);
                bytes.put_u8(0x41);
                bytes.put_u64_be(*node_id);
                Data(lwcid, bytes.freeze())
            }
            Message::UserMessage(msg) => msg.to_nt(),
            Message::Bytes(lwcid, bytes) => Data(lwcid, bytes),
        }
    }

    pub fn from_nt_event(event: nt::Event) -> Self
    {
        Message::expect_control(event)
            .or_else(Message::expect_bytes)
            .expect("If this was not a control it was a data related message")
    }

    pub fn expect_control(event: nt::Event) -> Result<Self, nt::Event> {
        use self::nt::ControlHeader;

        let (ch, lwcid) = event.expect_control()?;
        Ok(match ch {
            ControlHeader::CreateNewConnection => Message::CreateLightWeightConnectionId(lwcid),
            ControlHeader::CloseConnection => Message::CloseConnection(lwcid),
            ControlHeader::CloseEndPoint => Message::CloseEndPoint(lwcid),
            ControlHeader::CloseSocket => Message::CloseSocket(lwcid),
            ControlHeader::ProbeSocket => Message::ProbeSocket(lwcid),
            ControlHeader::ProbeSocketAck => Message::ProbeSocketAck(lwcid),
        })
    }

    pub fn expect_bytes(event: nt::Event) -> Result<Self, nt::Event>
    {
        let (lwcid, bytes) = event.expect_data()?;
        if let Some(msg) = decode_node_ack_or_syn(lwcid, &bytes) {
            return Ok(msg);
        }
        if let Some(msg) = AsNtEvent::try_from_nt(lwcid, &bytes) {
            return Ok(Message::UserMessage(msg));
        }
        Err(event)
    }
}

pub fn raw_msg_to_nt(msg: Message<Any>) ->  nt::Event {
    msg.to_nt_event()
}

fn decode_node_ack_or_syn<M>(lwcid: nt::LightWeightConnectionId, bytes: &Bytes) -> Option<Message<M>> {
    use bytes::{Buf, IntoBuf};
    if bytes.len() != 9 {
        return None;
    }
    let mut buf = bytes.into_buf();
    let key = buf.get_u8();
    let v = buf.get_u64_be();
    match key {
        0x53 => Some(Message::CreateNodeId(lwcid, NodeId::from(v))),
        0x41 => Some(Message::AckNodeId(lwcid, NodeId::from(v))),
        _ => None,
    }
}

#[derive(Clone, Debug)]
pub enum Response<T, E> {
    Ok(T),
    Err(E),
}
impl<T: se::Serialize, E: se::Serialize> se::Serialize for Response<T, E> {
    fn serialize<'se, W: Write>(
        &self,
        serializer: &'se mut Serializer<W>,
    ) -> cbor_event::Result<&'se mut Serializer<W>> {
        let serializer = serializer.write_array(cbor_event::Len::Len(2))?;
        match self {
            &Response::Ok(ref t) => serializer.serialize(&0u64)?.serialize(t),
            &Response::Err(ref e) => serializer.serialize(&1u64)?.serialize(e),
        }
    }
}
impl<T: de::Deserialize, E: de::Deserialize> de::Deserialize for Response<T, E> {
    fn deserialize<R: BufRead>(raw: &mut Deserializer<R>) -> cbor_event::Result<Self> {
        raw.tuple(2, "protocol::Response")?;
        let id = raw.deserialize()?;
        match id {
            0u64 => Ok(Response::Ok(raw.deserialize()?)),
            1u64 => Ok(Response::Err(raw.deserialize()?)),
            v => Err(cbor_event::Error::CustomError(format!(
                "Invalid Response Enum header expected 0 or 1 but got {}",
                v
            ))),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct GetBlockHeaders<T> {
    pub from: Vec<T>,
    pub to: Option<T>,
}
impl<T> GetBlockHeaders<T>
{
    pub fn is_tip(&self) -> bool {
        self.from.is_empty() && self.to.is_none()
    }
    pub fn tip() -> Self {
        GetBlockHeaders {
            from: Vec::new(),
            to: None,
        }
    }
    pub fn range(from: Vec<T>, to: T) -> Self {
        GetBlockHeaders {
            from: from,
            to: Some(to),
        }
    }
}
impl<T: cbor_event::Serialize> se::Serialize for GetBlockHeaders<T> {
    fn serialize<'se, W: Write>(
        &self,
        serializer: &'se mut Serializer<W>,
    ) -> cbor_event::Result<&'se mut Serializer<W>> {
        let serializer = serializer.write_array(cbor_event::Len::Len(2))?;
        let serializer = se::serialize_indefinite_array(self.from.iter(), serializer)?;
        match &self.to {
            &None => serializer.write_array(cbor_event::Len::Len(0)),
            &Some(ref h) => serializer
                .write_array(cbor_event::Len::Len(1))?
                .serialize(h),
        }
    }
}

impl<T: de::Deserialize> de::Deserialize for GetBlockHeaders<T> {
    fn deserialize<R: BufRead>(raw: &mut Deserializer<R>) -> cbor_event::Result<Self> {
        raw.tuple(2, "GetBlockHeader")?;
        let from = raw.deserialize()?;
        let to = {
            let len = raw.array()?;
            match len {
                cbor_event::Len::Len(0) => None,
                cbor_event::Len::Len(1) => {
                    let h = raw.deserialize()?;
                    Some(h)
                }
                len => {
                    return Err(cbor_event::Error::CustomError(format!(
                        "Len {:?} not supported for the `GetBlockHeader.to`",
                        len
                    )));
                }
            }
        };
        Ok(GetBlockHeaders { from: from, to: to })
    }
}

#[derive(Clone, Debug)]
pub struct BlockHeaders<T>(pub Vec<T>);
impl<T> From<Vec<T>> for BlockHeaders<T> {
    fn from(v: Vec<T>) -> Self {
        BlockHeaders(v)
    }
}

impl<T> se::Serialize for BlockHeaders<T>
where
    T: cbor_event::Serialize,
{
    fn serialize<'se, W: Write>(
        &self,
        serializer: &'se mut Serializer<W>,
    ) -> cbor_event::Result<&'se mut Serializer<W>> {
        se::serialize_fixed_array(self.0.iter(), serializer)
    }
}

impl<T: cbor_event::Deserialize> de::Deserialize for BlockHeaders<T> {
    fn deserialize<R: BufRead>(raw: &mut Deserializer<R>) -> cbor_event::Result<Self> {
        raw.deserialize().map(BlockHeaders)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetBlocks<T> {
    pub from: T,
    pub to: T,
}
impl<T> GetBlocks<T> {
    pub fn new(from: T, to: T) -> Self {
        GetBlocks { from, to }
    }
}
impl<T: se::Serialize> se::Serialize for GetBlocks<T> {
    fn serialize<'se, W: Write>(
        &self,
        serializer: &'se mut Serializer<W>,
    ) -> cbor_event::Result<&'se mut Serializer<W>> {
        serializer
            .write_array(cbor_event::Len::Len(2))?
            .serialize(&self.from)?
            .serialize(&self.to)
    }
}
impl<T: de::Deserialize> de::Deserialize for GetBlocks<T> {
    fn deserialize<R: BufRead>(raw: &mut Deserializer<R>) -> cbor_event::Result<Self> {
        raw.tuple(2, "GetBlockHeader")?;
        let from = raw.deserialize()?;
        let to = raw.deserialize()?;
        Ok(GetBlocks::new(from, to))
    }
}

#[cfg(test)]
fn random_headerhash<G: ::quickcheck::Gen>(g: &mut G) -> HeaderHash {
    let bytes: Vec<u8> = ::quickcheck::Arbitrary::arbitrary(g);
    HeaderHash::new(&bytes)
}

#[cfg(test)]
fn random_to<G: ::quickcheck::Gen>(g: &mut G) -> Option<HeaderHash> {
    let value: Option<()> = ::quickcheck::Arbitrary::arbitrary(g);
    value.map(|_| random_headerhash(g))
}

#[cfg(test)]
fn random_from<G: ::quickcheck::Gen>(g: &mut G) -> Vec<HeaderHash> {
    let num: usize = ::quickcheck::Arbitrary::arbitrary(g);
    ::std::iter::repeat_with(|| random_headerhash(g))
        .take(num)
        .collect()
}

#[cfg(test)]
impl ::quickcheck::Arbitrary for GetBlockHeaders {
    fn arbitrary<G: ::quickcheck::Gen>(g: &mut G) -> Self {
        let from = random_from(g);
        let to = random_to(g);
        GetBlockHeaders { from: from, to: to }
    }
}

#[cfg(test)]
impl ::quickcheck::Arbitrary for GetBlocks {
    fn arbitrary<G: ::quickcheck::Gen>(g: &mut G) -> Self {
        let from = random_headerhash(g);
        let to = random_headerhash(g);
        GetBlocks { from: from, to: to }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    quickcheck! {
        fn get_block_headers_encode_decode(command: GetBlockHeaders) -> bool {
            let encoded = cbor!(command).unwrap();
            let mut de = Deserializer::from(Cursor::new(&encoded));
            let decoded : GetBlockHeaders = de.deserialize_complete().unwrap();

            decoded == command
        }

        fn get_blocks_encode_decode(command: GetBlocks) -> bool {
            let encoded = cbor!(command).unwrap();
            let mut de = Deserializer::from(Cursor::new(&encoded));
            let decoded : GetBlocks = de.deserialize_complete().unwrap();

            decoded == command
        }
    }
}
