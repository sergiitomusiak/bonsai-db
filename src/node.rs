use anyhow::{anyhow, Result};
use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::u64;

use crate::free_list::FreeList;
use crate::{
    format::{read_u16, read_u32, read_u64, write_u16, write_u32, write_u64},
    tx::TransactionId,
};

const BRANCH_NODE: u16 = 1;
const LEAF_NODE: u16 = 2;
pub const FREELIST_NODE: u16 = 3;
pub const MIN_KEYS_PER_PAGE: usize = 2;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BranchInternalNode {
    pub key: Vec<u8>,
    pub node_id: NodeId,
}

impl BranchInternalNode {
    fn size(&self) -> u64 {
        (
            // page id
            size_of::<u64>() +
            // key len field
            size_of::<u16>() +
            // key
            self.key.len()
        ) as u64
    }

    fn read<R: Read>(reader: &mut R) -> Result<Self> {
        let address = read_u64(reader)?;
        let key_len = read_u16(reader)? as usize;
        let mut key = vec![0; key_len];
        reader.read_exact(&mut key)?;
        Ok(Self {
            key,
            node_id: NodeId::Address(address),
        })
    }

    fn write<W: Write>(&self, writer: &mut W) -> Result<u64> {
        let size = std::mem::size_of::<u64>() + std::mem::size_of::<u16>() + self.key.len();
        write_u64(writer, self.node_id.node_address())?;
        write_u16(writer, self.key.len() as u16)?;
        writer.write_all(&self.key)?;
        Ok(size as u64)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeafInternalNode {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl LeafInternalNode {
    fn size(&self) -> u64 {
        (
            // key len field
            size_of::<u16>() +
            // key
            self.key.len() +
            // value len field
            size_of::<u32>() +
            // value
            self.value.len()
        ) as u64
    }

    fn read<R: Read>(reader: &mut R) -> Result<Self> {
        // read key
        let key_len = read_u16(reader)? as usize;
        let mut key = vec![0; key_len];
        reader.read_exact(&mut key)?;

        // read value
        let val_len = read_u32(reader)? as usize;
        let mut value = vec![0; val_len];
        reader.read_exact(&mut value)?;
        Ok(Self { key, value })
    }

    fn write<W: Write>(&self, writer: &mut W) -> Result<()> {
        assert!(self.key.len() <= u16::MAX as usize);
        write_u16(writer, self.key.len() as u16)?;
        writer.write_all(&self.key)?;
        assert!(self.value.len() <= u32::MAX as usize);
        write_u32(writer, self.value.len() as u32)?;
        writer.write_all(&self.value)?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct NodeHeader {
    pub flags: u16,
    pub internal_nodes_len: u64,
    pub overflow_len: u64,
}

impl NodeHeader {
    pub fn read<R: Read>(reader: &mut R) -> Result<Self> {
        let flags = read_u16(reader)?;
        let internal_nodes_len = read_u64(reader)?;
        let overflow_len = read_u64(reader)?;
        Ok(Self {
            flags,
            internal_nodes_len,
            overflow_len,
        })
    }

    pub fn write<W: Write>(&self, writer: &mut W) -> Result<()> {
        write_u16(writer, self.flags)?;
        write_u64(writer, self.internal_nodes_len)?;
        write_u64(writer, self.overflow_len)?;
        Ok(())
    }

    pub fn size() -> u64 {
        // flags
        std::mem::size_of::<u16>() as u64 +
        // internal_nodes_len
        std::mem::size_of::<u64>() as u64 +
        // overflow_len
        std::mem::size_of::<u64>() as u64
    }
}

#[derive(Debug, Clone)]
pub struct MetaNode {
    pub page_size: u32,
    pub root_node: Address,
    pub free_list_node: Address,
    pub transaction_id: TransactionId,
}

impl MetaNode {
    pub fn size() -> usize {
        // page_size,
        size_of::<u32>() +
        // root_node
        size_of::<u64>() +
        // free_list_node
        size_of::<u64>() +
        // transaction_id
        size_of::<u64>() +
        // checksum
        size_of::<u32>()
    }

    pub fn page_size() -> u64 {
        1 << 10 // 1KiB
    }

    pub fn read<R: Read>(reader: &mut R) -> Result<Self> {
        let page_size = read_u32(reader)?;
        let root_node = read_u64(reader)? as Address;
        let free_list_node = read_u64(reader)? as Address;
        let transaction_id = read_u64(reader)? as TransactionId;
        let checksum = read_u32(reader)?;
        let meta_node = Self {
            page_size,
            root_node,
            free_list_node,
            transaction_id,
        };

        let expected_checksum = meta_node.checksum();
        if checksum != expected_checksum {
            return Err(anyhow!("corrupted file"));
        }

        Ok(meta_node)
    }

    pub fn write<W: Write>(&self, writer: &mut W) -> Result<()> {
        write_u32(writer, self.page_size)?;
        write_u64(writer, self.root_node)?;
        write_u64(writer, self.free_list_node)?;
        write_u64(writer, self.transaction_id)?;
        write_u32(writer, self.checksum())?;
        Ok(())
    }

    pub fn checksum(&self) -> u32 {
        let mut h = crc32fast::Hasher::new();
        h.write_u32(self.page_size);
        h.write_u64(self.root_node);
        h.write_u64(self.free_list_node);
        h.write_u64(self.transaction_id);
        h.finalize()
    }
}

#[derive(Debug, Clone)]
pub enum InternalNodes {
    Branch(Vec<BranchInternalNode>),
    Leaf(Vec<LeafInternalNode>),
}

impl InternalNodes {
    pub fn size(&self) -> u64 {
        let nodes_size: u64 = match self {
            Self::Branch(nodes) => nodes.iter().map(|node| node.size()).sum(),
            Self::Leaf(nodes) => nodes.iter().map(|node| node.size()).sum(),
        };
        NodeHeader::size() + nodes_size
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Branch(nodes) => nodes.len(),
            Self::Leaf(nodes) => nodes.len(),
        }
    }

    pub fn key_at(&self, index: usize) -> &[u8] {
        match self {
            Self::Branch(nodes) => nodes[index].key.as_ref(),
            Self::Leaf(nodes) => nodes[index].key.as_ref(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn first_child(&self) -> Option<&BranchInternalNode> {
        match self {
            Self::Branch(nodes) => nodes.first(),
            Self::Leaf(_) => None,
        }
    }

    pub fn next_dirty_child(&self, start_index: usize) -> Option<(u64, usize)> {
        if let Self::Branch(nodes) = self {
            for (index, node) in nodes[start_index..].iter().enumerate() {
                if let NodeId::Id(id) = node.node_id {
                    return Some((id, start_index + index));
                }
            }
        }
        None
    }

    pub fn remove_child_at(&mut self, index: usize) {
        let Self::Branch(nodes) = self else {
            panic!("cannot remove child from leaf node");
        };
        nodes.remove(index);
    }

    pub fn as_branch(&self) -> Option<&Vec<BranchInternalNode>> {
        let Self::Branch(ref nodes) = self else {
            return None;
        };
        Some(nodes)
    }

    pub fn merge(&mut self, other: InternalNodes) {
        match (self, other) {
            (Self::Branch(self_nodes), Self::Branch(mut other_nodes)) => {
                self_nodes.append(&mut other_nodes);
            }
            (Self::Leaf(self_nodes), Self::Leaf(mut other_nodes)) => {
                self_nodes.append(&mut other_nodes);
            }
            _ => panic!("incompatible nodes"),
        }
    }

    pub fn splice(&mut self, index: usize, child_nodes: Vec<BranchInternalNode>) {
        let Self::Branch(nodes) = self else {
            panic!("splicing leaf");
        };

        if nodes.is_empty() && index == 0 {
            *nodes = child_nodes;
        } else {
            nodes.splice(index..index + 1, child_nodes);
        }
    }

    pub fn split(self, threshold: u64) -> Vec<Self> {
        match self {
            Self::Branch(nodes) => Self::split_branch(nodes, threshold),
            Self::Leaf(nodes) => Self::split_leaf(nodes, threshold),
        }
    }

    pub fn set_page_address(&mut self, child_index: usize, child_page_address: Address) {
        match self {
            Self::Branch(nodes) => {
                assert!(matches!(nodes[child_index].node_id, NodeId::Id(_)));
                nodes[child_index].node_id = NodeId::Address(child_page_address);
            }
            Self::Leaf(_) => {
                panic!("cannot update");
            }
        }
    }

    fn split_branch(mut internal_nodes: Vec<BranchInternalNode>, threshold: u64) -> Vec<Self> {
        if internal_nodes.len() <= MIN_KEYS_PER_PAGE {
            return vec![Self::Branch(internal_nodes)];
        }
        let mut result = Vec::new();
        let mut size = NodeHeader::size();
        let mut new_node = Vec::new();
        let drain_len = internal_nodes.len() - MIN_KEYS_PER_PAGE;
        for internal_node in internal_nodes.drain(..drain_len) {
            if size + internal_node.size() > threshold && new_node.len() >= MIN_KEYS_PER_PAGE {
                result.push(Self::Branch(std::mem::take(&mut new_node)));
                size = NodeHeader::size();
            }
            size += internal_node.size();
            new_node.push(internal_node);
        }

        new_node.append(&mut internal_nodes);
        result.push(Self::Branch(new_node));
        result
    }

    fn split_leaf(mut internal_nodes: Vec<LeafInternalNode>, threshold: u64) -> Vec<Self> {
        if internal_nodes.len() <= MIN_KEYS_PER_PAGE {
            return vec![Self::Leaf(internal_nodes)];
        }
        let mut result = Vec::new();
        let mut size = NodeHeader::size();
        let mut new_node = Vec::new();
        let drain_len = internal_nodes.len() - MIN_KEYS_PER_PAGE;
        for internal_node in internal_nodes.drain(..drain_len) {
            if size + internal_node.size() > threshold && new_node.len() >= MIN_KEYS_PER_PAGE {
                result.push(Self::Leaf(std::mem::take(&mut new_node)));
                size = NodeHeader::size();
            }
            size += internal_node.size();
            new_node.push(internal_node);
        }

        new_node.append(&mut internal_nodes);
        result.push(Self::Leaf(new_node));
        result
    }

    pub fn read<R: Read>(reader: &mut R) -> Result<(NodeHeader, Self)> {
        let header = NodeHeader::read(reader)?;
        if header.flags == BRANCH_NODE {
            let mut nodes = Vec::new();
            for _ in 0..header.internal_nodes_len {
                nodes.push(BranchInternalNode::read(reader)?);
            }
            return Ok((header, Self::Branch(nodes)));
        }
        if header.flags == LEAF_NODE {
            let mut nodes = Vec::new();
            for _ in 0..header.internal_nodes_len {
                nodes.push(LeafInternalNode::read(reader)?);
            }
            return Ok((header, Self::Leaf(nodes)));
        }
        Err(anyhow!("invalid node type {}", header.flags))
    }

    pub fn write<W: Write>(&self, writer: &mut W, page_size: u64) -> Result<NodeHeader> {
        let node_header = self.write_header(writer, page_size)?;
        match self {
            Self::Branch(nodes) => {
                for node in nodes {
                    node.write(writer)?;
                }
            }
            Self::Leaf(nodes) => {
                for node in nodes {
                    node.write(writer)?;
                }
            }
        }
        Ok(node_header)
    }

    fn write_header<W: Write>(&self, writer: &mut W, page_size: u64) -> Result<NodeHeader> {
        let (nodes_len, nodes_size, flags) = match self {
            Self::Branch(nodes) => {
                let nodes_size: u64 = nodes.iter().map(|node| node.size()).sum();

                (nodes.len(), nodes_size, BRANCH_NODE)
            }
            Self::Leaf(nodes) => {
                let nodes_size: u64 = nodes.iter().map(|node| node.size()).sum();

                (nodes.len(), nodes_size, LEAF_NODE)
            }
        };

        let data_size = nodes_size + NodeHeader::size();
        let overflow_len: u64 = if data_size <= page_size {
            0
        } else {
            (data_size - page_size) / page_size
        };

        let node_header = NodeHeader {
            flags,
            internal_nodes_len: nodes_len as u64,
            overflow_len,
        };
        node_header.write(writer)?;
        Ok(node_header)
    }

    pub fn has_min_keys(&self) -> bool {
        match self {
            Self::Branch(nodes) => nodes.len() > 2,
            Self::Leaf(nodes) => nodes.len() > 1,
        }
    }

    pub fn is_leaf(&self) -> bool {
        matches!(self, Self::Leaf(..))
    }
}

pub type Address = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeId {
    Address(Address),
    Id(u64),
}

impl NodeId {
    pub fn node_address(&self) -> Address {
        let Self::Address(address) = self else {
            panic!("node id is not address");
        };
        *address
    }

    pub fn id(&self) -> u64 {
        let Self::Id(id) = self else {
            panic!("node address is not id");
        };
        *id
    }
}

#[derive(Debug, Default)]
struct Files {
    files_len: usize,
    files: Vec<File>,
}

#[derive(Clone, Debug)]
pub enum Node<'a> {
    Dirty(&'a InternalNodes),
    ReadOnly(Arc<(NodeHeader, InternalNodes)>),
}

impl Deref for Node<'_> {
    type Target = InternalNodes;

    fn deref(&self) -> &Self::Target {
        match &self {
            Self::Dirty(node) => node,
            Self::ReadOnly(node) => &node.as_ref().1,
        }
    }
}

impl AsRef<InternalNodes> for Node<'_> {
    fn as_ref(&self) -> &InternalNodes {
        match &self {
            Self::Dirty(node) => node,
            Self::ReadOnly(node) => &node.as_ref().1,
        }
    }
}

pub trait NodeReader {
    fn read_node(&self, node_id: NodeId) -> Result<Node<'_>>;
}

pub struct NodeManager {
    file_path: PathBuf,
    max_files: usize,
    files: Mutex<Files>,
    files_condvar: Condvar,
    page_size: u32,
    // nodes_cache: moka::sync::Cache<Address, Arc<InternalNodes>>,
}

impl NodeManager {
    pub fn new(
        file_path: impl AsRef<Path>,
        max_files: usize,
        page_size: u32,
        // initial_alignment: u64,
    ) -> Self {
        Self {
            file_path: file_path.as_ref().to_path_buf(),
            max_files,
            files: Mutex::default(),
            files_condvar: Condvar::new(),
            page_size,
            // initial_alignment,
            // nodes_cache: moka::sync::Cache::builder()
            //     .weigher(|_, node: &Arc<InternalNodes>| node.size())
            //     .max_capacity(cache_size as u64)
            //     .build(),
        }
    }

    pub fn write_node(&self, page_address: Address, node: &InternalNodes) -> Result<()> {
        let mut file = self.get_file()?;
        file.seek(SeekFrom::Start(page_address))?;
        node.write(&mut file, self.page_size as u64)?;
        self.release_file(file);
        Ok(())
    }

    pub fn write_free_list(&self, page_address: Address, free_list: &FreeList) -> Result<NodeHeader> {
        let mut file = self.get_file()?;
        file.seek(SeekFrom::Start(page_address))?;
        let node_header = free_list.write(&mut file, self.page_size)?;
        self.release_file(file);
        Ok(node_header)
    }

    pub fn write_meta(&self, meta_node: &MetaNode) -> Result<()> {
        let mut file = self.get_file()?;
        let page_address = (meta_node.transaction_id % 2) * MetaNode::page_size();
        file.seek(SeekFrom::Start(page_address))?;
        meta_node.write(&mut file)?;
        file.flush()?;
        self.release_file(file);
        Ok(())
    }

    pub fn read_node(&self, page_address: Address) -> Result<Arc<(NodeHeader, InternalNodes)>> {
        let mut file = self.get_file()?;
        file.seek(SeekFrom::Start(page_address))?;
        let node = InternalNodes::read(&mut file)?;
        self.release_file(file);
        Ok(Arc::new(node))
    }

    pub fn page_size(&self) -> u32 {
        self.page_size
    }

    pub fn size(&self) -> Result<u64> {
        let file = self.get_file()?;
        let len = file.metadata()?.len();
        self.release_file(file);
        Ok(len)
    }

    fn get_file(&self) -> Result<File> {
        let mut files = self.files.lock().expect("files lock");
        loop {
            if let Some(file) = files.files.pop() {
                return Ok(file);
            }

            if files.files_len < self.max_files {
                files.files_len += 1;
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(self.file_path.clone())?;
                return Ok(file);
            }

            files = self
                .files_condvar
                .wait(files)
                .map_err(|e| anyhow!("{e:?}"))?;
        }
    }

    fn release_file(&self, file: File) {
        let mut files = self.files.lock().expect("files lock");
        files.files.push(file);
        self.files_condvar.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn reads_branch_node() {
        let data = &[
            // flags
            0x00, 0x01, // internal nodes len
            0x00, 0x02, // overflow len
            0x00, 0x00, // page id
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, // key len
            0x00, 0x0A, // key
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, // page id
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x21, // key len
            0x00, 0x09, // key
            0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19,
        ];

        let mut cursor = Cursor::new(data);
        let (_, node) = InternalNodes::read(&mut cursor).unwrap();

        let InternalNodes::Branch(nodes) = node else {
            panic!("unexpected node type");
        };

        assert_eq!(
            nodes,
            vec![
                BranchInternalNode {
                    // node_id: NodeId::Address(16),
                    key: (1..=10).into_iter().collect::<Vec<u8>>(),
                    node_id: NodeId::Address(16),
                },
                BranchInternalNode {
                    // node_id: NodeId::Address(33),
                    key: (17..=25).into_iter().collect::<Vec<u8>>(),
                    node_id: NodeId::Address(33),
                }
            ]
        );
    }

    #[test]
    fn reads_leaf_node() {
        let data = &[
            // flags
            0x00, 0x02, // internal nodes len
            0x00, 0x02, // overflow len
            0x00, 0x00, // node 1
            // key len
            0x00, 0x0A, // key
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, // value len
            0x00, 0x00, 0x00, 0x10, // value
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E,
            0x0F, 0x10, // node 2
            // key len
            0x00, 0x09, // key
            0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, // value len
            0x00, 0x00, 0x00, 0x02, // value
            0x11, 0x12,
        ];

        let mut cursor = Cursor::new(data);
        let (_, node) = InternalNodes::read(&mut cursor).unwrap();

        let InternalNodes::Leaf(nodes) = node else {
            panic!("unexpected node type");
        };

        assert_eq!(
            nodes,
            vec![
                LeafInternalNode {
                    key: (1..=10).into_iter().collect::<Vec<u8>>(),
                    value: (1..=16).into_iter().collect::<Vec<u8>>()
                },
                LeafInternalNode {
                    key: (17..=25).into_iter().collect::<Vec<u8>>(),
                    value: vec![17, 18],
                },
            ]
        );
    }
}
