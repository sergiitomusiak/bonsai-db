use anyhow::{anyhow, Result};
use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};

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
    fn size(&self) -> usize {
        // page id
        size_of::<u64>() +
        // key len field
        size_of::<u16>() +
        // key
        self.key.len()
    }

    fn read<R: Read>(reader: &mut R) -> Result<Self> {
        let address = read_u64(reader)?;
        let key_len = read_u16(reader)? as usize;
        let mut key = Vec::with_capacity(key_len);
        key.resize(key_len, 0);
        reader.read_exact(&mut key)?;
        Ok(Self {
            key,
            node_id: NodeId::Address(address),
        })
    }

    fn write<W: Write>(&self, writer: &mut W) -> Result<()> {
        write_u64(writer, self.node_id.node_address())?;
        write_u16(writer, self.key.len() as u16)?;
        writer.write_all(&self.key)?;
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeafInternalNode {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl LeafInternalNode {
    fn size(&self) -> usize {
        // key len field
        size_of::<u16>() +
        // key
        self.key.len() +
        // value len field
        size_of::<u32>() +
        // value
        self.value.len()
    }

    fn read<R: Read>(reader: &mut R) -> Result<Self> {
        // read key
        let key_len = read_u16(reader)? as usize;
        let mut key = Vec::with_capacity(key_len);
        key.resize(key_len, 0);
        reader.read_exact(&mut key)?;

        // read value
        let val_len = read_u32(reader)? as usize;
        let mut value = Vec::with_capacity(val_len);
        value.resize(val_len, 0);
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

#[derive(Debug)]
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

    pub fn size() -> usize {
        // flags
        std::mem::size_of::<u16>() +
        // internal_nodes_len
        std::mem::size_of::<u32>() +
        // overflow_len
        std::mem::size_of::<u32>()
    }
}

#[derive(Debug, Clone)]
pub struct MetaNode {
    pub page_size: u32,
    pub root_node: Address,
    pub freelist_node: Address,
    pub transaction_id: TransactionId,
}

impl MetaNode {
    pub fn size() -> usize {
        // page_size,
        size_of::<u32>() +
        // root_node
        size_of::<u64>() +
        // freelist_node
        size_of::<u64>() +
        // transaction_id
        size_of::<u64>() +
        // checksum
        size_of::<u64>()
    }

    pub fn page_size() -> u64 {
        1 << 10 // 1KiB
    }

    pub fn read<R: Read>(reader: &mut R) -> Result<Self> {
        let page_size = read_u32(reader)?;
        let root_node = read_u64(reader)? as Address;
        let freelist_node = read_u64(reader)? as Address;
        let transaction_id = read_u64(reader)? as TransactionId;
        let checksum = read_u32(reader)?;
        let meta_node = Self {
            page_size,
            root_node,
            freelist_node,
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
        write_u64(writer, self.freelist_node)?;
        write_u64(writer, self.transaction_id)?;
        write_u32(writer, self.checksum())?;
        Ok(())
    }

    pub fn checksum(&self) -> u32 {
        let mut h = crc32fast::Hasher::new();
        h.write_u32(self.page_size);
        h.write_u64(self.root_node);
        h.write_u64(self.freelist_node);
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
    pub fn size(&self) -> u32 {
        let nodes_size: u32 = match self {
            Self::Branch(nodes) => nodes.iter().map(|node| node.size() as u32).sum(),
            Self::Leaf(nodes) => nodes.iter().map(|node| node.size() as u32).sum(),
        };
        (NodeHeader::size() as u32) + nodes_size
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

    pub fn split(self, threshold: usize) -> Vec<Self> {
        match self {
            Self::Branch(nodes) => Self::split_branch(nodes, threshold),
            Self::Leaf(nodes) => Self::split_leaf(nodes, threshold),
        }
    }

    fn split_branch(mut internal_nodes: Vec<BranchInternalNode>, threshold: usize) -> Vec<Self> {
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

    fn split_leaf(mut internal_nodes: Vec<LeafInternalNode>, threshold: usize) -> Vec<Self> {
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

    pub fn read<R: Read>(reader: &mut R) -> Result<Self> {
        let header = NodeHeader::read(reader)?;
        if header.flags == BRANCH_NODE {
            let mut nodes = Vec::new();
            for _ in 0..header.internal_nodes_len {
                nodes.push(BranchInternalNode::read(reader)?);
            }
            return Ok(Self::Branch(nodes));
        }
        if header.flags == LEAF_NODE {
            let mut nodes = Vec::new();
            for _ in 0..header.internal_nodes_len {
                nodes.push(LeafInternalNode::read(reader)?);
            }
            return Ok(Self::Leaf(nodes));
        }
        return Err(anyhow!("invalid node type {}", header.flags));
    }

    pub fn write<W: Write>(&self, writer: &mut W, page_size: usize) -> Result<()> {
        self.write_header(writer, page_size)?;
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
        Ok(())
    }

    fn write_header<W: Write>(&self, writer: &mut W, page_size: usize) -> Result<()> {
        let (nodes_len, nodes_size, flags) = match self {
            Self::Branch(nodes) => {
                let nodes_size: usize = nodes.iter().map(|node| node.size()).sum();

                (nodes.len(), nodes_size, BRANCH_NODE)
            }
            Self::Leaf(nodes) => {
                let nodes_size: usize = nodes.iter().map(|node| node.size()).sum();

                (nodes.len(), nodes_size, LEAF_NODE)
            }
        };

        let data_size = nodes_size + NodeHeader::size();
        let overflow_len: usize = if data_size <= page_size {
            0
        } else {
            (data_size - page_size) / page_size
        };

        NodeHeader {
            flags,
            internal_nodes_len: nodes_len as u64,
            overflow_len: overflow_len as u64,
        }
        .write(writer)
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

    // pub (crate) fn index_of(&self, node_id: &NodeId) -> Option<usize> {
    //     match self {
    //         Self::Branch(nodes) => {
    //             nodes.iter()
    //                 .position(|node| &node.node_id == node_id)
    //         }
    //         Self::Leaf(..) => None
    //     }
    // }

    // pub (crate) fn remove(&mut self, node_id: &NodeId) -> bool {
    //     match self {
    //         Self::Branch(nodes) => {
    //             let index = nodes.iter()
    //                 .position(|node| &node.node_id == node_id);
    //             if let Some(index) = index {
    //                 nodes.remove(index);
    //                 true
    //             } else {
    //                 false
    //             }
    //         }
    //         Self::Leaf(..) => {
    //             false
    //         }
    //     }
    // }
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
    ReadOnly(Arc<InternalNodes>),
}

impl<'a> Deref for Node<'a> {
    type Target = InternalNodes;

    fn deref(&self) -> &Self::Target {
        match &self {
            Self::Dirty(node) => node,
            Self::ReadOnly(node) => node.as_ref(),
        }
    }
}

impl<'a> AsRef<InternalNodes> for Node<'a> {
    fn as_ref(&self) -> &InternalNodes {
        match &self {
            Self::Dirty(node) => node,
            Self::ReadOnly(node) => node.as_ref(),
        }
    }
}

pub trait NodeReader {
    fn read_node<'a>(&'a self, node_id: NodeId) -> Result<Node<'a>>;
}

pub struct NodeManager {
    file_path: PathBuf,
    max_files: usize,
    files: Mutex<Files>,
    files_condvar: Condvar,
    page_size: u32,
    initial_alignment: u64,
    // nodes_cache: moka::sync::Cache<Address, Arc<InternalNodes>>,
}

impl NodeManager {
    pub fn new(
        file_path: impl AsRef<Path>,
        max_files: usize,
        page_size: u32,
        initial_alignment: u64,
    ) -> Self {
        Self {
            file_path: file_path.as_ref().to_path_buf(),
            max_files,
            files: Mutex::default(),
            files_condvar: Condvar::new(),
            page_size,
            initial_alignment,
            // nodes_cache: moka::sync::Cache::builder()
            //     .weigher(|_, node: &Arc<InternalNodes>| node.size())
            //     .max_capacity(cache_size as u64)
            //     .build(),
        }
    }

    pub fn write_node(&self, page_id: Address, node: &InternalNodes) -> Result<()> {
        let mut file = self.get_file()?;
        file.seek(SeekFrom::Start(
            self.initial_alignment + page_id as u64 * self.page_size as u64,
        ))?;
        let node = node.write(&mut file, self.page_size as usize)?;
        self.release_file(file);
        Ok(node)
    }

    pub fn read_node(&self, page_id: Address) -> Result<Arc<InternalNodes>> {
        let mut file = self.get_file()?;
        file.seek(SeekFrom::Start(
            self.initial_alignment + page_id as u64 * self.page_size as u64,
        ))?;
        let node = InternalNodes::read(&mut file)?;
        self.release_file(file);
        Ok(Arc::new(node))
    }

    pub fn page_size(&self) -> u32 {
        self.page_size
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

// pub struct NodeLocation {
//     pub address: Address,
//     pub index: usize,
//     pub exact_match: bool,
//     pub path: Vec<Address>,
// }

// pub fn find_node<'a, N, NG>(key: &[u8], root: Address, node_getter: &'a NG) -> Result<NodeLocation>
// where
//     N: AsRef<InternalNodes> + 'a,
//     NG: NodeGetter<'a, N>,
// {
//     let mut address = root;
//     let mut path = Vec::new();
//     loop {
//         // Get current node
//         let node = node_getter.get_node(address)?;
//         path.push(address);
//         let mut exact_match = false;

//         match node.as_ref() {
//             InternalNodes::Branch(nodes) => {
//                 let mut index = nodes.binary_search_by(|internal_node| {
//                     let res = internal_node.key[..].cmp(key);
//                     if res == Ordering::Equal {
//                         exact_match = true;
//                     }
//                     res
//                 })
//                 .unwrap_or_else(identity);

//                 if !exact_match && index > 0 {
//                     index -=1;
//                 }

//                 address = nodes[index].node_id.address();
//             },
//             InternalNodes::Leaf(nodes) => {
//                 let index = nodes.binary_search_by(|internal_node| {
//                     let res = internal_node.key[..].cmp(key);
//                     if res == Ordering::Equal {
//                         exact_match = true;
//                     }
//                     res
//                 })
//                 .unwrap_or_else(identity);

//                 return Ok(NodeLocation {
//                     address,
//                     index,
//                     exact_match,
//                     path,
//                 });
//             }
//         };
//     }
// }

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
        let node = InternalNodes::read(&mut cursor).unwrap();

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
        let node = InternalNodes::read(&mut cursor).unwrap();

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
