use crate::cursor::Cursor;
use crate::node::{
    Address, BranchInternalNode, InternalNodes, LeafInternalNode, Node, NodeHeader, NodeId,
    NodeReader, MIN_KEYS_PER_PAGE,
};
use crate::{DatabaseInternal, WriteState};
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::ops::Not;
use std::sync::Arc;

pub type TransactionId = u64;

pub struct ReadTransaction {
    database: Arc<DatabaseInternal>,
    root_node_id: NodeId,
    transaction_id: TransactionId,
}

impl Drop for ReadTransaction {
    fn drop(&mut self) {
        self.database.release_reader(self.transaction_id);
    }
}

impl ReadTransaction {
    pub fn new(database: Arc<DatabaseInternal>, root_node_address: Address, transaction_id: TransactionId) -> Self {
        Self {
            database,
            root_node_id: NodeId::Address(root_node_address),
            transaction_id,
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut cursor = Cursor::new(self.root_node_id, self)?;
        cursor.seek(key)?;
        if !cursor.is_valid() || cursor.key() != key {
            return Ok(None);
        }
        Ok(Some(cursor.value().to_vec()))
    }

    pub fn cursor(&self) -> Result<Cursor<'_>> {
        Cursor::new(self.root_node_id, self)
    }
}

impl NodeReader for ReadTransaction {
    fn read_node(&self, node_id: NodeId) -> Result<Node<'_>> {
        let node = match node_id {
            NodeId::Address(address) => {
                Node::ReadOnly(self.database.node_manager.read_node(address)?)
            }
            NodeId::Id(_) => {
                panic!("fetching dirty node in read tx")
            }
        };
        Ok(node)
    }
}

pub struct WriteTransaction {
    database: Arc<DatabaseInternal>,
    next_node_id: u64,
    nodes: HashMap<u64, InternalNodes>,
    parent: HashMap<u64, u64>,
    root_node_id: NodeId,
    pending_free_pages: Vec<(Address, NodeHeader)>,
    writer: Option<WriteState>,
    transaction_id: TransactionId,
}

impl NodeReader for WriteTransaction {
    fn read_node(&self, node_id: NodeId) -> Result<Node<'_>> {
        let node = match node_id {
            NodeId::Address(address) => {
                Node::ReadOnly(self.database.node_manager.read_node(address)?)
            }
            NodeId::Id(node_id) => {
                let node = self.nodes.get(&node_id).expect("tx nodes");
                Node::Dirty(node)
            }
        };
        Ok(node)
    }
}

impl WriteTransaction {
    pub fn new(database: Arc<DatabaseInternal>, writer: WriteState) -> Self {
        let transaction_id = writer.meta().transaction_id + 1;
        let root_node_address = writer.meta().root_node;
        Self {
            database,
            next_node_id: 1,
            nodes: HashMap::new(),
            parent: HashMap::new(),
            root_node_id: NodeId::Address(root_node_address),
            pending_free_pages: Vec::new(),
            writer: Some(writer),
            transaction_id,
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut cursor = Cursor::new(self.root_node_id, self)?;
        cursor.seek(key)?;
        if !cursor.is_valid() || cursor.key() != key {
            return Ok(None);
        }
        Ok(Some(cursor.value().to_vec()))
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.update(Update::Put(key.to_vec(), value.to_vec()))
    }

    pub fn remove(&mut self, key: &[u8]) -> Result<()> {
        self.update(Update::Delete(key.to_vec()))
    }

    pub fn cursor(&self) -> Result<Cursor<'_>> {
        Cursor::new(self.root_node_id, self)
    }

    pub fn commit(mut self) -> Result<()> {
        if let Err(e) = self.commit_internal() {
            self.rollback()?;
            return Err(e);
        }
        Ok(())
    }

    pub fn rollback(&mut self) -> Result<()> {
        let writer = self.writer.as_mut().expect("writer");
        writer.free_list.rollback(self.transaction_id);
        Ok(())
    }

    fn commit_internal(&mut self) -> Result<()> {
        self.merge()?;
        self.split()?;
        let NodeId::Id(node_id) = self.root_node_id else {
            return Ok(());
        };
        let (root_node_address, _) = self.traverse_write(node_id)?;
        self.root_node_id = NodeId::Address(root_node_address);
        self.write_meta_node()?;
        // self.database.node_manager.inv
        Ok(())
    }

    fn write_free_list(&mut self) -> Result<(Address, NodeHeader)> {
        let free_list_size = {
            let writer = self.writer.as_mut().expect("writer");
            let size = NodeHeader::size() + writer.free_list.size() as u64;
            let page_size = self.database.page_size as u64;
            let pages = size.div_ceil(page_size);
            assert!(pages > 0);
            writer.free_list.free(
                self.transaction_id,
                writer.free_list_node_address,
                writer.free_list_header.overflow_len,
                page_size,
            );

            for (page_address, header) in self.pending_free_pages.drain(..) {
                writer.free_list.free(
                    self.transaction_id,
                    page_address,
                    header.overflow_len,
                    page_size,
                );
            }

            writer.free_list.size() as u64 + NodeHeader::size()
        };

        let page_address = self.allocate(free_list_size)?;
        // println!("FREE LIST SIZE FOR ALLOCATION: {free_list_size:?}, ADDRESS: {page_address:?}");
        let writer = self.writer.as_ref().expect("writer");
        let node_header = self
            .database
            .node_manager
            .write_free_list(page_address, &writer.free_list)?;
        Ok((page_address, node_header))
    }

    fn traverse_write(&mut self, node_id: u64) -> Result<(Address, Vec<u8>)> {
        let mut child_ref = {
            let node = self.nodes.get(&node_id).expect("node");
            node.next_dirty_child(0)
        };

        while let Some((child_node_id, child_node_index)) = child_ref {
            let (child_page_address, child_key) = self.traverse_write(child_node_id)?;
            child_ref = {
                let node = self.nodes.get_mut(&node_id).expect("node");
                node.set_page_address(child_node_index, child_page_address);
                node.set_child_key(child_node_index, child_key);
                node.next_dirty_child(child_node_index + 1)
            };
        }

        let node_size = { self.nodes.get(&node_id).expect("tx node").size() };
        let page_address = self.allocate(node_size)?;
        let node = self.nodes.get(&node_id).expect("tx node");
        let key = if node.is_empty() {
            // When database is empty, return empty first key on root level
            assert!(!self.parent.contains_key(&node_id));
            Vec::new()
        } else {
            node.key_at(0).to_vec()
        };
        self.database.node_manager.write_node(page_address, node)?;
        Ok((page_address, key))
    }

    fn write_meta_node(&mut self) -> Result<()> {
        let (free_list_node_address, free_list_header) = self.write_free_list()?;
        let writer = self.writer.as_mut().expect("writer");
        // println!("COMMITTING FREE LIST: {:?}", writer.free_list.summary());
        let mut meta = writer.meta().clone();
        meta.transaction_id = self.transaction_id;
        meta.root_node = self.root_node_id.node_address();
        meta.free_list_node = free_list_node_address;
        self.database.node_manager.write_meta(&meta)?;
        *writer.meta_mut() = meta;
        writer.free_list_header = free_list_header;
        writer.free_list_node_address = free_list_node_address;
        writer.free_list.commit_allocations();
        // println!("COMMITTED FREE LIST: {:?}", writer.free_list.summary());
        Ok(())
    }

    pub fn traverse(&mut self) {
        self.traverse_inner(self.root_node_id);
    }

    fn allocate(&mut self, required_size: u64) -> Result<Address> {
        let page_size = self.database.page_size as u64;
        let required_pages = required_size.div_ceil(page_size);
        let writer = self
            .writer
            .as_mut()
            .expect("tx writer");
        let page_address = writer
            .free_list
            .allocate(required_pages, page_size);
        if let Some(page_address) = page_address {
            writer.free_list.register_allocation(page_address, self.transaction_id);
            return Ok(page_address);
        }
        let file_size = self.database.node_manager.size()?;
        let align = file_size % page_size;
        assert!(align == 0);

        let page_address = writer.meta().end_address;
        let next_end_address = writer.meta().end_address + required_pages * page_size;
        if next_end_address > file_size {
            // grow
            const GB: u64 = 1 << 30;
            let new_file_size = if file_size >= GB {
                GB.div_ceil(page_size) * page_size
            } else {
                file_size << 1
            };
            let new_file_size = std::cmp::max(new_file_size, next_end_address);
            self.database.node_manager.set_size(new_file_size)?;
        };
        writer.meta_mut().end_address = next_end_address;
        writer.free_list.register_allocation(page_address, self.transaction_id);
        Ok(page_address)
    }

    fn traverse_inner(&self, node_id: NodeId) {
        //let node = self.nodes.get(&node_id).expect("node");
        let node = self.read_node(node_id).expect("read node");
        let node = node.as_ref();
        match node {
            InternalNodes::Branch(nodes) => {
                println!("Branch = {node_id:?}, Size={:?}", node.size());
                let mut children_ids = Vec::new();
                for n in nodes {
                    println!(
                        "\t{key:?} ->\t{child_node_id:?}",
                        key = String::from_utf8_lossy(&n.key),
                        child_node_id = n.node_id,
                    );

                    children_ids.push(n.node_id);
                }

                // let children_ids = children.remove(&node_id).expect("children");
                for c in children_ids {
                    self.traverse_inner(c);
                }
            }
            InternalNodes::Leaf(nodes) => {
                println!("Leaf = {node_id:?}, Size={:?}", node.size());
                for n in nodes {
                    println!(
                        "\t{key:?} ->\t{value:?}",
                        key = String::from_utf8_lossy(&n.key),
                        value = String::from_utf8_lossy(&n.value),
                    );
                }
            }
        }
    }

    pub fn merge(&mut self) -> Result<()> {
        if let NodeId::Id(node_id) = self.root_node_id {
            self.traverse_merge(node_id, 0)?;
        }
        Ok(())
    }

    pub fn split(&mut self) -> Result<()> {
        let NodeId::Id(_) = self.root_node_id else {
            return Ok(());
        };

        loop {
            let split_count = self.traverse_split(self.root_node_id.id(), 0)?;
            if split_count < 2 {
                break;
            }
        }

        Ok(())
    }

    pub fn traverse_merge(&mut self, node_id: u64, node_index: usize) -> Result<bool> {
        let mut child_ref = {
            let node = self.nodes.get(&node_id).expect("node");
            node.next_dirty_child(0)
        };

        while let Some((child_node_id, mut child_node_index)) = child_ref {
            // If child node was merged with either its left or right sibliing
            // then right sibling is moved to its place and child_node_index should
            // keep looking start next iteration from same position.
            if !self.traverse_merge(child_node_id, child_node_index)? {
                child_node_index += 1;
            }
            child_ref = {
                let node = self.nodes.get(&node_id).expect("node");
                node.next_dirty_child(child_node_index)
            };
        }

        let node = self.nodes.get(&node_id).expect("tx node");
        let page_size = self.database.node_manager.page_size() as u64;
        let merge_threshold = page_size / 4;
        if node.size() < merge_threshold || !node.has_min_keys() {
            self.merge_node(node_id, node_index)
        } else {
            Ok(false)
        }
    }

    fn update(&mut self, update: Update) -> Result<()> {
        // Find node for update
        let mut cursor = Cursor::new(self.root_node_id, self)?;
        cursor.seek_internal(update.key())?;

        // Fast check if deleted key does not exist
        if let Update::Delete(key) = &update {
            if !cursor.is_valid() || cursor.key() != key {
                return Ok(());
            }
        }

        let mut stack = cursor.stack;
        // Collect new dirty nodes
        let mut new_dirty_nodes = Vec::new();
        let mut existing_dirty_node = None;
        while let Some(node_ref) = stack.pop() {
            match &node_ref.node {
                Node::ReadOnly(node) => {
                    new_dirty_nodes.push((
                        node_ref.index,
                        node_ref.node_id.node_address(),
                        node.as_ref().clone(),
                    ));
                }
                Node::Dirty(_) => {
                    existing_dirty_node = Some((node_ref.index, node_ref.node_id.id()));
                    break;
                }
            }
        }

        self.pending_free_pages.extend(
            new_dirty_nodes
                .iter()
                .map(|(_, node_address, (header, _))| (*node_address, header.clone())),
        );
        new_dirty_nodes.reverse();
        // existing_dirty_nodes.reverse();

        let has_new_dirty_nodes = !new_dirty_nodes.is_empty();
        let (index, nodes, mut last_node_id) = if new_dirty_nodes.is_empty() {
            let (index, node_id) = existing_dirty_node.expect("existing dirty node");
            let node = self.nodes.get_mut(&node_id).expect("node must exist");
            let InternalNodes::Leaf(ref mut nodes) = node else {
                panic!("expected leaf node");
            };
            (index, nodes, node_id)
        } else {
            let (index, _node_address, node) = new_dirty_nodes
                .pop()
                .ok_or_else(|| anyhow!("database is corrupted"))?;
            let node_id = self.insert_new(node.1);
            let node = self.nodes.get_mut(&node_id).expect("node must exist");
            let InternalNodes::Leaf(ref mut nodes) = node else {
                panic!("expected leaf node");
            };
            (index, nodes, node_id)
        };

        match &update {
            Update::Put(key, value) => {
                if index < nodes.len() && key == &nodes[index].key {
                    nodes[index].value = value.to_vec();
                } else {
                    nodes.insert(
                        index,
                        LeafInternalNode {
                            key: key.to_vec(),
                            value: value.to_vec(),
                        },
                    );
                }
            }
            Update::Delete(_) => {
                // No need to check index boundary because it was done in
                // fast check earlier.
                nodes.remove(index);
            }
        };

        while let Some((index, _node_address, mut node)) = new_dirty_nodes.pop() {
            let InternalNodes::Branch(ref mut nodes) = node.1 else {
                panic!("expected branch node");
            };
            nodes[index].node_id = NodeId::Id(last_node_id);
            let inserted_node_id = self.insert_new(node.1);
            self.insert_parent(last_node_id, inserted_node_id);
            last_node_id = inserted_node_id;
        }

        if has_new_dirty_nodes {
            if let Some((index, node_id)) = existing_dirty_node {
                let node = self.nodes.get_mut(&node_id).expect("node must exist");
                let InternalNodes::Branch(ref mut nodes) = node else {
                    panic!("expected branch node");
                };
                nodes[index].node_id = NodeId::Id(last_node_id);
                self.insert_parent(last_node_id, node_id);
            }
        }

        if existing_dirty_node.is_none() {
            self.root_node_id = NodeId::Id(last_node_id);
        }

        Ok(())
    }

    fn merge_node(&mut self, node_id: u64, node_index: usize) -> Result<bool> {
        let is_root = self.parent.contains_key(&node_id).not();

        // If root node is a branch and only has one node then collapse it.
        if is_root {
            let root = self.nodes.get_mut(&node_id).expect("root node");

            if root.is_empty() {
                let parent = InternalNodes::Leaf(Vec::new());
                let parent_id = self.insert_new(parent);
                self.root_node_id = NodeId::Id(parent_id);
                return Ok(false);
            }

            // If root node is a branch and only has one child node then collapse it.
            let new_root_id = root
                .as_branch()
                .filter(|nodes| nodes.len() == 1)
                .map(|nodes| nodes[0].node_id.id());

            if let Some(new_root_id) = new_root_id {
                self.root_node_id = NodeId::Id(new_root_id);
                let parent = self.parent.remove(&new_root_id);
                assert_eq!(parent, Some(node_id));
                let removed = self.nodes.remove(&node_id).is_some();
                assert!(removed, "root node must be removed");
            }
            return Ok(true);
        }

        // If node has no children then remove it
        let parent_id = *self.parent.get(&node_id).expect("node parent");
        {
            let node = self.nodes.get(&node_id).expect("node");
            if node.is_empty() {
                let removed = self.parent.remove(&node_id).is_some();
                assert!(removed, "parent node must be removed");
                let removed = self.nodes.remove(&node_id).is_some();
                assert!(removed, "empty node must be removed");
                self.nodes
                    .get_mut(&parent_id)
                    .expect("parent")
                    .remove_child_at(node_index);
                return Ok(true);
            }
        }

        // Parent must have at least two nodes
        {
            let parent = self.nodes.get(&parent_id).expect("parent node");
            if parent.len() < 2 {
                return Ok(false);
            }
            //assert!(parent.len() > 1, "parent must have at least 2 children");
        }

        let sibling_node_id = if node_index == 0 {
            self.get_child_at_index(parent_id, node_index + 1)?
        } else {
            self.get_child_at_index(parent_id, node_index - 1)?
        };

        if node_index == 0 {
            // merge with next sibling
            let next_sibling = self.nodes.remove(&sibling_node_id).expect("next sibling");

            if let InternalNodes::Branch(ref child_nodes) = next_sibling {
                for child_node in child_nodes {
                    let NodeId::Id(child_node_id) = child_node.node_id else {
                        continue;
                    };
                    let old_parent = self.parent.remove(&child_node_id).expect("old parent");
                    assert_eq!(old_parent, sibling_node_id, "invalid reparenting");
                    self.parent.insert(child_node_id, node_id);
                }
            }

            self.nodes
                .get_mut(&node_id)
                .expect("node")
                .merge(next_sibling);

            self.nodes
                .get_mut(&parent_id)
                .expect("node")
                .remove_child_at(node_index + 1);

            let removed = self.parent.remove(&sibling_node_id).is_some();
            assert!(removed, "parent node must be removed");
        } else {
            // merge with previous sibling
            let node = self.nodes.remove(&node_id).expect("current node");

            if let InternalNodes::Branch(ref child_nodes) = node {
                for child_node in child_nodes {
                    let NodeId::Id(child_node_id) = child_node.node_id else {
                        continue;
                    };
                    let old_parent = self.parent.remove(&child_node_id).expect("old parent");
                    assert_eq!(old_parent, node_id, "invalid reparenting");
                    self.parent.insert(child_node_id, sibling_node_id);
                }
            }

            self.nodes
                .get_mut(&sibling_node_id)
                .expect("node")
                .merge(node);

            self.nodes
                .get_mut(&parent_id)
                .expect("node")
                .remove_child_at(node_index);

            let removed = self.parent.remove(&node_id).is_some();
            assert!(removed, "parent node must be removed");
        }

        Ok(true)
    }

    fn traverse_split(&mut self, node_id: u64, node_index: usize) -> Result<usize> {
        let mut child_ref = {
            let node = self.nodes.get(&node_id).expect("node");
            node.next_dirty_child(0)
        };

        while let Some((child_node_id, mut child_node_index)) = child_ref {
            let split_nodes_count = self.traverse_split(child_node_id, child_node_index)?;
            child_node_index += split_nodes_count;
            child_ref = {
                let node = self.nodes.get(&node_id).expect("node");
                node.next_dirty_child(child_node_index)
            };
        }

        let node = self.nodes.get(&node_id).expect("tx node");
        let page_size = self.database.node_manager.page_size() as u64;
        let node_size = node.size();
        let node_len = node.len();
        if node_size > page_size && node_len >= (MIN_KEYS_PER_PAGE * 2) {
            let split_nodes_count = self.split_node(node_id, node_index)?;
            Ok(split_nodes_count)
        } else {
            Ok(1)
        }
    }

    fn split_node(&mut self, node_id: u64, node_index: usize) -> Result<usize> {
        let page_size = self.database.node_manager.page_size();
        let mut nodes = self
            .nodes
            .remove(&node_id)
            .expect("split node")
            .split(page_size as u64);

        if nodes.len() == 1 {
            // If node was not split just re-insert it back
            let nodes = nodes.pop().expect("nodes");
            let inserted_back = self.nodes.insert(node_id, nodes).is_none();
            assert!(inserted_back, "insert back unsplit node");
            return Ok(1);
        }

        let parent_id = if let Some(parent_id) = self.parent.get(&node_id).copied() {
            let removed = self.parent.remove(&node_id).is_some();
            assert!(removed, "parent node must be present");
            parent_id
        } else {
            let parent = InternalNodes::Branch(Vec::new());
            let parent_id = self.insert_new(parent);
            self.root_node_id = NodeId::Id(parent_id);
            parent_id
        };

        let nodes = nodes
            .into_iter()
            .map(|node| {
                let key = node.key_at(0).to_vec();
                let child_id = self.insert_new(node);
                self.insert_parent(child_id, parent_id);
                BranchInternalNode {
                    node_id: NodeId::Id(child_id),
                    key,
                }
            })
            .collect::<Vec<_>>();

        // reparent child nodes of the split node
        for node in nodes.iter() {
            let new_parent_id = node.node_id.id();
            let nodes = self.nodes
                .get(&new_parent_id).expect("split node");

            match nodes {
                InternalNodes::Branch(nodes) => {
                    for node in nodes {
                        let NodeId::Id(child_node_id) = node.node_id else {
                            continue;
                        };
                        let old_parent = self.parent.remove(&child_node_id).expect("old parent");
                        assert_eq!(old_parent, node_id, "invalid reparenting");
                        self.parent.insert(child_node_id, new_parent_id);
                    }
                },
                InternalNodes::Leaf(_) => break,
            }
        }

        let nodes_len = nodes.len();
        let parent = self.nodes.get_mut(&parent_id).expect("parent");
        parent.splice(node_index, nodes);
        Ok(nodes_len)
    }

    fn get_child_at_index(&mut self, node_id: u64, child_index: usize) -> Result<u64> {
        let node = self.nodes.get(&node_id).expect("node must exist");
        let InternalNodes::Branch(nodes) = node else {
            panic!("expect branch node");
        };

        match nodes[child_index].node_id {
            NodeId::Id(child_node_id) => Ok(child_node_id),
            NodeId::Address(page_address) => {
                let (header, node) = self
                    .database
                    .node_manager
                    .read_node(page_address)?
                    .as_ref()
                    .clone();

                let child_node_id = self.insert_new(node);
                self.pending_free_pages.push((page_address, header));
                self.insert_parent(child_node_id, node_id);

                let node = self.nodes.get_mut(&node_id).expect("node must exist");
                let InternalNodes::Branch(nodes) = node else {
                    panic!("expect branch node");
                };
                nodes[child_index].node_id = NodeId::Id(child_node_id);

                Ok(child_node_id)
            }
        }
    }

    fn insert_new(&mut self, node: InternalNodes) -> u64 {
        let id = self.next_node_id;
        self.next_node_id += 1;
        let added = self.nodes.insert(id, node).is_none();
        assert!(added, "replacing existing dirty node");
        id
    }

    fn insert_parent(&mut self, child_id: u64, parent_id: u64) {
        let added = self.parent.insert(child_id, parent_id).is_none();
        assert!(added, "replacing existing child parent mapping");
    }
}

impl Drop for WriteTransaction {
    fn drop(&mut self) {
        self.database
            .release_writer(self.writer.take().expect("writer tx must own writer token"));
    }
}

enum Update {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

impl Update {
    fn key(&self) -> &[u8] {
        match &self {
            Self::Put(key, _) => key,
            Self::Delete(key) => key,
        }
    }
}
