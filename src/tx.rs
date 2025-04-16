use crate::cursor::Cursor;
use crate::node::{
    Address, BranchInternalNode, InternalNodes, LeafInternalNode, Node, NodeHeader, NodeId,
    NodeReader, MIN_KEYS_PER_PAGE,
};
use crate::{DatabaseInternal, Writer};
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::ops::Not;
use std::sync::Arc;

pub type TransactionId = u64;

pub struct WriteTransaction {
    database: Arc<DatabaseInternal>,
    next_node_id: u64,
    nodes: HashMap<u64, InternalNodes>,
    parent: HashMap<u64, u64>,
    root_node_id: NodeId,
    pending_free_pages: Vec<(Address, NodeHeader)>,
    writer: Option<Writer>,
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
    pub fn new(database: Arc<DatabaseInternal>, writer: Writer) -> Self {
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
        let root_node_address = self.traverse_write(node_id)?;
        self.root_node_id = NodeId::Address(root_node_address);
        self.write_meta_node()?;
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

            writer.free_list.size()
        };

        let page_address = self.allocate(free_list_size as u64)?;
        let writer = self.writer.as_ref().expect("writer");
        let node_header = self
            .database
            .node_manager
            .write_free_list(page_address, &writer.free_list)?;
        Ok((page_address, node_header))
    }

    fn traverse_write(&mut self, node_id: u64) -> Result<Address> {
        let mut child_ref = {
            let node = self.nodes.get(&node_id).expect("node");
            node.next_dirty_child(0)
        };

        while let Some((child_node_id, child_node_index)) = child_ref {
            let child_page_address = self.traverse_write(child_node_id)?;
            child_ref = {
                let node = self.nodes.get_mut(&node_id).expect("node");
                node.set_page_address(child_node_index, child_page_address);
                node.next_dirty_child(child_node_index + 1)
            };
        }

        let node_size = { self.nodes.get(&node_id).expect("tx node").size() };
        let page_address = self.allocate(node_size)?;
        let node = self.nodes.get(&node_id).expect("tx node");
        self.database.node_manager.write_node(page_address, node)?;
        Ok(page_address)
    }

    fn write_meta_node(&mut self) -> Result<()> {
        let (free_list_node_address, free_list_header) = self.write_free_list()?;

        let writer = self.writer.as_mut().expect("writer");
        let mut meta = writer.meta().clone();
        meta.transaction_id = self.transaction_id;
        meta.root_node = self.root_node_id.node_address();
        meta.free_list_node = free_list_node_address;
        self.database.node_manager.write_meta(&meta)?;
        *writer.meta_mut() = meta;

        println!("COMMITTING FREE LIST: {:?}", writer.free_list);
        writer.free_list_header = free_list_header;
        writer.free_list_node_address = free_list_node_address;
        writer.free_list.commit_allocations();
        println!("COMMITTED FREE LIST: {:?}", writer.free_list);

        Ok(())
    }

    pub fn traverse(&mut self) {
        self.traverse_inner(self.root_node_id);
    }

    fn allocate(&mut self, required_size: u64) -> Result<Address> {
        let page_size = self.database.page_size as u64;
        let required_pages = required_size.div_ceil(page_size);
        let page_address = self
            .writer
            .as_mut()
            .expect("tx writer")
            .free_list
            .allocate(required_pages, page_size);
        if let Some(page_address) = page_address {
            return Ok(page_address);
        }
        let file_size = self.database.node_manager.size()?;
        let align = file_size % page_size;
        let page_address = if align != 0 {
            file_size + (page_size - align)
        } else {
            file_size
        };
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
        if let NodeId::Id(node_id) = self.root_node_id {
            self.traverse_split(node_id, 0)?;
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
            self.merge_node(node_id, node_index)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn update(&mut self, update: Update) -> Result<()> {
        // Find node for update
        let mut cursor = Cursor::new(self.root_node_id, self)?;
        cursor.seek(update.key())?;

        // Fast check if deleted key does not exist
        if let Update::Delete(key) = &update {
            if !cursor.is_valid() || cursor.key() != key {
                return Ok(());
            }
        }

        let mut stack = cursor.stack;

        // Collect new dirty nodes
        let mut new_dirty_nodes = Vec::new();
        let mut existing_dirty_nodes = Vec::new();
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
                    existing_dirty_nodes.push((node_ref.index, node_ref.node_id.id()));
                    break;
                }
            }
        }

        // Collect existing dirty nodes
        while let Some(node_ref) = stack.pop() {
            match &node_ref.node {
                Node::Dirty(_) => {
                    existing_dirty_nodes.push((node_ref.index, node_ref.node_id.id()));
                }
                Node::ReadOnly(_) => {
                    panic!("unexpected read-only node");
                }
            }
        }

        self.pending_free_pages.extend(
            new_dirty_nodes
                .iter()
                .map(|(_, node_address, (header, _))| (*node_address, header.clone())),
        );
        new_dirty_nodes.reverse();
        existing_dirty_nodes.reverse();

        let has_new_dirty_nodes = !new_dirty_nodes.is_empty();
        let (index, nodes, mut last_node_id) = if new_dirty_nodes.is_empty() {
            let (index, node_id) = existing_dirty_nodes
                .pop()
                .ok_or_else(|| anyhow!("database is corrupted"))?;
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

        let items_shifted = match &update {
            Update::Put(key, value) => {
                if index < nodes.len() && key == &nodes[index].key {
                    nodes[index].value = value.to_vec();
                    false
                } else {
                    nodes.insert(
                        index,
                        LeafInternalNode {
                            key: key.to_vec(),
                            value: value.to_vec(),
                        },
                    );
                    index == 0
                }
            }
            Update::Delete(_) => {
                // No need to check index boundary because it was done in
                // fast check earlier.
                nodes.remove(index);
                true
            }
        };

        // Create new dirty branch nodes with potentially updated key item
        let mut update_branch_key = items_shifted && index == 0;
        while let Some((index, _node_address, mut node)) = new_dirty_nodes.pop() {
            let InternalNodes::Branch(ref mut nodes) = node.1 else {
                panic!("expected branch node");
            };

            nodes[index].node_id = NodeId::Id(last_node_id);
            if update_branch_key {
                nodes[index].key = update.key().to_vec();
            }

            update_branch_key &= index == 0;
            let inserted_node_id = self.insert_new(node.1);
            self.insert_parent(last_node_id, inserted_node_id);
            last_node_id = inserted_node_id;
        }

        if has_new_dirty_nodes {
            if let Some((index, node_id)) = existing_dirty_nodes.last() {
                let node = self.nodes.get_mut(node_id).expect("node must exist");

                let InternalNodes::Branch(ref mut nodes) = node else {
                    panic!("expected branch node");
                };
                nodes[*index].node_id = NodeId::Id(last_node_id);
                self.insert_parent(last_node_id, *node_id);
            }
        }

        // Update existing dirty branch nodes with potentially updated key item
        while let Some((index, node_id)) = existing_dirty_nodes.pop() {
            if update_branch_key {
                let node = self.nodes.get_mut(&node_id).expect("node must exist");

                let InternalNodes::Branch(ref mut nodes) = node else {
                    panic!("expected branch node");
                };

                nodes[index].key = update.key().to_vec();
            }
            update_branch_key &= index == 0;
            last_node_id = node_id;
        }

        self.root_node_id = NodeId::Id(last_node_id);

        Ok(())
    }

    fn merge_node(&mut self, node_id: u64, node_index: usize) -> Result<()> {
        let is_root = self.parent.contains_key(&node_id).not();

        // If root node is a branch and only has one node then collapse it.
        if is_root {
            let root = self.nodes.get_mut(&node_id).expect("root node");

            if root.is_empty() {
                let parent = InternalNodes::Leaf(Vec::new());
                let parent_id = self.insert_new(parent);
                self.root_node_id = NodeId::Id(parent_id);
                return Ok(());
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
            return Ok(());
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
                return Ok(());
            }
        }

        // Parent must have at least two nodes
        {
            let parent = self.nodes.get(&parent_id).expect("parent node");
            assert!(parent.len() > 1, "parent must have at least 2 children");
        }

        let sibling_node_id = if node_index == 0 {
            self.get_child_at_index(parent_id, node_index + 1)?
        } else {
            self.get_child_at_index(parent_id, node_index - 1)?
        };

        if node_index == 0 {
            // merge with next sibling
            let next_sibling = self.nodes.remove(&sibling_node_id).expect("next sibling");

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

        Ok(())
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

        let child_nodes = nodes
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

        let child_nodes_len = child_nodes.len();
        let parent = self.nodes.get_mut(&parent_id).expect("parent");
        parent.splice(node_index, child_nodes);
        Ok(child_nodes_len)
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
