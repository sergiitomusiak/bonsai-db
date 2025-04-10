use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::ops::Not;
use std::sync::Arc;
use crate::cursor::Cursor;
use crate::node::{InternalNodes, LeafInternalNode, Node, NodeId, NodeReader};
use crate::DatabaseState;

pub type TransactionId = u64;

pub struct WriteTransaction {
    // meta: MetaNode,
    // pending: HashMap<Vec<u8>, Option<Vec<u8>>>,
    // nodes: Tree<PageId>,
    // nodes: HashMap<PageId, Node>,
    state: Arc<DatabaseState>,
    next_node_id: u64,
    nodes: HashMap<u64, InternalNodes>,
    parent: HashMap<u64, u64>,
    children: HashMap<u64, Vec<u64>>,
    leaves: Vec<u64>,
    root_node_id: NodeId,
}

impl NodeReader for WriteTransaction {
    fn read_node<'a>(&'a self, node_id: NodeId) -> Result<Node<'a>> {
        let node = match node_id {
            NodeId::Address(address) => {
                Node::ReadOnly(self.state.node_manager.read_node(address)?)
            },
            NodeId::Id(node_id) => {
                let node = self.nodes.get(&node_id).expect("tx nodes");
                Node::Dirty(node)
            },
        };
        Ok(node)
    }
}

impl WriteTransaction {
    pub fn new(state: Arc<DatabaseState>, root_node_id: NodeId) -> Self {
        Self {
            state,
            next_node_id: 1,
            nodes: HashMap::new(),
            parent: HashMap::new(),
            children: HashMap::new(),
            leaves: Vec::new(),
            root_node_id,
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

    pub fn cursor<'a>(&'a self) -> Result<Cursor<'a>> {
        Cursor::new(self.root_node_id, self)
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
                    new_dirty_nodes.push((node_ref.index, node.as_ref().clone()));
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

        new_dirty_nodes.reverse();
        existing_dirty_nodes.reverse();

        let has_new_dirty_nodes = !new_dirty_nodes.is_empty();

        let (index, nodes, mut last_node_id) = if new_dirty_nodes.is_empty() {
            let (index, node_id) = existing_dirty_nodes
                .pop()
                .ok_or_else(|| anyhow!("database is corrupted"))?;
            let node = self.nodes.get_mut(&node_id)
                .expect("node must exist");
            let InternalNodes::Leaf(ref mut nodes) = node else {
                panic!("expected leaf node");
            };
            (index, nodes, node_id)
        } else {
            let (index, node) = new_dirty_nodes
                .pop()
                .ok_or_else(|| anyhow!("database is corrupted"))?;
            let node_id = self.insert_new(node);
            let node = self.nodes.get_mut(&node_id)
                .expect("node must exist");
            let InternalNodes::Leaf(ref mut nodes) = node else {
                panic!("expected leaf node");
            };
            (index, nodes, node_id)
        };

        self.leaves.push(last_node_id);

        let items_shifted = match &update {
            Update::Put(key, value) => {
                if index < nodes.len() && key == &nodes[index].key {
                    nodes[index].value = value.to_vec();
                    false
                } else {
                    nodes.insert(index, LeafInternalNode { key: key.to_vec(), value: value.to_vec() });
                    true
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
        while let Some((index, mut node)) = new_dirty_nodes.pop() {
            let InternalNodes::Branch(ref mut nodes) = node else {
                panic!("expected branch node");
            };

            nodes[index].node_id = NodeId::Id(last_node_id);
            if update_branch_key {
                nodes[index].key = update.key().to_vec();
            }

            update_branch_key &= index == 0;
            let inserted_node_id = self.insert_new(node);
            self.insert_parent(last_node_id, inserted_node_id);
            last_node_id = inserted_node_id;
        }

        if has_new_dirty_nodes {
            if let Some((index, node_id)) = existing_dirty_nodes.last() {
                let node = self.nodes.get_mut(&node_id)
                    .expect("node must exist");

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
                let node = self.nodes.get_mut(&node_id)
                    .expect("node must exist");

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

    pub fn commit(mut self) -> Result<()> {
        self.rebalance()?;
        // TODO: Allocate and write dirty nodes
        // TODO: Update meta pages
        // TODO: Clean-up unused pages
        // TODO: Update database state
        Ok(())
    }

    pub fn traverse(&mut self) {
        let NodeId::Id(node_id) = self.root_node_id else {
            println!("No dirty nodes");
            return;
        };
        let mut children = self.children.clone();
        self.traverse_inner(node_id, &mut children);
    }

    fn traverse_inner(&self, node_id: u64, children: &mut HashMap<u64, Vec<u64>>) {
        let node = self.nodes.get(&node_id).expect("node");
        match node {
            InternalNodes::Branch(nodes) => {
                println!("Branch = {node_id}");
                for n in nodes {
                    println!("\t{key:?} ->\t{child_node_id:?}",
                        key = String::from_utf8_lossy(&n.key),
                        child_node_id = n.node_id,
                    );
                }

                let children_ids = children.remove(&node_id).expect("children");
                for c in children_ids {
                    self.traverse_inner(c, children);
                }
            }
            InternalNodes::Leaf(nodes) => {
                println!("Leaf = {node_id}");
                for n in nodes {
                    println!("\t{key:?} ->\t{value:?}",
                        key = String::from_utf8_lossy(&n.key),
                        value = String::from_utf8_lossy(&n.value),
                    );
                }
            }
        }
    }

    fn rebalance_inner(&mut self, node_id: u64, children: &mut HashMap<u64, Vec<u64>>) -> Result<()> {
        let node = self.nodes.get(&node_id).expect("node");
        match node {
            InternalNodes::Branch(nodes) => {
                println!("Branch = {node_id}");
                for n in nodes {
                    println!("\t{key:?} ->\t{child_node_id:?}",
                        key = String::from_utf8_lossy(&n.key),
                        child_node_id = n.node_id,
                    );
                }

                let children_ids = children.remove(&node_id).expect("children");
                for c in children_ids {
                    self.rebalance_inner(c, children)?;
                }
            },
            InternalNodes::Leaf(nodes) => {
                println!("Leaf = {node_id}");
                for n in nodes {
                    println!("\t{key:?} ->\t{value:?}",
                        key = String::from_utf8_lossy(&n.key),
                        value = String::from_utf8_lossy(&n.value),
                    );
                }
            }
        }
        Ok(())
    }

    fn rebalance(&mut self) -> Result<()> {
        const MIN_KEY_PER_PAGE: usize = 2;
        while let Some(node_id) = self.leaves.pop() {
            let node = self.nodes.get(&node_id).expect("tx node");
            let page_size = self.state.node_manager.page_size();
            let merge_threshold = page_size / 4;
            if node.size() < merge_threshold && !node.has_min_keys() {
                // var threshold = n.bucket.tx.db.pageSize / 4
                // if n.size() > threshold && len(n.inodes) > n.minKeys() {
                //     return
                // }
                self.merge(node_id)?;
            } else if node.len() >= (MIN_KEY_PER_PAGE*2) && node.size() > page_size {
                // // Ignore the split if the page doesn't have at least enough nodes for
                // // two pages or if the nodes can fit in a single page.
                // if len(n.inodes) <= (minKeysPerPage*2) || n.sizeLessThan(pageSize) {
                // 	return n, nil
                // }
                self.split(node_id)?;
            }
        }
        Ok(())
    }

    fn merge(&mut self, node_id: u64) -> Result<()> {
        // If root node is a branch and only has one node then collapse it.
        let is_root = self
            .parent
            .contains_key(&node_id)
            .not();

        let root = is_root
            .then(|| node_id)
            .and_then(|node_id| self.nodes.get_mut(&node_id));

        // If root node is a branch and only has one node then collapse it.
        let new_root_id = if let Some(InternalNodes::Branch(nodes)) = root {
            if nodes.len() == 1 {
                nodes[0].node_id.id().into()
            } else {
                None
            }
        } else {
            None
        };

        if is_root {
            if let Some(new_root_id) = new_root_id {
                self.root_node_id = NodeId::Id(new_root_id);
                let parent = self.parent.remove(&new_root_id);
                assert_eq!(parent, Some(node_id));
                let children = self.children.remove(&node_id);
                assert_eq!(children, Some(vec![new_root_id]));
            }
            return Ok(());
        }

        // If node has no children then remove it
        let node = self.nodes.get(&node_id).expect("node");
        let parent_id = self.parent.remove(&node_id).expect("node parent");
        if node.len() == 0 {
            let children = self.children.get_mut(&parent_id).expect("children");
            let index = children.iter()
                .position(|child_id| *child_id == node_id)
                .expect("node child");
            children.swap_remove(index);
            return Ok(())
        }

        // Parent must have at least two nodes
        let parent = self.nodes.get(&parent_id).expect("parent node");
        assert!(parent.len() > 1, "parent must have at least 2 children");

        // Merge with a sibling
        todo!();

        // Ok(())
    }

    fn split(&mut self, node_id: u64) -> Result<()> {
        // // If we have at least the minimum number of keys and adding another
        // // node would put us over the threshold then exit and return.
        // if i >= minKeysPerPage && sz+elsize > threshold {
        //     break
        // }
        todo!()
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
        self.children.entry(parent_id).or_default().push(child_id);
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

// pub struct ReadTransaction {
//     state: Arc<DatabaseState>,
// }

// impl ReadTransaction {
//     fn get(&self, key: &[u8]) -> Option<&[u8]> {
//         todo!()
//     }
// }

// impl<'a> NodeGetter<'a, NodeRef<'a>> for WriteTransaction {
//     fn get_node(&'a self, page_id: PageId) -> Result<NodeRef<'a>> {
//         let node_ref= if let Some(node) = self.nodes.get(&page_id) {
//             NodeRef::Ref(node)
//         } else {
//             NodeRef::Arc(self.state.node_getter.get_node(page_id)?.clone())
//         };
//         Ok(node_ref)
//     }
// }

// enum NodeRef<'a> {
//     Ref(&'a Node),
//     Arc(Arc<Node>),
// }

// impl<'a> AsRef<Node> for NodeRef<'a> {
//     fn as_ref(&self) -> &Node {
//         match self {
//             Self::Ref(node) => node,
//             Self::Arc(node) => node.as_ref(),
//         }
//     }
// }

// impl WriteTransaction {
//     fn get(&self, key: &[u8]) -> Result<Option<Value>> {
//         if let Some(pending_value) = self.pending.get(key) {
//             return Ok(pending_value.clone())
//         }

//         let node_location = find_node(key, self.meta.root_node, &self.state.as_ref().node_getter)?;
//         if !node_location.exact_match {
//             return Ok(None);
//         }

//         let node = self
//             .state
//             .node_getter
//             .get_node(node_location.address)?;

//         let InternalNodes::Leaf(ref nodes) = node.as_ref() else {
//             return Err(anyhow!("corrupted database"));
//         };

//         let value = nodes[node_location.index].value.clone();
//         Ok(Some(value))
//     }

//     fn put(&mut self, key: Key, value: Value) -> Result<()> {
//         self.pending.insert(key, Some(value));
//         Ok(())
//     }

//     fn remove(&mut self, key: &[u8]) {
//         self.pending.insert(key.to_vec(), None);
//     }

//     fn commit(self) -> Result<()> {
//         // apply changes to leaf nodes
//         let mut pending_tree = Tree::new(self.meta.root_node);
//         let mut pending_nodes = HashMap::new();
//         for (key, pending_value) in self.pending {
//             let node_location = find_node(&key, self.meta.root_node, &self.state.as_ref().node_getter)?;
//             if pending_value.is_none() && !node_location.exact_match {
//                 // deleted item does not exist in database
//                 // so there is nothing to do for this item
//                 continue;
//             }

//             let mut entry = pending_nodes.entry(NodeId::Address(node_location.address));
//             let node = match entry {
//                 Entry::Occupied(ref mut entry) => entry.get_mut(),
//                 Entry::Vacant(entry) => {
//                     let node = self
//                         .state
//                         .node_getter
//                         .get_node(node_location.address)?
//                         .as_ref()
//                         .clone();

//                     entry.insert(node)
//                 }
//             };

//             let InternalNodes::Leaf(ref mut nodes) = node else {
//                 return Err(anyhow!("database is corrupted"));
//             };

//             pending_tree.insert_path(node_location.path);

//             if let Some(new_value) = pending_value {
//                 // upsert item
//                 if node_location.exact_match {
//                     nodes[node_location.index].value = new_value;
//                 } else {
//                     nodes.insert(node_location.index, LeafInternalNode {
//                         key,
//                         value: new_value,
//                     });
//                 }
//             } else {
//                 // delete item
//                 nodes.remove(node_location.index);
//             }
//         }

//         // fetch pending branch nodes
//         pending_tree.for_each(|tree_node| {
//             if pending_nodes.contains_key(&tree_node.node_id) {
//                 return Ok(());
//             }
//             let node = self.state
//                 .node_getter
//                 .get_node(tree_node.node_id.address())?
//                 .as_ref()
//                 .clone();
//             pending_nodes.insert(tree_node.node_id, node);
//             Ok(())
//         })?;

//         // merge nodes that are too small
//         // pending_tree.merge(&mut pending_nodes, self.state.as_ref());

//         // split nodes that are too large
//         pending_tree.split(self.state.as_ref());

//         // TODO: write nodes to file
//         Ok(())
//     }

//     fn rollback(self) {
//         todo!()
//     }
// }

// pub struct ReadTransaction {
//     id: TransactionId,
//     root: Address,
//     meta: MetaNode,
//     state: Arc<DatabaseState>,
// }

// impl ReadTransaction {
//     fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
//         let node_location =
//             find_node(key, self.meta.root_node, &self.state.node_getter)?;
//         if !node_location.exact_match {
//             return Ok(None);
//         }

//         let node = self
//             .state
//             .node_getter
//             .get_node(node_location.address)?;

//         let InternalNodes::Leaf(ref nodes) = node.as_ref() else {
//             return Err(anyhow!("corrupted database"));
//         };

//         let value = nodes[node_location.index].value.clone();
//         Ok(Some(value))
//     }

//     fn close(self) {
//         todo!()
//     }
// }

// struct Tree {
//     root: TreeNode,
//     // state: Arc<DatabaseState>,
//     // internal_nodes: HashMap<NodeId, InternalNodes>,
// }

// struct TreeNode {
//     node_id: NodeId,
//     children: HashMap<NodeId, TreeNode>,
// }

// impl Tree {
//     fn new(root_node_address: Address) -> Self {
//         Self {
//             root: TreeNode {
//                 node_id: NodeId::Address(root_node_address),
//                 children: HashMap::default(),
//             },
//         }
//     }

//     fn insert_path(&mut self, mut path: Vec<Address>) {
//         path.reverse();
//         let root_address = path.pop().expect("path not empty");
//         assert_eq!(root_address, self.root.node_id.address());
//         let mut children = &mut self.root.children;
//         while let Some(address) = path.pop() {
//             let node_id = NodeId::Address(address);
//             children = &mut children
//                 .entry(node_id)
//                 .or_insert_with(|| TreeNode {
//                     node_id: NodeId::Address(address),
//                     children: HashMap::default(),
//                 })
//                 .children;
//         }
//     }

//     fn merge(&mut self, nodes: &mut HashMap<NodeId, InternalNodes>, remove_nodes: &mut HashSet<Address>, state: &DatabaseState) -> Result<()> {
//         let _ = Self::merge_internal(&mut self.root, nodes, remove_nodes, state)?;
//         Ok(())
//     }

//     fn merge_internal(
//         tree_node: &mut TreeNode,
//         nodes: &mut HashMap<NodeId, InternalNodes>,
//         remove_nodes: &mut HashSet<Address>,
//         state: &DatabaseState,
//     ) -> Result<()> {
//         for (_, child_node) in tree_node.children.iter_mut() {
//             Self::merge_internal(child_node, nodes, remove_nodes, state)?;
//         }

//         let threshold = state.page_size / 4;
//         let mut child_node_ids = tree_node
//             .children
//             .iter()
//             .map(|(node_id, _)| *node_id)
//             .collect::<Vec<_>>();

//         while let Some(child_node_id) = child_node_ids.pop() {
//             let child_node = nodes.get_mut(&child_node_id).expect("pending node");
//             if child_node.size() > threshold && child_node.has_min_keys() {
//                 continue;
//             }

//             // No children, just remove the node
//             if child_node.size() == 0 {
//                 let removed = tree_node.children.remove(&child_node_id).is_some();
//                 assert!(removed);
//                 let removed = nodes.get_mut(&tree_node.node_id)
//                     .expect("merge parent node")
//                     .remove(&child_node_id);
//                 // let removed = child_node.remove(&child_node_id);
//                 assert!(removed);
//                 remove_nodes.insert(child_node_id.address());
//                 continue;
//             }


//         }


//         // let threshold = state.page_size / 4;
//         // for child_node_id in child_node_ids {
//         //     let child_node = nodes.get_mut(&child_node_id).expect("pending node");
//         //     if child_node.size() > threshold && child_node.has_min_keys() {
//         //         continue;
//         //     }

//         //     // No children, just remove the node
//         //     if child_node.size() == 0 {
//         //         let removed = tree_node.children.remove(&child_node_id).is_some();
//         //         assert!(removed);
//         //         let removed = nodes.get_mut(&tree_node.node_id)
//         //             .expect("merge parent node")
//         //             .remove(&child_node_id);
//         //         // let removed = child_node.remove(&child_node_id);
//         //         assert!(removed);
//         //         remove_nodes.insert(child_node_id.address());
//         //         continue;
//         //     }

//         //     // assert!()

//         //     // find sibling to merge with
//         //     let internal_nodes = nodes.get_mut(&tree_node.node_id)
//         //         .expect("merge parent node");
//         //     let child_index = internal_nodes.index_of(&child_node_id).expect("child index");
//         //     let target_index = if child_index == 0 {
//         //         child_index + 1
//         //     } else {
//         //         child_index - 1
//         //     };
//         //     // Self::merge_internal(child_node, nodes, state)?;
//         //     // check if node is too small a
//         // }

//         todo!()
//     }

//     fn split(&mut self, state: &DatabaseState) {
//     }

//     fn for_each<F: FnMut(&TreeNode) -> Result<()>>(&self, mut f: F) -> Result<()> {
//         f(&self.root)?;
//         for_each(&self.root, &mut f)
//     }
// }

// fn for_each<F: FnMut(&TreeNode) -> Result<()>>(node: &TreeNode, f: &mut F) -> Result<()> {
//     for (_, child_node) in node.children.iter() {
//         f(child_node)?;
//         for_each(child_node, f)?;
//     }
//     Ok(())
// }

// struct Rebalancer {
//     tree: Tree,
//     pending_nodes: HashMap<NodeId, InternalNodes>,
//     free_nodes: HashSet<Address>,
//     state: Arc<DatabaseState>,
//     id: u64,
// }

// #[derive(Clone, Copy)]
// enum ChildMergeResult {
//     Continue,
//     Remove,
//     ReplaceWith(NodeId),
// }

// impl Rebalancer {
//     fn merge(&mut self, tree: &mut Tree) -> Result<()> {
//         let _ = self.merge_internal(&mut tree.root)?;
//         Ok(())
//     }

//     fn merge_internal(
//         &mut self,
//         tree_node: &mut TreeNode,
//         // nodes: &mut HashMap<NodeId, InternalNodes>,
//         // remove_nodes: &mut HashSet<Address>,
//         // state: &DatabaseState,
//     ) -> Result<ChildMergeResult> {
//         let mut merge_results = Vec::new();
//         for (child_node_id, child_node) in tree_node.children.iter_mut() {
//             merge_results.push((*child_node_id, self.merge_internal(child_node)?));
//         }

//         for (child_node_id, merge_result) in merge_results {
//             match merge_result {
//                 ChildMergeResult::Continue => {},
//                 ChildMergeResult::Remove => {
//                     let removed = tree_node.children.remove(&child_node_id).is_some();
//                     assert!(removed);
//                 },
//                 ChildMergeResult::ReplaceWith(node_id) => {
//                     let removed = tree_node.children.remove(&child_node_id).is_some();
//                     assert!(removed);
//                 },
//             };
//         }

//         let threshold = self.state.page_size / 4;
//         let mut child_node_ids = tree_node
//             .children
//             .iter()
//             .map(|(node_id, _)| *node_id)
//             .collect::<Vec<_>>();

//         let internal_nodes = self.pending_nodes
//             .remove(&tree_node.node_id)
//             .expect("merge parent node");

//         while let Some(child_node_id) = child_node_ids.pop() {
//             let child_node = self
//                 .pending_nodes
//                 .get_mut(&child_node_id)
//                 .expect("pending node");

//             if child_node.size() > threshold && child_node.has_min_keys() {
//                 // Child no is occupying enough space and has enough entries
//                 // to stay as and should not be merged.
//                 continue;
//             }

//             if child_node.size() == 0 {
//                 // Child node has no children, just remove the node
//                 let removed = tree_node.children.remove(&child_node_id).is_some();
//                 assert!(removed);
//                 let removed = self.pending_nodes
//                     .get_mut(&tree_node.node_id)
//                     .expect("merge parent node")
//                     .remove(&child_node_id);
//                 assert!(removed);
//                 self.free_nodes.insert(child_node_id.address());
//                 continue;
//             }

//             if internal_nodes.len() < 2 {
//                 break;
//             }

//             // find sibling to merge with
//             let child_index = internal_nodes
//                 .index_of(&child_node_id)
//                 .expect("child index");

//             let target_index = if child_index == 0 {
//                 child_index + 1
//             } else {
//                 child_index - 1
//             };

//             // get node at target_index
//             // merge child node and target node into new node
//             // free child and target nodes
//         }

//         let result = if internal_nodes.size() == 0 {
//             // Current node is empty and should be deleted from its parent
//             ChildMergeResult::Remove
//         } else if internal_nodes.len() == 1 {
//             if let Some(first_child) = internal_nodes.first_child() {
//                 // Current branch node should be replaced by its single child
//                 ChildMergeResult::ReplaceWith(first_child.node_id)
//             } else {
//                 ChildMergeResult::Continue
//             }
//         } else {
//             ChildMergeResult::Continue
//         };

//         self.pending_nodes.insert(tree_node.node_id, internal_nodes);
//         Ok(result)


//         //     // No children, just remove the node
//         //     if child_node.size() == 0 {
//         //         let removed = tree_node.children.remove(&child_node_id).is_some();
//         //         assert!(removed);
//         //         let removed = nodes.get_mut(&tree_node.node_id)
//         //             .expect("merge parent node")
//         //             .remove(&child_node_id);
//         //         // let removed = child_node.remove(&child_node_id);
//         //         assert!(removed);
//         //         remove_nodes.insert(child_node_id.address());
//         //         continue;
//         //     }

//         //     // assert!()

//         //     // find sibling to merge with
//         //     let internal_nodes = nodes.get_mut(&tree_node.node_id)
//         //         .expect("merge parent node");
//         //     let child_index = internal_nodes.index_of(&child_node_id).expect("child index");
//         //     let target_index = if child_index == 0 {
//         //         child_index + 1
//         //     } else {
//         //         child_index - 1
//         //     };
//         //     // Self::merge_internal(child_node, nodes, state)?;
//         //     // check if node is too small a
//         // }

//         // todo!()
//     }
// }

// // struct Rebalance {
// //     pending_tree: Tree<PageId>,
// //     pending_nodes: HashMap<PageId, InternalNodes>,
// // }

// // impl Rebalance {
// //     fn rebalance(&mut self, parent: Option<PageId>, node: &PageId) {
// //         // rebalance child nodes
// //         // TODO

// //         let Some(parent) = parent else {
// //             todo!()
// //             // handle parent node update
// //         };

// //         // node has to be merged
// //         // if it has too few children or its size is less than page_size*0.25

// //         // node has to be split, otherwise
// //     }
// // }