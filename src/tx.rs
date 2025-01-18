use anyhow::{anyhow, Result};
use std::collections::HashSet;
//use std::collections::BTreeMap;
use std::collections::{hash_map::Entry, HashMap};
use std::sync::Arc;
use crate::{Key, Value};
use crate::node::{find_node, Address, InternalNodes, LeafInternalNode, MetaNode, NodeGetter, NodeId};
use crate::DatabaseState;

pub type TransactionId = u64;

pub struct WriteTransaction {
    meta: MetaNode,
    pending: HashMap<Vec<u8>, Option<Vec<u8>>>,
    // nodes: Tree<PageId>,
    // nodes: HashMap<PageId, Node>,
    state: Arc<DatabaseState>,
}

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

impl WriteTransaction {
    fn get(&self, key: &[u8]) -> Result<Option<Value>> {
        if let Some(pending_value) = self.pending.get(key) {
            return Ok(pending_value.clone())
        }

        let node_location = find_node(key, self.meta.root_node, &self.state.as_ref().node_getter)?;
        if !node_location.exact_match {
            return Ok(None);
        }

        let node = self
            .state
            .node_getter
            .get_node(node_location.address)?;

        let InternalNodes::Leaf(ref nodes) = node.as_ref() else {
            return Err(anyhow!("corrupted database"));
        };

        let value = nodes[node_location.index].value.clone();
        Ok(Some(value))
    }

    fn put(&mut self, key: Key, value: Value) -> Result<()> {
        self.pending.insert(key, Some(value));
        Ok(())
    }

    fn remove(&mut self, key: &[u8]) {
        self.pending.insert(key.to_vec(), None);
    }

    fn commit(self) -> Result<()> {
        // apply changes to leaf nodes
        let mut pending_tree = Tree::new(self.meta.root_node);
        let mut pending_nodes = HashMap::new();
        for (key, pending_value) in self.pending {
            let node_location = find_node(&key, self.meta.root_node, &self.state.as_ref().node_getter)?;
            if pending_value.is_none() && !node_location.exact_match {
                // deleted item does not exist in database
                // so there is nothing to do for this item
                continue;
            }

            let mut entry = pending_nodes.entry(NodeId::Address(node_location.address));
            let node = match entry {
                Entry::Occupied(ref mut entry) => entry.get_mut(),
                Entry::Vacant(entry) => {
                    let node = self
                        .state
                        .node_getter
                        .get_node(node_location.address)?
                        .as_ref()
                        .clone();

                    entry.insert(node)
                }
            };

            let InternalNodes::Leaf(ref mut nodes) = node else {
                return Err(anyhow!("database is corrupted"));
            };

            pending_tree.insert_path(node_location.path);

            if let Some(new_value) = pending_value {
                // upsert item
                if node_location.exact_match {
                    nodes[node_location.index].value = new_value;
                } else {
                    nodes.insert(node_location.index, LeafInternalNode {
                        key,
                        value: new_value,
                    });
                }
            } else {
                // delete item
                nodes.remove(node_location.index);
            }
        }

        // fetch pending branch nodes
        pending_tree.for_each(|tree_node| {
            if pending_nodes.contains_key(&tree_node.node_id) {
                return Ok(());
            }
            let node = self.state
                .node_getter
                .get_node(tree_node.node_id.address())?
                .as_ref()
                .clone();
            pending_nodes.insert(tree_node.node_id, node);
            Ok(())
        })?;

        // merge nodes that are too small
        // pending_tree.merge(&mut pending_nodes, self.state.as_ref());

        // split nodes that are too large
        pending_tree.split(self.state.as_ref());

        // TODO: write nodes to file
        Ok(())
    }

    fn rollback(self) {
        todo!()
    }
}

pub struct ReadTransaction {
    id: TransactionId,
    root: Address,
    meta: MetaNode,
    state: Arc<DatabaseState>,
}

impl ReadTransaction {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let node_location =
            find_node(key, self.meta.root_node, &self.state.node_getter)?;
        if !node_location.exact_match {
            return Ok(None);
        }

        let node = self
            .state
            .node_getter
            .get_node(node_location.address)?;

        let InternalNodes::Leaf(ref nodes) = node.as_ref() else {
            return Err(anyhow!("corrupted database"));
        };

        let value = nodes[node_location.index].value.clone();
        Ok(Some(value))
    }

    fn close(self) {
        todo!()
    }
}

struct Tree {
    root: TreeNode,
    // state: Arc<DatabaseState>,
    // internal_nodes: HashMap<NodeId, InternalNodes>,
}

struct TreeNode {
    node_id: NodeId,
    children: HashMap<NodeId, TreeNode>,
}

impl Tree {
    fn new(root_node_address: Address) -> Self {
        Self {
            root: TreeNode {
                node_id: NodeId::Address(root_node_address),
                children: HashMap::default(),
            },
        }
    }

    fn insert_path(&mut self, mut path: Vec<Address>) {
        path.reverse();
        let root_address = path.pop().expect("path not empty");
        assert_eq!(root_address, self.root.node_id.address());
        let mut children = &mut self.root.children;
        while let Some(address) = path.pop() {
            let node_id = NodeId::Address(address);
            children = &mut children
                .entry(node_id)
                .or_insert_with(|| TreeNode {
                    node_id: NodeId::Address(address),
                    children: HashMap::default(),
                })
                .children;
        }
    }

    fn merge(&mut self, nodes: &mut HashMap<NodeId, InternalNodes>, remove_nodes: &mut HashSet<Address>, state: &DatabaseState) -> Result<()> {
        let _ = Self::merge_internal(&mut self.root, nodes, remove_nodes, state)?;
        Ok(())
    }

    fn merge_internal(
        tree_node: &mut TreeNode,
        nodes: &mut HashMap<NodeId, InternalNodes>,
        remove_nodes: &mut HashSet<Address>,
        state: &DatabaseState,
    ) -> Result<()> {
        for (_, child_node) in tree_node.children.iter_mut() {
            Self::merge_internal(child_node, nodes, remove_nodes, state)?;
        }

        let threshold = state.page_size / 4;
        let mut child_node_ids = tree_node
            .children
            .iter()
            .map(|(node_id, _)| *node_id)
            .collect::<Vec<_>>();

        while let Some(child_node_id) = child_node_ids.pop() {
            let child_node = nodes.get_mut(&child_node_id).expect("pending node");
            if child_node.size() > threshold && child_node.has_min_keys() {
                continue;
            }

            // No children, just remove the node
            if child_node.size() == 0 {
                let removed = tree_node.children.remove(&child_node_id).is_some();
                assert!(removed);
                let removed = nodes.get_mut(&tree_node.node_id)
                    .expect("merge parent node")
                    .remove(&child_node_id);
                // let removed = child_node.remove(&child_node_id);
                assert!(removed);
                remove_nodes.insert(child_node_id.address());
                continue;
            }


        }


        // let threshold = state.page_size / 4;
        // for child_node_id in child_node_ids {
        //     let child_node = nodes.get_mut(&child_node_id).expect("pending node");
        //     if child_node.size() > threshold && child_node.has_min_keys() {
        //         continue;
        //     }

        //     // No children, just remove the node
        //     if child_node.size() == 0 {
        //         let removed = tree_node.children.remove(&child_node_id).is_some();
        //         assert!(removed);
        //         let removed = nodes.get_mut(&tree_node.node_id)
        //             .expect("merge parent node")
        //             .remove(&child_node_id);
        //         // let removed = child_node.remove(&child_node_id);
        //         assert!(removed);
        //         remove_nodes.insert(child_node_id.address());
        //         continue;
        //     }

        //     // assert!()

        //     // find sibling to merge with
        //     let internal_nodes = nodes.get_mut(&tree_node.node_id)
        //         .expect("merge parent node");
        //     let child_index = internal_nodes.index_of(&child_node_id).expect("child index");
        //     let target_index = if child_index == 0 {
        //         child_index + 1
        //     } else {
        //         child_index - 1
        //     };
        //     // Self::merge_internal(child_node, nodes, state)?;
        //     // check if node is too small a
        // }

        todo!()
    }

    fn split(&mut self, state: &DatabaseState) {
    }

    fn for_each<F: FnMut(&TreeNode) -> Result<()>>(&self, mut f: F) -> Result<()> {
        f(&self.root)?;
        for_each(&self.root, &mut f)
    }
}

fn for_each<F: FnMut(&TreeNode) -> Result<()>>(node: &TreeNode, f: &mut F) -> Result<()> {
    for (_, child_node) in node.children.iter() {
        f(child_node)?;
        for_each(child_node, f)?;
    }
    Ok(())
}

struct Rebalancer {
    tree: Tree,
    pending_nodes: HashMap<NodeId, InternalNodes>,
    free_nodes: HashSet<Address>,
    state: Arc<DatabaseState>,
    id: u64,
}

#[derive(Clone, Copy)]
enum ChildMergeResult {
    Continue,
    Remove,
    ReplaceWith(NodeId),
}

impl Rebalancer {
    fn merge(&mut self, tree: &mut Tree) -> Result<()> {
        let _ = self.merge_internal(&mut tree.root)?;
        Ok(())
    }

    fn merge_internal(
        &mut self,
        tree_node: &mut TreeNode,
        // nodes: &mut HashMap<NodeId, InternalNodes>,
        // remove_nodes: &mut HashSet<Address>,
        // state: &DatabaseState,
    ) -> Result<ChildMergeResult> {
        let mut merge_results = Vec::new();
        for (child_node_id, child_node) in tree_node.children.iter_mut() {
            merge_results.push((*child_node_id, self.merge_internal(child_node)?));
        }

        for (child_node_id, merge_result) in merge_results {
            match merge_result {
                ChildMergeResult::Continue => {},
                ChildMergeResult::Remove => {
                    let removed = tree_node.children.remove(&child_node_id).is_some();
                    assert!(removed);
                },
                ChildMergeResult::ReplaceWith(node_id) => {
                    let removed = tree_node.children.remove(&child_node_id).is_some();
                    assert!(removed);
                },
            };
        }

        let threshold = self.state.page_size / 4;
        let mut child_node_ids = tree_node
            .children
            .iter()
            .map(|(node_id, _)| *node_id)
            .collect::<Vec<_>>();

        let internal_nodes = self.pending_nodes
            .remove(&tree_node.node_id)
            .expect("merge parent node");

        while let Some(child_node_id) = child_node_ids.pop() {
            let child_node = self
                .pending_nodes
                .get_mut(&child_node_id)
                .expect("pending node");

            if child_node.size() > threshold && child_node.has_min_keys() {
                // Child no is occupying enough space and has enough entries
                // to stay as and should not be merged.
                continue;
            }

            if child_node.size() == 0 {
                // Child node has no children, just remove the node
                let removed = tree_node.children.remove(&child_node_id).is_some();
                assert!(removed);
                let removed = self.pending_nodes
                    .get_mut(&tree_node.node_id)
                    .expect("merge parent node")
                    .remove(&child_node_id);
                assert!(removed);
                self.free_nodes.insert(child_node_id.address());
                continue;
            }

            if internal_nodes.len() < 2 {
                break;
            }

            // find sibling to merge with
            let child_index = internal_nodes
                .index_of(&child_node_id)
                .expect("child index");

            let target_index = if child_index == 0 {
                child_index + 1
            } else {
                child_index - 1
            };

            // get node at target_index
            // merge child node and target node into new node
            // free child and target nodes
        }

        let result = if internal_nodes.size() == 0 {
            // Current node is empty and should be deleted from its parent
            ChildMergeResult::Remove
        } else if internal_nodes.len() == 1 {
            if let Some(first_child) = internal_nodes.first_child() {
                // Current branch node should be replaced by its single child
                ChildMergeResult::ReplaceWith(first_child.node_id)
            } else {
                ChildMergeResult::Continue
            }
        } else {
            ChildMergeResult::Continue
        };

        self.pending_nodes.insert(tree_node.node_id, internal_nodes);
        Ok(result)


        //     // No children, just remove the node
        //     if child_node.size() == 0 {
        //         let removed = tree_node.children.remove(&child_node_id).is_some();
        //         assert!(removed);
        //         let removed = nodes.get_mut(&tree_node.node_id)
        //             .expect("merge parent node")
        //             .remove(&child_node_id);
        //         // let removed = child_node.remove(&child_node_id);
        //         assert!(removed);
        //         remove_nodes.insert(child_node_id.address());
        //         continue;
        //     }

        //     // assert!()

        //     // find sibling to merge with
        //     let internal_nodes = nodes.get_mut(&tree_node.node_id)
        //         .expect("merge parent node");
        //     let child_index = internal_nodes.index_of(&child_node_id).expect("child index");
        //     let target_index = if child_index == 0 {
        //         child_index + 1
        //     } else {
        //         child_index - 1
        //     };
        //     // Self::merge_internal(child_node, nodes, state)?;
        //     // check if node is too small a
        // }

        // todo!()
    }
}

// struct Rebalance {
//     pending_tree: Tree<PageId>,
//     pending_nodes: HashMap<PageId, InternalNodes>,
// }

// impl Rebalance {
//     fn rebalance(&mut self, parent: Option<PageId>, node: &PageId) {
//         // rebalance child nodes
//         // TODO

//         let Some(parent) = parent else {
//             todo!()
//             // handle parent node update
//         };

//         // node has to be merged
//         // if it has too few children or its size is less than page_size*0.25

//         // node has to be split, otherwise
//     }
// }