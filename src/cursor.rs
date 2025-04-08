use anyhow::Result;

use crate::node::{InternalNodes, Node, NodeId, NodeReader};

pub struct Cursor<'a> {
    pub root_node_id: NodeId,
    pub stack: Vec<CursorNodeRef<'a>>,
    pub node_reader: &'a dyn NodeReader,
    // pub node_manager: Arc<NodeManager>,
    // pub state: Arc<DatabaseState>,
    // pub current_node: Arc<InternalNodes>,
}

#[derive(Clone, Debug)]
pub struct CursorNodeRef<'a> {
    pub node: Node<'a>,
    pub node_id: NodeId,
    pub index: usize,
}

impl<'a> Cursor<'a> {
    pub fn new(root_node_id: NodeId, node_reader: &'a dyn NodeReader) -> Result<Self> {
        let root = node_reader.read_node(root_node_id)?;
        let mut cursor = Self {
            root_node_id,
            node_reader,
            stack: vec![CursorNodeRef{node: root, index: 0, node_id: root_node_id}],
        };
        cursor.move_to_first_leaf()?;
        Ok(cursor)
    }

    pub fn is_valid(&self) -> bool {
        let Some(element) = self.stack.last() else {
            return false;
        };
        element.index < element.node.len()
    }

    pub fn key(&self) -> &[u8] {
        assert!(self.is_valid(), "cursor must be valid");
        let element = self.stack.last().expect("cursor stack top");
        let InternalNodes::Leaf(ref nodes) = element.node.as_ref() else {
            panic!("cursor must point to a leaf node");
        };
        nodes[element.index].key.as_ref()
    }

    pub fn value(&self) -> &[u8] {
        assert!(self.is_valid(), "cursor must be valid");
        let element = self.stack.last().expect("cursor stack top");
        let InternalNodes::Leaf(ref nodes) = element.node.as_ref() else {
            panic!("cursor must point to a leaf node");
        };
        nodes[element.index].value.as_ref()
    }

    pub fn first(&mut self) -> Result<()> {
        self.stack.drain(1..);
        let element = self.stack.last_mut().expect("cursor stack top");
        element.index = 0;
        self.move_to_first_leaf()?;
        if self.stack.last().expect("cursor stack top").node.is_empty() {
            self.next()?;
        }
        Ok(())
    }

    pub fn last(&mut self) -> Result<()> {
        self.stack.drain(1..);
        let element = self.stack.last_mut().expect("cursor stack top");
        element.index = if element.node.is_empty() { 0 } else { element.node.len() - 1 };
        self.move_to_last_leaf()?;
        if self.stack.last().expect("cursor stack top").node.is_empty() {
            self.next()?;
        }
        Ok(())
    }

    pub fn prev(&mut self) -> Result<bool> {
        let element = self.stack.last_mut().expect("cursor stack top");
        if element.index > 0 {
            assert!(element.node.is_leaf(), "cursor must point to a leaf node");
            element.index -= 1;
            return Ok(true);
        }

        loop {
            let mut last_index = None;
            for i in (0..self.stack.len()).rev() {
                let element = &mut self.stack[i];
                if element.index > 0 {
                    element.index -= 1;
                    last_index = Some(i);
                    break
                }
            }

            let Some(last_index) = last_index else {
                return Ok(false);
            };

            self.stack.drain(last_index+1..);
            self.move_to_last_leaf()?;

            if !self.stack.last().expect("cursor stack top").node.is_empty() {
                break
            }
        }
        Ok(true)
    }

    pub fn next(&mut self) -> Result<()> {
        let element = self.stack.last_mut().expect("cursor stack top");
        if element.index < element.node.len()-1 {
            assert!(element.node.is_leaf(), "cursor must point to a leaf node");
            element.index += 1;
            return Ok(());
        }

        loop {
            let mut last_index = None;
            for i in (0..self.stack.len()).rev() {
                let element = &mut self.stack[i];
                if element.index < element.node.len()-1 {
                    element.index += 1;
                    last_index = Some(i);
                    break
                }
            }

            let Some(last_index) = last_index else {
                self.stack.last_mut().expect("cursor stack top").index += 1;
                break;
            };

            self.stack.drain(last_index+1..);
            self.move_to_first_leaf()?;

            if !self.stack.last().expect("cursor stack top").node.is_empty() {
                break
            }
        }
        Ok(())
    }

    pub fn seek(&mut self, key: &[u8]) -> Result<()> {
        self.stack.drain(1..);

        loop {
            let element = self.stack.last_mut().expect("cursor stack top");

            // binary search on current node
            match element.node.as_ref() {
                InternalNodes::Branch(nodes) => {
                    let index = nodes.binary_search_by(|node| {
                        let node_key = &node.key[..];
                        node_key.cmp(key)
                    }).unwrap_or_else(|index| if index > 0 { index - 1 } else { 0 });

                    element.index = index;
                    let node_id = nodes[element.index].node_id;
                    let node = self.node_reader.read_node(node_id)?;
                    self.stack.push(CursorNodeRef { node, index: 0, node_id });
                },
                InternalNodes::Leaf(nodes) => {
                    if element.node.is_empty() {
                        self.next()?;
                    } else {
                        let index = nodes.binary_search_by(|node| {
                            let node_key = &node.key[..];
                            node_key.cmp(key)
                        }).unwrap_or_else(|index| index);

                        if index > element.node.len()-1 {
                            element.index = element.node.len()-1;
                            self.next()?;
                        } else {
                            element.index = index;
                        }
                    }

                    break;
                },
            };
        }

        Ok(())
    }

    // fn node_ref(&self) -> CursorNodeRef {
    //     assert!(self.is_valid(), "cursor must be valid");
    //     self.stack.last().expect("cursor stack top").clone()
    // }

    fn move_to_first_leaf(&mut self) -> Result<()> {
        loop {
            let element = self
                .stack
                .last()
                .expect("cursor stack last element");

            let InternalNodes::Branch(ref nodes) = element.node.as_ref() else {
                break;
            };

            let node_address = nodes[element.index].node_id;
            let node = self.node_reader.read_node(node_address)?;

            self.stack.push(CursorNodeRef { node, index: 0, node_id: node_address });
        }
        Ok(())
    }

    fn move_to_last_leaf(&mut self) -> Result<()> {
        loop {
            let element = self
                .stack
                .last()
                .expect("cursor stack last element");

            let InternalNodes::Branch(ref nodes) = element.node.as_ref() else {
                break;
            };

            let node_address = nodes[element.index].node_id;
            let node = self.node_reader.read_node(node_address)?;
            let index = if node.is_empty() { 0 } else { node.len() - 1 };
            self.stack.push(CursorNodeRef { node, index, node_id: node_address });
        }
        Ok(())
    }

}