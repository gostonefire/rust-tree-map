pub mod multi_file_tree_map;
pub mod tree_map;
mod utils;

pub type NodeId = usize;

pub struct NodeData {
    pub node_id: NodeId,
    node_pos: u64,
    pub parent: Option<NodeId>,
    pub hits: u64,
    pub score: u64,
    first_child_pos: u64,
    pub n_children: u32,
    pub max_children: u32,
}

#[derive(Clone)]
pub enum OpenMode {
    TruncateCreate,
    OpenCreate,
    MustExist,
}

pub struct Iter {
    key_vals: Vec<(u16, NodeId)>,
}

impl Iterator for Iter {
    type Item = (u16, NodeId);
    fn next(&mut self) -> Option<Self::Item> {
        self.key_vals.pop()
    }
}
