use std::fmt::{Display, Formatter};

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

#[derive(Debug)]
pub enum TreeFileError {
    NonExistingFiles,
    NonExistingNode,
    LogicError {msg: String},
    FileIOError {msg: String},
}

impl Display for TreeFileError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TreeFileError::NonExistingFiles => {
                write!(f, "NonExistingFiles: tried to open a non existing tree file in open mode MustExist")
            },
            TreeFileError::NonExistingNode => {
                write!(f, "NonExistingNode: node does not exists in tree")
            },
            TreeFileError::LogicError {msg} => {
                write!(f, "LogicError: {}", msg)
            },
            TreeFileError::FileIOError {msg} => {
                write!(f, "FileIOError: {}", msg)
            },
        }
    }
}