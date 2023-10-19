use std::fs::{read_dir, remove_file};
use rust_tree_map::multi_file_tree_map::MultiFileTreeMap;
use rust_tree_map::OpenMode::TruncateCreate;

const MAP_PATH: &str = "tests/test_data";

#[allow(dead_code)]
fn remove_files<F>(tree_map: MultiFileTreeMap<F>)
    where F: Fn(u16) -> u8
{
    drop(tree_map);

    for entry in read_dir(MAP_PATH).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_file() {
            remove_file(path).unwrap();
        }
    }
}

#[test]
fn create_a_new_tree() {
    let splitter: fn(u16) -> u8 = |k| {(k >> 8) as u8};
    //let key1 = ((10 << 8) + 1) as u16;

    let res = MultiFileTreeMap::new(MAP_PATH, 2, TruncateCreate, splitter);
    assert!(res.is_ok(), "tree not created");

    if let Ok(mut t) = res {
        let top = t.get_top();
        assert_eq!(top, 0, "top node shall always have node id 0 (zero)");

        let node = t.get_node(top);
        assert!(node.is_ok(), "could not get node");

        if let Ok(nd) = node {
            assert_eq!(nd.node_id, 0, "should have node id 0, got {}", nd.node_id);
            assert_eq!(nd.hits, 0, "should have 0 hits, got {}", nd.hits);
            assert_eq!(nd.score, 0, "should have score 0, got {}", nd.score);
            assert!(nd.parent.is_none(), "got parent for top node");
        }
    }

}