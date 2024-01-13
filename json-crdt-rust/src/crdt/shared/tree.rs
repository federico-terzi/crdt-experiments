use std::collections::VecDeque;

use enum_as_inner::EnumAsInner;
use heapless::Vec as StackVec;
use rustc_hash::FxHashMap;

use crate::SequenceBlockId;

#[derive(Clone, PartialEq)]
pub struct SequenceTree<Items: SequenceItems, const BRANCH_SIZE: usize, const LEAF_SIZE: usize> {
    blocks: Vec<SequenceBlock<Items>>,

    nodes: Vec<Node<BRANCH_SIZE, LEAF_SIZE>>,
    root: NodeIndex,
    start: NodeIndex,
    end: NodeIndex,

    // TODO: optimize to id_to_node: FxHashMap<ClientId, Vec<NodeIndex>>,
    block_children: FxHashMap<SequenceBlockId, Vec<SequenceBlockId>>,
    root_blocks: Vec<SequenceBlockId>,
    sequence_id_to_node: FxHashMap<SequenceBlockId, NodeIndex>,
}

impl<Items: SequenceItems, const BRANCH_SIZE: usize, const LEAF_SIZE: usize>
    SequenceTree<Items, BRANCH_SIZE, LEAF_SIZE>
{
    pub fn new() -> Self {
        let mut nodes = vec![Node::new_root()];

        Self {
            blocks: Vec::new(),
            nodes,
            root: 0,
            start: 0,
            end: 0,
            block_children: FxHashMap::default(),
            root_blocks: Vec::new(),
            sequence_id_to_node: FxHashMap::default(),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &Items> {
        // println!(
        //     "sizeof SequenceBlockId {}",
        //     std::mem::size_of::<SequenceBlockId>()
        // );
        // println!("blocks {}", self.blocks.len());
        // println!("block size {}", std::mem::size_of::<SequenceBlock<Items>>());
        // println!("nodes {}", self.nodes.len());
        // println!(
        //     "node size {}",
        //     std::mem::size_of::<Node<BRANCH_SIZE, LEAF_SIZE>>()
        // );
        // println!("block_children {}", self.block_children.len());
        // println!("sequence_id_to_node {}", self.sequence_id_to_node.len());

        SequenceTreeIterator::new(self)
    }

    pub fn find_id_starting_at_position(&self, position: u32) -> Option<SequenceBlockId> {
        let mut current_node_index: Option<NodeIndex> = Some(self.root);
        let mut current_position = 0;

        loop {
            let node = &self.nodes[current_node_index.expect("node should exist") as usize];
            match node {
                Node::Branch(branch_node) => {
                    for branch in branch_node.items.iter() {
                        if current_position + branch.total_size > position {
                            current_node_index = Some(branch.node);
                            break;
                        } else {
                            current_position += branch.total_size;
                        }
                    }
                }
                Node::Leaf(_) => {
                    while let Some(node_index) = current_node_index {
                        let leaf_node = &self.nodes[node_index as usize]
                            .as_leaf()
                            .expect("not a leaf");

                        for block_index in leaf_node.items.iter() {
                            let block = &self.blocks[*block_index];

                            if block.deleted {
                                continue;
                            }

                            if current_position + block.items.len() as u32 > position {
                                let offset = position - current_position;
                                return Some(SequenceBlockId {
                                    client_id: block.id.client_id.clone(),
                                    sequence: block.id.sequence + offset as u32,
                                });
                            } else {
                                current_position += block.items.len() as u32;
                            }
                        }

                        current_node_index = leaf_node.next_block;
                    }

                    return None;
                }
            }
        }
    }
    pub fn find_id_ending_at_position(&self, position: u32) -> Option<SequenceBlockId> {
        if position == 0 {
            return None;
        }

        let mut current_node_index: Option<NodeIndex> = Some(self.root);
        let mut current_position = 0;

        loop {
            let node = &self.nodes[current_node_index.expect("node should exist") as usize];
            match node {
                Node::Branch(branch_node) => {
                    for branch in branch_node.items.iter() {
                        if current_position + branch.total_size >= position {
                            current_node_index = Some(branch.node);
                            break;
                        } else {
                            current_position += branch.total_size;
                        }
                    }
                }
                Node::Leaf(_) => {
                    while let Some(node_index) = current_node_index {
                        let leaf_node = &self.nodes[node_index as usize]
                            .as_leaf()
                            .expect("not a leaf");

                        for block_index in leaf_node.items.iter() {
                            let block = &self.blocks[*block_index];

                            if block.deleted {
                                continue;
                            }

                            if current_position + block.items.len() as u32 >= position {
                                let offset = position - current_position - 1;
                                return Some(SequenceBlockId {
                                    client_id: block.id.client_id.clone(),
                                    sequence: block.id.sequence + offset as u32,
                                });
                            } else {
                                current_position += block.items.len() as u32;
                            }
                        }

                        current_node_index = leaf_node.next_block;
                    }

                    return None;
                }
            }
        }
    }

    pub fn last_block(&self) -> Option<SequenceBlockId> {
        let last_leaf = self.nodes[self.end as usize].as_leaf().expect("not a leaf");
        let last_block_index = last_leaf.items.last()?;
        let block = &self.blocks[*last_block_index];
        Some(SequenceBlockId {
            client_id: block.id.client_id.clone(),
            sequence: block.id.sequence + block.items.len() as u32 - 1,
        })
    }

    pub fn insert(&mut self, block: SequenceBlock<Items>) {
        let block_id = block.id.clone();
        let virtual_left_block_id = block.left.clone();

        let left_block_id = if let Some(left) = &virtual_left_block_id {
            Some(self.get_or_split_block_ending_at(left))
        } else {
            None
        };

        // When possible, we should try to merge the new items with an existing block.
        // This allows us to reduce the overhead per block that we generate
        let should_merge = if let (Some(virtual_left), Some(real_left)) =
            (&virtual_left_block_id, &left_block_id)
        {
            self.is_block_mergeable(virtual_left, real_left, &block_id)
        } else {
            false
        };

        if should_merge {
            let left_block_id = left_block_id.expect("left block should exist");
            self.merge_block(block, left_block_id)
        } else {
            self.insert_block(block, left_block_id)
        }
    }

    pub fn delete(&mut self, from: &SequenceBlockId, to: &SequenceBlockId) {
        let start_block_id = self.get_or_split_block_starting_at(from);
        let end_block_id = self.get_or_split_block_ending_at(to);

        let mut current_node_index = self.sequence_id_to_node[&start_block_id];

        let mut size_reductions_per_node: FxHashMap<NodeIndex, u32> = FxHashMap::default();

        let mut inside = false;
        'outer: loop {
            let current_node = &self.nodes[current_node_index as usize]
                .as_leaf()
                .expect("not a leaf");

            for item_index in &current_node.items {
                let block = &mut self.blocks[*item_index];
                if block.id == start_block_id {
                    inside = true;
                }

                if inside {
                    block.deleted = true;

                    size_reductions_per_node
                        .entry(current_node_index)
                        .and_modify(|e| {
                            *e += block.items.len() as u32;
                        })
                        .or_insert(block.items.len() as u32);
                }

                if block.id == end_block_id {
                    inside = false;
                    break 'outer;
                }
            }

            current_node_index = current_node.next_block.expect("next block should exist");
        }

        debug_assert!(
            size_reductions_per_node.len() > 0,
            "at least one node should have been modified"
        );

        // Update the parent metrics to reflect the deletion
        for (leaf_node_index, size_reduction) in size_reductions_per_node.iter() {
            self.subtract_size_metrics_recursively(*leaf_node_index, *size_reduction);
        }
    }

    fn is_block_mergeable(
        &self,
        virtual_left: &SequenceBlockId,
        real_left: &SequenceBlockId,
        current: &SequenceBlockId,
    ) -> bool {
        if virtual_left.client_id != current.client_id {
            return false;
        }

        if virtual_left.sequence + 1 != current.sequence {
            return false;
        }

        let containing_node = self
            .sequence_id_to_node
            .get(real_left)
            .cloned()
            .expect("node should exist");
        let left_block = self.find_block(&containing_node, real_left);
        if left_block.deleted {
            return false;
        }

        true
    }

    fn merge_block(&mut self, block: SequenceBlock<Items>, left_block_id: SequenceBlockId) {
        let left_node_index = self
            .sequence_id_to_node
            .get(&left_block_id)
            .cloned()
            .expect("node should exist");
        let left_block = self.find_block_mut(&left_node_index, &left_block_id);
        assert!(!left_block.deleted, "left block should not be deleted");

        let new_items_count = block.items.len();

        left_block.items.push(block.items);

        // Update parent metrics recursively
        let leaf_node = &self.nodes[left_node_index as usize]
            .as_leaf()
            .expect("not a leaf");
        let mut current_parent = leaf_node.parent.clone();
        let mut target_node = left_node_index;
        while let Some(parent) = current_parent {
            let parent_node = &mut self.nodes[parent as usize]
                .as_branch_mut()
                .expect("not a branch");
            for item in parent_node.items.iter_mut() {
                if item.node == target_node {
                    item.total_size += new_items_count as u32;
                    break;
                }
            }
            target_node = parent;
            current_parent = parent_node.parent;
        }
    }

    fn insert_block(
        &mut self,
        block: SequenceBlock<Items>,
        left_block_id: Option<SequenceBlockId>,
    ) {
        let block_id = block.id.clone();
        if let Some(left) = &left_block_id {
            self.block_children
                .entry(left.clone())
                .or_insert_with(Vec::new)
                .push(block.id.clone());
        } else {
            self.root_blocks.push(block.id.clone());
        }

        let block_index: SequenceBlockIndex = self.blocks.len();
        self.blocks.push(block);

        let actual_left_id = match left_block_id {
            // 1. If root and there are no other roots, add it as a first element on the left
            None if self.root_blocks.len() == 1 => None,

            // 2. If root and there are other roots, determine the order between the roots, and
            //    place it at the right of all the descendents of the previous element
            None if self.root_blocks.len() > 1 => {
                let sorted_roots = self.deterministic_id_sort(&self.root_blocks);
                let current_element_index = sorted_roots
                    .iter()
                    .position(|id| id == &block_id)
                    .expect("current element should exist");

                if current_element_index == 0 {
                    None
                } else {
                    let previous_root = &sorted_roots[current_element_index - 1];
                    let latest_descendent = self.find_latest_descendent(&previous_root);
                    Some(latest_descendent)
                }
            }
            Some(left) => {
                let parent_children = &self.block_children[&left];
                assert!(parent_children.len() > 0, "parent should have children");

                if parent_children.len() == 1 {
                    // 3. If has "left", check if this parent has other children. If no, place it at the right of it
                    Some(left)
                } else {
                    // 4. If has "left" and parent has other children, determine the order between the children
                    //    and add it at the right of all the descendents of the previous
                    let sorted_parent_children = self.deterministic_id_sort(parent_children);
                    let current_element_index = sorted_parent_children
                        .iter()
                        .position(|id| id == &block_id)
                        .expect("current element should exist");

                    if current_element_index == 0 {
                        Some(left)
                    } else {
                        let previous_child = &sorted_parent_children[current_element_index - 1];
                        let latest_descendent = self.find_latest_descendent(&previous_child);
                        Some(latest_descendent)
                    }
                }
            }
            _ => unreachable!(),
        };

        let target_node_index: NodeIndex = if let Some(actual_left_id) = &actual_left_id {
            let containing_node = self
                .sequence_id_to_node
                .get(actual_left_id)
                .expect("actual left containing node should exist in cache");

            *containing_node
        } else {
            self.start
        };

        self.insert_block_in_node(block_index, actual_left_id, target_node_index);
    }

    fn get_or_split_block_starting_at(&mut self, position: &SequenceBlockId) -> SequenceBlockId {
        let node_index = self.sequence_id_to_node.get(position).cloned();

        if let Some(_) = node_index {
            return position.clone();
        } else {
            // Not in cache, find the earliest block scrolling left and split at the appropriate position
            for sequence_id in (0..position.sequence).rev() {
                let id = SequenceBlockId {
                    client_id: position.client_id.clone(),
                    sequence: sequence_id,
                };

                let node_index = self.sequence_id_to_node.get(&id).cloned();

                if let Some(node_index) = node_index {
                    let block = self.find_block(&node_index, &id);
                    let offset = position.sequence - block.id.sequence;
                    self.split_block(&node_index, &id, offset);
                    return position.clone();
                }
            }
        }

        panic!("unable to find the starting block")
    }

    fn get_or_split_block_ending_at(&mut self, position: &SequenceBlockId) -> SequenceBlockId {
        let node_index = self.sequence_id_to_node.get(position).cloned();

        if let Some(node_index) = node_index {
            let block = self.find_block(&node_index, position);
            let block_id = block.id.clone();
            if block.items.len() == 1 {
                // Left block is already in cache, and has length 1, so we can connect it directly
                return position.clone();
            } else {
                // Left block is already in cache, but with a greater length, split forward
                self.split_block(&node_index, &block_id, 1);
                return block_id;
            }
        } else {
            // Not in cache, find the earliest block scrolling left and split at the appropriate position
            for sequence_id in (0..position.sequence).rev() {
                let id = SequenceBlockId {
                    client_id: position.client_id.clone(),
                    sequence: sequence_id,
                };

                let node_index = self.sequence_id_to_node.get(&id).cloned();

                if let Some(node_index) = node_index {
                    let block = self.find_block(&node_index, &id);
                    let offset = position.sequence - block.id.sequence;

                    if offset == block.items.len() as u32 - 1 {
                        // No need to split, as we are referring to the last element in the block
                        return id;
                    } else {
                        self.split_block(&node_index, &id, offset + 1);
                        return id;
                    }
                }
            }
        }

        panic!("unable to find the ending block")
    }

    fn find_block(
        &self,
        containing_node: &NodeIndex,
        block_id: &SequenceBlockId,
    ) -> &SequenceBlock<Items> {
        let node = &self.nodes[*containing_node as usize]
            .as_leaf()
            .expect("not a leaf");
        for item in &node.items {
            let block = &self.blocks[*item];
            if &block.id == block_id {
                return block;
            }
        }

        panic!("unable to find the block")
    }

    fn find_block_mut(
        &mut self,
        containing_node: &NodeIndex,
        block_id: &SequenceBlockId,
    ) -> &mut SequenceBlock<Items> {
        let node = &self.nodes[*containing_node as usize]
            .as_leaf()
            .expect("not a leaf");
        for item in &node.items {
            let block = &self.blocks[*item];
            if &block.id == block_id {
                let block_mut = &mut self.blocks[*item];
                return block_mut;
            }
        }

        panic!("unable to find the block")
    }

    fn find_block_index(&self, containing_node: &NodeIndex, block_id: &SequenceBlockId) -> usize {
        let node = &self.nodes[*containing_node as usize]
            .as_leaf()
            .expect("not a leaf");
        for block_index in node.items.iter() {
            let block = &self.blocks[*block_index];
            if &block.id == block_id {
                return *block_index;
            }
        }

        panic!("unable to find the block index")
    }

    fn subtract_size_metrics_recursively(&mut self, leaf_node_index: NodeIndex, reduction: u32) {
        let leaf_node = &self.nodes[leaf_node_index as usize]
            .as_leaf()
            .expect("not a leaf");
        let mut current_parent = leaf_node.parent.clone();
        let mut target_node = leaf_node_index;
        while let Some(parent) = current_parent {
            let parent_node = &mut self.nodes[parent as usize]
                .as_branch_mut()
                .expect("not a branch");
            for item in parent_node.items.iter_mut() {
                if item.node == target_node {
                    item.total_size -= reduction;
                    break;
                }
            }
            target_node = parent;
            current_parent = parent_node.parent;
        }
    }

    fn split_block(&mut self, containing_node: &NodeIndex, block: &SequenceBlockId, offset: u32) {
        let block_index = self.find_block_index(containing_node, block);

        let (right_block_index, right_content_size) = {
            let left_block = &mut self.blocks[block_index];
            let right_content = left_block.items.split(offset as usize);
            let right_content_size = right_content.len() as u32;
            let right_block = SequenceBlock::<Items> {
                id: SequenceBlockId {
                    client_id: left_block.id.client_id.clone(),
                    sequence: left_block.id.sequence + offset,
                },
                deleted: left_block.deleted,
                items: right_content,
                left: Some(left_block.id.clone()),
            };

            debug_assert!(
                left_block.items.len() > 0,
                "left block should have at least one element"
            );
            debug_assert!(
                right_block.items.len() > 0,
                "right block should have at least one element"
            );

            let right_block_index = self.blocks.len();
            self.blocks.push(right_block);
            (right_block_index, right_content_size)
        };

        self.subtract_size_metrics_recursively(*containing_node, right_content_size);
        self.insert_block_in_node(right_block_index, Some(block.clone()), *containing_node);
    }

    fn find_latest_descendent(&self, parent: &SequenceBlockId) -> SequenceBlockId {
        let mut to_visit = VecDeque::new();
        to_visit.push_back(parent);

        while let Some(current) = to_visit.pop_back() {
            match self.block_children.get(current) {
                None => return current.clone(),
                Some(children) if children.len() == 0 => return current.clone(),
                Some(children) => {
                    to_visit.extend(children);
                }
            }
        }

        panic!("unable to find the latest decendants of {:?}", parent);
    }

    fn deterministic_id_sort(&self, ids: &[SequenceBlockId]) -> Vec<SequenceBlockId> {
        let mut ids = Vec::from(ids);

        // TODO: make sure that the client ID sorting order is globally deterministic
        ids.sort_by(|a, b| {
            if a.client_id == b.client_id {
                // A more recent item has precedence
                b.sequence.cmp(&a.sequence)
            } else {
                a.client_id.cmp(&b.client_id)
            }
        });

        ids
    }

    fn insert_block_in_node(
        &mut self,
        block_index: SequenceBlockIndex,
        actual_left_id: Option<SequenceBlockId>,
        node_index: NodeIndex,
    ) {
        let leaves_to_explore = {
            let leaf_node = &self.nodes[node_index as usize]
                .as_leaf()
                .expect("not a leaf");
            if leaf_node.is_full() {
                vec![node_index, self.split_leaf(node_index)]
            } else {
                vec![node_index]
            }
        };

        let insertion_leaf = match actual_left_id {
            Some(actual_left_id) => {
                let mut insertion_leaf = None;
                for leaf in leaves_to_explore.iter() {
                    let leaf_node = &mut self.nodes[*leaf as usize]
                        .as_leaf_mut()
                        .expect("not a leaf");
                    let left_item_index = leaf_node.items.iter().position(|item| {
                        let block = &self.blocks[*item];
                        block.id == actual_left_id
                    });
                    if let Some(left_item_index) = left_item_index {
                        leaf_node
                            .items
                            .insert(left_item_index + 1, block_index)
                            .expect("insertion failed");
                        insertion_leaf = Some(leaf);
                        break;
                    }
                }
                assert!(
                    insertion_leaf.is_some(),
                    "unable to find the left item in the leaf"
                );
                *insertion_leaf.expect("insertion leaf should exist")
            }
            None => {
                assert!(
                    leaves_to_explore[0] == self.start,
                    "only the start node should be explored"
                );

                let leaf_node = &mut self.nodes[self.start as usize]
                    .as_leaf_mut()
                    .expect("not a leaf");
                leaf_node
                    .items
                    .insert(0, block_index)
                    .expect("insertion failed");

                self.start
            }
        };

        // Update the parent metrics
        let block = &self.blocks[block_index];
        let block_size = block.items.len() as u32;

        let leaf_node = &self.nodes[insertion_leaf as usize]
            .as_leaf()
            .expect("not a leaf");
        let mut current_parent = leaf_node.parent.clone();
        let mut target_node = insertion_leaf;
        while let Some(parent) = current_parent {
            let parent_node = &mut self.nodes[parent as usize]
                .as_branch_mut()
                .expect("not a branch");
            for item in parent_node.items.iter_mut() {
                if item.node == target_node {
                    item.total_size += block_size;
                    item.item_count += 1;
                    break;
                }
            }
            target_node = parent;
            current_parent = parent_node.parent;
        }

        // Update the sequence cache
        self.sequence_id_to_node
            .insert(block.id.clone(), insertion_leaf);
    }

    fn split_leaf(&mut self, node_index: NodeIndex) -> NodeIndex {
        let (left_node_id, right_node_id, parent_id) = {
            let left_node_id = node_index;
            let right_node_id = self.nodes.len() as NodeIndex;

            let left_node = &mut self.nodes[left_node_id as usize];
            let left_node = left_node.as_leaf_mut().expect("not a leaf");
            let left_node_parent = left_node.parent.clone();

            let mut right_node = LeafNode::<LEAF_SIZE> {
                id: right_node_id,
                parent: left_node.parent,
                items: StackVec::new(),
                next_block: left_node.next_block,
                previous_block: Some(left_node.id),
            };

            while let Some(item) = left_node.items.pop() {
                right_node.items.push(item).expect("insertion failed");

                if right_node.items.len() >= LEAF_SIZE / 2 {
                    break;
                }
            }
            right_node.items.reverse();
            left_node.next_block = Some(right_node.id);

            for index in right_node.items.iter() {
                let block = &self.blocks[*index];
                self.sequence_id_to_node
                    .insert(block.id.clone(), right_node.id);
            }

            self.nodes.push(Node::Leaf(right_node));

            (left_node_id, right_node_id, left_node_parent)
        };

        if left_node_id == self.end {
            self.end = right_node_id;
        }

        if let Some(parent) = parent_id {
            self.insert_branch_item(parent, left_node_id, right_node_id);
        } else {
            self.create_upper_root(left_node_id, right_node_id);
        }

        right_node_id
    }

    fn insert_branch_item(
        &mut self,
        branch: NodeIndex,
        left_item: NodeIndex,
        right_item: NodeIndex,
    ) {
        let branch_node = &self.nodes[branch as usize];
        let branches_to_explore = {
            if branch_node.is_full() {
                vec![branch, self.split_branch(branch)]
            } else {
                vec![branch]
            }
        };

        let mut has_inserted = false;
        for branch in branches_to_explore.iter() {
            let branch_node = &mut self.nodes[*branch as usize];
            let branch_node = branch_node.as_branch_mut().expect("not a branch");
            let left_item_index = branch_node
                .items
                .iter()
                .position(|item| item.node == left_item);

            if let Some(left_item_index) = left_item_index {
                branch_node
                    .items
                    .insert(
                        left_item_index + 1,
                        BranchItem {
                            node: right_item,
                            total_size: 0,
                            item_count: 0,
                        },
                    )
                    .expect("insertion failed");
                let right_node = &mut self.nodes[right_item as usize];
                right_node.set_parent(*branch);

                has_inserted = true;
                break;
            }
        }
        assert!(has_inserted, "unable to find the left item in the branch");

        // Update the metrics
        for branch in branches_to_explore.iter() {
            let branch_node = &self.nodes[*branch as usize];
            let branch_node = branch_node.as_branch().expect("not a branch");

            let new_items_metrics: Vec<(u32, u32)> = branch_node
                .items
                .iter()
                .map(|item| {
                    let items_count = self.get_items_count_for_node(item.node);
                    let total_size = self.get_total_size_for_node(item.node);
                    (items_count, total_size)
                })
                .collect();

            let branch_node = &mut self.nodes[*branch as usize];
            let branch_node = branch_node.as_branch_mut().expect("not a branch");

            for (index, item) in branch_node.items.iter_mut().enumerate() {
                item.item_count = new_items_metrics[index].0;
                item.total_size = new_items_metrics[index].1;
            }
        }
    }

    fn split_branch(&mut self, node_index: NodeIndex) -> NodeIndex {
        let (left_node_id, right_node_id, parent_id) = {
            let left_node_id = node_index;
            let right_node_id = self.nodes.len() as NodeIndex;

            let left_node = &mut self.nodes[left_node_id as usize];
            let left_node = left_node.as_branch_mut().expect("not a branch");
            let left_node_parent = left_node.parent.clone();

            let mut right_node = BranchNode::<BRANCH_SIZE> {
                id: right_node_id,
                parent: left_node.parent,
                items: StackVec::new(),
            };

            while let Some(item) = left_node.items.pop() {
                right_node.items.push(item).expect("insertion failed");

                if right_node.items.len() >= BRANCH_SIZE / 2 {
                    break;
                }
            }
            right_node.items.reverse();

            for item in right_node.items.iter_mut() {
                let node = &mut self.nodes[item.node as usize];
                node.set_parent(right_node_id);
            }

            self.nodes.push(Node::Branch(right_node));

            (left_node_id, right_node_id, left_node_parent)
        };

        if let Some(parent) = parent_id {
            self.insert_branch_item(parent, left_node_id, right_node_id);
        } else {
            self.create_upper_root(left_node_id, right_node_id);
        }

        right_node_id
    }

    fn create_upper_root(&mut self, left: NodeIndex, right: NodeIndex) {
        let new_root = BranchNode::<BRANCH_SIZE> {
            id: self.nodes.len() as NodeIndex,
            parent: None,
            items: StackVec::new(),
        };
        let new_root_id = new_root.id.clone();
        self.nodes.push(Node::Branch(new_root));
        self.root = new_root_id.clone();

        let left_node: &mut Node<BRANCH_SIZE, LEAF_SIZE> = &mut self.nodes[left as usize];
        left_node.set_parent(new_root_id.clone());
        let left_total_size = self.get_total_size_for_node(left);
        let left_items_count = self.get_items_count_for_node(left);

        let right_node: &mut Node<BRANCH_SIZE, LEAF_SIZE> = &mut self.nodes[right as usize];
        right_node.set_parent(new_root_id.clone());
        let right_total_size = self.get_total_size_for_node(right);
        let right_items_count = self.get_items_count_for_node(right);

        let root_node = &mut self.nodes[self.root as usize]
            .as_branch_mut()
            .expect("not a branch");
        root_node
            .items
            .push(BranchItem {
                node: left,
                total_size: left_total_size,
                item_count: left_items_count,
            })
            .expect("insertion failed");
        root_node
            .items
            .push(BranchItem {
                node: right,
                total_size: right_total_size,
                item_count: right_items_count,
            })
            .expect("insertion failed");
    }

    fn get_total_size_for_node(&self, node_index: NodeIndex) -> u32 {
        let node = &self.nodes[node_index as usize];
        match node {
            Node::Branch(branch_node) => branch_node.items.iter().map(|item| item.total_size).sum(),
            Node::Leaf(leaf_node) => leaf_node
                .items
                .iter()
                .map(|item| {
                    let block = &self.blocks[*item];
                    if block.deleted {
                        0
                    } else {
                        block.items.len() as u32
                    }
                })
                .sum(),
        }
    }

    fn get_items_count_for_node(&self, node_index: NodeIndex) -> u32 {
        let node = &self.nodes[node_index as usize];
        match node {
            Node::Branch(branch_node) => branch_node.items.iter().map(|item| item.item_count).sum(),
            Node::Leaf(leaf_node) => leaf_node.items.len() as u32,
        }
    }

    // TODO: remove/improve
    // pub fn validate(&self) {
    //     self.validate_branch_metrics();
    // }

    // fn validate_branch_metrics(&self) {
    //     for node in self.nodes.iter() {
    //         match node {
    //             Node::Branch(branch) => {
    //                 for item in branch.items.iter() {
    //                     let actual_total_size = self.get_total_size_for_node(item.node);
    //                     let actual_item_count = self.get_items_count_for_node(item.node);
    //                     assert_eq!(actual_total_size, item.total_size);
    //                     assert_eq!(actual_item_count, item.item_count);
    //                 }
    //             }
    //             _ => {}
    //         }
    //     }
    // }

    // TODO: hide under test flag
    pub fn render_mermaid_tree(&self) -> String {
        let mut commands: Vec<String> = Vec::new();
        commands.push("flowchart TB".to_string());

        for node in self.nodes.iter() {
            match node {
                Node::Branch(branch) => {
                    let mut subcommands = Vec::new();

                    for (index, item) in branch.items.iter().enumerate() {
                        // let block = &self.blocks[item.node];
                        subcommands.push(format!(
                            "branchitem{}{}[Size: {:?}, Count: {:?}]",
                            branch.id, index, item.total_size, item.item_count
                        ));
                        subcommands.push(format!(
                            "branchitem{}{} --> {}",
                            branch.id, index, item.node,
                        ));
                    }

                    commands.push(format!("subgraph {}", branch.id));
                    commands.push("direction TB".to_string());
                    commands.extend(subcommands);
                    commands.push("end".to_string());

                    if let Some(parent) = branch.parent {
                        commands.push(format!("{} -- parent --> {}", branch.id, parent,));
                    }
                }
                Node::Leaf(leaf) => {
                    let mut subcommands = Vec::new();

                    for (index, item) in leaf.items.iter().enumerate() {
                        let block = &self.blocks[*item];
                        subcommands
                            .push(format!("leafitem{}{}[{:?}]", leaf.id, index, block.items));
                    }

                    commands.push(format!("subgraph {}", leaf.id));
                    commands.push("direction TB".to_string());
                    commands.extend(subcommands);
                    commands.push("end".to_string());

                    if let Some(parent) = leaf.parent {
                        commands.push(format!("{} -- parent --> {}", leaf.id, parent,));
                    }
                }
            }
        }

        commands.join("\n")
    }

    // #[cfg(test)]
    pub fn render_debug_tree(&self) -> String {
        let mut buffer = String::new();

        self.generate_debug_tree_recursively(self.root, &mut buffer);

        buffer
    }

    // #[cfg(test)]
    fn generate_debug_tree_recursively(&self, node_index: NodeIndex, buffer: &mut String) {
        let node = &self.nodes[node_index as usize];
        match node {
            Node::Branch(branch_node) => {
                buffer.push_str("B(");
                for (index, item) in branch_node.items.iter().enumerate() {
                    if index > 0 {
                        buffer.push_str(",");
                    }

                    buffer.push_str(&format!("[{}:{}]", item.total_size, item.item_count));

                    self.generate_debug_tree_recursively(item.node, buffer);
                }
                buffer.push_str(")");
            }
            Node::Leaf(leaf_node) => {
                buffer.push_str("L(");
                for (index, item) in leaf_node.items.iter().enumerate() {
                    if index > 0 {
                        buffer.push_str(",");
                    }

                    let block = &self.blocks[*item];
                    if block.deleted {
                        buffer.push_str("~");
                    }
                    buffer.push_str(&format!("{:?}", block.items));
                }
                buffer.push_str(")");
            }
        }
    }
}

pub trait Sizable {
    fn len(&self) -> usize;
}

pub trait Splittable {
    fn split(&mut self, offset: usize) -> Self;
}

pub trait Mergeable {
    fn push(&mut self, items: Self);
}

pub trait SequenceItems: Sizable + Splittable + Mergeable + std::fmt::Debug {}

impl Sizable for String {
    fn len(&self) -> usize {
        self.len()
    }
}

impl Splittable for String {
    fn split(&mut self, offset: usize) -> Self {
        let right_part = self.split_off(offset);
        return right_part;
    }
}

impl Mergeable for String {
    fn push(&mut self, items: Self) {
        self.push_str(&items)
    }
}

impl SequenceItems for String {}

// TODO: convert to u32?
type SequenceBlockIndex = usize;

#[derive(Clone, PartialEq)]
pub struct SequenceBlock<Items: SequenceItems> {
    pub id: SequenceBlockId,
    pub items: Items,
    pub left: Option<SequenceBlockId>,
    pub deleted: bool,
}

impl<Items: SequenceItems> SequenceBlock<Items> {
    pub fn new(id: SequenceBlockId, items: Items, left: Option<SequenceBlockId>) -> Self {
        Self {
            id,
            items,
            left,
            deleted: false,
        }
    }
}

type NodeIndex = u32;

#[derive(Debug, Clone, EnumAsInner, PartialEq)]
enum Node<const BRANCH_SIZE: usize, const LEAF_SIZE: usize> {
    Branch(BranchNode<BRANCH_SIZE>),
    Leaf(LeafNode<LEAF_SIZE>),
}

impl<const BRANCH_SIZE: usize, const LEAF_SIZE: usize> Node<BRANCH_SIZE, LEAF_SIZE> {
    pub fn new_root() -> Self {
        Self::Leaf(LeafNode {
            id: 0,
            items: StackVec::new(),
            parent: None,
            next_block: None,
            previous_block: None,
        })
    }

    pub fn is_full(&self) -> bool {
        match self {
            Self::Branch(branch_node) => branch_node.is_full(),
            Self::Leaf(leaf_node) => leaf_node.is_full(),
        }
    }

    pub fn set_parent(&mut self, parent: NodeIndex) {
        match self {
            Self::Branch(branch_node) => branch_node.parent = Some(parent),
            Self::Leaf(leaf_node) => leaf_node.parent = Some(parent),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct BranchNode<const BRANCH_SIZE: usize> {
    id: NodeIndex,
    parent: Option<NodeIndex>,
    items: StackVec<BranchItem, BRANCH_SIZE>,
}

#[derive(Debug, Clone, PartialEq)]
struct BranchItem {
    node: NodeIndex,
    total_size: u32,
    item_count: u32,
}

impl<const BRANCH_SIZE: usize> BranchNode<BRANCH_SIZE> {
    // TODO: is it necessary?
    pub fn is_full(&self) -> bool {
        self.items.len() >= BRANCH_SIZE
    }
}

#[derive(Debug, Clone, PartialEq)]
struct LeafNode<const LEAF_SIZE: usize> {
    id: NodeIndex,
    parent: Option<NodeIndex>,
    items: StackVec<SequenceBlockIndex, LEAF_SIZE>,
    next_block: Option<NodeIndex>,
    previous_block: Option<NodeIndex>,
}

impl<const LEAF_SIZE: usize> LeafNode<LEAF_SIZE> {
    // TODO: is it necessary?
    pub fn is_full(&self) -> bool {
        self.items.len() >= LEAF_SIZE
    }

    // pub fn insert_at_index(
    //     &mut self,
    //     index: usize,
    //     item: TItem,
    //     new_id_if_split: NodeIndex,
    // ) -> Option<Self> {
    //     if self.is_full() {
    //         let mut new_leaf = self.split(new_id_if_split);

    //         if index < self.items.len() {
    //             self.items.insert(index, item).expect("insertion failed");
    //         } else {
    //             new_leaf
    //                 .items
    //                 .insert(index - self.items.len(), item)
    //                 .expect("insertion failed");
    //         }

    //         Some(new_leaf)
    //     } else {
    //         self.items.insert(index, item).expect("insertion failed");

    //         None
    //     }
    // }

    // pub fn split(&mut self, new_id: NodeIndex) -> Self {
    //     let mut new_leaf = Self {
    //         id: new_id,
    //         parent: self.parent,
    //         items: StackVec::new(),
    //         next_block: self.next_block,
    //         previous_block: Some(self.id),
    //     };

    //     while let Some(item) = self.items.pop() {
    //         new_leaf.items.push(item).expect("insertion failed");

    //         if new_leaf.items.len() >= LEAF_SIZE / 2 {
    //             break;
    //         }
    //     }

    //     new_leaf.items.reverse();

    //     self.next_block = Some(new_leaf.id);

    //     new_leaf
    // }
}

// impl<TItem: RangeTreeItem, const LEAF_SIZE: usize> Sizeable for LeafNode<TItem, LEAF_SIZE> {
//     fn get_total_size(&self) -> u32 {
//         self.items.iter().map(|item| item.get_size()).sum()
//     }

//     fn get_items_count(&self) -> u32 {
//         self.items.len() as u32
//     }
// }

// pub trait Sizeable {
//     fn get_total_size(&self) -> u32;
//     fn get_items_count(&self) -> u32;
// }

// pub trait RangeTreeItem: std::fmt::Debug + Clone {
//     fn get_size(&self) -> u32;
// }

// #[derive(Debug, Clone)]
// pub struct GetItemResult<'a, TItem: RangeTreeItem> {
//     pub item: &'a TItem,
//     pub offset: usize,
// }

pub struct SequenceTreeIterator<
    'a,
    Items: SequenceItems,
    const BRANCH_SIZE: usize,
    const LEAF_SIZE: usize,
> {
    tree: &'a SequenceTree<Items, BRANCH_SIZE, LEAF_SIZE>,
    current_node: NodeIndex,
    current_index: usize,
}

impl<'a, Items: SequenceItems, const BRANCH_SIZE: usize, const LEAF_SIZE: usize>
    SequenceTreeIterator<'a, Items, BRANCH_SIZE, LEAF_SIZE>
{
    pub fn new(tree: &'a SequenceTree<Items, BRANCH_SIZE, LEAF_SIZE>) -> Self {
        let current_node = tree.start;

        Self {
            tree,
            current_node,
            current_index: 0,
        }
    }
}

impl<'a, Items: SequenceItems, const BRANCH_SIZE: usize, const LEAF_SIZE: usize> Iterator
    for SequenceTreeIterator<'a, Items, BRANCH_SIZE, LEAF_SIZE>
{
    type Item = &'a Items;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let current_node = &self.tree.nodes[self.current_node as usize];
            let current_leaf = current_node.as_leaf().expect("not a leaf");

            if self.current_index >= current_leaf.items.len() {
                if let Some(next_node) = current_leaf.next_block {
                    self.current_node = next_node;
                    self.current_index = 0;

                    continue;
                } else {
                    return None;
                }
            } else {
                let item = current_leaf
                    .items
                    .get(self.current_index)
                    .expect("item should exist");
                self.current_index += 1;

                let block = &self.tree.blocks[*item];

                if block.deleted {
                    continue;
                }

                return Some(&block.items);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestSequenceTree = SequenceTree<String, 2, 2>;
    type TestSequenceBlock = SequenceBlock<String>;

    fn render_as_string(tree: &TestSequenceTree) -> String {
        let mut result = String::new();

        for item in tree.iter() {
            result.push_str(&item);
        }

        result
    }

    #[test]
    fn test_insert_perfect_boundaries() {
        let mut tree: TestSequenceTree = SequenceTree::new();

        tree.insert(TestSequenceBlock::new(
            SequenceBlockId::new(0, 0),
            "Hello".to_string(),
            None,
        ));

        println!("{}", tree.render_debug_tree());

        tree.insert(TestSequenceBlock::new(
            SequenceBlockId::new(0, 5),
            "World".to_string(),
            Some(SequenceBlockId::new(0, 4)),
        ));

        println!("{}", tree.render_debug_tree());

        assert_eq!(render_as_string(&tree), "HelloWorld");
        assert_eq!(&tree.render_debug_tree(), r#"L("HelloWorld")"#);

        // Here the root should be full, expecting a split

        tree.insert(TestSequenceBlock::new(
            SequenceBlockId::new(0, 10),
            "Another".to_string(),
            None,
        ));

        println!("{}", tree.render_debug_tree());

        assert_eq!(render_as_string(&tree), "AnotherHelloWorld");
        assert_eq!(&tree.render_debug_tree(), r#"L("Another","HelloWorld")"#);

        tree.insert(TestSequenceBlock::new(
            SequenceBlockId::new(0, 17),
            "Test".to_string(),
            Some(SequenceBlockId::new(0, 9)),
        ));

        println!("{}", tree.render_debug_tree());

        assert_eq!(render_as_string(&tree), "AnotherHelloWorldTest");
        assert_eq!(
            &tree.render_debug_tree(),
            r#"B([7:1]L("Another"),[14:2]L("HelloWorld","Test"))"#
        );

        // Here we should see another branching of the root
        tree.insert(TestSequenceBlock::new(
            SequenceBlockId::new(0, 21),
            "Ending".to_string(),
            Some(SequenceBlockId::new(0, 20)),
        ));

        println!("{}", tree.render_debug_tree());

        assert_eq!(render_as_string(&tree), "AnotherHelloWorldTestEnding");
        assert_eq!(
            &tree.render_debug_tree(),
            r#"B([7:1]L("Another"),[20:2]L("HelloWorld","TestEnding"))"#
        );
    }

    #[test]
    fn test_insert_splitting_boundaries() {
        let mut tree: TestSequenceTree = SequenceTree::new();

        tree.insert(TestSequenceBlock::new(
            SequenceBlockId::new(0, 0),
            "ABC".to_string(),
            None,
        ));

        println!("{}", tree.render_debug_tree());

        tree.insert(TestSequenceBlock::new(
            SequenceBlockId::new(0, 3),
            "DE".to_string(),
            Some(SequenceBlockId::new(0, 0)),
        ));

        println!("{}", tree.render_debug_tree());

        assert_eq!(render_as_string(&tree), "ADEBC");
        assert_eq!(
            &tree.render_debug_tree(),
            r#"B([3:2]L("A","DE"),[2:1]L("BC"))"#
        );

        tree.insert(TestSequenceBlock::new(
            SequenceBlockId::new(0, 5),
            "F".to_string(),
            None,
        ));

        println!("{}", tree.render_debug_tree());

        assert_eq!(render_as_string(&tree), "FADEBC");
        assert_eq!(
            &tree.render_debug_tree(),
            r#"B([4:3]B([2:2]L("F","A"),[2:1]L("DE")),[2:1]B([2:1]L("BC")))"#
        );

        tree.insert(TestSequenceBlock::new(
            SequenceBlockId::new(0, 6),
            "G".to_string(),
            Some(SequenceBlockId::new(0, 0)),
        ));

        println!("{}", tree.render_debug_tree());

        assert_eq!(render_as_string(&tree), "FAGDEBC");
        assert_eq!(
            &tree.render_debug_tree(),
            r#"B([5:4]B([3:3]B([1:1]L("F"),[2:2]L("A","G")),[2:1]B([2:1]L("DE"))),[2:1]B([2:1]B([2:1]L("BC"))))"#
        );
    }

    #[test]
    fn test_delete_perfect_boundaries() {
        let mut tree: TestSequenceTree = SequenceTree::new();

        tree.insert(TestSequenceBlock::new(
            SequenceBlockId::new(0, 0),
            "Hello".to_string(),
            None,
        ));

        println!("{}", tree.render_debug_tree());

        tree.insert(TestSequenceBlock::new(
            SequenceBlockId::new(0, 5),
            "World".to_string(),
            Some(SequenceBlockId::new(0, 4)),
        ));

        println!("{}", tree.render_debug_tree());

        assert_eq!(render_as_string(&tree), "HelloWorld");
        assert_eq!(&tree.render_debug_tree(), r#"L("HelloWorld")"#);

        tree.delete(&SequenceBlockId::new(0, 0), &SequenceBlockId::new(0, 4));

        println!("{}", tree.render_debug_tree());

        assert_eq!(render_as_string(&tree), "World");
        assert_eq!(&tree.render_debug_tree(), r#"L(~"Hello","World")"#);
    }

    #[test]
    fn test_delete_multiple_words_boundaries() {
        let mut tree: TestSequenceTree = SequenceTree::new();

        tree.insert(TestSequenceBlock::new(
            SequenceBlockId::new(0, 0),
            "Hello".to_string(),
            None,
        ));

        println!("{}", tree.render_debug_tree());

        tree.insert(TestSequenceBlock::new(
            SequenceBlockId::new(0, 5),
            "World".to_string(),
            Some(SequenceBlockId::new(0, 4)),
        ));

        println!("{}", tree.render_debug_tree());

        assert_eq!(render_as_string(&tree), "HelloWorld");
        assert_eq!(&tree.render_debug_tree(), r#"L("HelloWorld")"#);

        tree.delete(&SequenceBlockId::new(0, 0), &SequenceBlockId::new(0, 9));

        println!("{}", tree.render_debug_tree());

        assert_eq!(render_as_string(&tree), "");
        assert_eq!(&tree.render_debug_tree(), r#"L(~"HelloWorld")"#);
    }

    #[test]
    fn test_delete_across_boundaries() {
        let mut tree: TestSequenceTree = SequenceTree::new();

        tree.insert(TestSequenceBlock::new(
            SequenceBlockId::new(0, 0),
            "Hello".to_string(),
            None,
        ));

        println!("{}", tree.render_debug_tree());

        tree.insert(TestSequenceBlock::new(
            SequenceBlockId::new(0, 5),
            "World".to_string(),
            Some(SequenceBlockId::new(0, 4)),
        ));

        println!("{}", tree.render_debug_tree());

        assert_eq!(render_as_string(&tree), "HelloWorld");
        assert_eq!(&tree.render_debug_tree(), r#"L("HelloWorld")"#);

        tree.delete(&SequenceBlockId::new(0, 2), &SequenceBlockId::new(0, 7));

        println!("{}", tree.render_debug_tree());

        assert_eq!(render_as_string(&tree), "Held");
        assert_eq!(
            &tree.render_debug_tree(),
            r#"B([2:1]L("He"),[2:2]L(~"lloWor","ld"))"#
        );
    }

    #[test]
    fn insert_and_delete_sequence() {
        let mut tree: TestSequenceTree = SequenceTree::new();

        tree.insert(TestSequenceBlock::new(
            SequenceBlockId::new(0, 0),
            "Hello".to_string(),
            None,
        ));

        println!("{}", tree.render_debug_tree());

        tree.insert(TestSequenceBlock::new(
            SequenceBlockId::new(0, 5),
            "World".to_string(),
            Some(SequenceBlockId::new(0, 4)),
        ));

        println!("{}", tree.render_debug_tree());

        assert_eq!(render_as_string(&tree), "HelloWorld");
        assert_eq!(&tree.render_debug_tree(), r#"L("HelloWorld")"#);

        tree.delete(&SequenceBlockId::new(0, 2), &SequenceBlockId::new(0, 7));

        println!("{}", tree.render_debug_tree());

        assert_eq!(render_as_string(&tree), "Held");
        assert_eq!(
            &tree.render_debug_tree(),
            r#"B([2:1]L("He"),[2:2]L(~"lloWor","ld"))"#
        );

        tree.insert(TestSequenceBlock::new(
            SequenceBlockId::new(0, 10),
            "Ending".to_string(),
            Some(SequenceBlockId::new(0, 9)),
        ));

        assert_eq!(render_as_string(&tree), "HeldEnding");
    }

    // #[test]
    // fn test_get_item_starting_at_position() {
    //     let mut tree: SequenceTree<TestItem, 2, 2> = SequenceTree::new();

    //     tree.insert_at_index(
    //         0,
    //         TestItem {
    //             id: 1,
    //             value: "Hello".to_string(),
    //         },
    //     );

    //     assert_eq!(tree.get_item_starting_at_position(0).unwrap().item.id, 1);
    //     assert_eq!(tree.get_item_starting_at_position(0).unwrap().offset, 0);

    //     assert_eq!(tree.get_item_starting_at_position(2).unwrap().item.id, 1);
    //     assert_eq!(tree.get_item_starting_at_position(2).unwrap().offset, 2);

    //     assert_eq!(tree.get_item_starting_at_position(5).is_none(), true);

    //     tree.insert_at_index(
    //         1,
    //         TestItem {
    //             id: 2,
    //             value: "World".to_string(),
    //         },
    //     );

    //     // Text: HelloWorld
    //     assert_eq!(tree.get_item_starting_at_position(5).unwrap().item.id, 2);
    //     assert_eq!(tree.get_item_starting_at_position(5).unwrap().offset, 0);

    //     tree.insert_at_index(
    //         1,
    //         TestItem {
    //             id: 3,
    //             value: "Beautiful".to_string(),
    //         },
    //     );

    //     // Test: HelloBeautifulWorld

    //     assert_eq!(tree.get_item_starting_at_position(5).unwrap().item.id, 3);
    //     assert_eq!(tree.get_item_starting_at_position(5).unwrap().offset, 0);

    //     assert_eq!(tree.get_item_starting_at_position(9).unwrap().item.id, 3);
    //     assert_eq!(tree.get_item_starting_at_position(9).unwrap().offset, 4);
    // }

    // #[test]
    // fn test_get_item_starting_at_position_with_zero_sized_elements() {
    //     let mut tree: SequenceTree<TestItem, 2, 2> = SequenceTree::new();

    //     tree.insert_at_index(
    //         0,
    //         TestItem {
    //             id: 1,
    //             value: "Hello".to_string(),
    //         },
    //     );
    //     tree.insert_at_index(
    //         1,
    //         TestItem {
    //             id: 2,
    //             value: "".to_string(),
    //         },
    //     );
    //     tree.insert_at_index(
    //         2,
    //         TestItem {
    //             id: 3,
    //             value: "".to_string(),
    //         },
    //     );
    //     tree.insert_at_index(
    //         3,
    //         TestItem {
    //             id: 3,
    //             value: "World".to_string(),
    //         },
    //     );

    //     // Text: HelloWorld
    //     //           ^ two empty spaces here

    //     assert_eq!(tree.get_item_starting_at_position(5).unwrap().item.id, 3);
    //     assert_eq!(tree.get_item_starting_at_position(5).unwrap().offset, 0);
    // }

    // #[test]
    // fn test_get_item_ending_at_position() {
    //     let mut tree: SequenceTree<TestItem, 2, 2> = SequenceTree::new();

    //     tree.insert_at_index(
    //         0,
    //         TestItem {
    //             id: 1,
    //             value: "Hello".to_string(),
    //         },
    //     );

    //     assert!(tree.get_item_ending_at_position(0).is_none());

    //     assert_eq!(tree.get_item_ending_at_position(4).unwrap().item.id, 1);
    //     assert_eq!(tree.get_item_ending_at_position(4).unwrap().offset, 3);

    //     assert_eq!(tree.get_item_ending_at_position(5).unwrap().item.id, 1);
    //     assert_eq!(tree.get_item_ending_at_position(5).unwrap().offset, 4);

    //     tree.insert_at_index(
    //         1,
    //         TestItem {
    //             id: 2,
    //             value: "World".to_string(),
    //         },
    //     );

    //     // Text: HelloWorld
    //     assert_eq!(tree.get_item_ending_at_position(5).unwrap().item.id, 1);
    //     assert_eq!(tree.get_item_ending_at_position(5).unwrap().offset, 4);

    //     assert_eq!(tree.get_item_ending_at_position(6).unwrap().item.id, 2);
    //     assert_eq!(tree.get_item_ending_at_position(6).unwrap().offset, 0);

    //     assert_eq!(tree.get_item_ending_at_position(10).unwrap().item.id, 2);
    //     assert_eq!(tree.get_item_ending_at_position(10).unwrap().offset, 4);

    //     tree.insert_at_index(
    //         1,
    //         TestItem {
    //             id: 3,
    //             value: "Beautiful".to_string(),
    //         },
    //     );

    //     // Test: HelloBeautifulWorld

    //     assert_eq!(tree.get_item_ending_at_position(5).unwrap().item.id, 1);
    //     assert_eq!(tree.get_item_ending_at_position(5).unwrap().offset, 4);

    //     assert_eq!(tree.get_item_ending_at_position(10).unwrap().item.id, 3);
    //     assert_eq!(tree.get_item_ending_at_position(10).unwrap().offset, 4);
    // }

    // #[test]
    // fn test_get_item_ending_at_position_with_zero_sized_elements() {
    //     let mut tree: SequenceTree<TestItem, 2, 2> = SequenceTree::new();

    //     tree.insert_at_index(
    //         0,
    //         TestItem {
    //             id: 1,
    //             value: "Hello".to_string(),
    //         },
    //     );
    //     tree.insert_at_index(
    //         1,
    //         TestItem {
    //             id: 2,
    //             value: "".to_string(),
    //         },
    //     );
    //     tree.insert_at_index(
    //         2,
    //         TestItem {
    //             id: 3,
    //             value: "".to_string(),
    //         },
    //     );
    //     tree.insert_at_index(
    //         3,
    //         TestItem {
    //             id: 3,
    //             value: "World".to_string(),
    //         },
    //     );

    //     // Text: HelloWorld
    //     //           ^ two empty spaces here

    //     assert_eq!(tree.get_item_ending_at_position(5).unwrap().item.id, 1);
    //     assert_eq!(tree.get_item_ending_at_position(5).unwrap().offset, 4);
    // }

    // TODO: test with concurrent edits (multiple roots, multiple non-roots)
}
