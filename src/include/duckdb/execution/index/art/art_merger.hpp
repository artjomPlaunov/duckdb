//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/art_merger.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/stack.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node_handle.hpp"

namespace duckdb {

//! ARTMerger merges two ART nodes and their subtrees, until
//! 1. triggering a constraint violation.
//! 2. all nodes have been processed.
//! Must be initialized via its Init function before calling Merge,
//! otherwise, the stack is empty and there is nothing to do.
class ARTMerger {
public:
	ARTMerger() = delete;
	ARTMerger(ArenaAllocator &arena, ART &art) : arena(arena), art(art) {
	}

public:
	//! Init the merge by setting the initial nodes. left_ptr is the root of the ART merged into.
	void Init(NodePtr &left_ptr, const NodePtr right_ptr);
	//! Merge until (1) triggering constraint violation or (2) all nodes have been processed.
	ARTConflictType Merge();

private:
	//! NodeEntry holds a node on the stack.
	//! The ARTMerger always merges into the left node.
	//! If inside a gate, status is always GATE_SET, otherwise, it is GATE_NOT_SET.
	//! The depth resets when entering a gate.
	//! The left NodePtr is stored as its location: the child at left_byte of the inner node left_parent_ptr,
	//! the child of the prefix left_parent_ptr, or the root, if left_parent_ptr is empty.
	struct NodeEntry {
		NodeEntry() = delete;
		NodeEntry(const NodePtr left_parent_ptr, const uint8_t left_byte, const NodePtr right_ptr,
		          const GateStatus status, const idx_t depth)
		    : left_parent_ptr(left_parent_ptr), left_byte(left_byte), right_ptr(right_ptr), status(status),
		      depth(depth) {};

		NodePtr left_parent_ptr;
		uint8_t left_byte;
		NodePtr right_ptr;
		GateStatus status;
		idx_t depth;
	};

	//! The arena holds any temporary memory allocated during the Merge phase.
	ArenaAllocator &arena;
	//! The ART holding the node memory.
	ART &art;
	//! The root of the ART merged into.
	optional_ptr<NodePtr> root_ptr;
	//! The stack. While merging, NodeEntry elements are pushed onto of the stack.
	stack<NodeEntry> s;

private:
	// When pushing anything on the stack, we ensure that:
	// - if left is LEAF_INLINED, then right is also LEAF_INLINED.
	// - if left is PREFIX, then right is also PREFIX (except for PREFIX + LEAF_INLINED).
	// left_ptr is the NodePtr at the location (left_parent_ptr, left_byte), pinned by the caller.
	void Emplace(NodePtr &left_ptr, const NodePtr left_parent_ptr, const uint8_t left_byte, NodePtr right_ptr,
	             const GateStatus parent_status, const idx_t depth);
	//! Pins the location of the entry's left NodePtr.
	NodePtrHandle PinLeft(const NodeEntry &entry);

	ARTConflictType MergeNodeAndInlined(NodePtr &left_ptr, NodeEntry &entry);

	array_ptr<uint8_t> GetBytes(NodePtr &leaf_ptr);
	void MergeLeaves(NodePtr &left_ptr, NodeEntry &entry);

	NodeChildren ExtractChildren(NodePtr &node_ptr);
	void MergeNodes(NodePtr &left_ptr, NodeEntry &entry);

	//! Merges a node and a prefix.
	//! pos determines up to which position we need to reduce the prefix.
	//! If we call into this function via MergePrefixes (case: one prefix contains the other),
	//! then pos is the length of the shorter prefix.
	void MergeNodeAndPrefix(NodePtr &node_ptr, NodePtr &prefix_ptr, const GateStatus parent_status,
	                        const idx_t parent_depth, const uint8_t pos);
	void MergeNodeAndPrefix(NodePtr &node_ptr, NodePtr &prefix_ptr, const GateStatus parent_status,
	                        const idx_t parent_depth);
	void MergePrefixes(NodePtr &left_ptr, NodeEntry &entry);
};

} // namespace duckdb
