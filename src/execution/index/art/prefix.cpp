#include "duckdb/execution/index/art/prefix.hpp"

#include "duckdb/common/swap.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/base_leaf.hpp"
#include "duckdb/execution/index/art/base_node.hpp"
#include "duckdb/execution/index/art/const_prefix_handle.hpp"
#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/execution/index/art/prefix_handle.hpp"

namespace duckdb {

Prefix::Prefix(const ART &art, const NodePtr node_ptr, const bool is_mutable, const bool set_in_memory) {
	if (!set_in_memory) {
		data = NodePtr::GetAllocator(art, PREFIX).Get(node_ptr, is_mutable);
	} else {
		data = NodePtr::GetAllocator(art, PREFIX).GetIfLoaded(node_ptr);
		if (!data) {
			child_slot = nullptr;
			in_memory = false;
			return;
		}
	}
	child_slot = &PrefixHandle::ChildRefWithCount(data, art.PrefixCount());
	in_memory = true;
}

uint8_t Prefix::GetByte(const ART &art, const NodePtr &node_ptr, const uint8_t pos) {
	D_ASSERT(node_ptr.GetType() == PREFIX);
	ConstPrefixHandle prefix(art, node_ptr);
	return prefix.GetByte(pos);
}

Prefix Prefix::NewInternal(ART &art, NodePtr &node_ptr, const data_ptr_t data, const uint8_t count,
                           const idx_t offset) {
	node_ptr = NodePtr::GetAllocator(art, PREFIX).New();
	node_ptr.SetMetadata(static_cast<uint8_t>(PREFIX));

	Prefix prefix(art, node_ptr, true);
	prefix.data[art.PrefixCount()] = count;
	if (data) {
		D_ASSERT(count);
		memcpy(prefix.data, data + offset, count);
	}
	prefix.child_slot->Clear();
	return prefix;
}

void Prefix::New(ART &art, reference<NodePtr> &node_ptr_ref, const ARTKey &key, const idx_t depth, idx_t count) {
	idx_t offset = 0;

	while (count) {
		auto min = MinValue(UnsafeNumericCast<idx_t>(art.PrefixCount()), count);
		auto this_count = UnsafeNumericCast<uint8_t>(min);
		auto prefix = NewInternal(art, node_ptr_ref, key.data, this_count, offset + depth);

		node_ptr_ref = *prefix.child_slot;
		offset += this_count;
		count -= this_count;
	}
}

} // namespace duckdb
