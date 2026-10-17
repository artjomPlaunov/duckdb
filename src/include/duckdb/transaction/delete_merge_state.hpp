//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/delete_merge_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class IndexEntry;
class TableIndexList;

//! Unique-index deletes applied at commit by merging the local delete indexes into the main indexes.
class DeleteMergeState {
public:
	//! Merges the local delete indexes of a table's unique index entries into the main indexes.
	void MergeTable(TableIndexList &indexes, TableIndexList &delete_indexes, optional_idx active_checkpoint);
	//! The index entries whose deletes were fully merged: the undo walk skips them.
	const reference_set_t<IndexEntry> &CoveredEntries() const {
		return covered;
	}
	//! Detaches the adopted delete indexes once no snapshot can see the deleted rows.
	void Cleanup();
	//! Re-inserts the merged deletes into the main indexes after a failed commit.
	void Revert(optional_idx active_checkpoint);

private:
	struct DeleteMergeRecord {
		//! The main index entry the local deletes were merged into.
		shared_ptr<IndexEntry> entry;
		//! The transaction-local delete index, owned beyond the local storage flush.
		shared_ptr<IndexEntry> delete_entry;
		//! The number of merged (key, row id) pairs, in the delete index's key order.
		idx_t merged_count = 0;
	};
	vector<DeleteMergeRecord> records;
	reference_set_t<IndexEntry> covered;
};

} // namespace duckdb
