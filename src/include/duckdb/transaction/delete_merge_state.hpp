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
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class IndexEntry;
class TableIndexList;

//! Tracks unique-index deletes applied at commit by merging the transaction-local delete indexes
//! into the main indexes, so that the commit undo-walk can skip the covered indexes.
class DeleteMergeState {
public:
	//! Merges the local delete indexes of a table's unique index entries into the main indexes.
	void MergeTable(TableIndexList &indexes, TableIndexList &delete_indexes, optional_idx active_checkpoint);
	//! Returns the index entries whose removals were fully applied at flush.
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
		//! The transaction-local delete index, kept alive beyond the local storage flush.
		shared_ptr<IndexEntry> delete_entry;
		//! The number of merged (key, row id) pairs, in the delete index's key order.
		idx_t merged_count = 0;
	};
	vector<DeleteMergeRecord> records;
	reference_set_t<IndexEntry> covered;
};

} // namespace duckdb
