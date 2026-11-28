//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/commit_index_changes.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {
class ClientContext;
class ColumnDataCollection;
class DuckTransaction;
class IndexEntry;
class LocalTableStorage;

//! The live local rows of a commit whose key in a unique index belongs to a row the transaction deleted. Their row
//! ids are inserted next to the deleted one (a gate), which the removal after the commit marker takes out.
struct IndexSwaps {
	explicit IndexSwaps(shared_ptr<IndexEntry> entry_p) : entry(std::move(entry_p)) {
	}

	shared_ptr<IndexEntry> entry;
	//! The positions of the rows among the live local rows in scan order, sorted
	vector<idx_t> positions;
};

//! The live local rows of a commit for one index, sorted by key before the commit lock
struct SortedIndexRows {
	explicit SortedIndexRows(shared_ptr<IndexEntry> entry_p);
	SortedIndexRows(SortedIndexRows &&other) noexcept;
	SortedIndexRows &operator=(SortedIndexRows &&other) noexcept;
	~SortedIndexRows();

	shared_ptr<IndexEntry> entry;
	//! The table columns that the index reads, stored in this order before the row positions
	vector<column_t> column_ids;
	//! The columns, then the encoded position of each row (see GetPosition)
	unique_ptr<ColumnDataCollection> rows;
};

//! What the commit of a table's local rows changes in its unique indexes, derived from the local rows and the stored
//! keys of the transaction's deletes. Prepared before the commit lock, completed under it for indexes added since.
class CommitIndexChanges {
public:
	//! Finds the swaps of the unique indexes that are not prepared yet. Returns a constraint violation, before anything
	//! shared is changed, if the live local rows insert one deleted key twice.
	ErrorData Prepare(DuckTransaction &transaction, LocalTableStorage &storage);
	//! Sorts the live local rows by key for each bound ART index of a large enough append
	void PrepareSorted(ClientContext &context, DuckTransaction &transaction, LocalTableStorage &storage);
	//! The swaps of the entry, or nullptr if it has none
	optional_ptr<const IndexSwaps> GetSwaps(const IndexEntry &entry) const;
	//! The sorted live rows of the entry, or nullptr if they were not sorted
	optional_ptr<const SortedIndexRows> GetSortedRows(const IndexEntry &entry) const;
	//! Frees the sorted rows of the entry once they are appended
	void ReleaseSortedRows(const IndexEntry &entry);
	//! The position among the live local rows of a stored row position
	idx_t GetPosition(uint64_t row_position) const;

private:
	//! The unique indexes whose swaps were prepared
	vector<IndexSwaps> prepared;
	//! The sorted live rows of the indexes
	vector<SortedIndexRows> sorted;
	//! The number of live local rows before each row group, for the stored row positions
	vector<idx_t> row_group_offsets;
	//! Whether the transaction deleted no committed row of the table: then there are no swaps
	bool no_deletes = false;
};

} // namespace duckdb
