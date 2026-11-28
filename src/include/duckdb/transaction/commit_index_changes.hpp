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
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/index/art/art_key.hpp"

namespace duckdb {
class ArenaAllocator;
class DuckTransaction;
class IndexEntry;
class LocalTableStorage;

//! Generates the ART keys of an index for chunks in table layout, with a private copy of the index expressions
class IndexKeyGenerator {
public:
	IndexKeyGenerator(DatabaseInstance &db, shared_ptr<IndexEntry> entry);

	//! Generates the keys of the chunk (empty for NULL), or returns false if the entry was retired
	bool GenerateKeys(DataChunk &table_chunk, ArenaAllocator &arena, unsafe_vector<ARTKey> &keys);
	bool IsRetired() const {
		return retired;
	}

	shared_ptr<IndexEntry> entry;
	//! The values of the index expressions of the last chunk
	DataChunk key_chunk;

private:
	vector<unique_ptr<Expression>> expressions;
	ExpressionExecutor executor;
	bool retired = false;
};

//! Whether the left key sorts before the right key
bool ARTKeyLessThan(const ARTKey &left, const ARTKey &right);

//! The live local rows of a commit that insert a key of a row the transaction deleted, next to that row id
struct IndexSwaps {
	explicit IndexSwaps(shared_ptr<IndexEntry> entry_p) : entry(std::move(entry_p)) {
	}

	shared_ptr<IndexEntry> entry;
	//! The positions of the rows among the live local rows in scan order, sorted
	vector<idx_t> positions;
};

//! The swaps of a table's unique indexes, prepared before the commit lock and completed under it
class CommitIndexChanges {
public:
	//! Prepares the unique indexes not prepared yet; a deleted key inserted twice is a violation
	ErrorData Prepare(DuckTransaction &transaction, LocalTableStorage &storage);
	//! The swaps of the entry, or nullptr if it has none
	optional_ptr<const IndexSwaps> GetSwaps(const IndexEntry &entry) const;
	//! Verifies the rows no statement end checked against the rows the transaction sees, except the swaps
	ErrorData VerifyUncheckedRows(DuckTransaction &transaction, LocalTableStorage &storage);
	//! From now on, a duplicate among the appended rows (from row_start) is a statement error
	void MarkImmediateErrors(row_t row_start) {
		mark_immediate_errors = true;
		append_row_start = row_start;
	}
	//! Records a constraint error of the entry while appending the chunk
	void RecordAppendError(IndexEntry &entry, const ErrorData &error, DataChunk &chunk) const;
	bool HasImmediateError() const {
		return immediate_error;
	}

private:
	//! The unique indexes whose swaps were prepared
	vector<IndexSwaps> prepared;
	//! Whether the transaction deleted no committed row of the table: then there are no swaps
	bool no_deletes = false;
	bool mark_immediate_errors = false;
	row_t append_row_start = 0;
	mutable bool immediate_error = false;
};

} // namespace duckdb
