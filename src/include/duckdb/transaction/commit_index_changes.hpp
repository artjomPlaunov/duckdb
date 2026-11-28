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

//! Generates the ART keys of an index for chunks in table layout, with a private copy of the index expressions: other
//! commits and statements use the executor of the index meanwhile
class IndexKeyGenerator {
public:
	IndexKeyGenerator(DatabaseInstance &db, shared_ptr<IndexEntry> entry);

	//! Executes the index expressions on the chunk into key_chunk and generates its keys (empty for NULL keys).
	//! Returns false if the entry was retired.
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

//! The live local rows of a commit whose key in a unique index belongs to a row the transaction deleted. Their row
//! ids are inserted next to the deleted one (a gate), which the removal after the commit marker takes out.
struct IndexSwaps {
	explicit IndexSwaps(shared_ptr<IndexEntry> entry_p) : entry(std::move(entry_p)) {
	}

	shared_ptr<IndexEntry> entry;
	//! The positions of the rows among the live local rows in scan order, sorted
	vector<idx_t> positions;
};

//! What the commit of a table's local rows changes in its unique indexes, derived from the local rows and the stored
//! keys of the transaction's deletes. Prepared before the commit lock, completed under it for indexes added since.
class CommitIndexChanges {
public:
	//! Finds the swaps of the unique indexes that are not prepared yet. Returns a constraint violation, before anything
	//! shared is changed, if the live local rows insert one deleted key twice.
	ErrorData Prepare(DuckTransaction &transaction, LocalTableStorage &storage);
	//! The swaps of the entry, or nullptr if it has none
	optional_ptr<const IndexSwaps> GetSwaps(const IndexEntry &entry) const;
	//! Verifies the live local rows that no statement end checked against the committed rows of the unique indexes
	//! that are not deferred, with the statement-time errors. The swaps are skipped: their keys are the transaction's.
	ErrorData VerifyUncheckedRows(DuckTransaction &transaction, LocalTableStorage &storage);
	//! From now on, a constraint error of a unique index that is not deferred is the error of a statement if the key is
	//! the key of another of the appended rows (starting at row_start)
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
