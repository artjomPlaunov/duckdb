//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/index_build_log.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/enums/scan_options.hpp"

namespace duckdb {

//! The rows that commits after an index build's scan point append or delete, so that the build can catch up on them
//! before the index is published. A committed update of an indexed column fails the build instead.
class IndexBuildLog {
public:
	IndexBuildLog(transaction_t creator_id, transaction_t build_commit, unordered_set<column_t> column_ids);

	//! The transaction that builds the index
	const transaction_t creator_id;

public:
	//! How the build scans the table: the rows live after the build commit, seen by the creator
	ScanOptions GetScanOptions() const;
	//! The last commit whose changes the built index holds
	transaction_t GetCaughtUpCommit() const {
		return caught_up_commit;
	}
	//! Returns the number of logged rows the build has not caught up on
	idx_t PendingRows() const;

	//! Logs consecutive rows that a commit appended or deleted
	void LogRows(transaction_t commit_id, row_t row_start, idx_t count);
	//! Logs a committed update of a column
	void LogUpdate(transaction_t commit_id, column_t column_id);

	//! Takes the sorted, distinct rows that the commits up to and including this one changed. Throws if one of them
	//! updated an indexed column.
	vector<row_t> TakeChanges(transaction_t commit_id);
	//! Marks the changes up to and including this commit as caught up on
	void SetCaughtUp(transaction_t commit_id);

private:
	struct LoggedRows {
		transaction_t commit_id;
		row_t row_start;
		idx_t count;
	};

	const transaction_t build_commit;
	const unordered_set<column_t> column_ids;
	transaction_t caught_up_commit;

	mutable mutex lock;
	vector<LoggedRows> logged_rows;
	idx_t pending_rows = 0;
	//! The first commit that updated an indexed column, if any
	transaction_t first_update_commit = MAX_TRANSACTION_ID;
};

} // namespace duckdb
