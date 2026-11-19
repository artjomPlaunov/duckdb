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

//! Consecutive rows of a table
struct RowRange {
	row_t row_start;
	idx_t count;
};

//! The rows that the commits after an index build's build point append or delete, for the build to catch up on.
class IndexBuildLog {
public:
	IndexBuildLog(transaction_t creator_id, transaction_t build_commit, unordered_set<column_t> column_ids);

	//! The transaction that builds the index
	const transaction_t creator_id;

public:
	//! How the build scans the table: the rows live after the build commit, seen by the creator
	ScanOptions GetScanOptions() const;
	//! The table columns of the index
	const unordered_set<column_t> &GetColumnIds() const {
		return column_ids;
	}
	//! The last commit whose changes the built index holds
	transaction_t GetCaughtUpCommit() const {
		return caught_up_commit;
	}
	//! Returns the number of logged rows the build has not caught up on
	idx_t PendingRows() const;

	//! Logs consecutive rows that a commit appended or deleted
	void LogRows(transaction_t commit_id, row_t row_start, idx_t count);
	//! Takes the rows that the commits up to and including this one changed, as sorted, disjoint ranges
	vector<RowRange> TakeChanges(transaction_t commit_id);
	//! Marks the changes up to and including this commit as caught up on
	void SetCaughtUp(transaction_t commit_id);

private:
	struct LoggedRows {
		transaction_t commit_id;
		RowRange rows;
	};

	const unordered_set<column_t> column_ids;
	transaction_t caught_up_commit;

	mutable mutex lock;
	vector<LoggedRows> logged_rows;
};

} // namespace duckdb
