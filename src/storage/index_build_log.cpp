#include "duckdb/storage/table/index_build_log.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/numeric_utils.hpp"

namespace duckdb {

IndexBuildLog::IndexBuildLog(transaction_t creator_id_p, transaction_t build_commit,
                             unordered_set<column_t> column_ids_p)
    : creator_id(creator_id_p), column_ids(std::move(column_ids_p)), caught_up_commit(build_commit) {
}

ScanOptions IndexBuildLog::GetScanOptions() const {
	// the scan runs before any catch-up, so the caught-up commit is the build point
	ScanOptions options(TransactionData(creator_id, VisibilityBound::Through(caught_up_commit)));
	// the creator's own deletes reach the index through its commit
	options.delete_type = DeletedScanType::OMIT_COMMITTED_DELETES;
	options.update_type = UpdateScanType::DISALLOW_UPDATES;
	return options;
}

idx_t IndexBuildLog::PendingRows() const {
	lock_guard<mutex> guard(lock);
	idx_t pending_rows = 0;
	for (auto &entry : logged_rows) {
		pending_rows += entry.rows.count;
	}
	return pending_rows;
}

void IndexBuildLog::LogRows(transaction_t commit_id, row_t row_start, idx_t count) {
	lock_guard<mutex> guard(lock);
	logged_rows.push_back(LoggedRows {commit_id, RowRange {row_start, count}});
}

vector<RowRange> IndexBuildLog::TakeChanges(transaction_t commit_id) {
	vector<RowRange> ranges;
	{
		// committers log under the commit lock: sort after releasing the lock
		lock_guard<mutex> guard(lock);
		// a commit after commit_id may still be logging its ranges: keep them for a later round
		idx_t kept = 0;
		for (auto &entry : logged_rows) {
			if (entry.commit_id > commit_id) {
				logged_rows[kept++] = entry;
				continue;
			}
			ranges.push_back(entry.rows);
		}
		logged_rows.resize(kept);
	}
	std::sort(ranges.begin(), ranges.end(),
	          [](const RowRange &a, const RowRange &b) { return a.row_start < b.row_start; });
	// merge overlapping and adjacent ranges
	idx_t merged = 0;
	for (auto &range : ranges) {
		if (merged > 0) {
			auto &last = ranges[merged - 1];
			auto last_end = last.row_start + UnsafeNumericCast<row_t>(last.count);
			if (range.row_start <= last_end) {
				auto end = MaxValue(last_end, range.row_start + UnsafeNumericCast<row_t>(range.count));
				last.count = UnsafeNumericCast<idx_t>(end - last.row_start);
				continue;
			}
		}
		ranges[merged++] = range;
	}
	ranges.resize(merged);
	return ranges;
}

void IndexBuildLog::SetCaughtUp(transaction_t commit_id) {
	D_ASSERT(commit_id >= caught_up_commit);
	caught_up_commit = commit_id;
}

} // namespace duckdb
