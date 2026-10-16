#include "duckdb/transaction/delete_merge_state.hpp"

#include "duckdb/storage/table/index_entry.hpp"
#include "duckdb/storage/table/table_index_list.hpp"

namespace duckdb {

void DeleteMergeState::MergeTable(TableIndexList &indexes, TableIndexList &delete_indexes,
                                  const reference_set_t<IndexEntry> &incomplete_delete_entries,
                                  const optional_idx active_checkpoint) {
	// Snapshot the entries: the merge must not hold the entry-list lock across batches.
	vector<shared_ptr<IndexEntry>> entries;
	for (auto entry : indexes.IndexEntries()) {
		entries.push_back(std::move(entry));
	}
	for (auto &entry : entries) {
		// Uniqueness and the index type are re-checked under the entry lock in MergeLocalDeletes:
		// a concurrent rollback can retire the entry at any point.
		if (entry->GetBindState() != IndexBindState::BOUND) {
			continue;
		}
		auto delete_entry = delete_indexes.FindEntry(*entry);
		if (!delete_entry) {
			continue;
		}
		if (incomplete_delete_entries.find(*delete_entry) != incomplete_delete_entries.end()) {
			// The delete index missed deletes that happened before it was created.
			continue;
		}
		records.emplace_back();
		auto &record = records.back();
		record.entry = entry;
		record.delete_entry = delete_entry;
		bool complete = false;
		try {
			complete = entry->MergeLocalDeletes(*delete_entry, active_checkpoint, record.merged_count,
			                                    record.in_use_was_empty);
		} catch (...) {
			// Pin the delete index before the local storage dies: the revert needs its keys.
			delete_indexes.ExtractEntry(*delete_entry);
			throw;
		}
		if (complete) {
			covered.insert(*entry);
		} else if (record.merged_count == 0) {
			// Nothing was applied: keep the delete index for append-time exclusion checks.
			records.pop_back();
			continue;
		}
		// Take sole ownership of the delete index: the local storage dies before the commit
		// outcome is known, and its destructor would retire the entry.
		delete_indexes.ExtractEntry(*delete_entry);
	}
}

void DeleteMergeState::FinalizeCommit(const ActiveTransactionState active_transactions) {
	if (active_transactions == ActiveTransactionState::NO_OTHER_TRANSACTIONS) {
		for (auto &record : records) {
			if (record.merged_count == 0) {
				continue;
			}
			record.entry->FinalizeLocalDeleteMerge(*record.delete_entry, record.merged_count, record.in_use_was_empty);
		}
	}
	// Otherwise the copies stay in deleted_rows_in_use; cleanup removes them via the fetch path.
	records.clear();
	covered.clear();
}

void DeleteMergeState::Revert(const optional_idx active_checkpoint) {
	for (idx_t i = records.size(); i > 0; i--) {
		auto &record = records[i - 1];
		if (record.merged_count == 0) {
			continue;
		}
		record.entry->RevertLocalDeleteMerge(*record.delete_entry, record.merged_count, active_checkpoint);
	}
	records.clear();
	covered.clear();
}

} // namespace duckdb
