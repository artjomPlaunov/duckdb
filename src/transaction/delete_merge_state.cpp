#include "duckdb/transaction/delete_merge_state.hpp"

#include "duckdb/storage/table/index_entry.hpp"
#include "duckdb/storage/table/table_index_list.hpp"

namespace duckdb {

void DeleteMergeState::MergeTable(TableIndexList &indexes, TableIndexList &delete_indexes,
                                  const optional_idx active_checkpoint) {
	// Snapshot the entries: the merge must not hold the entry-list lock across batches.
	vector<shared_ptr<IndexEntry>> entries;
	for (auto entry : indexes.IndexEntries()) {
		entries.push_back(std::move(entry));
	}
	for (auto &entry : entries) {
		if (entry->GetBindState() != IndexBindState::BOUND) {
			continue;
		}
		// Non-unique indexes also have (empty) delete indexes: only unique indexes track their deletes.
		auto delete_entry = delete_indexes.FindEntry(*entry);
		if (!delete_entry || !delete_entry->IsUnique()) {
			continue;
		}
		// The local storage dies before the commit outcome is known: take over the delete index.
		delete_indexes.ExtractEntry(delete_entry->GetIndexOid());
		records.emplace_back();
		auto &record = records.back();
		record.entry = entry;
		record.delete_entry = std::move(delete_entry);
		if (entry->MergeLocalDeletes(record.delete_entry, active_checkpoint, record.merged_count)) {
			covered.insert(*entry);
		}
	}
}

void DeleteMergeState::Cleanup() {
	for (auto &record : records) {
		record.entry->DetachLocalDeleteMerge(*record.delete_entry);
	}
	records.clear();
	covered.clear();
}

void DeleteMergeState::Revert(const optional_idx active_checkpoint) {
	for (idx_t i = records.size(); i > 0; i--) {
		auto &record = records[i - 1];
		record.entry->RevertLocalDeleteMerge(*record.delete_entry, record.merged_count, active_checkpoint);
	}
	records.clear();
	covered.clear();
}

} // namespace duckdb
