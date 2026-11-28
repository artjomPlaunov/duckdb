#include "duckdb/transaction/commit_index_changes.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/storage/table/index_entry.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/table_index_list.hpp"
#include "duckdb/transaction/commit_state.hpp"
#include "duckdb/transaction/delete_info.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/local_storage.hpp"

namespace duckdb {

IndexKeyGenerator::IndexKeyGenerator(DatabaseInstance &db, shared_ptr<IndexEntry> entry_p)
    : entry(std::move(entry_p)), executor(db) {
	auto handle = entry->TryGetReadHandle<BoundIndex>();
	if (!handle) {
		retired = true;
		return;
	}
	expressions = (*handle)->CopyBoundExpressions();
	for (auto &expression : expressions) {
		executor.AddExpression(*expression);
	}
	key_chunk.Initialize(Allocator::DefaultAllocator(), (*handle)->GetLogicalTypes());
}

bool IndexKeyGenerator::GenerateKeys(DataChunk &table_chunk, ArenaAllocator &arena, unsafe_vector<ARTKey> &keys) {
	keys.clear();
	keys.resize(table_chunk.size());
	if (retired) {
		return false;
	}
	key_chunk.Reset();
	executor.Execute(table_chunk, key_chunk);
	auto handle = entry->TryGetReadHandle<BoundIndex>();
	if (!handle) {
		retired = true;
		return false;
	}
	(*handle)->Cast<ART>().GenerateKeys(arena, key_chunk, keys);
	return true;
}

bool ARTKeyLessThan(const ARTKey &left, const ARTKey &right) {
	return right > left;
}

namespace {

//! The keys of one unique index that the transaction deleted, and the live local rows that insert them again
struct PreparedIndex {
	PreparedIndex(DatabaseInstance &db, shared_ptr<IndexEntry> entry)
	    : swaps(entry), generator(db, std::move(entry)), arena(Allocator::DefaultAllocator()) {
	}

	IndexSwaps swaps;
	IndexKeyGenerator generator;
	ArenaAllocator arena;
	unsafe_vector<ARTKey> deleted_keys;
	//! Whether a live local row inserts the deleted key again
	vector<bool> inserted;
};

} // namespace

ErrorData CommitIndexChanges::Prepare(DuckTransaction &transaction, LocalTableStorage &storage) {
	if (no_deletes) {
		return ErrorData();
	}
	auto &table = storage.table_ref.get();
	auto &table_info = *table.GetDataTableInfo();
	auto &db = table.db.GetDatabase();

	vector<shared_ptr<IndexEntry>> entries;
	for (auto entry : table_info.GetIndexes().BuiltIndexEntries()) {
		if (entry->GetBindState() != IndexBindState::BOUND || !entry->IsUnique() ||
		    entry->GetIndexType() != ART::TYPE_NAME) {
			continue;
		}
		bool is_prepared = false;
		for (auto &prepared_entry : prepared) {
			is_prepared = is_prepared || prepared_entry.entry.get() == entry.get();
		}
		if (!is_prepared) {
			entries.push_back(std::move(entry));
		}
	}
	if (entries.empty()) {
		return ErrorData();
	}
	vector<unique_ptr<PreparedIndex>> indexes;
	for (auto &entry : entries) {
		indexes.push_back(make_uniq<PreparedIndex>(db, entry));
	}

	// the keys of the committed rows the transaction deleted, from the values the deletes stored
	bool has_deletes = false;
	unsafe_vector<ARTKey> keys;
	transaction.ScanDeletes(&table_info, [&](DeleteInfo &info) {
		has_deletes = true;
		auto &version_table = IndexDataRemover::GetNewestStorage(info);
		row_t row_numbers[STANDARD_VECTOR_SIZE];
		IndexDataRemover::GetRowIds(info, row_numbers);
		Vector row_ids(LogicalType::ROW_TYPE, data_ptr_cast(row_numbers), info.count);
		DataChunk stored_values;
		DataChunk table_values;
		auto index_values = IndexDataRemover::ReadIndexValues(info, version_table, stored_values, table_values);
		DataChunk fetch_chunk;
		DataChunk rows;
		version_table.GetIndexedRows(row_ids, info.count, index_values.get(), fetch_chunk, rows);
		for (auto &index : indexes) {
			index->generator.GenerateKeys(rows, index->arena, keys);
			for (idx_t i = 0; i < info.count; i++) {
				if (!keys[i].Empty()) {
					index->deleted_keys.push_back(keys[i]);
				}
			}
		}
	});
	if (!has_deletes) {
		no_deletes = true;
		return ErrorData();
	}

	bool has_deleted_keys = false;
	for (auto &index : indexes) {
		std::sort(index->deleted_keys.begin(), index->deleted_keys.end(), ARTKeyLessThan);
		index->inserted.resize(index->deleted_keys.size(), false);
		has_deleted_keys = has_deleted_keys || !index->deleted_keys.empty();
	}

	// the live local rows that insert a deleted key again, in the order of the commit's append
	auto &collection = storage.GetCollection();
	if (has_deleted_keys && collection.GetTotalRows() > storage.deleted_rows) {
		auto indexed_columns = table_info.GetIndexes().GetIndexedColumns();
		vector<StorageIndex> column_ids;
		for (auto &column_id : indexed_columns) {
			column_ids.emplace_back(column_id);
		}
		std::sort(column_ids.begin(), column_ids.end());
		auto &types = collection.GetTypes();
		DataChunk table_chunk;
		table_chunk.InitializeEmpty(types);
		ArenaAllocator chunk_arena(Allocator::DefaultAllocator());
		idx_t position = 0;
		for (auto &index_chunk : collection.Chunks(transaction, column_ids)) {
			for (idx_t c = 0; c < types.size(); c++) {
				table_chunk.data[c].Reference(Value(types[c]), count_t(index_chunk.size()));
			}
			for (idx_t i = 0; i < column_ids.size(); i++) {
				table_chunk.data[column_ids[i].GetPrimaryIndex()].Reference(index_chunk.data[i]);
			}
			table_chunk.CheckCardinality(index_chunk.size());
			for (auto &index : indexes) {
				if (index->deleted_keys.empty()) {
					continue;
				}
				chunk_arena.Reset();
				index->generator.GenerateKeys(table_chunk, chunk_arena, keys);
				for (idx_t i = 0; i < index_chunk.size(); i++) {
					if (keys[i].Empty()) {
						continue;
					}
					auto it = std::lower_bound(index->deleted_keys.begin(), index->deleted_keys.end(), keys[i],
					                           ARTKeyLessThan);
					if (it == index->deleted_keys.end() || !(*it == keys[i])) {
						continue;
					}
					auto key_idx = NumericCast<idx_t>(it - index->deleted_keys.begin());
					if (index->inserted[key_idx]) {
						if (!index->generator.entry->IsDeferred() && !storage.RowsChecked()) {
							transaction.MarkStatementConstraintError();
						}
						return ErrorData(
						    ConstraintException("PRIMARY KEY or UNIQUE constraint violation: duplicate key \"%s\"",
						                        BoundIndex::AppendRowError(index->generator.key_chunk, i)));
					}
					index->inserted[key_idx] = true;
					index->swaps.positions.push_back(position + i);
				}
			}
			position += index_chunk.size();
		}
	}
	for (auto &index : indexes) {
		if (!index->generator.IsRetired()) {
			prepared.push_back(std::move(index->swaps));
		}
	}
	return ErrorData();
}

//! The rows that the committed state of the index holds for the keys of the chunk (in table layout)
static unsafe_vector<row_t> GetKeyHolders(IndexEntry &entry, DataChunk &chunk) {
	unsafe_vector<row_t> holders;
	auto handle = entry.TryGetReadHandle<BoundIndex>();
	if (!handle) {
		return holders;
	}
	auto &db = (*handle)->db.GetDatabase();
	handle.reset();
	IndexKeyGenerator generator(db, entry.shared_from_this());
	ArenaAllocator arena(Allocator::DefaultAllocator());
	unsafe_vector<ARTKey> keys;
	if (!generator.GenerateKeys(chunk, arena, keys)) {
		return holders;
	}
	SelectionVector sel(chunk.size());
	idx_t count = 0;
	for (idx_t i = 0; i < chunk.size(); i++) {
		if (!keys[i].Empty()) {
			sel.set_index(count++, i);
		}
	}
	DataChunk key_values;
	key_values.InitializeEmpty(generator.key_chunk.GetTypes());
	key_values.Slice(generator.key_chunk, sel, count);
	entry.ScanCommittedRowIds(key_values, holders);
	return holders;
}

void CommitIndexChanges::RecordAppendError(IndexEntry &entry, const ErrorData &error, DataChunk &chunk) const {
	if (!mark_immediate_errors || error.Type() != ExceptionType::CONSTRAINT || entry.IsDeferred()) {
		return;
	}
	// a key that only the appended rows hold is a duplicate among the statement's rows
	for (auto row_id : GetKeyHolders(entry, chunk)) {
		if (row_id < append_row_start) {
			return;
		}
	}
	immediate_error = true;
}

//! Throws the statement-time error if a row of the chunk has the key of a row that the transaction sees. A key of a row
//! committed after its start is left to the append, which reports it as a commit conflict.
static void VerifyAgainstVisibleRows(DuckTransaction &transaction, DataTable &table, IndexEntry &entry,
                                     DataChunk &chunk) {
	try {
		entry.VerifyAppend(nullptr, chunk, nullptr);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		if (error.Type() != ExceptionType::CONSTRAINT) {
			throw;
		}
		for (auto row_id : GetKeyHolders(entry, chunk)) {
			if (table.CanFetch(transaction, row_id)) {
				throw;
			}
		}
	}
}

ErrorData CommitIndexChanges::VerifyUncheckedRows(DuckTransaction &transaction, LocalTableStorage &storage) {
	auto &table = storage.table_ref.get();
	auto &collection = storage.GetCollection();
	if (collection.GetTotalRows() <= storage.deleted_rows) {
		return ErrorData();
	}
	vector<shared_ptr<IndexEntry>> entries;
	for (auto entry : table.GetDataTableInfo()->GetIndexes().BuiltIndexEntries()) {
		if (entry->GetBindState() == IndexBindState::BOUND && entry->IsUnique() && !entry->IsDeferred() &&
		    entry->GetIndexType() == ART::TYPE_NAME) {
			entries.push_back(std::move(entry));
		}
	}
	if (entries.empty()) {
		return ErrorData();
	}
	auto indexed_columns = table.GetDataTableInfo()->GetIndexes().GetIndexedColumns();
	vector<StorageIndex> column_ids;
	for (auto &column_id : indexed_columns) {
		column_ids.emplace_back(column_id);
	}
	std::sort(column_ids.begin(), column_ids.end());
	auto &types = collection.GetTypes();
	DataChunk table_chunk;
	table_chunk.InitializeEmpty(types);
	idx_t position = 0;
	try {
		for (auto &index_chunk : collection.Chunks(transaction, column_ids)) {
			for (idx_t c = 0; c < types.size(); c++) {
				table_chunk.data[c].Reference(Value(types[c]), count_t(index_chunk.size()));
			}
			for (idx_t i = 0; i < column_ids.size(); i++) {
				table_chunk.data[column_ids[i].GetPrimaryIndex()].Reference(index_chunk.data[i]);
			}
			table_chunk.CheckCardinality(index_chunk.size());
			for (auto &entry : entries) {
				auto swaps = GetSwaps(*entry);
				if (!swaps) {
					VerifyAgainstVisibleRows(transaction, table, *entry, table_chunk);
					continue;
				}
				SelectionVector sel(index_chunk.size());
				idx_t count = 0;
				auto it = std::lower_bound(swaps->positions.begin(), swaps->positions.end(), position);
				for (idx_t i = 0; i < index_chunk.size(); i++) {
					if (it != swaps->positions.end() && *it == position + i) {
						it++;
						continue;
					}
					sel.set_index(count++, i);
				}
				DataChunk other_rows;
				other_rows.InitializeEmpty(types);
				other_rows.Slice(table_chunk, sel, count);
				VerifyAgainstVisibleRows(transaction, table, *entry, other_rows);
			}
			position += index_chunk.size();
		}
	} catch (std::exception &ex) {
		return ErrorData(ex);
	}
	return ErrorData();
}

optional_ptr<const IndexSwaps> CommitIndexChanges::GetSwaps(const IndexEntry &entry) const {
	for (auto &swaps : prepared) {
		if (swaps.entry.get() == &entry) {
			return swaps.positions.empty() ? nullptr : &swaps;
		}
	}
	return nullptr;
}

} // namespace duckdb
