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

namespace {

//! The keys of one unique index that the transaction deleted, and the live local rows that insert them again
struct PreparedIndex {
	PreparedIndex(DatabaseInstance &db, shared_ptr<IndexEntry> entry_p)
	    : swaps(std::move(entry_p)), executor(db), arena(Allocator::DefaultAllocator()) {
		auto handle = swaps.entry->GetReadHandle<BoundIndex>();
		expressions = handle->CopyBoundExpressions();
		for (auto &expression : expressions) {
			executor.AddExpression(*expression);
		}
		key_chunk.Initialize(Allocator::DefaultAllocator(), handle->GetLogicalTypes());
	}

	IndexSwaps swaps;
	//! A private copy of the index expressions: other commits use the executor of the index meanwhile
	vector<unique_ptr<Expression>> expressions;
	ExpressionExecutor executor;
	DataChunk key_chunk;
	ArenaAllocator arena;
	unsafe_vector<ARTKey> deleted_keys;
	//! Whether a live local row inserts the deleted key again
	vector<bool> inserted;

	//! Executes the index expressions on a chunk in table layout and generates its keys
	void GenerateKeys(DataChunk &table_chunk, ArenaAllocator &key_arena, unsafe_vector<ARTKey> &keys) {
		key_chunk.Reset();
		executor.Execute(table_chunk, key_chunk);
		keys.resize(key_chunk.size());
		auto handle = swaps.entry->GetReadHandle<BoundIndex>();
		handle->Cast<ART>().GenerateKeys(key_arena, key_chunk, keys);
	}
};

bool KeyLessThan(const ARTKey &left, const ARTKey &right) {
	return right > left;
}

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
	transaction.ScanDeletes(table_info, [&](DeleteInfo &info) {
		has_deletes = true;
		auto &version_table = info.table->GetStorage();
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
			index->GenerateKeys(rows, index->arena, keys);
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
		std::sort(index->deleted_keys.begin(), index->deleted_keys.end(), KeyLessThan);
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
				index->GenerateKeys(table_chunk, chunk_arena, keys);
				for (idx_t i = 0; i < index_chunk.size(); i++) {
					if (keys[i].Empty()) {
						continue;
					}
					auto it =
					    std::lower_bound(index->deleted_keys.begin(), index->deleted_keys.end(), keys[i], KeyLessThan);
					if (it == index->deleted_keys.end() || !(*it == keys[i])) {
						continue;
					}
					auto key_idx = NumericCast<idx_t>(it - index->deleted_keys.begin());
					if (index->inserted[key_idx]) {
						return ErrorData(
						    ConstraintException("PRIMARY KEY or UNIQUE constraint violation: duplicate key \"%s\"",
						                        BoundIndex::AppendRowError(index->key_chunk, i)));
					}
					index->inserted[key_idx] = true;
					index->swaps.positions.push_back(position + i);
				}
			}
			position += index_chunk.size();
		}
	}
	for (auto &index : indexes) {
		prepared.push_back(std::move(index->swaps));
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
