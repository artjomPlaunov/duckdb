#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
#include "duckdb/planner/constraints/bound_foreign_key_constraint.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/storage/table/index_entry.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/table_index_list.hpp"
#include "duckdb/transaction/commit_foreign_keys.hpp"
#include "duckdb/transaction/commit_state.hpp"
#include "duckdb/transaction/delete_info.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/local_storage.hpp"

namespace duckdb {

//! Adds the keys of the transaction's deleted committed rows to the local delete indexes of the unique indexes, from
//! the deletes that each one does not hold yet (all of them for an index added since)
static idx_t FillDeleteIndexes(DuckTransaction &transaction, DataTable &table, LocalTableStorage &storage) {
	auto &local_indexes = storage.GetLocalIndexes();
	auto &delete_indexes = storage.GetDeleteIndexes();
	for (auto entry : table.GetDataTableInfo()->GetIndexes().BuiltIndexEntries()) {
		if (entry->GetBindState() != IndexBindState::BOUND || !entry->IsUnique()) {
			continue;
		}
		if (!delete_indexes.FindEntry(*entry)) {
			entry->InitializeLocalDeleteIndex(delete_indexes);
		}
	}
	vector<shared_ptr<IndexEntry>> targets;
	vector<idx_t> first_deletes;
	idx_t first_delete = NumericLimits<idx_t>::Maximum();
	for (auto entry : delete_indexes.IndexEntries()) {
		auto absorbed = local_indexes.deletes_in_index[entry->GetIndexOid()];
		targets.push_back(entry);
		first_deletes.push_back(absorbed);
		first_delete = MinValue(first_delete, absorbed);
	}

	idx_t delete_idx = 0;
	transaction.ScanDeletes(table.GetDataTableInfo().get(), [&](DeleteInfo &info) {
		if (delete_idx++ < first_delete) {
			return;
		}
		auto &version_table = IndexDataRemover::GetNewestStorage(info);
		row_t row_numbers[STANDARD_VECTOR_SIZE];
		IndexDataRemover::GetRowIds(info, row_numbers);
		Vector row_ids(LogicalType::ROW_TYPE, data_ptr_cast(row_numbers), info.count);
		DataChunk stored_values;
		DataChunk table_values;
		auto index_values = IndexDataRemover::ReadIndexValues(info, version_table, stored_values, table_values);
		DataChunk fetch_chunk;
		DataChunk rows;
		if (!version_table.GetIndexedRows(row_ids, info.count, index_values.get(), fetch_chunk, rows)) {
			return;
		}
		for (idx_t i = 0; i < targets.size(); i++) {
			if (delete_idx > first_deletes[i]) {
				targets[i]->AppendToDeleteIndexes(rows, row_ids);
			}
		}
	});
	for (auto &target : targets) {
		local_indexes.deletes_in_index[target->GetIndexOid()] = delete_idx;
	}
	return delete_idx;
}

//! Calls the callback with the indexed columns (in table layout) and the row ids of the live local rows in
//! [first_row, total_rows)
static void ScanNewRows(DuckTransaction &transaction, DataTable &table, LocalTableStorage &storage, idx_t first_row,
                        idx_t total_rows, const std::function<void(DataChunk &rows, Vector &row_ids)> &callback) {
	auto &collection = storage.GetCollection();
	if (first_row >= total_rows) {
		return;
	}
	auto indexed_columns = table.GetDataTableInfo()->GetIndexes().GetIndexedColumns();
	vector<StorageIndex> column_ids;
	for (auto column_id : indexed_columns) {
		column_ids.emplace_back(column_id);
	}
	std::sort(column_ids.begin(), column_ids.end());
	column_ids.emplace_back(COLUMN_IDENTIFIER_ROW_ID);
	auto &types = collection.GetTypes();
	vector<LogicalType> scan_types;
	for (idx_t i = 0; i + 1 < column_ids.size(); i++) {
		scan_types.push_back(types[column_ids[i].GetPrimaryIndex()]);
	}
	scan_types.push_back(LogicalType::ROW_TYPE);

	TableScanState scan_state;
	scan_state.Initialize(column_ids);
	// the local rows are numbered from MAX_ROW_ID
	auto row_start = NumericCast<idx_t>(MAX_ROW_ID);
	collection.InitializeScanWithOffset(QueryContext(), scan_state.table_state, column_ids, row_start + first_row,
	                                    row_start + total_rows);
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), scan_types);
	DataChunk rows;
	rows.InitializeEmpty(types);
	auto first_row_id = NumericCast<row_t>(row_start + first_row);
	auto end_row_id = NumericCast<row_t>(row_start + total_rows);
	while (true) {
		chunk.Reset();
		scan_state.table_state.Scan(transaction, chunk);
		if (chunk.size() == 0) {
			break;
		}
		// the scan starts at the vector of the first row
		auto &chunk_row_ids = chunk.data.back();
		chunk_row_ids.Flatten();
		auto row_id_data = FlatVector::GetData<row_t>(chunk_row_ids);
		SelectionVector sel(chunk.size());
		idx_t count = 0;
		for (idx_t i = 0; i < chunk.size(); i++) {
			if (row_id_data[i] >= first_row_id && row_id_data[i] < end_row_id) {
				sel.set_index(count++, i);
			}
		}
		if (count == 0) {
			continue;
		}
		if (count != chunk.size()) {
			chunk.Slice(sel, count);
		}
		for (idx_t c = 0; c < types.size(); c++) {
			rows.data[c].Reference(Value(types[c]), count_t(count));
		}
		for (idx_t i = 0; i + 1 < column_ids.size(); i++) {
			rows.data[column_ids[i].GetPrimaryIndex()].Reference(chunk.data[i]);
		}
		rows.CheckCardinality(count);
		Vector row_ids(Vector::Ref(chunk.data.back()));
		row_ids.Flatten();
		callback(rows, row_ids);
	}
}

void DataTable::VerifyStatementConstraints(ClientContext &context, DuckTableEntry &table_entry) {
	if (!HasIndexes()) {
		return;
	}
	auto &transaction = DuckTransaction::Get(context, db);
	auto &local_storage = transaction.GetLocalStorage();
	info->BindIndexes(context);
	auto storage = local_storage.GetOrCreateStorage(context.GetContext(), *this);
	auto &local_indexes = storage->GetLocalIndexes();
	lock_guard<mutex> guard(local_indexes.check_lock);
	// connections that share the transaction can append meanwhile: the check covers the rows up to now
	auto flush_key = storage->LockShared();

	// the keys the transaction removed so far are excluded from the unique checks
	auto delete_count = FillDeleteIndexes(transaction, *this, *storage);

	auto &collection = storage->GetCollection();
	auto total_rows = collection.GetTotalRows();
	auto first_row = local_indexes.checked_rows;

	// a unique index added since the local storage was created gets an append index with the checked rows
	auto &append_indexes = storage->GetAppendIndexes();
	TableIndexList new_append_indexes;
	for (auto entry : info->indexes.BuiltIndexEntries()) {
		if (entry->GetBindState() == IndexBindState::BOUND && entry->IsUnique() && !append_indexes.FindEntry(*entry)) {
			entry->InitializeLocalAppendIndex(new_append_indexes);
		}
	}
	if (!new_append_indexes.Empty() && first_row > 0) {
		ScanNewRows(transaction, *this, *storage, 0, first_row,
		            [&](DataChunk &rows, Vector &row_ids) { new_append_indexes.Append(rows, row_ids); });
	}
	for (auto entry : new_append_indexes.IndexEntries()) {
		append_indexes.AddEntry(std::move(entry));
	}
	if (first_row < total_rows) {
		// unique keys: the new rows against the committed rows the transaction keeps, then against its own rows
		auto &delete_indexes = storage->GetDeleteIndexes();
		ScanNewRows(transaction, *this, *storage, first_row, total_rows, [&](DataChunk &rows, Vector &row_ids) {
			if (HasUniqueIndexes()) {
				info->indexes.VerifyUniqueIndexes(&delete_indexes, rows, nullptr);
			}
			if (!append_indexes.Empty()) {
				append_indexes.Append(rows, row_ids);
			}
		});
		local_indexes.checked_rows = total_rows;

		// foreign keys: every key of the new rows needs a parent, which can be one of the rows of the statement
		for (auto &constraint : table_entry.GetConstraints()) {
			if (constraint->type != ConstraintType::FOREIGN_KEY) {
				continue;
			}
			auto &foreign_key = constraint->Cast<ForeignKeyConstraint>().info;
			if (!foreign_key.IsAppendConstraint()) {
				continue;
			}
			physical_index_set_t pk_keys(foreign_key.pk_keys.begin(), foreign_key.pk_keys.end());
			physical_index_set_t fk_keys(foreign_key.fk_keys.begin(), foreign_key.fk_keys.end());
			BoundForeignKeyConstraint bound_foreign_key(foreign_key, std::move(pk_keys), std::move(fk_keys));
			ScanNewRows(transaction, *this, *storage, first_row, total_rows, [&](DataChunk &rows, Vector &) {
				VerifyAppendForeignKeyConstraint(storage.get(), bound_foreign_key, context, rows);
			});
		}
	}

	if (local_indexes.checked_deletes < delete_count) {
		// referenced keys: the parent rows deleted by the statement must not have children left
		CommitForeignKeyCheck::VerifyDeletes(context, transaction, *this, table_entry, local_indexes.checked_deletes);
		local_indexes.checked_deletes = delete_count;
	}
}

void DataTable::VerifyStatementEnd(ClientContext &context, DuckTableEntry &table_entry, bool return_chunk,
                                   bool other_writers) {
	if (context.transaction.IsAutoCommit() && !return_chunk && !other_writers) {
		// the commit verifies the constraints, with the statement-time errors
		return;
	}
	table_entry.GetStorage().VerifyStatementConstraints(context, table_entry);
}

} // namespace duckdb
