#include "duckdb/transaction/commit_foreign_keys.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/types/conflict_manager.hpp"
#include "duckdb/common/types/constraint_conflict_info.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/storage/table/index_entry.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/table_index_list.hpp"
#include "duckdb/transaction/commit_index_changes.hpp"
#include "duckdb/transaction/commit_state.hpp"
#include "duckdb/transaction/delete_info.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/local_storage.hpp"

namespace duckdb {

namespace {

//! A table whose rows the commit inserts or deletes
struct ChangedTable {
	reference<DataTable> table;
	//! An entry of the table, for its constraints
	reference<DuckTableEntry> entry;
	//! The live local rows, if any
	shared_ptr<LocalTableStorage> storage;
	bool has_deletes = false;
};

//! A sorted list of ART keys; every key can be taken once
class KeyList {
public:
	KeyList() : arena(Allocator::DefaultAllocator()) {
	}

	void Add(const unsafe_vector<ARTKey> &new_keys, idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			if (!new_keys[i].Empty()) {
				keys.push_back(ARTKey::CreateARTKeyFromBytes(arena, new_keys[i].data, new_keys[i].len));
			}
		}
	}
	void Sort() {
		std::sort(keys.begin(), keys.end(), ARTKeyLessThan);
		taken.resize(keys.size(), false);
	}
	bool Contains(const ARTKey &key) const {
		return std::binary_search(keys.begin(), keys.end(), key, ARTKeyLessThan);
	}
	//! Takes one instance of the key that is not taken yet, if any
	bool Take(const ARTKey &key) {
		auto it = std::lower_bound(keys.begin(), keys.end(), key, ARTKeyLessThan);
		for (; it != keys.end() && *it == key; it++) {
			auto idx = NumericCast<idx_t>(it - keys.begin());
			if (!taken[idx]) {
				taken[idx] = true;
				return true;
			}
		}
		return false;
	}

private:
	ArenaAllocator arena;
	unsafe_vector<ARTKey> keys;
	vector<bool> taken;
};

//! Calls the callback with the indexed columns (in table layout) and the row ids of the committed rows the transaction
//! deleted from the table
void ScanDeletedRows(DuckTransaction &transaction, DataTable &table,
                     const std::function<void(DataChunk &rows, Vector &row_ids)> &callback, idx_t first_delete = 0) {
	idx_t delete_idx = 0;
	transaction.ScanDeletes(table.GetDataTableInfo().get(), [&](DeleteInfo &info) {
		if (delete_idx++ < first_delete) {
			return;
		}
		auto &version_table = IndexDataRemover::GetNewestStorage(info);
		Vector row_ids(LogicalType::ROW_TYPE, info.count);
		IndexDataRemover::GetRowIds(info, row_ids);
		DataChunk stored_values;
		DataChunk table_values;
		auto index_values = IndexDataRemover::ReadIndexValues(info, version_table, stored_values, table_values);
		DataChunk fetch_chunk;
		DataChunk rows;
		if (version_table.GetIndexedRows(row_ids, info.count, index_values.get(), fetch_chunk, rows)) {
			callback(rows, row_ids);
		}
	});
}

//! Calls the callback with the indexed columns (in table layout) of the live local rows of the table
void ScanLocalRows(DuckTransaction &transaction, LocalTableStorage &storage,
                   const std::function<void(DataChunk &rows)> &callback) {
	auto &collection = storage.GetCollection();
	if (collection.GetTotalRows() <= storage.deleted_rows) {
		return;
	}
	auto indexed_columns = storage.table_ref.get().GetDataTableInfo()->GetIndexes().GetIndexedColumns();
	vector<StorageIndex> column_ids;
	for (auto column_id : indexed_columns) {
		column_ids.emplace_back(column_id);
	}
	std::sort(column_ids.begin(), column_ids.end());
	auto &types = collection.GetTypes();
	DataChunk rows;
	rows.InitializeEmpty(types);
	for (auto &chunk : collection.Chunks(transaction, column_ids)) {
		for (idx_t c = 0; c < types.size(); c++) {
			rows.data[c].Reference(Value(types[c]), count_t(chunk.size()));
		}
		for (idx_t i = 0; i < column_ids.size(); i++) {
			rows.data[column_ids[i].GetPrimaryIndex()].Reference(chunk.data[i]);
		}
		rows.CheckCardinality(chunk.size());
		callback(rows);
	}
}

//! References the key columns of a chunk of one table in the layout of the other table of a foreign key
void MapKeyColumns(DataChunk &source, const vector<PhysicalIndex> &source_keys,
                   const vector<PhysicalIndex> &target_keys, const vector<LogicalType> &target_types,
                   DataChunk &result) {
	result.InitializeEmpty(target_types);
	for (idx_t c = 0; c < target_types.size(); c++) {
		result.data[c].Reference(Value(target_types[c]), count_t(source.size()));
	}
	for (idx_t i = 0; i < source_keys.size(); i++) {
		result.data[target_keys[i].index].Reference(source.data[source_keys[i].index]);
	}
	result.CheckCardinality(source.size());
}

vector<LogicalType> GetPhysicalTypes(TableCatalogEntry &entry) {
	vector<LogicalType> types;
	for (auto &column : entry.GetColumns().Physical()) {
		types.push_back(column.Type());
	}
	return types;
}

class ForeignKeyVerifier {
public:
	ForeignKeyVerifier(ClientContext &context, DuckTransaction &transaction)
	    : context(context), transaction(transaction), local_storage(transaction.GetLocalStorage()) {
	}

	//! Verifies the parent side of the foreign keys of the table for the given removed local rows
	void VerifyRemovedLocalRows(DataTable &table, DuckTableEntry &entry, DataChunk &rows) {
		ChangedTable changed {table, entry, nullptr, true};
		for (auto &constraint : entry.GetConstraints()) {
			if (constraint->type != ConstraintType::FOREIGN_KEY) {
				continue;
			}
			auto &foreign_key = constraint->Cast<ForeignKeyConstraint>().info;
			if (foreign_key.IsDeleteConstraint()) {
				VerifyRemovedParentKeys(changed, foreign_key,
				                        [&](const std::function<void(DataChunk & rows)> &check) { check(rows); });
			}
		}
	}

	//! Verifies the parent side of the foreign keys of the table for its deletes from the first_delete-th on
	void VerifyDeletes(DataTable &table, DuckTableEntry &entry, idx_t first_delete) {
		ChangedTable changed {table, entry, nullptr, true};
		for (auto &constraint : entry.GetConstraints()) {
			if (constraint->type != ConstraintType::FOREIGN_KEY) {
				continue;
			}
			auto &foreign_key = constraint->Cast<ForeignKeyConstraint>().info;
			if (foreign_key.IsDeleteConstraint()) {
				VerifyDeletedParentRows(changed, foreign_key, first_delete);
			}
		}
	}

	void Verify() {
		GatherChangedTables();
		for (auto &changed : changed_tables) {
			for (auto &constraint : changed.entry.get().GetConstraints()) {
				if (constraint->type != ConstraintType::FOREIGN_KEY) {
					continue;
				}
				auto &foreign_key = constraint->Cast<ForeignKeyConstraint>().info;
				if (foreign_key.IsAppendConstraint() && changed.storage) {
					VerifyChildRows(changed, foreign_key);
				}
				if (foreign_key.IsDeleteConstraint() && changed.has_deletes) {
					VerifyDeletedParentRows(changed, foreign_key);
				}
			}
		}
	}

private:
	void GatherChangedTables() {
		for (auto &storage : local_storage.GetStorages()) {
			auto entry = storage->table_entry.load();
			if (storage->is_dropped || !entry || storage->GetCollection().GetTotalRows() <= storage->deleted_rows) {
				continue;
			}
			GetChangedTable(storage->table_ref.get(), *entry).storage = storage;
		}
		transaction.ScanDeletes(nullptr, [&](DeleteInfo &info) {
			// the constraints of the newest version: e.g. a dropped child table no longer has a foreign key
			auto &table = IndexDataRemover::GetNewestStorage(info);
			auto &entry = IndexDataRemover::GetNewestEntry(info);
			GetChangedTable(table, entry).has_deletes = true;
		});
	}

	ChangedTable &GetChangedTable(DataTable &table, DuckTableEntry &entry) {
		auto &table_info = *table.GetDataTableInfo();
		for (auto &changed : changed_tables) {
			if (changed.table.get().GetDataTableInfo().get() == &table_info) {
				return changed;
			}
		}
		changed_tables.push_back(ChangedTable {table, entry, nullptr, false});
		return changed_tables.back();
	}

	//! The other table of the foreign key in the transaction's view: in the schema of the table
	optional_ptr<TableCatalogEntry> GetPartner(DuckTableEntry &entry, const ForeignKeyInfo &foreign_key) {
		CatalogTransaction catalog_transaction(entry.ParentCatalog().GetDatabase(), transaction.GetTransactionId(),
		                                       transaction.GetSnapshotView().visibility_bound);
		auto partner = entry.ParentSchema().GetEntry(catalog_transaction, CatalogType::TABLE_ENTRY, foreign_key.table);
		if (!partner || partner->type != CatalogType::TABLE_ENTRY) {
			// the other table was dropped
			return nullptr;
		}
		auto &partner_entry = partner->Cast<TableCatalogEntry>();
		partner_entry.GetStorage().BindIndexes(context);
		return &partner_entry;
	}

	shared_ptr<LocalTableStorage> GetLocalRows(DataTable &table) {
		auto storage = local_storage.GetStorage(table);
		if (!storage || storage->is_dropped || storage->GetCollection().GetTotalRows() <= storage->deleted_rows) {
			return nullptr;
		}
		return storage;
	}

	//! Every new child key is in the committed parent index and not deleted by the transaction, or among the live
	//! local parent rows. Rows that insert a key of a child row the transaction deleted keep that row's reference.
	void VerifyChildRows(ChangedTable &child, const ForeignKeyInfo &foreign_key) {
		auto &child_table = child.table.get();
		auto parent_entry_ptr = GetPartner(child.entry, foreign_key);
		if (!parent_entry_ptr) {
			return;
		}
		auto &parent_entry = *parent_entry_ptr;
		auto &parent_table = parent_entry.GetStorage();
		child_table.BindIndexes(context);
		auto parent_index = parent_table.GetDataTableInfo()->GetIndexes().FindForeignKeyIndex(
		    foreign_key.pk_keys, ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE);
		auto child_index = child_table.GetDataTableInfo()->GetIndexes().FindForeignKeyIndex(
		    foreign_key.fk_keys, ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE);
		if (!parent_index || !child_index) {
			return;
		}
		auto &db = child_table.db.GetDatabase();
		ArenaAllocator arena(Allocator::DefaultAllocator());
		unsafe_vector<ARTKey> keys;

		IndexKeyGenerator child_keys(db, child_index);
		KeyList deleted_child_keys;
		ScanDeletedRows(transaction, child_table, [&](DataChunk &rows, Vector &) {
			arena.Reset();
			child_keys.GenerateKeys(rows, arena, keys);
			deleted_child_keys.Add(keys, rows.size());
		});
		deleted_child_keys.Sort();

		auto deleted_parents = make_shared_ptr<IndexEntry>(parent_index->CreateEmptyCopy(), parent_index->GetIndexOid(),
		                                                   ConstraintCheckMode::DEFAULT);
		ScanDeletedRows(transaction, parent_table, [&](DataChunk &rows, Vector &row_ids) {
			deleted_parents->AppendToDeleteIndexes(rows, row_ids);
		});

		IndexKeyGenerator parent_keys(db, parent_index);
		KeyList local_parent_keys;
		if (auto parent_storage = GetLocalRows(parent_table)) {
			ScanLocalRows(transaction, *parent_storage, [&](DataChunk &rows) {
				arena.Reset();
				parent_keys.GenerateKeys(rows, arena, keys);
				local_parent_keys.Add(keys, rows.size());
			});
		}
		local_parent_keys.Sort();

		auto parent_types = GetPhysicalTypes(parent_entry);
		unordered_set<column_t> no_columns;
		ConflictInfo conflict_info(no_columns, false);
		ScanLocalRows(transaction, *child.storage, [&](DataChunk &rows) {
			arena.Reset();
			child_keys.GenerateKeys(rows, arena, keys);
			SelectionVector sel(rows.size());
			idx_t count = 0;
			for (idx_t i = 0; i < rows.size(); i++) {
				if (!keys[i].Empty() && !deleted_child_keys.Take(keys[i])) {
					sel.set_index(count++, i);
				}
			}
			if (count == 0) {
				return;
			}
			DataChunk new_children;
			new_children.InitializeEmpty(rows.GetTypes());
			new_children.Slice(rows, sel, count);
			DataChunk referenced;
			MapKeyColumns(new_children, foreign_key.fk_keys, foreign_key.pk_keys, parent_types, referenced);

			ConflictManager manager(VerifyExistenceType::APPEND_FK, count, &conflict_info);
			manager.SetMode(ConflictManagerMode::SCAN);
			parent_index->VerifyForeignKey(deleted_parents, referenced, manager, true);
			unsafe_vector<ARTKey> referenced_keys;
			parent_keys.GenerateKeys(referenced, arena, referenced_keys);
			for (idx_t i = 0; i < count; i++) {
				if (manager.HasConflicts() && manager.GetFirstValidity().RowIsValid(i)) {
					continue;
				}
				if (referenced_keys[i].Empty() || local_parent_keys.Contains(referenced_keys[i])) {
					continue;
				}
				transaction.MarkStatementConstraintError();
				throw ConstraintException(
				    parent_index->GetConstraintViolationMessage(VerifyExistenceType::APPEND_FK, i, referenced));
			}
		});
	}

	//! No committed child row the transaction keeps, and no live local child row, references a key of a parent row the
	//! transaction deleted, unless a live local parent row inserts the key again.
	void VerifyDeletedParentRows(ChangedTable &parent, const ForeignKeyInfo &foreign_key, idx_t first_delete = 0) {
		VerifyRemovedParentKeys(parent, foreign_key, [&](const std::function<void(DataChunk & rows)> &check) {
			ScanDeletedRows(
			    transaction, parent.table.get(), [&](DataChunk &rows, Vector &) { check(rows); }, first_delete);
		});
	}

	//! The parent rows that scan_rows gives (in table layout) are removed: no child may reference their keys
	void VerifyRemovedParentKeys(ChangedTable &parent, const ForeignKeyInfo &foreign_key,
	                             const std::function<void(const std::function<void(DataChunk &rows)> &)> &scan_rows) {
		auto &parent_table = parent.table.get();
		auto child_entry_ptr = GetPartner(parent.entry, foreign_key);
		if (!child_entry_ptr) {
			// the child table was dropped
			return;
		}
		auto &child_entry = *child_entry_ptr;
		auto &child_table = child_entry.GetStorage();
		auto child_index = child_table.GetDataTableInfo()->GetIndexes().FindForeignKeyIndex(
		    foreign_key.fk_keys, ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE);
		if (!child_index) {
			return;
		}
		auto &db = parent_table.db.GetDatabase();
		auto child_types = GetPhysicalTypes(child_entry);
		ArenaAllocator arena(Allocator::DefaultAllocator());
		unsafe_vector<ARTKey> keys;
		IndexKeyGenerator child_keys(db, child_index);

		KeyList local_parent_keys;
		if (auto parent_storage = GetLocalRows(parent_table)) {
			ScanLocalRows(transaction, *parent_storage, [&](DataChunk &rows) {
				DataChunk as_child;
				MapKeyColumns(rows, foreign_key.pk_keys, foreign_key.fk_keys, child_types, as_child);
				arena.Reset();
				child_keys.GenerateKeys(as_child, arena, keys);
				local_parent_keys.Add(keys, rows.size());
			});
		}
		local_parent_keys.Sort();

		KeyList local_child_keys;
		if (auto child_storage = GetLocalRows(child_table)) {
			ScanLocalRows(transaction, *child_storage, [&](DataChunk &rows) {
				arena.Reset();
				child_keys.GenerateKeys(rows, arena, keys);
				local_child_keys.Add(keys, rows.size());
			});
		}
		local_child_keys.Sort();

		unordered_set<row_t> deleted_children;
		ScanDeletedRows(transaction, child_table, [&](DataChunk &rows, Vector &row_ids) {
			auto ids = FlatVector::GetData<row_t>(row_ids);
			for (idx_t i = 0; i < rows.size(); i++) {
				deleted_children.insert(ids[i]);
			}
		});

		scan_rows([&](DataChunk &rows) {
			DataChunk referenced;
			MapKeyColumns(rows, foreign_key.pk_keys, foreign_key.fk_keys, child_types, referenced);
			arena.Reset();
			child_keys.GenerateKeys(referenced, arena, keys);
			SelectionVector sel(rows.size());
			idx_t count = 0;
			for (idx_t i = 0; i < rows.size(); i++) {
				if (keys[i].Empty() || local_parent_keys.Contains(keys[i])) {
					continue;
				}
				if (local_child_keys.Contains(keys[i])) {
					transaction.MarkStatementConstraintError();
					throw ConstraintException(
					    child_index->GetConstraintViolationMessage(VerifyExistenceType::DELETE_FK, i, referenced));
				}
				sel.set_index(count++, i);
			}
			if (count == 0 || !IsReferenced(*child_index, child_keys, sel, count, deleted_children)) {
				return;
			}
			// find the referenced key for the error
			for (idx_t i = 0; i < count; i++) {
				SelectionVector row_sel(1);
				row_sel.set_index(0, sel.get_index(i));
				if (IsReferenced(*child_index, child_keys, row_sel, 1, deleted_children)) {
					transaction.MarkStatementConstraintError();
					throw ConstraintException(child_index->GetConstraintViolationMessage(VerifyExistenceType::DELETE_FK,
					                                                                     sel.get_index(i), referenced));
				}
			}
			throw InternalException("a deleted parent key is referenced, but by none of the deleted rows");
		});
	}

	//! Whether a committed child row that the transaction does not delete has one of the selected keys of the last
	//! chunk
	static bool IsReferenced(IndexEntry &child_index, IndexKeyGenerator &child_keys, const SelectionVector &sel,
	                         idx_t count, const unordered_set<row_t> &deleted_children) {
		DataChunk key_values;
		key_values.InitializeEmpty(child_keys.key_chunk.GetTypes());
		key_values.Slice(child_keys.key_chunk, sel, count);
		unsafe_vector<row_t> row_ids;
		child_index.ScanCommittedRowIds(key_values, row_ids);
		for (auto row_id : row_ids) {
			if (deleted_children.find(row_id) == deleted_children.end()) {
				return true;
			}
		}
		return false;
	}

private:
	ClientContext &context;
	DuckTransaction &transaction;
	LocalStorage &local_storage;
	vector<ChangedTable> changed_tables;
};

} // namespace

void CommitForeignKeyCheck::Verify(ClientContext &context, DuckTransaction &transaction) {
	ForeignKeyVerifier verifier(context, transaction);
	verifier.Verify();
}

void CommitForeignKeyCheck::VerifyDeletes(ClientContext &context, DuckTransaction &transaction, DataTable &table,
                                          DuckTableEntry &entry, idx_t first_delete) {
	ForeignKeyVerifier verifier(context, transaction);
	verifier.VerifyDeletes(table, entry, first_delete);
}

void CommitForeignKeyCheck::VerifyRemovedLocalRows(ClientContext &context, DuckTransaction &transaction,
                                                   DataTable &table, DuckTableEntry &entry, DataChunk &rows) {
	ForeignKeyVerifier verifier(context, transaction);
	verifier.VerifyRemovedLocalRows(table, entry, rows);
}

} // namespace duckdb
