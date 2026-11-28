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
#include "duckdb/common/sorting/sort.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/storage/table/scan_state.hpp"

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

SortedIndexRows::SortedIndexRows(shared_ptr<IndexEntry> entry_p) : entry(std::move(entry_p)) {
}

SortedIndexRows::SortedIndexRows(SortedIndexRows &&other) noexcept = default;
SortedIndexRows &SortedIndexRows::operator=(SortedIndexRows &&other) noexcept = default;
SortedIndexRows::~SortedIndexRows() = default;

namespace {

//! A row position of the sorted rows: the row group, and the position among the live rows of the row group
uint64_t EncodeRowPosition(idx_t row_group, idx_t position) {
	D_ASSERT(position <= NumericLimits<uint32_t>::Maximum());
	return (uint64_t(row_group) << 32) | uint64_t(position);
}

//! The sort of one index's live rows
struct IndexSort {
	SortedIndexRows rows;
	unique_ptr<Sort> sort;
	unique_ptr<GlobalSinkState> sink;
	unique_ptr<GlobalSourceState> source;
	explicit IndexSort(shared_ptr<IndexEntry> entry) : rows(std::move(entry)) {
	}
};

//! What the tasks that sort the live rows share
struct CommitSortState {
	CommitSortState(ClientContext &context, DuckTransaction &transaction, RowGroupCollection &collection)
	    : context(context), transaction(transaction), collection(collection) {
	}

	ClientContext &context;
	DuckTransaction &transaction;
	RowGroupCollection &collection;
	vector<reference<SegmentNode<RowGroup>>> row_groups;
	//! The live rows of each row group
	vector<idx_t> live_rows;
	//! The indexed columns of the table, sorted
	vector<StorageIndex> column_ids;
	vector<unique_ptr<IndexSort>> sorts;
	atomic<idx_t> next_row_group {0};
};

//! Scans row groups of the live local rows and sinks them into the sort of every index
class CommitSortSinkTask : public BaseExecutorTask {
public:
	CommitSortSinkTask(TaskExecutor &executor, CommitSortState &state) : BaseExecutorTask(executor), state(state) {
	}

	void ExecuteTask() override {
		auto &context = state.context;
		ThreadContext thread(context);
		ExecutionContext execution(context, thread, nullptr);
		InterruptState interrupt;
		vector<unique_ptr<LocalSinkState>> local_sinks;
		for (auto &index_sort : state.sorts) {
			local_sinks.push_back(index_sort->sort->GetLocalSinkState(execution));
		}

		auto &types = state.collection.GetTypes();
		DataChunk scan_chunk;
		vector<LogicalType> scan_types;
		for (auto &column_id : state.column_ids) {
			scan_types.push_back(types[column_id.GetPrimaryIndex()]);
		}
		scan_chunk.Initialize(Allocator::Get(context), scan_types);
		vector<LogicalType> sink_types = types;
		sink_types.push_back(LogicalType::UBIGINT);
		DataChunk sink_chunk;
		sink_chunk.InitializeEmpty(sink_types);
		Vector row_positions(LogicalType::UBIGINT);

		while (true) {
			auto row_group_idx = state.next_row_group.fetch_add(1);
			if (row_group_idx >= state.row_groups.size()) {
				break;
			}
			auto &node = state.row_groups[row_group_idx].get();
			auto row_start = node.GetRowStart();
			idx_t row_count = node.GetNode().count;
			idx_t live_rows = 0;
			if (row_count > 0) {
				TableScanState scan_state;
				scan_state.Initialize(state.column_ids, nullptr);
				state.collection.InitializeScanWithOffset(QueryContext(), scan_state.local_state, state.column_ids,
				                                          row_start, row_start + row_count);
				while (true) {
					scan_chunk.Reset();
					scan_state.local_state.Scan(state.transaction, scan_chunk);
					if (scan_chunk.size() == 0) {
						break;
					}
					auto count = scan_chunk.size();
					for (idx_t c = 0; c < types.size(); c++) {
						sink_chunk.data[c].Reference(Value(types[c]), count_t(count));
					}
					for (idx_t i = 0; i < state.column_ids.size(); i++) {
						sink_chunk.data[state.column_ids[i].GetPrimaryIndex()].Reference(scan_chunk.data[i]);
					}
					auto writer = FlatVector::Writer<uint64_t>(row_positions, count);
					for (idx_t i = 0; i < count; i++) {
						writer.WriteValue(EncodeRowPosition(row_group_idx, live_rows + i));
					}
					sink_chunk.data.back().Reference(row_positions);
					sink_chunk.CheckCardinality(count);
					for (idx_t s = 0; s < state.sorts.size(); s++) {
						OperatorSinkInput sink_input {*state.sorts[s]->sink, *local_sinks[s], interrupt};
						state.sorts[s]->sort->Sink(execution, sink_chunk, sink_input);
					}
					live_rows += count;
				}
			}
			state.live_rows[row_group_idx] = live_rows;
		}

		for (idx_t s = 0; s < state.sorts.size(); s++) {
			OperatorSinkCombineInput combine_input {*state.sorts[s]->sink, *local_sinks[s], interrupt};
			state.sorts[s]->sort->Combine(execution, combine_input);
		}
	}

	string TaskType() const override {
		return "CommitSortSinkTask";
	}

private:
	CommitSortState &state;
};

//! Materializes a part of the sorted rows of one index
class CommitSortMaterializeTask : public BaseExecutorTask {
public:
	CommitSortMaterializeTask(TaskExecutor &executor, ClientContext &context, IndexSort &index_sort)
	    : BaseExecutorTask(executor), context(context), index_sort(index_sort) {
	}

	void ExecuteTask() override {
		ThreadContext thread(context);
		ExecutionContext execution(context, thread, nullptr);
		InterruptState interrupt;
		auto local_source = index_sort.sort->GetLocalSourceState(execution, *index_sort.source);
		OperatorSourceInput source_input {*index_sort.source, *local_source, interrupt};
		index_sort.sort->MaterializeColumnData(execution, source_input);
	}

	string TaskType() const override {
		return "CommitSortMaterializeTask";
	}

private:
	ClientContext &context;
	IndexSort &index_sort;
};

} // namespace

void CommitIndexChanges::PrepareSorted(ClientContext &context, DuckTransaction &transaction,
                                       LocalTableStorage &storage) {
	auto &collection = storage.GetCollection();
	auto live_rows = collection.GetTotalRows() - storage.deleted_rows;
	if (storage.index_append_mode == IndexAppendMode::IGNORE_DUPLICATES ||
	    live_rows < Settings::Get<DebugCommitIndexSortThresholdSetting>(context)) {
		return;
	}
	auto &table_info = *storage.table_ref.get().GetDataTableInfo();
	try {
		CommitSortState state(context, transaction, collection);
		for (auto entry : table_info.GetIndexes().BuiltIndexEntries()) {
			if (entry->GetBindState() != IndexBindState::BOUND || entry->GetIndexType() != ART::TYPE_NAME) {
				continue;
			}
			auto index_sort = make_uniq<IndexSort>(entry);
			auto handle = entry->GetReadHandle<BoundIndex>();
			vector<BoundOrderByNode> orders;
			for (auto &expression : handle->CopyBoundExpressions()) {
				orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, std::move(expression));
			}
			auto &column_ids = index_sort->rows.column_ids;
			column_ids = handle->GetColumnIds();
			std::sort(column_ids.begin(), column_ids.end());
			column_ids.erase(std::unique(column_ids.begin(), column_ids.end()), column_ids.end());
			vector<idx_t> projection_map(column_ids.begin(), column_ids.end());
			projection_map.push_back(collection.GetTypes().size());
			vector<LogicalType> sink_types = collection.GetTypes();
			sink_types.push_back(LogicalType::UBIGINT);
			index_sort->sort = make_uniq<Sort>(context, orders, sink_types, std::move(projection_map), true);
			index_sort->sink = index_sort->sort->GetGlobalSinkState(context);
			state.sorts.push_back(std::move(index_sort));
		}
		if (state.sorts.empty()) {
			return;
		}
		for (auto &column_id : table_info.GetIndexes().GetIndexedColumns()) {
			state.column_ids.emplace_back(column_id);
		}
		std::sort(state.column_ids.begin(), state.column_ids.end());
		auto row_groups = collection.GetRowGroups();
		for (auto &node : row_groups->SegmentNodes()) {
			state.row_groups.push_back(node);
		}
		state.live_rows.resize(state.row_groups.size(), 0);

		auto &scheduler = TaskScheduler::GetScheduler(context);
		auto thread_count = MinValue<idx_t>(NumericCast<idx_t>(scheduler.NumberOfThreads()), state.row_groups.size());
		{
			TaskExecutor executor(context);
			for (idx_t i = 0; i < MaxValue<idx_t>(thread_count, 1); i++) {
				executor.ScheduleTask(make_uniq<CommitSortSinkTask>(executor, state));
			}
			executor.WorkOnTasks();
		}
		InterruptState interrupt;
		for (auto &index_sort : state.sorts) {
			OperatorSinkFinalizeInput finalize_input {*index_sort->sink, interrupt};
			index_sort->sort->Finalize(context, finalize_input);
			index_sort->source = index_sort->sort->GetGlobalSourceState(context, *index_sort->sink);
		}
		{
			TaskExecutor executor(context);
			for (auto &index_sort : state.sorts) {
				for (idx_t i = 0; i < MaxValue<idx_t>(thread_count, 1); i++) {
					executor.ScheduleTask(make_uniq<CommitSortMaterializeTask>(executor, context, *index_sort));
				}
			}
			executor.WorkOnTasks();
		}

		idx_t offset = 0;
		row_group_offsets.clear();
		for (auto &count : state.live_rows) {
			row_group_offsets.push_back(offset);
			offset += count;
		}
		if (offset != live_rows) {
			throw InternalException("Commit index sort scanned %llu live rows, expected %llu", offset, live_rows);
		}
		ThreadContext thread(context);
		ExecutionContext execution(context, thread, nullptr);
		for (auto &index_sort : state.sorts) {
			auto local_source = index_sort->sort->GetLocalSourceState(execution, *index_sort->source);
			OperatorSourceInput source_input {*index_sort->source, *local_source, interrupt};
			index_sort->rows.rows = index_sort->sort->GetColumnData(source_input);
			sorted.push_back(std::move(index_sort->rows));
		}
	} catch (std::exception &ex) {
		ErrorData error(ex);
		if (Exception::InvalidatesDatabase(error.Type()) || error.Type() == ExceptionType::INTERNAL) {
			throw;
		}
		// sorting only changes the order of the inserts: insert in scan order instead
		sorted.clear();
	}
}

void CommitIndexChanges::ReleaseSortedRows(const IndexEntry &entry) {
	for (auto &rows : sorted) {
		if (rows.entry.get() == &entry) {
			rows.rows.reset();
		}
	}
}

idx_t CommitIndexChanges::GetPosition(uint64_t row_position) const {
	return row_group_offsets[row_position >> 32] + (row_position & NumericLimits<uint32_t>::Maximum());
}

optional_ptr<const SortedIndexRows> CommitIndexChanges::GetSortedRows(const IndexEntry &entry) const {
	for (auto &rows : sorted) {
		if (rows.entry.get() == &entry) {
			return &rows;
		}
	}
	return nullptr;
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
