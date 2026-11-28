//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/commit_foreign_keys.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {
class ClientContext;
class DataTable;
class DuckTableEntry;
class DuckTransaction;

//! Verifies the foreign keys a commit changes against the committed state at the commit position: under the commit
//! lock, before the commit changes a shared index. Throws a ConstraintException with the statement-time text.
class CommitForeignKeyCheck {
public:
	static void Verify(ClientContext &context, DuckTransaction &transaction);
	//! Verifies that no child references a key of a parent row of the table deleted from its first_delete-th delete
	//! entry on, unless the transaction inserted the key again
	static void VerifyDeletes(ClientContext &context, DuckTransaction &transaction, DataTable &table,
	                          DuckTableEntry &entry, idx_t first_delete);
};

} // namespace duckdb
