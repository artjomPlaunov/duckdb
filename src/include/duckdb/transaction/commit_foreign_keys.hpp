//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/commit_foreign_keys.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {
class ClientContext;
class DuckTransaction;

//! Verifies the foreign keys a commit changes against the committed state at the commit position: under the commit
//! lock, before the commit changes a shared index. Throws a ConstraintException with the statement-time text.
class CommitForeignKeyCheck {
public:
	static void Verify(ClientContext &context, DuckTransaction &transaction);
};

} // namespace duckdb
