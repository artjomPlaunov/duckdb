//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/prefix.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

//! Prefix holds the layout constants of a prefix node.
//! The prefix contains up to the ART's prefix size bytes and an additional byte for the count.
//! It also contains a child NodePtr.
class Prefix {
public:
	static constexpr NType PREFIX = NType::PREFIX;

	static constexpr uint8_t ROW_ID_SIZE = sizeof(row_t);
	static constexpr uint8_t ROW_ID_COUNT = ROW_ID_SIZE - 1;
	static constexpr uint8_t DEPRECATED_COUNT = 15;
	// The child pointer and the in_memory boolean.
	static constexpr uint8_t METADATA_SIZE = sizeof(NodePtr) + 1;

public:
	Prefix() = delete;
};
} // namespace duckdb
