#pragma once

#include "duckdb.hpp"

namespace duckdb {

//! Registers functions whose results match other systems exactly: BigQuery, Spark, Kafka and ClickHouse.
void RegisterInteropFunctions(ExtensionLoader &loader);

} // namespace duckdb
