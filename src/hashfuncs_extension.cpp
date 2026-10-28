#include "hashfuncs_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "xxhash.h"
#include "rapidhash.h"
#include "MurmurHash3.h"
#include "query_farm_telemetry.hpp"
namespace duckdb {

static constexpr const char *HASHFUNCS_VERSION = "2026102701";

namespace {

// Each algorithm hashes a byte range with a seed. The unseeded SQL functions pass seed 0, which every library
// defines to give the same result as its unseeded entry point.
struct XXH32Op {
	using result_t = uint32_t;
	using seed_t = uint32_t;
	static result_t Hash(const void *data, idx_t size, seed_t seed) {
		return XXH32(data, size, seed);
	}
};

struct XXH64Op {
	using result_t = uint64_t;
	using seed_t = uint64_t;
	static result_t Hash(const void *data, idx_t size, seed_t seed) {
		return XXH64(data, size, seed);
	}
};

struct XXH3_64Op {
	using result_t = uint64_t;
	using seed_t = uint64_t;
	static result_t Hash(const void *data, idx_t size, seed_t seed) {
		return XXH3_64bits_withSeed(data, size, seed);
	}
};

struct XXH3_128Op {
	using result_t = uhugeint_t;
	using seed_t = uint64_t;
	static result_t Hash(const void *data, idx_t size, seed_t seed) {
		auto hash = XXH3_128bits_withSeed(data, size, seed);
		// The historical xxh3_128() layout is kept for stored-key compatibility: uhugeint_t(upper, lower) puts low64
		// in the upper half. xxh3_128_hex() returns xxHash's canonical byte order instead.
		return uhugeint_t(hash.low64, hash.high64);
	}
};

struct RapidhashOp {
	using result_t = uint64_t;
	using seed_t = uint64_t;
	static result_t Hash(const void *data, idx_t size, seed_t seed) {
		return rapidhash_withSeed(data, size, seed);
	}
};

struct RapidhashMicroOp {
	using result_t = uint64_t;
	using seed_t = uint64_t;
	static result_t Hash(const void *data, idx_t size, seed_t seed) {
		return rapidhashMicro_withSeed(data, size, seed);
	}
};

struct RapidhashNanoOp {
	using result_t = uint64_t;
	using seed_t = uint64_t;
	static result_t Hash(const void *data, idx_t size, seed_t seed) {
		return rapidhashNano_withSeed(data, size, seed);
	}
};

struct MurmurHash3_32Op {
	using result_t = uint32_t;
	using seed_t = uint32_t;
	static result_t Hash(const void *data, idx_t size, seed_t seed) {
		result_t result;
		MurmurHash3_x86_32(data, static_cast<int>(size), seed, &result);
		return result;
	}
};

// The 128-bit MurmurHash3 variants write their two 64-bit words straight into the uhugeint_t, so on little-endian
// platforms the first word is the lower half.
struct MurmurHash3_128Op {
	using result_t = uhugeint_t;
	using seed_t = uint32_t;
	static result_t Hash(const void *data, idx_t size, seed_t seed) {
		result_t result;
		MurmurHash3_x86_128(data, static_cast<int>(size), seed, &result);
		return result;
	}
};

struct MurmurHash3_X64_128Op {
	using result_t = uhugeint_t;
	using seed_t = uint32_t;
	static result_t Hash(const void *data, idx_t size, seed_t seed) {
		result_t result;
		MurmurHash3_x64_128(data, static_cast<int>(size), seed, &result);
		return result;
	}
};

// Fixed-width values hash their in-memory bytes, so the hash depends on the value's width: 42::INTEGER and
// 42::BIGINT hash differently.
template <class T>
struct ValueBytes {
	using input_t = T;
	template <class OP>
	static typename OP::result_t Hash(const T &value, typename OP::seed_t seed) {
		return OP::Hash(&value, sizeof(T), seed);
	}
};

// VARCHAR and BLOB values hash their contents.
struct StringBytes {
	using input_t = string_t;
	template <class OP>
	static typename OP::result_t Hash(const string_t &value, typename OP::seed_t seed) {
		return OP::Hash(value.GetData(), value.GetSize(), seed);
	}
};

// UUIDs hash their 16 RFC 4122 bytes, matching a hash of the UUID's binary form in other systems.
struct UUIDBytes {
	using input_t = hugeint_t;
	template <class OP>
	static typename OP::result_t Hash(const hugeint_t &value, typename OP::seed_t seed) {
		data_t bytes[16];
		UUID::ToBlob(value, bytes);
		return OP::Hash(bytes, sizeof(bytes), seed);
	}
};

// Hashes each row straight from the input vector's memory. DuckDB's Unary/BinaryExecutor pass every value by copy,
// and hashing the bytes of a just-copied short string or integer measured about 20% slower.
template <class OP, class KEY, bool SEEDED>
void HashFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	using input_t = typename KEY::input_t;
	using seed_t = typename OP::seed_t;
	using result_t = typename OP::result_t;

	// Constant arguments are hashed once into a constant result.
	bool all_constant = true;
	for (auto &arg : args.data) {
		all_constant = all_constant && arg.GetVectorType() == VectorType::CONSTANT_VECTOR;
	}
	const idx_t count = all_constant ? 1 : args.size();
	result.SetVectorType(all_constant ? VectorType::CONSTANT_VECTOR : VectorType::FLAT_VECTOR);
	auto results = all_constant ? ConstantVector::GetData<result_t>(result) : FlatVector::GetData<result_t>(result);

	UnifiedVectorFormat input_data;
	args.data[0].ToUnifiedFormat(count, input_data);
	auto inputs = UnifiedVectorFormat::GetData<input_t>(input_data);
	UnifiedVectorFormat seed_data;
	if constexpr (SEEDED) {
		args.data[1].ToUnifiedFormat(count, seed_data);
	}

	for (idx_t i = 0; i < count; i++) {
		const auto input_idx = input_data.sel->get_index(i);
		bool valid = input_data.validity.RowIsValid(input_idx);
		seed_t seed = 0;
		if constexpr (SEEDED) {
			const auto seed_idx = seed_data.sel->get_index(i);
			valid = valid && seed_data.validity.RowIsValid(seed_idx);
			if (valid) {
				seed = UnifiedVectorFormat::GetData<seed_t>(seed_data)[seed_idx];
			}
		}
		if (!valid) {
			if (all_constant) {
				ConstantVector::SetNull(result, true);
			} else {
				FlatVector::SetNull(result, i, true);
			}
			continue;
		}
		results[i] = KEY::template Hash<OP>(inputs[input_idx], seed);
	}
}

template <class OP, class KEY>
scalar_function_t HashImplementation(bool seeded) {
	if (seeded) {
		return HashFunction<OP, KEY, true>;
	}
	return HashFunction<OP, KEY, false>;
}

// Picks the implementation for an input type already accepted by HashInputType().
template <class OP>
scalar_function_t GetHashFunction(const LogicalType &type, bool seeded) {
	if (type.id() == LogicalTypeId::UUID) {
		return HashImplementation<OP, UUIDBytes>(seeded);
	}
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		return HashImplementation<OP, ValueBytes<bool>>(seeded);
	case PhysicalType::INT8:
		return HashImplementation<OP, ValueBytes<int8_t>>(seeded);
	case PhysicalType::INT16:
		return HashImplementation<OP, ValueBytes<int16_t>>(seeded);
	case PhysicalType::INT32:
		return HashImplementation<OP, ValueBytes<int32_t>>(seeded);
	case PhysicalType::INT64:
		return HashImplementation<OP, ValueBytes<int64_t>>(seeded);
	case PhysicalType::INT128:
		return HashImplementation<OP, ValueBytes<hugeint_t>>(seeded);
	case PhysicalType::UINT8:
		return HashImplementation<OP, ValueBytes<uint8_t>>(seeded);
	case PhysicalType::UINT16:
		return HashImplementation<OP, ValueBytes<uint16_t>>(seeded);
	case PhysicalType::UINT32:
		return HashImplementation<OP, ValueBytes<uint32_t>>(seeded);
	case PhysicalType::UINT64:
		return HashImplementation<OP, ValueBytes<uint64_t>>(seeded);
	case PhysicalType::UINT128:
		return HashImplementation<OP, ValueBytes<uhugeint_t>>(seeded);
	case PhysicalType::FLOAT:
		return HashImplementation<OP, ValueBytes<float>>(seeded);
	case PhysicalType::DOUBLE:
		return HashImplementation<OP, ValueBytes<double>>(seeded);
	case PhysicalType::INTERVAL:
		return HashImplementation<OP, ValueBytes<interval_t>>(seeded);
	case PhysicalType::VARCHAR:
		return HashImplementation<OP, StringBytes>(seeded);
	default:
		throw InternalException("hashfuncs: no hash implementation for type %s", type.ToString());
	}
}

// Returns the type a value is hashed as, rejecting types without a stable byte representation while the query is
// bound rather than when it runs.
LogicalType HashInputType(const string &function_name, const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_NS:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::INTERVAL:
	case LogicalTypeId::UUID:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
		return type;
	// A NULL literal hashes to NULL. ENUM values hash their label, so they match a hash of the same VARCHAR.
	case LogicalTypeId::SQLNULL:
	case LogicalTypeId::ENUM:
		return LogicalType::VARCHAR;
	case LogicalTypeId::UNKNOWN:
		throw ParameterNotResolvedException();
	default:
		throw BinderException(
		    "%s cannot hash values of type %s. Supported types are BOOLEAN, integers, floating-point, "
		    "DECIMAL, DATE, TIME, TIMESTAMP, INTERVAL, UUID, ENUM, VARCHAR and BLOB",
		    function_name, type.ToString());
	}
}

template <class OP>
unique_ptr<FunctionData> HashBind(ClientContext &context, ScalarFunction &bound_function,
                                  vector<unique_ptr<Expression>> &arguments) {
	auto input_type = HashInputType(bound_function.name, arguments[0]->return_type);
	bound_function.arguments[0] = input_type;
	bound_function.function = GetHashFunction<OP>(input_type, arguments.size() == 2);
	return nullptr;
}

// Use xxHash's canonical serialization: high64 then low64, each big-endian.
// The numeric xxh3_128() layout is intentionally unchanged for stored-key
// compatibility. Its historical UHUGEINT representation has swapped halves.
string_t CanonicalXXH128Hex(Vector &result, XXH128_hash_t hash) {
	XXH128_canonical_t canonical;
	XXH128_canonicalFromHash(&canonical, hash);
	constexpr char digits[] = "0123456789abcdef";
	char hex[32];
	for (idx_t i = 0; i < sizeof(canonical.digest); i++) {
		hex[2 * i] = digits[canonical.digest[i] >> 4];
		hex[2 * i + 1] = digits[canonical.digest[i] & 0x0f];
	}
	return StringVector::AddString(result, hex, sizeof(hex));
}

void XXH3_128HexFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t>(
	    args.data[0], result, args.size(),
	    [&](const string_t &value) {
		    return CanonicalXXH128Hex(result, XXH3_128bits(value.GetData(), value.GetSize()));
	    },
	    FunctionErrors::CANNOT_ERROR);
}

void XXH3_128HexFunctionWithSeed(DataChunk &args, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<string_t, uint64_t, string_t>(
	    args.data[0], args.data[1], result, args.size(), [&](const string_t &value, uint64_t seed) {
		    return CanonicalXXH128Hex(result, XXH3_128bits_withSeed(value.GetData(), value.GetSize(), seed));
	    });
}

// Registers an algorithm as an unseeded and a seeded overload that accept any supported input type.
template <class OP>
void RegisterHashFunction(ExtensionLoader &loader, const string &name, const LogicalType &result_type,
                          const LogicalType &seed_type, const string &description, const string &seeded_description) {
	ScalarFunctionSet set(name);
	set.AddFunction(ScalarFunction({LogicalType::ANY}, result_type, nullptr, HashBind<OP>));
	set.AddFunction(ScalarFunction({LogicalType::ANY, seed_type}, result_type, nullptr, HashBind<OP>));
	CreateScalarFunctionInfo info(set);
	info.descriptions.push_back({/* parameter_types */ {LogicalType::ANY},
	                             /* parameter_names */ {"value"},
	                             /* description */ description,
	                             /* examples */ {name + "('hello')"},
	                             /* categories */ {"hash"}});
	info.descriptions.push_back({/* parameter_types */ {LogicalType::ANY, seed_type},
	                             /* parameter_names */ {"value", "seed"},
	                             /* description */ seeded_description,
	                             /* examples */ {name + "('hello', 42)"},
	                             /* categories */ {"hash"}});
	loader.RegisterFunction(info);
}

} // namespace

static void LoadInternal(ExtensionLoader &loader) {
	RegisterHashFunction<XXH32Op>(loader, "xxh32", LogicalType::UINTEGER, LogicalType::UINTEGER,
	                              "Computes a 32-bit xxHash (XXH32) non-cryptographic hash of the input",
	                              "Computes a 32-bit xxHash (XXH32) non-cryptographic hash of the input with a seed");

	RegisterHashFunction<XXH64Op>(loader, "xxh64", LogicalType::UBIGINT, LogicalType::UBIGINT,
	                              "Computes a 64-bit xxHash (XXH64) non-cryptographic hash of the input",
	                              "Computes a 64-bit xxHash (XXH64) non-cryptographic hash of the input with a seed");

	RegisterHashFunction<XXH3_64Op>(
	    loader, "xxh3_64", LogicalType::UBIGINT, LogicalType::UBIGINT,
	    "Computes a 64-bit xxHash3 (XXH3_64) non-cryptographic hash of the input. Faster than XXH64 for short inputs",
	    "Computes a 64-bit xxHash3 (XXH3_64) non-cryptographic hash of the input with a seed");

	RegisterHashFunction<XXH3_128Op>(
	    loader, "xxh3_128", LogicalType::UHUGEINT, LogicalType::UBIGINT,
	    "Computes a 128-bit xxHash3 (XXH3_128) non-cryptographic hash of the input",
	    "Computes a 128-bit xxHash3 (XXH3_128) non-cryptographic hash of the input with a seed");

	// XXH3_128_hex - 128-bit xxHash3 as canonical hex string
	const vector<LogicalType> hex_input_types {LogicalType::VARCHAR, LogicalType::BLOB};
	ScalarFunctionSet xxh3_128_hex_set("xxh3_128_hex");
	for (auto &input_type : hex_input_types) {
		xxh3_128_hex_set.AddFunction(ScalarFunction({input_type}, LogicalType::VARCHAR, XXH3_128HexFunction));
		xxh3_128_hex_set.AddFunction(
		    ScalarFunction({input_type, LogicalType::UBIGINT}, LogicalType::VARCHAR, XXH3_128HexFunctionWithSeed));
	}
	CreateScalarFunctionInfo xxh3_128_hex_info(xxh3_128_hex_set);
	for (auto &input_type : hex_input_types) {
		const string example_value = input_type == LogicalType::BLOB ? "'hello'::BLOB" : "'hello'";
		xxh3_128_hex_info.descriptions.push_back(
		    {/* parameter_types */ {input_type},
		     /* parameter_names */ {"value"},
		     /* description */
		     "Computes a 128-bit xxHash3 hash and returns it as a 32-character lowercase hex string "
		     "in canonical byte order (high64 || low64), matching Python xxhash.xxh3_128().hexdigest()",
		     /* examples */ {"xxh3_128_hex(" + example_value + ")"},
		     /* categories */ {"hash"}});
		xxh3_128_hex_info.descriptions.push_back(
		    {/* parameter_types */ {input_type, LogicalType::UBIGINT},
		     /* parameter_names */ {"value", "seed"},
		     /* description */
		     "Computes a 128-bit xxHash3 hash with a seed and returns it as a 32-character lowercase "
		     "hex string in canonical byte order",
		     /* examples */ {"xxh3_128_hex(" + example_value + ", 42)"},
		     /* categories */ {"hash"}});
	}
	loader.RegisterFunction(xxh3_128_hex_info);

	RegisterHashFunction<RapidhashOp>(
	    loader, "rapidhash", LogicalType::UBIGINT, LogicalType::UBIGINT,
	    "Computes a 64-bit RapidHash non-cryptographic hash of the input. Very fast for all input sizes",
	    "Computes a 64-bit RapidHash non-cryptographic hash of the input with a seed");

	RegisterHashFunction<RapidhashMicroOp>(
	    loader, "rapidhash_micro", LogicalType::UBIGINT, LogicalType::UBIGINT,
	    "Computes a 64-bit RapidHash Micro non-cryptographic hash of the input. Smaller code than RapidHash, with "
	    "identical results for inputs up to 80 bytes",
	    "Computes a 64-bit RapidHash Micro non-cryptographic hash of the input with a seed");

	RegisterHashFunction<RapidhashNanoOp>(
	    loader, "rapidhash_nano", LogicalType::UBIGINT, LogicalType::UBIGINT,
	    "Computes a 64-bit RapidHash Nano non-cryptographic hash of the input. Smallest RapidHash variant, with "
	    "identical results for inputs up to 48 bytes",
	    "Computes a 64-bit RapidHash Nano non-cryptographic hash of the input with a seed");

	RegisterHashFunction<MurmurHash3_32Op>(
	    loader, "murmurhash3_32", LogicalType::UINTEGER, LogicalType::UINTEGER,
	    "Computes a 32-bit MurmurHash3 non-cryptographic hash of the input",
	    "Computes a 32-bit MurmurHash3 non-cryptographic hash of the input with a seed");

	RegisterHashFunction<MurmurHash3_128Op>(
	    loader, "murmurhash3_128", LogicalType::UHUGEINT, LogicalType::UINTEGER,
	    "Computes a 128-bit MurmurHash3 (x86 variant) non-cryptographic hash of the input",
	    "Computes a 128-bit MurmurHash3 (x86 variant) non-cryptographic hash of the input with a seed");

	RegisterHashFunction<MurmurHash3_X64_128Op>(
	    loader, "murmurhash3_x64_128", LogicalType::UHUGEINT, LogicalType::UINTEGER,
	    "Computes a 128-bit MurmurHash3 (x64 variant) non-cryptographic hash of the input. Optimized for 64-bit "
	    "platforms",
	    "Computes a 128-bit MurmurHash3 (x64 variant) non-cryptographic hash of the input with a seed");

	QueryFarmSendTelemetry(loader, "hashfuncs", HASHFUNCS_VERSION);
}

void HashfuncsExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string HashfuncsExtension::Name() {
	return "hashfuncs";
}

std::string HashfuncsExtension::Version() const {
	return HASHFUNCS_VERSION;
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(hashfuncs, loader) {
	duckdb::LoadInternal(loader);
}
}
