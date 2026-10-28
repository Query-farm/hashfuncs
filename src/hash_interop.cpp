#include "hash_interop.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "xxhash.h"
#include "MurmurHash3.h"
#include "duckdb/common/types/uuid.hpp"
#include "vendored_hashes.hpp"

#include <cmath>
#include <cstring>

namespace duckdb {

namespace {

void SetResultNull(Vector &result, bool constant, idx_t row) {
	if (constant) {
		ConstantVector::SetNull(result, true);
	} else {
		FlatVector::SetNull(result, row, true);
	}
}

// Hashes VARCHAR or BLOB rows with HASHER::Hash(data, size), straight from the input vector's memory.
template <class HASHER>
void StringHashFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	using result_t = typename HASHER::result_t;
	auto &input = args.data[0];
	const bool constant = input.GetVectorType() == VectorType::CONSTANT_VECTOR;
	const idx_t count = constant ? 1 : args.size();
	result.SetVectorType(constant ? VectorType::CONSTANT_VECTOR : VectorType::FLAT_VECTOR);
	auto results = constant ? ConstantVector::GetData<result_t>(result) : FlatVector::GetDataMutable<result_t>(result);

	UnifiedVectorFormat format;
	input.ToUnifiedFormat(format);
	auto values = UnifiedVectorFormat::GetData<string_t>(format);
	for (idx_t i = 0; i < count; i++) {
		const auto idx = format.sel->get_index(i);
		if (!format.validity.RowIsValid(idx)) {
			SetResultNull(result, constant, i);
			continue;
		}
		results[i] = HASHER::Hash(values[idx].GetData(), values[idx].GetSize());
	}
}

// BigQuery FARM_FINGERPRINT: FarmHash Fingerprint64 of the UTF-8 or binary bytes, as a signed INT64.
struct FarmFingerprintHasher {
	using result_t = int64_t;
	static result_t Hash(const char *data, idx_t size) {
		return static_cast<int64_t>(hashfuncs::FarmFingerprint64(data, size));
	}
};

// ClickHouse cityHash64 of a String: CityHash64 from CityHash v1.0.2.
struct ClickHouseCityHash64Hasher {
	using result_t = uint64_t;
	static result_t Hash(const char *data, idx_t size) {
		return hashfuncs::CityHash64V102(data, size);
	}
};

// Kafka's Utils.murmur2 (MurmurHash2 with Kafka's seed), which the default partitioner applies to serialized keys.
int32_t KafkaMurmur2(const char *data, idx_t size) {
	auto bytes = reinterpret_cast<const uint8_t *>(data);
	const uint32_t m = 0x5bd1e995;
	const int r = 24;
	uint32_t h = 0x9747b28c ^ static_cast<uint32_t>(size);
	const idx_t blocks = size / 4;
	for (idx_t i = 0; i < blocks; i++) {
		const idx_t offset = i * 4;
		uint32_t k = uint32_t(bytes[offset]) | uint32_t(bytes[offset + 1]) << 8 | uint32_t(bytes[offset + 2]) << 16 |
		             uint32_t(bytes[offset + 3]) << 24;
		k *= m;
		k ^= k >> r;
		k *= m;
		h *= m;
		h ^= k;
	}
	const idx_t tail = size & ~idx_t(3);
	switch (size % 4) {
	case 3:
		h ^= uint32_t(bytes[tail + 2]) << 16;
		[[fallthrough]];
	case 2:
		h ^= uint32_t(bytes[tail + 1]) << 8;
		[[fallthrough]];
	case 1:
		h ^= uint32_t(bytes[tail]);
		h *= m;
	}
	h ^= h >> 13;
	h *= m;
	h ^= h >> 15;
	return static_cast<int32_t>(h);
}

struct KafkaMurmur2Hasher {
	using result_t = int32_t;
	static result_t Hash(const char *data, idx_t size) {
		return KafkaMurmur2(data, size);
	}
};

// Kafka's default partitioner for a keyed record: toPositive(murmur2(key)) % partitions. A NULL key is not hashed by
// Kafka (it uses sticky partitioning), so it gives NULL.
void KafkaPartitionFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &keys = args.data[0];
	auto &partitions = args.data[1];
	const bool constant = keys.GetVectorType() == VectorType::CONSTANT_VECTOR &&
	                      partitions.GetVectorType() == VectorType::CONSTANT_VECTOR;
	const idx_t count = constant ? 1 : args.size();
	result.SetVectorType(constant ? VectorType::CONSTANT_VECTOR : VectorType::FLAT_VECTOR);
	auto results = constant ? ConstantVector::GetData<int32_t>(result) : FlatVector::GetDataMutable<int32_t>(result);

	UnifiedVectorFormat key_format;
	UnifiedVectorFormat partition_format;
	keys.ToUnifiedFormat(key_format);
	partitions.ToUnifiedFormat(partition_format);
	auto key_values = UnifiedVectorFormat::GetData<string_t>(key_format);
	auto partition_values = UnifiedVectorFormat::GetData<int64_t>(partition_format);
	for (idx_t i = 0; i < count; i++) {
		const auto key_idx = key_format.sel->get_index(i);
		const auto partition_idx = partition_format.sel->get_index(i);
		if (!key_format.validity.RowIsValid(key_idx) || !partition_format.validity.RowIsValid(partition_idx)) {
			SetResultNull(result, constant, i);
			continue;
		}
		const auto partition_count = partition_values[partition_idx];
		if (partition_count <= 0 || partition_count > NumericLimits<int32_t>::Maximum()) {
			throw InvalidInputException(
			    "kafka_partition: the number of partitions must be between 1 and 2147483647, got %lld",
			    static_cast<long long>(partition_count));
		}
		const auto &key = key_values[key_idx];
		const auto positive = KafkaMurmur2(key.GetData(), key.GetSize()) & 0x7fffffff;
		results[i] = static_cast<int32_t>(positive % partition_count);
	}
}

// Spark's hash() (Murmur3_x86_32) with its seeding: a value's hash becomes the seed of the next one.
struct SparkMurmur3 {
	using result_t = int32_t;
	static constexpr result_t SEED = 42;

	static result_t HashInt(int32_t value, result_t seed) {
		return Fmix(MixH1(uint32_t(seed), MixK1(uint32_t(value))), 4);
	}
	static result_t HashLong(int64_t value, result_t seed) {
		const auto bits = uint64_t(value);
		uint32_t h1 = MixH1(uint32_t(seed), MixK1(uint32_t(bits)));
		h1 = MixH1(h1, MixK1(uint32_t(bits >> 32)));
		return Fmix(h1, 8);
	}
	// Spark's hashUnsafeBytes: unlike standard MurmurHash3, each trailing byte is mixed as its own sign-extended block.
	static result_t HashBytes(const uint8_t *data, idx_t size, result_t seed) {
		const idx_t aligned = size - size % 4;
		uint32_t h1 = uint32_t(seed);
		for (idx_t i = 0; i < aligned; i += 4) {
			const uint32_t word = uint32_t(data[i]) | uint32_t(data[i + 1]) << 8 | uint32_t(data[i + 2]) << 16 |
			                      uint32_t(data[i + 3]) << 24;
			h1 = MixH1(h1, MixK1(word));
		}
		for (idx_t i = aligned; i < size; i++) {
			h1 = MixH1(h1, MixK1(uint32_t(int32_t(int8_t(data[i])))));
		}
		return Fmix(h1, uint32_t(size));
	}

private:
	static uint32_t RotateLeft(uint32_t value, int bits) {
		return (value << bits) | (value >> (32 - bits));
	}
	static uint32_t MixK1(uint32_t k1) {
		k1 *= 0xcc9e2d51;
		k1 = RotateLeft(k1, 15);
		return k1 * 0x1b873593;
	}
	static uint32_t MixH1(uint32_t h1, uint32_t k1) {
		h1 ^= k1;
		h1 = RotateLeft(h1, 13);
		return h1 * 5 + 0xe6546b64;
	}
	static result_t Fmix(uint32_t h1, uint32_t length) {
		h1 ^= length;
		h1 ^= h1 >> 16;
		h1 *= 0x85ebca6b;
		h1 ^= h1 >> 13;
		h1 *= 0xc2b2ae35;
		h1 ^= h1 >> 16;
		return int32_t(h1);
	}
};

// Spark's xxhash64(): standard XXH64 of the value's little-endian bytes, with the same seeding as hash().
struct SparkXXH64 {
	using result_t = int64_t;
	static constexpr result_t SEED = 42;

	static result_t HashInt(int32_t value, result_t seed) {
		uint8_t bytes[4];
		StoreLittleEndian(uint64_t(uint32_t(value)), bytes, sizeof(bytes));
		return int64_t(XXH64(bytes, sizeof(bytes), uint64_t(seed)));
	}
	static result_t HashLong(int64_t value, result_t seed) {
		uint8_t bytes[8];
		StoreLittleEndian(uint64_t(value), bytes, sizeof(bytes));
		return int64_t(XXH64(bytes, sizeof(bytes), uint64_t(seed)));
	}
	static result_t HashBytes(const uint8_t *data, idx_t size, result_t seed) {
		return int64_t(XXH64(data, size, uint64_t(seed)));
	}

private:
	static void StoreLittleEndian(uint64_t value, uint8_t *bytes, idx_t size) {
		for (idx_t i = 0; i < size; i++) {
			bytes[i] = uint8_t(value >> (8 * i));
		}
	}
};

// Java's Float.floatToIntBits after Spark folds -0.0 into 0.0; every NaN becomes the canonical NaN.
int32_t SparkFloatBits(float value) {
	if (value == 0.0f) {
		return 0;
	}
	if (std::isnan(value)) {
		return 0x7fc00000;
	}
	int32_t bits;
	memcpy(&bits, &value, sizeof(bits));
	return bits;
}

int64_t SparkDoubleBits(double value) {
	if (value == 0.0) {
		return 0;
	}
	if (std::isnan(value)) {
		return 0x7ff8000000000000LL;
	}
	int64_t bits;
	memcpy(&bits, &value, sizeof(bits));
	return bits;
}

// Writes Java's BigInteger.toByteArray() of a 128-bit value: minimal big-endian two's complement. Returns the offset
// of the first byte within the 16-byte buffer.
idx_t JavaBigIntegerBytes(hugeint_t value, uint8_t bytes[16]) {
	for (idx_t i = 0; i < 8; i++) {
		bytes[i] = uint8_t(uint64_t(value.upper) >> (56 - 8 * i));
		bytes[8 + i] = uint8_t(value.lower >> (56 - 8 * i));
	}
	idx_t start = 0;
	while (start < 15 && ((bytes[start] == 0x00 && (bytes[start + 1] & 0x80) == 0) ||
	                      (bytes[start] == 0xFF && (bytes[start + 1] & 0x80) != 0))) {
		start++;
	}
	return start;
}

int64_t DecimalUnscaledValue(const UnifiedVectorFormat &format, PhysicalType physical_type, idx_t idx) {
	switch (physical_type) {
	case PhysicalType::INT16:
		return UnifiedVectorFormat::GetData<int16_t>(format)[idx];
	case PhysicalType::INT32:
		return UnifiedVectorFormat::GetData<int32_t>(format)[idx];
	case PhysicalType::INT64:
		return UnifiedVectorFormat::GetData<int64_t>(format)[idx];
	default:
		throw InternalException("spark hash: unexpected DECIMAL storage");
	}
}

// Hashes one value the way Spark's HashExpression does, returning the next seed. NULLs leave the seed unchanged, and
// arrays and structs hash their elements in order, each seeded with the previous result.
template <class HASHER>
typename HASHER::result_t SparkHashValue(const RecursiveUnifiedVectorFormat &format, idx_t entry,
                                         typename HASHER::result_t seed) {
	const auto &unified = format.unified;
	const auto idx = unified.sel->get_index(entry);
	if (!unified.validity.RowIsValid(idx)) {
		return seed;
	}
	const auto &type = format.logical_type;
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return HASHER::HashInt(UnifiedVectorFormat::GetData<bool>(unified)[idx] ? 1 : 0, seed);
	case LogicalTypeId::TINYINT:
		return HASHER::HashInt(UnifiedVectorFormat::GetData<int8_t>(unified)[idx], seed);
	case LogicalTypeId::SMALLINT:
		return HASHER::HashInt(UnifiedVectorFormat::GetData<int16_t>(unified)[idx], seed);
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::DATE:
		return HASHER::HashInt(UnifiedVectorFormat::GetData<int32_t>(unified)[idx], seed);
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return HASHER::HashLong(UnifiedVectorFormat::GetData<int64_t>(unified)[idx], seed);
	case LogicalTypeId::FLOAT:
		return HASHER::HashInt(SparkFloatBits(UnifiedVectorFormat::GetData<float>(unified)[idx]), seed);
	case LogicalTypeId::DOUBLE:
		return HASHER::HashLong(SparkDoubleBits(UnifiedVectorFormat::GetData<double>(unified)[idx]), seed);
	case LogicalTypeId::DECIMAL: {
		// Spark hashes decimals of up to 18 digits as their unscaled long, and wider ones as the unscaled
		// BigInteger's bytes.
		if (DecimalType::GetWidth(type) <= 18) {
			return HASHER::HashLong(DecimalUnscaledValue(unified, type.InternalType(), idx), seed);
		}
		uint8_t bytes[16];
		const auto start = JavaBigIntegerBytes(UnifiedVectorFormat::GetData<hugeint_t>(unified)[idx], bytes);
		return HASHER::HashBytes(bytes + start, sizeof(bytes) - start, seed);
	}
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB: {
		const auto &value = UnifiedVectorFormat::GetData<string_t>(unified)[idx];
		return HASHER::HashBytes(const_data_ptr_cast(value.GetData()), value.GetSize(), seed);
	}
	case LogicalTypeId::LIST: {
		const auto &list = UnifiedVectorFormat::GetData<list_entry_t>(unified)[idx];
		for (idx_t i = 0; i < list.length; i++) {
			seed = SparkHashValue<HASHER>(format.children[0], list.offset + i, seed);
		}
		return seed;
	}
	case LogicalTypeId::ARRAY: {
		const auto array_size = ArrayType::GetSize(type);
		for (idx_t i = 0; i < array_size; i++) {
			seed = SparkHashValue<HASHER>(format.children[0], idx * array_size + i, seed);
		}
		return seed;
	}
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::TUPLE:
		for (auto &child : format.children) {
			seed = SparkHashValue<HASHER>(child, entry, seed);
		}
		return seed;
	default:
		throw InternalException("spark hash: unexpected type %s", type.ToString());
	}
}

// Updates each row's running hash with the row's value in `input`; NULLs leave the hash unchanged.
template <class HASHER, class T, class HASH_VALUE>
void SparkHashFlatColumn(Vector &input, idx_t count, typename HASHER::result_t *hashes, HASH_VALUE hash_value) {
	UnifiedVectorFormat format;
	input.ToUnifiedFormat(format);
	auto values = UnifiedVectorFormat::GetData<T>(format);
	for (idx_t i = 0; i < count; i++) {
		const auto idx = format.sel->get_index(i);
		if (format.validity.RowIsValid(idx)) {
			hashes[i] = hash_value(values[idx], hashes[i]);
		}
	}
}

template <class HASHER>
void SparkHashColumn(Vector &input, idx_t count, typename HASHER::result_t *hashes) {
	using result_t = typename HASHER::result_t;
	const auto &type = input.GetType();
	switch (type.id()) {
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::DATE:
		return SparkHashFlatColumn<HASHER, int32_t>(input, count, hashes,
		                                            [](int32_t v, result_t seed) { return HASHER::HashInt(v, seed); });
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return SparkHashFlatColumn<HASHER, int64_t>(input, count, hashes,
		                                            [](int64_t v, result_t seed) { return HASHER::HashLong(v, seed); });
	case LogicalTypeId::DOUBLE:
		return SparkHashFlatColumn<HASHER, double>(
		    input, count, hashes, [](double v, result_t seed) { return HASHER::HashLong(SparkDoubleBits(v), seed); });
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
		return SparkHashFlatColumn<HASHER, string_t>(input, count, hashes, [](const string_t &v, result_t seed) {
			return HASHER::HashBytes(const_data_ptr_cast(v.GetData()), v.GetSize(), seed);
		});
	default: {
		RecursiveUnifiedVectorFormat format;
		Vector::RecursiveToUnifiedFormat(input, format);
		for (idx_t i = 0; i < count; i++) {
			hashes[i] = SparkHashValue<HASHER>(format, i, hashes[i]);
		}
	}
	}
}

template <class HASHER>
void SparkHashFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	using result_t = typename HASHER::result_t;
	bool all_constant = true;
	for (auto &arg : args.data) {
		all_constant = all_constant && arg.GetVectorType() == VectorType::CONSTANT_VECTOR;
	}
	const idx_t count = all_constant ? 1 : args.size();
	result.SetVectorType(all_constant ? VectorType::CONSTANT_VECTOR : VectorType::FLAT_VECTOR);
	auto results =
	    all_constant ? ConstantVector::GetData<result_t>(result) : FlatVector::GetDataMutable<result_t>(result);

	// Spark chains each value's hash into the next as its seed, so the hashes can be computed a column at a time:
	// the type is dispatched once per column, and only nested columns recurse per value.
	for (idx_t i = 0; i < count; i++) {
		results[i] = HASHER::SEED;
	}
	for (idx_t col = 0; col < args.ColumnCount(); col++) {
		SparkHashColumn<HASHER>(args.data[col], count, results);
	}
}

// Returns the type Spark would hash a value as, or throws for types with no Spark equivalent. Spark has no unsigned
// integers, HUGEINT, TIME, INTERVAL or UUID, and rejects hashing MAPs by default.
LogicalType SparkInputType(const string &function_name, const LogicalType &type) {
	if (type.IsUnknown()) {
		throw ParameterNotResolvedException();
	}
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
		return type;
	// Spark timestamps have microsecond precision.
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		return LogicalType::TIMESTAMP;
	case LogicalTypeId::ENUM:
		return LogicalType::VARCHAR;
	// A NULL literal is skipped, like any NULL.
	case LogicalTypeId::SQLNULL:
		return LogicalType::INTEGER;
	// Nested types keep their type unless a child needs converting, so no cast is added for them.
	case LogicalTypeId::LIST: {
		auto child = SparkInputType(function_name, ListType::GetChildType(type));
		return child != ListType::GetChildType(type) ? LogicalType::LIST(child) : type;
	}
	case LogicalTypeId::ARRAY: {
		auto child = SparkInputType(function_name, ArrayType::GetChildType(type));
		return child != ArrayType::GetChildType(type) ? LogicalType::ARRAY(child, ArrayType::GetSize(type)) : type;
	}
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::TUPLE: {
		child_list_t<LogicalType> children;
		bool changed = false;
		for (auto &child : StructType::GetChildTypes(type)) {
			children.emplace_back(child.first, SparkInputType(function_name, child.second));
			changed = changed || children.back().second != child.second;
		}
		return changed ? LogicalType::STRUCT(std::move(children)) : type;
	}
	default:
		throw BinderException("%s cannot hash values of type %s, which has no Spark equivalent. Supported types are "
		                      "BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, DECIMAL, DATE, TIMESTAMP, "
		                      "TIMESTAMP WITH TIME ZONE, VARCHAR, BLOB, and LIST, ARRAY and STRUCT of those",
		                      function_name, type.ToString());
	}
}

// Runs before the arguments are cast, so each argument is cast to the type Spark would hash it as.
void SparkHashResolveTypes(ResolveScalarFunctionTypesInput &input) {
	auto &bound_function = input.GetBoundFunction();
	const auto &name = bound_function.GetName().GetIdentifierName();
	for (idx_t i = 0; i < input.GetArgumentCount(); i++) {
		bound_function.GetArguments()[i] = SparkInputType(name, input.GetArgumentType(i));
	}
}

// Declares a positional-only parameter with a real name, so duckdb_functions() reports it instead of col<N>.
FunctionParameter Positional(const string &name, LogicalType type) {
	return FunctionParameter(Identifier(name), std::move(type), {}, FunctionParameterKind::POSITIONAL_ONLY);
}

// Registers a function over VARCHAR and BLOB with the same description for both.
void RegisterStringFunction(ExtensionLoader &loader, const string &name, const LogicalType &result_type,
                            scalar_function_t function, const string &description) {
	ScalarFunctionSet set {Identifier(name)};
	for (auto &input_type : {LogicalType::VARCHAR, LogicalType::BLOB}) {
		set.AddFunction(ScalarFunction({Positional("value", input_type)}, result_type, function));
	}
	CreateScalarFunctionInfo info(set);
	for (auto &input_type : {LogicalType::VARCHAR, LogicalType::BLOB}) {
		const string example_value = input_type == LogicalType::BLOB ? "'hello'::BLOB" : "'hello'";
		info.descriptions.push_back({/* parameter_types */ {input_type},
		                             /* parameter_names */ {"value"},
		                             /* description */ description,
		                             /* examples */ {name + "(" + example_value + ")"},
		                             /* categories */ {"hash"}});
	}
	loader.RegisterFunction(info);
}

void RegisterSparkFunction(ExtensionLoader &loader, const string &name, const LogicalType &result_type,
                           scalar_function_t function, const string &description) {
	ScalarFunction spark_function(Identifier(name), {}, result_type, function);
	spark_function.GetSignature().AddParameter("value", LogicalType::ANY);
	spark_function.GetSignature().AddArgs("values", LogicalType::ANY);
	spark_function.SetResolveTypesCallback(SparkHashResolveTypes);
	spark_function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	CreateScalarFunctionInfo info(spark_function);
	info.descriptions.push_back({/* parameter_types */ {LogicalType::ANY},
	                             /* parameter_names */ {"value"},
	                             /* description */ description,
	                             /* examples */ {name + "('Spark', [123], 2)"},
	                             /* categories */ {"hash"}});
	loader.RegisterFunction(info);
}

// Iceberg's 32-bit hash (spec, Appendix B): MurmurHash3 x86_32 with seed 0 over the value's Iceberg encoding.
int32_t IcebergHashBytes(const void *data, idx_t size) {
	if (size > static_cast<idx_t>(NumericLimits<int32_t>::Maximum())) {
		throw InvalidInputException("Iceberg hashing supports values of at most 2147483647 bytes, got %llu",
		                            static_cast<unsigned long long>(size));
	}
	uint32_t hash;
	MurmurHash3_x86_32(data, static_cast<int>(size), 0, &hash);
	return static_cast<int32_t>(hash);
}

// Every integer type hashes as an 8-byte little-endian long, so int and long values hash the same.
int32_t IcebergHashLong(int64_t value) {
	uint8_t bytes[8];
	for (idx_t i = 0; i < 8; i++) {
		bytes[i] = uint8_t(uint64_t(value) >> (8 * i));
	}
	return IcebergHashBytes(bytes, sizeof(bytes));
}

// Nanosecond timestamps are floored to microseconds before hashing, so they hash like the same microsecond value.
int64_t NanosToMicros(int64_t nanos) {
	return nanos / 1000 - (nanos % 1000 < 0 ? 1 : 0);
}

int32_t IcebergHashDecimal(hugeint_t unscaled) {
	uint8_t bytes[16];
	const auto start = JavaBigIntegerBytes(unscaled, bytes);
	return IcebergHashBytes(bytes + start, sizeof(bytes) - start);
}

// Writes the Iceberg hash of every row of `input` into `hashes`, and whether the row is non-NULL into `valid`.
template <class T, class HASH>
void IcebergHashRows(Vector &input, idx_t count, int32_t *hashes, bool *valid, HASH hash) {
	UnifiedVectorFormat format;
	input.ToUnifiedFormat(format);
	auto values = UnifiedVectorFormat::GetData<T>(format);
	for (idx_t i = 0; i < count; i++) {
		const auto idx = format.sel->get_index(i);
		valid[i] = format.validity.RowIsValid(idx);
		if (valid[i]) {
			hashes[i] = hash(values[idx]);
		}
	}
}

void IcebergHashVector(Vector &input, idx_t count, int32_t *hashes, bool *valid) {
	const auto &type = input.GetType();
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return IcebergHashRows<int8_t>(input, count, hashes, valid, IcebergHashLong);
	case LogicalTypeId::SMALLINT:
		return IcebergHashRows<int16_t>(input, count, hashes, valid, IcebergHashLong);
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::DATE:
		return IcebergHashRows<int32_t>(input, count, hashes, valid, IcebergHashLong);
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return IcebergHashRows<int64_t>(input, count, hashes, valid, IcebergHashLong);
	case LogicalTypeId::UTINYINT:
		return IcebergHashRows<uint8_t>(input, count, hashes, valid, IcebergHashLong);
	case LogicalTypeId::USMALLINT:
		return IcebergHashRows<uint16_t>(input, count, hashes, valid, IcebergHashLong);
	case LogicalTypeId::UINTEGER:
		return IcebergHashRows<uint32_t>(input, count, hashes, valid, IcebergHashLong);
	case LogicalTypeId::TIMESTAMP_NS:
		return IcebergHashRows<int64_t>(input, count, hashes, valid,
		                                [](int64_t nanos) { return IcebergHashLong(NanosToMicros(nanos)); });
	case LogicalTypeId::DECIMAL:
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return IcebergHashRows<int16_t>(input, count, hashes, valid,
			                                [](int16_t v) { return IcebergHashDecimal(hugeint_t(v)); });
		case PhysicalType::INT32:
			return IcebergHashRows<int32_t>(input, count, hashes, valid,
			                                [](int32_t v) { return IcebergHashDecimal(hugeint_t(v)); });
		case PhysicalType::INT64:
			return IcebergHashRows<int64_t>(input, count, hashes, valid,
			                                [](int64_t v) { return IcebergHashDecimal(hugeint_t(v)); });
		default:
			return IcebergHashRows<hugeint_t>(input, count, hashes, valid, IcebergHashDecimal);
		}
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
		return IcebergHashRows<string_t>(input, count, hashes, valid, [](const string_t &value) {
			return IcebergHashBytes(value.GetData(), value.GetSize());
		});
	case LogicalTypeId::UUID:
		return IcebergHashRows<hugeint_t>(input, count, hashes, valid, [](hugeint_t value) {
			data_t bytes[16];
			UUID::ToBlob(value, bytes);
			return IcebergHashBytes(bytes, sizeof(bytes));
		});
	default:
		throw InternalException("iceberg hash: unexpected type %s", type.ToString());
	}
}

// Returns the type a value is hashed as for Iceberg, or throws for types Iceberg can't bucket: BOOLEAN, FLOAT,
// DOUBLE, BIGINT-overflowing integers (UBIGINT, HUGEINT, UHUGEINT), INTERVAL, TIME WITH TIME ZONE and nested types.
LogicalType IcebergInputType(const string &function_name, const LogicalType &type) {
	if (type.IsUnknown()) {
		throw ParameterNotResolvedException();
	}
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::UUID:
		return type;
	// Iceberg timestamps have microsecond precision; these casts are exact.
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
		return LogicalType::TIMESTAMP;
	// An ENUM is stored in Iceberg as its string label; a NULL literal hashes to NULL.
	case LogicalTypeId::ENUM:
	case LogicalTypeId::SQLNULL:
		return LogicalType::VARCHAR;
	default:
		throw BinderException("%s cannot hash values of type %s: Iceberg can't bucket it. Supported types are "
		                      "integers up to BIGINT, DECIMAL, DATE, TIME, TIMESTAMP (any precision), TIMESTAMPTZ, "
		                      "VARCHAR, BLOB and UUID",
		                      function_name, type.ToString());
	}
}

// Runs before the arguments are cast, so the value is cast to the type Iceberg would hash it as.
void IcebergResolveTypes(ResolveScalarFunctionTypesInput &input) {
	auto &bound_function = input.GetBoundFunction();
	bound_function.GetArguments()[0] =
	    IcebergInputType(bound_function.GetName().GetIdentifierName(), input.GetArgumentType(0));
}

// Reads a bucket count argument, which must fit Iceberg's and Jump Hash's 32-bit bucket counts.
int32_t BucketCount(const char *function_name, int64_t buckets) {
	if (buckets <= 0 || buckets > NumericLimits<int32_t>::Maximum()) {
		throw InvalidInputException("%s: the number of buckets must be between 1 and 2147483647, got %lld",
		                            function_name, static_cast<long long>(buckets));
	}
	return static_cast<int32_t>(buckets);
}

// iceberg_hash(value) and iceberg_bucket(value, n): BUCKETED selects the bucket form.
template <bool BUCKETED>
void IcebergFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	bool all_constant = true;
	for (auto &arg : args.data) {
		all_constant = all_constant && arg.GetVectorType() == VectorType::CONSTANT_VECTOR;
	}
	const idx_t count = all_constant ? 1 : args.size();
	result.SetVectorType(all_constant ? VectorType::CONSTANT_VECTOR : VectorType::FLAT_VECTOR);
	auto results =
	    all_constant ? ConstantVector::GetData<int32_t>(result) : FlatVector::GetDataMutable<int32_t>(result);

	vector<int32_t> hashes(count);
	unique_ptr<bool[]> valid(new bool[count]);
	IcebergHashVector(args.data[0], count, hashes.data(), valid.get());

	UnifiedVectorFormat bucket_format;
	if (BUCKETED) {
		args.data[1].ToUnifiedFormat(bucket_format);
	}
	for (idx_t i = 0; i < count; i++) {
		if (!valid[i]) {
			SetResultNull(result, all_constant, i);
			continue;
		}
		if (!BUCKETED) {
			results[i] = hashes[i];
			continue;
		}
		const auto bucket_idx = bucket_format.sel->get_index(i);
		if (!bucket_format.validity.RowIsValid(bucket_idx)) {
			SetResultNull(result, all_constant, i);
			continue;
		}
		const auto buckets =
		    BucketCount("iceberg_bucket", UnifiedVectorFormat::GetData<int64_t>(bucket_format)[bucket_idx]);
		results[i] = (hashes[i] & NumericLimits<int32_t>::Maximum()) % buckets;
	}
}

// Jump consistent hash (Lamping and Veach, "A Fast, Minimal Memory, Consistent Hash Algorithm", 2014). When the
// bucket count grows from n to n + 1, only about 1/(n + 1) of keys move. Guava's Hashing.consistentHash returns the
// same buckets.
int32_t JumpConsistentHash(uint64_t key, int32_t buckets) {
	int64_t bucket = -1;
	int64_t next = 0;
	while (next < buckets) {
		bucket = next;
		key = key * 2862933555777941757ULL + 1;
		next = static_cast<int64_t>(static_cast<double>(bucket + 1) *
		                            (static_cast<double>(int64_t(1) << 31) / static_cast<double>((key >> 33) + 1)));
	}
	return static_cast<int32_t>(bucket);
}

// The key is read as its 64 bits, so a BIGINT and the UBIGINT with the same bits land in the same bucket.
template <class KEY>
void JumpHashFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &keys = args.data[0];
	auto &buckets = args.data[1];
	const bool constant =
	    keys.GetVectorType() == VectorType::CONSTANT_VECTOR && buckets.GetVectorType() == VectorType::CONSTANT_VECTOR;
	const idx_t count = constant ? 1 : args.size();
	result.SetVectorType(constant ? VectorType::CONSTANT_VECTOR : VectorType::FLAT_VECTOR);
	auto results = constant ? ConstantVector::GetData<int32_t>(result) : FlatVector::GetDataMutable<int32_t>(result);

	UnifiedVectorFormat key_format;
	UnifiedVectorFormat bucket_format;
	keys.ToUnifiedFormat(key_format);
	buckets.ToUnifiedFormat(bucket_format);
	auto key_values = UnifiedVectorFormat::GetData<KEY>(key_format);
	auto bucket_values = UnifiedVectorFormat::GetData<int64_t>(bucket_format);
	for (idx_t i = 0; i < count; i++) {
		const auto key_idx = key_format.sel->get_index(i);
		const auto bucket_idx = bucket_format.sel->get_index(i);
		if (!key_format.validity.RowIsValid(key_idx) || !bucket_format.validity.RowIsValid(bucket_idx)) {
			SetResultNull(result, constant, i);
			continue;
		}
		results[i] = JumpConsistentHash(static_cast<uint64_t>(key_values[key_idx]),
		                                BucketCount("jump_hash", bucket_values[bucket_idx]));
	}
}

} // namespace

void RegisterInteropFunctions(ExtensionLoader &loader) {
	RegisterStringFunction(loader, "farm_fingerprint", LogicalType::BIGINT, StringHashFunction<FarmFingerprintHasher>,
	                       "Computes FarmHash Fingerprint64 of the input as a signed BIGINT, matching BigQuery's "
	                       "FARM_FINGERPRINT");

	RegisterSparkFunction(loader, "spark_hash", LogicalType::INTEGER, SparkHashFunction<SparkMurmur3>,
	                      "Computes Spark's hash() of one or more values: Murmur3 x86_32 with seed 42, each value "
	                      "seeding the next, with Spark's type rules. NULLs are skipped. TIMESTAMP hashes like Spark's "
	                      "TIMESTAMP_NTZ and TIMESTAMPTZ like Spark's TIMESTAMP, both as microseconds");

	RegisterSparkFunction(loader, "spark_xxhash64", LogicalType::BIGINT, SparkHashFunction<SparkXXH64>,
	                      "Computes Spark's xxhash64() of one or more values: XXH64 with seed 42, each value seeding "
	                      "the next, with Spark's type rules. NULLs are skipped. TIMESTAMP hashes like Spark's "
	                      "TIMESTAMP_NTZ and TIMESTAMPTZ like Spark's TIMESTAMP, both as microseconds");

	RegisterStringFunction(loader, "kafka_murmur2", LogicalType::INTEGER, StringHashFunction<KafkaMurmur2Hasher>,
	                       "Computes Kafka's murmur2 hash of a serialized record key, as used by Kafka's default "
	                       "partitioner");

	ScalarFunctionSet kafka_partition_set("kafka_partition");
	for (auto &key_type : {LogicalType::VARCHAR, LogicalType::BLOB}) {
		kafka_partition_set.AddFunction(
		    ScalarFunction({Positional("key", key_type), Positional("partitions", LogicalType::BIGINT)},
		                   LogicalType::INTEGER, KafkaPartitionFunction));
	}
	// An invalid partition count raises an error while the query runs.
	kafka_partition_set.SetFallible();
	CreateScalarFunctionInfo kafka_partition_info(kafka_partition_set);
	for (auto &key_type : {LogicalType::VARCHAR, LogicalType::BLOB}) {
		const string example_key = key_type == LogicalType::BLOB ? "'order-42'::BLOB" : "'order-42'";
		kafka_partition_info.descriptions.push_back(
		    {/* parameter_types */ {key_type, LogicalType::BIGINT},
		     /* parameter_names */ {"key", "partitions"},
		     /* description */
		     "Returns the partition Kafka's default partitioner assigns a record with this serialized key: "
		     "toPositive(murmur2(key)) % partitions",
		     /* examples */ {"kafka_partition(" + example_key + ", 12)"},
		     /* categories */ {"hash"}});
	}
	loader.RegisterFunction(kafka_partition_info);

	// Iceberg's bucket transform and the 32-bit hash it uses. Both can raise an error while running (an invalid
	// bucket count, or a value over 2 GiB), so they are fallible.
	ScalarFunction iceberg_hash(Identifier("iceberg_hash"), {Positional("value", LogicalType::ANY)},
	                            LogicalType::INTEGER, IcebergFunction<false>);
	iceberg_hash.SetResolveTypesCallback(IcebergResolveTypes);
	iceberg_hash.SetFallible();
	CreateScalarFunctionInfo iceberg_hash_info(iceberg_hash);
	iceberg_hash_info.descriptions.push_back(
	    {/* parameter_types */ {LogicalType::ANY},
	     /* parameter_names */ {"value"},
	     /* description */
	     "Computes Iceberg's 32-bit bucket hash of the input (spec Appendix B): MurmurHash3 x86_32 with seed 0 over "
	     "Iceberg's encoding of the value. Integers of every width hash alike, DECIMAL hashes its unscaled value and "
	     "timestamps hash as microseconds",
	     /* examples */ {"iceberg_hash('iceberg')", "iceberg_hash(34)"},
	     /* categories */ {"hash"}});
	loader.RegisterFunction(iceberg_hash_info);

	ScalarFunction iceberg_bucket(Identifier("iceberg_bucket"),
	                              {Positional("value", LogicalType::ANY), Positional("buckets", LogicalType::BIGINT)},
	                              LogicalType::INTEGER, IcebergFunction<true>);
	iceberg_bucket.SetResolveTypesCallback(IcebergResolveTypes);
	iceberg_bucket.SetFallible();
	CreateScalarFunctionInfo iceberg_bucket_info(iceberg_bucket);
	iceberg_bucket_info.descriptions.push_back(
	    {/* parameter_types */ {LogicalType::ANY, LogicalType::BIGINT},
	     /* parameter_names */ {"value", "buckets"},
	     /* description */
	     "Returns the Iceberg bucket[N] partition of the input: (iceberg_hash(value) & 2147483647) % buckets, as "
	     "Iceberg's bucket transform computes it",
	     /* examples */ {"iceberg_bucket('iceberg', 16)", "iceberg_bucket(34, 16)"},
	     /* categories */ {"hash"}});
	loader.RegisterFunction(iceberg_bucket_info);

	ScalarFunctionSet jump_hash_set("jump_hash");
	for (auto &key_type : {LogicalType::UBIGINT, LogicalType::BIGINT}) {
		ScalarFunction jump_hash(
		    {Positional("key", key_type), Positional("buckets", LogicalType::BIGINT)}, LogicalType::INTEGER,
		    key_type == LogicalType::UBIGINT ? JumpHashFunction<uint64_t> : JumpHashFunction<int64_t>);
		// An invalid bucket count raises an error while the query runs.
		jump_hash.SetFallible();
		jump_hash_set.AddFunction(jump_hash);
	}
	CreateScalarFunctionInfo jump_hash_info(jump_hash_set);
	for (auto &key_type : {LogicalType::UBIGINT, LogicalType::BIGINT}) {
		jump_hash_info.descriptions.push_back(
		    {/* parameter_types */ {key_type, LogicalType::BIGINT},
		     /* parameter_names */ {"key", "buckets"},
		     /* description */
		     "Assigns a 64-bit key to one of `buckets` buckets with Jump consistent hash: when the bucket count grows "
		     "from n to n + 1, only about 1/(n + 1) of keys move. Hash the value first, e.g. jump_hash(xxh3_64(x), n). "
		     "Matches Guava's Hashing.consistentHash",
		     /* examples */ {"jump_hash(xxh3_64('user-42'), 16)"},
		     /* categories */ {"hash"}});
	}
	loader.RegisterFunction(jump_hash_info);

	RegisterStringFunction(loader, "clickhouse_cityhash64", LogicalType::UBIGINT,
	                       StringHashFunction<ClickHouseCityHash64Hasher>,
	                       "Computes CityHash64 (CityHash v1.0.2) of the input, matching ClickHouse's cityHash64 of a "
	                       "String");
}

} // namespace duckdb
