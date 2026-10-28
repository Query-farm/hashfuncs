#include "hash_interop.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "xxhash.h"
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
	auto results = constant ? ConstantVector::GetData<result_t>(result) : FlatVector::GetData<result_t>(result);

	UnifiedVectorFormat format;
	input.ToUnifiedFormat(count, format);
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
	auto results = constant ? ConstantVector::GetData<int32_t>(result) : FlatVector::GetData<int32_t>(result);

	UnifiedVectorFormat key_format;
	UnifiedVectorFormat partition_format;
	keys.ToUnifiedFormat(count, key_format);
	partitions.ToUnifiedFormat(count, partition_format);
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
	input.ToUnifiedFormat(count, format);
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
		Vector::RecursiveToUnifiedFormat(input, count, format);
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
	auto results = all_constant ? ConstantVector::GetData<result_t>(result) : FlatVector::GetData<result_t>(result);

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
	case LogicalTypeId::STRUCT: {
		child_list_t<LogicalType> children;
		bool changed = false;
		for (auto &child : StructType::GetChildTypes(type)) {
			children.emplace_back(child.first, SparkInputType(function_name, child.second));
			changed = changed || children.back().second != child.second;
		}
		return changed ? LogicalType::STRUCT(std::move(children)) : type;
	}
	case LogicalTypeId::UNKNOWN:
		throw ParameterNotResolvedException();
	default:
		throw BinderException("%s cannot hash values of type %s, which has no Spark equivalent. Supported types are "
		                      "BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, DECIMAL, DATE, TIMESTAMP, "
		                      "TIMESTAMP WITH TIME ZONE, VARCHAR, BLOB, and LIST, ARRAY and STRUCT of those",
		                      function_name, type.ToString());
	}
}

unique_ptr<FunctionData> SparkHashBind(ClientContext &context, ScalarFunction &bound_function,
                                       vector<unique_ptr<Expression>> &arguments) {
	bound_function.arguments.clear();
	for (auto &argument : arguments) {
		bound_function.arguments.push_back(SparkInputType(bound_function.name, argument->return_type));
	}
	return nullptr;
}

// Registers a function over VARCHAR and BLOB with the same description for both.
void RegisterStringFunction(ExtensionLoader &loader, const string &name, const LogicalType &result_type,
                            scalar_function_t function, const string &description) {
	ScalarFunctionSet set(name);
	for (auto &input_type : {LogicalType::VARCHAR, LogicalType::BLOB}) {
		set.AddFunction(ScalarFunction({input_type}, result_type, function));
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
	ScalarFunction spark_function(name, {LogicalType::ANY}, result_type, function, SparkHashBind);
	spark_function.varargs = LogicalType::ANY;
	spark_function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	CreateScalarFunctionInfo info(spark_function);
	info.descriptions.push_back({/* parameter_types */ {LogicalType::ANY},
	                             /* parameter_names */ {"value"},
	                             /* description */ description,
	                             /* examples */ {name + "('Spark', [123], 2)"},
	                             /* categories */ {"hash"}});
	loader.RegisterFunction(info);
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
		ScalarFunction kafka_partition({key_type, LogicalType::BIGINT}, LogicalType::INTEGER, KafkaPartitionFunction);
		// An invalid partition count raises an error while the query runs.
		kafka_partition.SetFallible();
		kafka_partition_set.AddFunction(kafka_partition);
	}
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

	RegisterStringFunction(loader, "clickhouse_cityhash64", LogicalType::UBIGINT,
	                       StringHashFunction<ClickHouseCityHash64Hasher>,
	                       "Computes CityHash64 (CityHash v1.0.2) of the input, matching ClickHouse's cityHash64 of a "
	                       "String");
}

} // namespace duckdb
