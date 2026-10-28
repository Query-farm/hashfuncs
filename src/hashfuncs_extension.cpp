#include "hashfuncs_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "xxhash.h"
#include "rapidhash.h"
#include "MurmurHash3.h"
// rapidhash.h and wyhash.h both define these helper macros without guards; drop rapidhash's copies so a future
// rapidhash update with different definitions can't clash with wyhash's.
#undef _likely_
#undef _unlikely_
#include "third_party/wyhash/wyhash.h"
#include "third_party/komihash/komihash.h"
#include "hash_interop.hpp"
#include "vendored_hashes.hpp"
#include "query_farm_telemetry.hpp"
namespace duckdb {

static constexpr const char *HASHFUNCS_VERSION = "2026102801";

// The per-row hash wrappers must inline into HashFunction's loop: when the compiler declined to inline RapidHash's
// wrapper, short strings hashed 16% slower.
#if defined(_MSC_VER)
#define HASHFUNCS_ALWAYS_INLINE __forceinline
#elif defined(__GNUC__) || defined(__clang__)
#define HASHFUNCS_ALWAYS_INLINE inline __attribute__((always_inline))
#else
#define HASHFUNCS_ALWAYS_INLINE inline
#endif

namespace {

// Each algorithm hashes a byte range with a seed. The unseeded SQL functions pass seed 0, which every library
// defines to give the same result as its unseeded entry point.
struct XXH32Op {
	using result_t = uint32_t;
	using seed_t = uint32_t;
	HASHFUNCS_ALWAYS_INLINE static result_t Hash(const void *data, idx_t size, seed_t seed) {
		return XXH32(data, size, seed);
	}
};

struct XXH64Op {
	using result_t = uint64_t;
	using seed_t = uint64_t;
	HASHFUNCS_ALWAYS_INLINE static result_t Hash(const void *data, idx_t size, seed_t seed) {
		return XXH64(data, size, seed);
	}
};

struct XXH3_64Op {
	using result_t = uint64_t;
	using seed_t = uint64_t;
	HASHFUNCS_ALWAYS_INLINE static result_t Hash(const void *data, idx_t size, seed_t seed) {
		return XXH3_64bits_withSeed(data, size, seed);
	}
};

struct XXH3_128Op {
	using result_t = uhugeint_t;
	using seed_t = uint64_t;
	HASHFUNCS_ALWAYS_INLINE static result_t Hash(const void *data, idx_t size, seed_t seed) {
		auto hash = XXH3_128bits_withSeed(data, size, seed);
		// The historical xxh3_128() layout is kept for stored-key compatibility: uhugeint_t(upper, lower) puts low64
		// in the upper half. xxh3_128_hex() returns xxHash's canonical byte order instead.
		return uhugeint_t(hash.low64, hash.high64);
	}
};

struct RapidhashOp {
	using result_t = uint64_t;
	using seed_t = uint64_t;
	HASHFUNCS_ALWAYS_INLINE static result_t Hash(const void *data, idx_t size, seed_t seed) {
		return rapidhash_withSeed(data, size, seed);
	}
};

struct RapidhashMicroOp {
	using result_t = uint64_t;
	using seed_t = uint64_t;
	HASHFUNCS_ALWAYS_INLINE static result_t Hash(const void *data, idx_t size, seed_t seed) {
		return rapidhashMicro_withSeed(data, size, seed);
	}
};

struct RapidhashNanoOp {
	using result_t = uint64_t;
	using seed_t = uint64_t;
	HASHFUNCS_ALWAYS_INLINE static result_t Hash(const void *data, idx_t size, seed_t seed) {
		return rapidhashNano_withSeed(data, size, seed);
	}
};

// MurmurHash3_x86_32 as in the reference implementation, inlined so the loop specializes on a fixed-width input's
// constant size instead of calling the library through an out-parameter.
struct MurmurHash3_32Op {
	using result_t = uint32_t;
	using seed_t = uint32_t;
	HASHFUNCS_ALWAYS_INLINE static uint32_t Rotl(uint32_t value, int bits) {
		return (value << bits) | (value >> (32 - bits));
	}
	HASHFUNCS_ALWAYS_INLINE static uint32_t MixK1(uint32_t k1) {
		k1 *= 0xcc9e2d51;
		k1 = Rotl(k1, 15);
		return k1 * 0x1b873593;
	}
	HASHFUNCS_ALWAYS_INLINE static result_t Hash(const void *data, idx_t size, seed_t seed) {
		auto bytes = static_cast<const uint8_t *>(data);
		const idx_t nblocks = size / 4;
		uint32_t h1 = seed;
		for (idx_t i = 0; i < nblocks; i++) {
			uint32_t k1;
			memcpy(&k1, bytes + i * 4, sizeof(k1));
			h1 ^= MixK1(k1);
			h1 = Rotl(h1, 13);
			h1 = h1 * 5 + 0xe6546b64;
		}
		const uint8_t *tail = bytes + nblocks * 4;
		uint32_t k1 = 0;
		switch (size & 3) {
		case 3:
			k1 ^= uint32_t(tail[2]) << 16;
			[[fallthrough]];
		case 2:
			k1 ^= uint32_t(tail[1]) << 8;
			[[fallthrough]];
		case 1:
			k1 ^= tail[0];
			h1 ^= MixK1(k1);
		}
		h1 ^= static_cast<uint32_t>(size);
		h1 ^= h1 >> 16;
		h1 *= 0x85ebca6b;
		h1 ^= h1 >> 13;
		h1 *= 0xc2b2ae35;
		h1 ^= h1 >> 16;
		return h1;
	}
};

// The 128-bit MurmurHash3 functions take the input length as an int, so longer inputs (such as a huge nested value)
// are rejected rather than silently truncated. Those functions are registered as fallible because of it.
int MurmurHash3Length(idx_t size) {
	if (size > static_cast<idx_t>(NumericLimits<int32_t>::Maximum())) {
		throw InvalidInputException("MurmurHash3 can hash at most 2147483647 bytes, got %llu",
		                            static_cast<unsigned long long>(size));
	}
	return static_cast<int>(size);
}

// The 128-bit MurmurHash3 variants write their two 64-bit words straight into the uhugeint_t, so on little-endian
// platforms the first word is the lower half.
struct MurmurHash3_128Op {
	static constexpr bool CAN_THROW = true;
	using result_t = uhugeint_t;
	using seed_t = uint32_t;
	HASHFUNCS_ALWAYS_INLINE static result_t Hash(const void *data, idx_t size, seed_t seed) {
		result_t result;
		MurmurHash3_x86_128(data, MurmurHash3Length(size), seed, &result);
		return result;
	}
};

struct MurmurHash3_X64_128Op {
	static constexpr bool CAN_THROW = true;
	using result_t = uhugeint_t;
	using seed_t = uint32_t;
	HASHFUNCS_ALWAYS_INLINE static result_t Hash(const void *data, idx_t size, seed_t seed) {
		result_t result;
		MurmurHash3_x64_128(data, MurmurHash3Length(size), seed, &result);
		return result;
	}
};

struct WyhashOp {
	using result_t = uint64_t;
	using seed_t = uint64_t;
	HASHFUNCS_ALWAYS_INLINE static result_t Hash(const void *data, idx_t size, seed_t seed) {
		return wyhash(data, size, seed, _wyp);
	}
};

struct KomihashOp {
	using result_t = uint64_t;
	using seed_t = uint64_t;
	HASHFUNCS_ALWAYS_INLINE static result_t Hash(const void *data, idx_t size, seed_t seed) {
		return komihash(data, size, seed);
	}
};

// SipHash takes a 128-bit key: the UHUGEINT's 16 little-endian bytes, lower half first. Without a key it uses
// all-zero key bytes.
template <uint64_t (*SIPHASH)(const void *, size_t, const uint8_t *)>
struct SipHashOp {
	using result_t = uint64_t;
	using seed_t = uhugeint_t;
	HASHFUNCS_ALWAYS_INLINE static result_t Hash(const void *data, idx_t size, seed_t key) {
		uint8_t key_bytes[16];
		for (idx_t i = 0; i < 8; i++) {
			key_bytes[i] = static_cast<uint8_t>(key.lower >> (8 * i));
			key_bytes[8 + i] = static_cast<uint8_t>(key.upper >> (8 * i));
		}
		return SIPHASH(data, size, key_bytes);
	}
};

using SipHash13Op = SipHashOp<hashfuncs::SipHash13>;
using SipHash24Op = SipHashOp<hashfuncs::SipHash24>;

// The seed of CRC-32C is the CRC of data hashed before this value, so checksums can be extended.
struct Crc32cOp {
	using result_t = uint32_t;
	using seed_t = uint32_t;
	HASHFUNCS_ALWAYS_INLINE static result_t Hash(const void *data, idx_t size, seed_t previous_crc) {
		return hashfuncs::Crc32cExtend(previous_crc, data, size);
	}
};

// FNV-1a has no seed; it is registered without a seeded overload.
template <class T, T OFFSET_BASIS, T PRIME>
struct Fnv1aOp {
	using result_t = T;
	using seed_t = T;
	HASHFUNCS_ALWAYS_INLINE static result_t Hash(const void *data, idx_t size, seed_t) {
		auto bytes = static_cast<const uint8_t *>(data);
		T hash = OFFSET_BASIS;
		for (idx_t i = 0; i < size; i++) {
			hash = (hash ^ bytes[i]) * PRIME;
		}
		return hash;
	}
};

using Fnv1a32Op = Fnv1aOp<uint32_t, 0x811c9dc5u, 0x01000193u>;
using Fnv1a64Op = Fnv1aOp<uint64_t, 0xcbf29ce484222325ull, 0x100000001b3ull>;

// A reader loads an argument vector and hashes its rows: Lookup() finds a row's position and validity, and Hash()
// hashes the value there.

// Fixed-width values hash their in-memory bytes, so the hash depends on the value's width: 42::INTEGER and
// 42::BIGINT hash differently.
template <class T>
struct ValueReader {
	UnifiedVectorFormat format;
	const T *data = nullptr;

	void Initialize(Vector &input, idx_t count) {
		input.ToUnifiedFormat(format);
		data = UnifiedVectorFormat::GetData<T>(format);
	}
	HASHFUNCS_ALWAYS_INLINE bool Lookup(idx_t row, idx_t &idx) const {
		idx = format.sel->get_index(row);
		return format.validity.RowIsValid(idx);
	}
	template <class OP>
	HASHFUNCS_ALWAYS_INLINE typename OP::result_t Hash(idx_t row, idx_t idx, typename OP::seed_t seed) {
		return OP::Hash(&data[idx], sizeof(T), seed);
	}
};

// VARCHAR and BLOB values hash their contents.
struct StringReader : ValueReader<string_t> {
	template <class OP>
	HASHFUNCS_ALWAYS_INLINE typename OP::result_t Hash(idx_t row, idx_t idx, typename OP::seed_t seed) {
		return OP::Hash(data[idx].GetData(), data[idx].GetSize(), seed);
	}
};

// UUIDs hash their 16 RFC 4122 bytes, matching a hash of the UUID's binary form in other systems.
struct UUIDReader : ValueReader<hugeint_t> {
	template <class OP>
	HASHFUNCS_ALWAYS_INLINE typename OP::result_t Hash(idx_t row, idx_t idx, typename OP::seed_t seed) {
		data_t bytes[16];
		UUID::ToBlob(data[idx], bytes);
		return OP::Hash(bytes, sizeof(bytes), seed);
	}
};

// Nested values (STRUCT, LIST, MAP, ARRAY) are encoded into one byte string and hashed once. The encoding is
// unambiguous for a given type, so ('a|', 'b') and ('a', '|b') hash differently:
//   field           0x00 if NULL, otherwise 0x01 followed by the value
//   VARCHAR, BLOB   length as a little-endian uint64, then the bytes
//   LIST, ARRAY     element count as a little-endian uint64, then each element as a field
//   MAP             a LIST of (key, value) STRUCTs
//   STRUCT          each child as a field, in order; field names are not encoded
//   UUID            its 16 RFC 4122 bytes
//   anything else   the same bytes it hashes as on its own
// ARRAY and LIST values with the same elements therefore hash the same.
class NestedEncoder {
public:
	// Resolves each node's type once per chunk, so encoding a value dispatches on a compact kind instead of the
	// LogicalType, and reads data/selection/validity through cached pointers.
	void Initialize(const RecursiveUnifiedVectorFormat &format) {
		Plan(root, format);
	}
	// Encodes the (valid) value at row `entry` and returns its size; the bytes are at Data().
	idx_t Encode(idx_t entry) {
		size = 0;
		AppendValue(root, entry, root.sel->get_index(entry), false);
		return size;
	}
	const data_t *Data() const {
		return buffer.get();
	}

private:
	enum class Kind : uint8_t { FIXED1, FIXED2, FIXED4, FIXED8, FIXED16, UUID, STRING, STRUCT, LIST, ARRAY };
	struct Node {
		Kind kind;
		bool leaf = false;
		idx_t array_size = 0;
		const SelectionVector *sel = nullptr;
		const ValidityMask *validity = nullptr;
		const data_t *data = nullptr;
		vector<Node> children;
	};

	Node root;
	unsafe_unique_array<data_t> buffer;
	idx_t size = 0;
	idx_t capacity = 0;

	static Kind FixedKind(idx_t width) {
		switch (width) {
		case 1:
			return Kind::FIXED1;
		case 2:
			return Kind::FIXED2;
		case 4:
			return Kind::FIXED4;
		case 8:
			return Kind::FIXED8;
		case 16:
			return Kind::FIXED16;
		default:
			throw InternalException("hashfuncs: unexpected fixed width %llu", width);
		}
	}

	static void Plan(Node &node, const RecursiveUnifiedVectorFormat &format) {
		const auto &type = format.logical_type;
		node.sel = format.unified.sel;
		node.validity = &format.unified.validity;
		node.data = format.unified.data;
		node.leaf = true;
		if (type.id() == LogicalTypeId::UUID) {
			node.kind = Kind::UUID;
			return;
		}
		switch (type.InternalType()) {
		case PhysicalType::STRUCT:
			node.leaf = false;
			node.kind = Kind::STRUCT;
			break;
		case PhysicalType::LIST:
			node.leaf = false;
			node.kind = Kind::LIST;
			break;
		case PhysicalType::ARRAY:
			node.leaf = false;
			node.kind = Kind::ARRAY;
			node.array_size = ArrayType::GetSize(type);
			break;
		case PhysicalType::VARCHAR:
			node.kind = Kind::STRING;
			return;
		case PhysicalType::BOOL:
		case PhysicalType::INT8:
		case PhysicalType::INT16:
		case PhysicalType::INT32:
		case PhysicalType::INT64:
		case PhysicalType::INT128:
		case PhysicalType::UINT8:
		case PhysicalType::UINT16:
		case PhysicalType::UINT32:
		case PhysicalType::UINT64:
		case PhysicalType::UINT128:
		case PhysicalType::FLOAT:
		case PhysicalType::DOUBLE:
		case PhysicalType::INTERVAL:
			node.kind = FixedKind(GetTypeIdSize(type.InternalType()));
			return;
		default:
			throw InternalException("hashfuncs: no nested encoding for type %s", type.ToString());
		}
		node.children.resize(format.children.size());
		for (idx_t i = 0; i < format.children.size(); i++) {
			Plan(node.children[i], format.children[i]);
		}
	}

	// Returns room for `extra` more bytes at the end of the encoding.
	inline data_t *Reserve(idx_t extra) {
		if (size + extra > capacity) {
			Grow(size + extra);
		}
		return buffer.get() + size;
	}
	void Grow(idx_t needed) {
		const idx_t new_capacity = MaxValue<idx_t>(NextPowerOfTwo(needed), 256);
		auto new_buffer = make_unsafe_uniq_array_uninitialized<data_t>(new_capacity);
		if (size > 0) {
			memcpy(new_buffer.get(), buffer.get(), size);
		}
		buffer = std::move(new_buffer);
		capacity = new_capacity;
	}
	static inline data_t *StoreLength(data_t *out, uint64_t length) {
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
		length = __builtin_bswap64(length);
#endif
		memcpy(out, &length, sizeof(length));
		return out + 8;
	}
	template <idx_t WIDTH>
	inline void AppendFixed(const Node &node, idx_t idx, bool marker) {
		auto out = Reserve(WIDTH + 1);
		*out = 1;
		memcpy(out + marker, node.data + idx * WIDTH, WIDTH);
		size += WIDTH + marker;
	}

	// Appends a field; leaf values are encoded inline, nested ones recurse.
	HASHFUNCS_ALWAYS_INLINE void AppendField(const Node &node, idx_t entry) {
		const auto idx = node.sel->get_index(entry);
		if (!node.validity->RowIsValid(idx)) {
			*Reserve(1) = 0;
			size++;
			return;
		}
		if (node.leaf) {
			AppendLeaf(node, idx, true);
		} else {
			AppendValue(node, entry, idx, true);
		}
	}

	// Appends a valid fixed-width, UUID or string value, preceded by the 0x01 field marker when `marker` is set.
	HASHFUNCS_ALWAYS_INLINE void AppendLeaf(const Node &node, idx_t idx, bool marker) {
		switch (node.kind) {
		case Kind::FIXED1:
			return AppendFixed<1>(node, idx, marker);
		case Kind::FIXED2:
			return AppendFixed<2>(node, idx, marker);
		case Kind::FIXED4:
			return AppendFixed<4>(node, idx, marker);
		case Kind::FIXED8:
			return AppendFixed<8>(node, idx, marker);
		case Kind::FIXED16:
			return AppendFixed<16>(node, idx, marker);
		case Kind::UUID: {
			auto out = Reserve(17);
			*out = 1;
			UUID::ToBlob(reinterpret_cast<const hugeint_t *>(node.data)[idx], out + marker);
			size += 16 + marker;
			return;
		}
		case Kind::STRING: {
			const auto &value = reinterpret_cast<const string_t *>(node.data)[idx];
			const auto length = value.GetSize();
			auto out = Reserve(9 + length);
			*out = 1;
			out = StoreLength(out + marker, length);
			memcpy(out, value.GetData(), length);
			size += marker + 8 + length;
			return;
		}
		default:
			throw InternalException("hashfuncs: unexpected leaf encoding kind");
		}
	}

	// Appends a valid nested value, preceded by the 0x01 field marker when `marker` is set.
	void AppendValue(const Node &node, idx_t entry, idx_t idx, bool marker) {
		if (marker) {
			*Reserve(1) = 1;
			size++;
		}
		switch (node.kind) {
		case Kind::STRUCT:
			for (auto &child : node.children) {
				AppendField(child, entry);
			}
			break;
		case Kind::LIST: {
			const auto &list = reinterpret_cast<const list_entry_t *>(node.data)[idx];
			StoreLength(Reserve(8), list.length);
			size += 8;
			for (idx_t i = 0; i < list.length; i++) {
				AppendField(node.children[0], list.offset + i);
			}
			break;
		}
		case Kind::ARRAY: {
			StoreLength(Reserve(8), node.array_size);
			size += 8;
			for (idx_t i = 0; i < node.array_size; i++) {
				AppendField(node.children[0], idx * node.array_size + i);
			}
			break;
		}
		default:
			throw InternalException("hashfuncs: unexpected nested encoding kind");
		}
	}
};

struct NestedReader {
	RecursiveUnifiedVectorFormat format;
	NestedEncoder encoder;

	void Initialize(Vector &input, idx_t count) {
		Vector::RecursiveToUnifiedFormat(input, format);
		encoder.Initialize(format);
	}
	HASHFUNCS_ALWAYS_INLINE bool Lookup(idx_t row, idx_t &idx) const {
		idx = format.unified.sel->get_index(row);
		return format.unified.validity.RowIsValid(idx);
	}
	template <class OP>
	HASHFUNCS_ALWAYS_INLINE typename OP::result_t Hash(idx_t row, idx_t idx, typename OP::seed_t seed) {
		const auto size = encoder.Encode(row);
		return OP::Hash(encoder.Data(), size, seed);
	}
};

// Hashes each row straight from the input vector's memory. DuckDB's Unary/BinaryExecutor pass every value by copy,
// and hashing the bytes of a just-copied short string or integer measured about 20% slower.
template <class OP, class READER, bool SEEDED>
void HashFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	using seed_t = typename OP::seed_t;
	using result_t = typename OP::result_t;

	// Constant arguments are hashed once into a constant result.
	bool all_constant = true;
	for (auto &arg : args.data) {
		all_constant = all_constant && arg.GetVectorType() == VectorType::CONSTANT_VECTOR;
	}
	const idx_t count = all_constant ? 1 : args.size();
	result.SetVectorType(all_constant ? VectorType::CONSTANT_VECTOR : VectorType::FLAT_VECTOR);
	auto results =
	    all_constant ? ConstantVector::GetData<result_t>(result) : FlatVector::GetDataMutable<result_t>(result);

	READER reader;
	reader.Initialize(args.data[0], count);
	UnifiedVectorFormat seed_data;
	if constexpr (SEEDED) {
		args.data[1].ToUnifiedFormat(seed_data);
	}

	for (idx_t i = 0; i < count; i++) {
		idx_t input_idx;
		bool valid = reader.Lookup(i, input_idx);
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
		results[i] = reader.template Hash<OP>(i, input_idx, seed);
	}
}

template <class OP, class READER>
scalar_function_t HashImplementation(bool seeded) {
	if (seeded) {
		return HashFunction<OP, READER, true>;
	}
	return HashFunction<OP, READER, false>;
}

// Picks the implementation for an input type already accepted by HashInputType().
template <class OP>
scalar_function_t GetHashFunction(const LogicalType &type, bool seeded) {
	if (type.id() == LogicalTypeId::UUID) {
		return HashImplementation<OP, UUIDReader>(seeded);
	}
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		return HashImplementation<OP, ValueReader<bool>>(seeded);
	case PhysicalType::INT8:
		return HashImplementation<OP, ValueReader<int8_t>>(seeded);
	case PhysicalType::INT16:
		return HashImplementation<OP, ValueReader<int16_t>>(seeded);
	case PhysicalType::INT32:
		return HashImplementation<OP, ValueReader<int32_t>>(seeded);
	case PhysicalType::INT64:
		return HashImplementation<OP, ValueReader<int64_t>>(seeded);
	case PhysicalType::INT128:
		return HashImplementation<OP, ValueReader<hugeint_t>>(seeded);
	case PhysicalType::UINT8:
		return HashImplementation<OP, ValueReader<uint8_t>>(seeded);
	case PhysicalType::UINT16:
		return HashImplementation<OP, ValueReader<uint16_t>>(seeded);
	case PhysicalType::UINT32:
		return HashImplementation<OP, ValueReader<uint32_t>>(seeded);
	case PhysicalType::UINT64:
		return HashImplementation<OP, ValueReader<uint64_t>>(seeded);
	case PhysicalType::UINT128:
		return HashImplementation<OP, ValueReader<uhugeint_t>>(seeded);
	case PhysicalType::FLOAT:
		return HashImplementation<OP, ValueReader<float>>(seeded);
	case PhysicalType::DOUBLE:
		return HashImplementation<OP, ValueReader<double>>(seeded);
	case PhysicalType::INTERVAL:
		return HashImplementation<OP, ValueReader<interval_t>>(seeded);
	case PhysicalType::VARCHAR:
		return HashImplementation<OP, StringReader>(seeded);
	case PhysicalType::STRUCT:
	case PhysicalType::LIST:
	case PhysicalType::ARRAY:
		return HashImplementation<OP, NestedReader>(seeded);
	default:
		throw InternalException("hashfuncs: no hash implementation for type %s", type.ToString());
	}
}

// Returns the type a value is hashed as, rejecting types without a stable byte representation while the query is
// bound rather than when it runs. NULL literals and ENUMs become VARCHAR, also inside nested types: a NULL literal
// hashes to NULL, and ENUM values hash their label so they match a hash of the same VARCHAR.
LogicalType HashInputType(const string &function_name, const LogicalType &type) {
	if (type.IsUnknown()) {
		throw ParameterNotResolvedException();
	}
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
	case LogicalTypeId::SQLNULL:
	case LogicalTypeId::ENUM:
		return LogicalType::VARCHAR;
	// Nested types keep their type unless a child needs converting, so no cast is added for them.
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::TUPLE: {
		child_list_t<LogicalType> children;
		bool changed = false;
		for (auto &child : StructType::GetChildTypes(type)) {
			children.emplace_back(child.first, HashInputType(function_name, child.second));
			changed = changed || children.back().second != child.second;
		}
		return changed ? LogicalType::STRUCT(std::move(children)) : type;
	}
	case LogicalTypeId::LIST: {
		auto child = HashInputType(function_name, ListType::GetChildType(type));
		return child != ListType::GetChildType(type) ? LogicalType::LIST(child) : type;
	}
	case LogicalTypeId::MAP: {
		auto key = HashInputType(function_name, MapType::KeyType(type));
		auto value = HashInputType(function_name, MapType::ValueType(type));
		return key != MapType::KeyType(type) || value != MapType::ValueType(type) ? LogicalType::MAP(key, value) : type;
	}
	case LogicalTypeId::ARRAY: {
		auto child = HashInputType(function_name, ArrayType::GetChildType(type));
		return child != ArrayType::GetChildType(type) ? LogicalType::ARRAY(child, ArrayType::GetSize(type)) : type;
	}

	default:
		throw BinderException(
		    "%s cannot hash values of type %s. Supported types are BOOLEAN, integers, floating-point, "
		    "DECIMAL, DATE, TIME, TIMESTAMP, INTERVAL, UUID, ENUM, VARCHAR, BLOB, and STRUCT, LIST, "
		    "MAP and ARRAY of those",
		    function_name, type.ToString());
	}
}

// Runs before the arguments are cast, so the input type can be checked and the implementation chosen for it.
template <class OP>
void HashResolveTypes(ResolveScalarFunctionTypesInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto input_type = HashInputType(bound_function.GetName().GetIdentifierName(), input.GetArgumentType(0));
	bound_function.GetArguments()[0] = input_type;
	bound_function.SetFunctionCallback(GetHashFunction<OP>(input_type, input.GetArgumentCount() == 2));
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

// Declares a positional-only parameter with a real name, so duckdb_functions() reports it instead of col<N>.
FunctionParameter Positional(const string &name, LogicalType type) {
	return FunctionParameter(Identifier(name), std::move(type), {}, FunctionParameterKind::POSITIONAL_ONLY);
}

// Whether an algorithm can raise an error while hashing (OP::CAN_THROW); most cannot.
template <class OP, class = void>
struct HashCanThrow : std::false_type {};
template <class OP>
struct HashCanThrow<OP, std::void_t<decltype(OP::CAN_THROW)>> : std::integral_constant<bool, OP::CAN_THROW> {};

// Registers an algorithm as an unseeded and a seeded overload that accept any supported input type. An INVALID
// seed_type registers only the unseeded overload.
template <class OP>
void RegisterHashFunction(ExtensionLoader &loader, const string &name, const LogicalType &result_type,
                          const LogicalType &seed_type, const string &description, const string &seeded_description,
                          const string &seed_name = "seed") {
	const bool has_seed = seed_type.id() != LogicalTypeId::INVALID;
	ScalarFunctionSet set {Identifier(name)};
	auto make_overload = [&](std::initializer_list<FunctionParameter> parameters) {
		ScalarFunction function(parameters, result_type, nullptr);
		function.SetResolveTypesCallback(HashResolveTypes<OP>);
		if (HashCanThrow<OP>::value) {
			function.SetFallible();
		}
		return function;
	};
	set.AddFunction(make_overload({Positional("value", LogicalType::ANY)}));
	if (has_seed) {
		set.AddFunction(make_overload({Positional("value", LogicalType::ANY), Positional(seed_name, seed_type)}));
	}
	CreateScalarFunctionInfo info(set);
	info.descriptions.push_back({/* parameter_types */ {LogicalType::ANY},
	                             /* parameter_names */ {"value"},
	                             /* description */ description,
	                             /* examples */ {name + "('hello')", name + "((1, 'a', [2.5, 3.5]))"},
	                             /* categories */ {"hash"}});
	if (has_seed) {
		info.descriptions.push_back({/* parameter_types */ {LogicalType::ANY, seed_type},
		                             /* parameter_names */ {"value", seed_name},
		                             /* description */ seeded_description,
		                             /* examples */ {name + "('hello', 42)"},
		                             /* categories */ {"hash"}});
	}
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
		xxh3_128_hex_set.AddFunction(
		    ScalarFunction({Positional("value", input_type)}, LogicalType::VARCHAR, XXH3_128HexFunction));
		xxh3_128_hex_set.AddFunction(
		    ScalarFunction({Positional("value", input_type), Positional("seed", LogicalType::UBIGINT)},
		                   LogicalType::VARCHAR, XXH3_128HexFunctionWithSeed));
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

	RegisterHashFunction<WyhashOp>(
	    loader, "wyhash", LogicalType::UBIGINT, LogicalType::UBIGINT,
	    "Computes a 64-bit wyhash (final version 4) non-cryptographic hash of the input",
	    "Computes a 64-bit wyhash (final version 4) non-cryptographic hash of the input with a seed");

	RegisterHashFunction<KomihashOp>(
	    loader, "komihash", LogicalType::UBIGINT, LogicalType::UBIGINT,
	    "Computes a 64-bit komihash (version 5) non-cryptographic hash of the input",
	    "Computes a 64-bit komihash (version 5) non-cryptographic hash of the input with a "
	    "seed");

	RegisterHashFunction<SipHash13Op>(
	    loader, "siphash13", LogicalType::UBIGINT, LogicalType::UHUGEINT,
	    "Computes a 64-bit SipHash-1-3 keyed hash of the input with an all-zero key",
	    "Computes a 64-bit SipHash-1-3 keyed hash of the input. The 128-bit key is the UHUGEINT's 16 little-endian "
	    "bytes; keep it secret to resist hash-flooding",
	    "key");

	RegisterHashFunction<SipHash24Op>(
	    loader, "siphash24", LogicalType::UBIGINT, LogicalType::UHUGEINT,
	    "Computes a 64-bit SipHash-2-4 keyed hash of the input with an all-zero key",
	    "Computes a 64-bit SipHash-2-4 keyed hash of the input. The 128-bit key is the UHUGEINT's 16 little-endian "
	    "bytes; keep it secret to resist hash-flooding",
	    "key");

	RegisterHashFunction<Crc32cOp>(
	    loader, "crc32c", LogicalType::UINTEGER, LogicalType::UINTEGER,
	    "Computes the CRC-32C (Castagnoli) checksum of the input",
	    "Extends a CRC-32C checksum: crc32c(b, crc32c(a)) equals the checksum of a followed by b", "crc");

	RegisterHashFunction<Fnv1a32Op>(loader, "fnv1a_32", LogicalType::UINTEGER, LogicalType::INVALID,
	                                "Computes a 32-bit FNV-1a non-cryptographic hash of the input", "");

	RegisterHashFunction<Fnv1a64Op>(loader, "fnv1a_64", LogicalType::UBIGINT, LogicalType::INVALID,
	                                "Computes a 64-bit FNV-1a non-cryptographic hash of the input", "");

	RegisterInteropFunctions(loader);

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
