#include "hashfuncs_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/common/types/string_type.hpp"
#include "xxhash.h"
#include "rapidhash.h"
#include "MurmurHash3.h"
#include "query_farm_telemetry.hpp"
namespace duckdb {

namespace {
// Hash algorithm enumeration for template specialization
enum class HashAlgorithm {
	XXH32,
	XXH64,
	XXH3_64,
	XXH3_128,
	RAPIDHASH,
	//	RAPIDHASH_MICRO,
	//	RAPIDHASH_NANO,
	MURMURHASH3_32,
	MURMURHASH3_128,
	MURMURHASH3_X64_128
};

// Type trait to map hash algorithm to its seed type
template <HashAlgorithm Algorithm>
struct hash_seed_type {
	// Default case - will cause compilation error for unmapped algorithms
	using type = void;
};

// Specializations for each algorithm
template <>
struct hash_seed_type<HashAlgorithm::XXH32> {
	using type = uint32_t; // XXH32 uses 32-bit seed
};

template <>
struct hash_seed_type<HashAlgorithm::XXH64> {
	using type = uint64_t; // XXH64 uses 64-bit seed
};

template <>
struct hash_seed_type<HashAlgorithm::XXH3_64> {
	using type = uint64_t; // XXH3_64 uses 64-bit seed
};

template <>
struct hash_seed_type<HashAlgorithm::XXH3_128> {
	using type = uint64_t; // XXH3_128 uses 64-bit seed
};

template <>
struct hash_seed_type<HashAlgorithm::RAPIDHASH> {
	using type = uint64_t; // RapidHash typically uses 64-bit seed
};

// template <>
// struct hash_seed_type<HashAlgorithm::RAPIDHASH_MICRO> {
// 	using type = uint64_t; // RapidHash micro variant
// };

// template <>
// struct hash_seed_type<HashAlgorithm::RAPIDHASH_NANO> {
// 	using type = uint64_t; // RapidHash nano variant
// };

template <>
struct hash_seed_type<HashAlgorithm::MURMURHASH3_32> {
	using type = uint32_t; // MurmurHash3 32-bit uses 32-bit seed
};

template <>
struct hash_seed_type<HashAlgorithm::MURMURHASH3_128> {
	using type = uint32_t; // MurmurHash3 128-bit uses 32-bit seed
};

template <>
struct hash_seed_type<HashAlgorithm::MURMURHASH3_X64_128> {
	using type = uint32_t; // MurmurHash3 x64 128-bit uses 32-bit seed
};

template <HashAlgorithm Algorithm>
using hash_seed_type_t = typename hash_seed_type<Algorithm>::type;

template <typename TargetType, typename ResultType, HashAlgorithm Algorithm>
inline void hash_fixed_type_generic_with_seed(const UnifiedVectorFormat &input_vdata, const idx_t row_count,
                                              const Vector &input_vector, const UnifiedVectorFormat &seed_vdata,
                                              const Vector &seed_vector, ValidityMask &result_validity,
                                              ResultType *results) {
	auto inputs = FlatVector::GetData<TargetType>(input_vector);

	using SeedType = hash_seed_type_t<Algorithm>;
	auto seeds = FlatVector::GetData<SeedType>(seed_vector);

	for (idx_t i = 0; i < row_count; i++) {
		if (!input_vdata.validity.RowIsValid(i) || !seed_vdata.validity.RowIsValid(i)) {
			result_validity.SetInvalid(i);
			continue;
		}

		const auto input_idx = input_vdata.sel->get_index(i);
		const auto seed_value = seeds[seed_vdata.sel->get_index(i)];

		if constexpr (Algorithm == HashAlgorithm::XXH32) {
			// 32-bit hash using XXH32
			results[i] = XXH32(&inputs[input_idx], sizeof(TargetType), seed_value);
		} else if constexpr (Algorithm == HashAlgorithm::XXH64) {
			// 64-bit hash using XXH64
			results[i] = XXH64(&inputs[input_idx], sizeof(TargetType), seed_value);
		} else if constexpr (Algorithm == HashAlgorithm::XXH3_64) {
			// 64-bit hash using XXH3
			results[i] = XXH3_64bits_withSeed(&inputs[input_idx], sizeof(TargetType), seed_value);
		} else if constexpr (Algorithm == HashAlgorithm::RAPIDHASH) {
			// 64-bit hash using RapidHash
			results[i] = rapidhash_withSeed(&inputs[input_idx], sizeof(TargetType), seed_value);
			// } else if constexpr (Algorithm == HashAlgorithm::RAPIDHASH_MICRO) {
			// 	// 64-bit hash using RapidHash Micro
			// 	results[i] = rapidhashMicro_withSeed(&inputs[input_idx], sizeof(TargetType), seed_value);
			// } else if constexpr (Algorithm == HashAlgorithm::RAPIDHASH_NANO) {
			// 	// 64-bit hash using RapidHash Nano
			// 	results[i] = rapidhashNano_withSeed(&inputs[input_idx], sizeof(TargetType), seed_value);
		} else if constexpr (Algorithm == HashAlgorithm::MURMURHASH3_32) {
			// 32-bit hash using MurmurHash3
			MurmurHash3_x86_32(&inputs[input_idx], sizeof(TargetType), seed_value, &results[i]);
		} else if constexpr (Algorithm == HashAlgorithm::MURMURHASH3_128) {
			// 128-bit hash using MurmurHash3
			MurmurHash3_x86_128(&inputs[input_idx], sizeof(TargetType), seed_value, &results[i]);
		} else if constexpr (Algorithm == HashAlgorithm::MURMURHASH3_X64_128) {
			// 128-bit hash using MurmurHash3 x64
			MurmurHash3_x64_128(&inputs[input_idx], sizeof(TargetType), seed_value, &results[i]);
		} else if constexpr (Algorithm == HashAlgorithm::XXH3_128) {
			// 128-bit hash
			XXH128_hash_t hash128 = XXH3_128bits_withSeed(&inputs[input_idx], sizeof(TargetType), seed_value);
			results[i] = uhugeint_t {hash128.low64, hash128.high64};
		}
	}
}

// Template function for fixed-size types - now supports all hash algorithms
template <typename TargetType, typename ResultType, HashAlgorithm Algorithm>
inline void hash_fixed_type_generic(const UnifiedVectorFormat &vdata, const idx_t row_count, const Vector &vector,
                                    ValidityMask &result_validity, ResultType *results) {
	auto inputs = FlatVector::GetData<TargetType>(vector);

	for (idx_t i = 0; i < row_count; i++) {
		if (!vdata.validity.RowIsValid(i)) {
			result_validity.SetInvalid(i);
			continue;
		}

		const auto input_idx = vdata.sel->get_index(i);
		if constexpr (Algorithm == HashAlgorithm::XXH32) {
			// 32-bit hash using XXH32
			results[i] = XXH32(&inputs[input_idx], sizeof(TargetType), 0);
		} else if constexpr (Algorithm == HashAlgorithm::XXH64) {
			// 64-bit hash using XXH64
			results[i] = XXH64(&inputs[input_idx], sizeof(TargetType), 0);
		} else if constexpr (Algorithm == HashAlgorithm::XXH3_64) {
			// 64-bit hash using XXH3
			results[i] = XXH3_64bits(&inputs[input_idx], sizeof(TargetType));
		} else if constexpr (Algorithm == HashAlgorithm::RAPIDHASH) {
			// 64-bit hash using RapidHash
			results[i] = rapidhash(&inputs[input_idx], sizeof(TargetType));
			// } else if constexpr (Algorithm == HashAlgorithm::RAPIDHASH_MICRO) {
			// 	// 64-bit hash using RapidHash Micro
			// 	results[i] = rapidhashMicro(&inputs[input_idx], sizeof(TargetType));
			// } else if constexpr (Algorithm == HashAlgorithm::RAPIDHASH_NANO) {
			// 	// 64-bit hash using RapidHash Nano
			// 	results[i] = rapidhashNano(&inputs[input_idx], sizeof(TargetType));
		} else if constexpr (Algorithm == HashAlgorithm::MURMURHASH3_32) {
			// 32-bit hash using MurmurHash3
			MurmurHash3_x86_32(&inputs[input_idx], sizeof(TargetType), 0, &results[i]);
		} else if constexpr (Algorithm == HashAlgorithm::MURMURHASH3_128) {
			// 128-bit hash using MurmurHash3
			MurmurHash3_x86_128(&inputs[input_idx], sizeof(TargetType), 0, &results[i]);
		} else if constexpr (Algorithm == HashAlgorithm::MURMURHASH3_X64_128) {
			// 128-bit hash using MurmurHash3 x64
			MurmurHash3_x64_128(&inputs[input_idx], sizeof(TargetType), 0, &results[i]);
		} else if constexpr (Algorithm == HashAlgorithm::XXH3_128) {
			// 128-bit hash
			XXH128_hash_t hash128 = XXH3_128bits(&inputs[input_idx], sizeof(TargetType));
			results[i] = uhugeint_t {hash128.low64, hash128.high64};
		}
	}
}

// Generic hash function template
template <typename ResultType, HashAlgorithm Algorithm>
inline void hashfunc_generic(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input_vector = args.data[0];
	const auto row_count = args.size();

	// Early return for empty chunks
	if (row_count == 0) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		return;
	}

	UnifiedVectorFormat vdata;
	input_vector.ToUnifiedFormat(row_count, vdata);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_validity = FlatVector::Validity(result);
	auto results = FlatVector::GetData<ResultType>(result);

	const auto type_id = input_vector.GetType().id();

	switch (type_id) {
	case LogicalTypeId::BLOB:
	case LogicalTypeId::VARCHAR: {
		auto inputs = FlatVector::GetData<string_t>(input_vector);
		for (idx_t i = 0; i < row_count; i++) {
			if (!vdata.validity.RowIsValid(i)) {
				result_validity.SetInvalid(i);
				continue;
			}
			const auto &str = inputs[vdata.sel->get_index(i)];

			if constexpr (Algorithm == HashAlgorithm::XXH32) {
				// 32-bit hash using XXH32
				results[i] = XXH32(str.GetData(), str.GetSize(), 0);
			} else if constexpr (Algorithm == HashAlgorithm::XXH64) {
				// 64-bit hash using XXH64
				results[i] = XXH64(str.GetData(), str.GetSize(), 0);
			} else if constexpr (Algorithm == HashAlgorithm::XXH3_64) {
				results[i] = XXH3_64bits(str.GetData(), str.GetSize());
			} else if constexpr (Algorithm == HashAlgorithm::RAPIDHASH) {
				results[i] = rapidhash(str.GetData(), str.GetSize());
				// } else if constexpr (Algorithm == HashAlgorithm::RAPIDHASH_MICRO) {
				// 	results[i] = rapidhashMicro(str.GetData(), str.GetSize());
				// } else if constexpr (Algorithm == HashAlgorithm::RAPIDHASH_NANO) {
				// 	results[i] = rapidhashNano(str.GetData(), str.GetSize());
			} else if constexpr (Algorithm == HashAlgorithm::MURMURHASH3_32) {
				// 32-bit hash using MurmurHash3
				MurmurHash3_x86_32(str.GetData(), str.GetSize(), 0, &results[i]);
			} else if constexpr (Algorithm == HashAlgorithm::MURMURHASH3_128) {
				// 128-bit hash using MurmurHash3
				MurmurHash3_x86_128(str.GetData(), str.GetSize(), 0, &results[i]);
			} else if constexpr (Algorithm == HashAlgorithm::MURMURHASH3_X64_128) {
				// 128-bit hash using MurmurHash3 x64
				MurmurHash3_x64_128(str.GetData(), str.GetSize(), 0, &results[i]);
			} else if constexpr (Algorithm == HashAlgorithm::XXH3_128) {
				// 128-bit hash
				XXH128_hash_t hash128 = XXH3_128bits(str.GetData(), str.GetSize());
				results[i] = uhugeint_t {hash128.low64, hash128.high64};
			}
		}
		break;
	}

	case LogicalTypeId::HUGEINT:
		hash_fixed_type_generic<hugeint_t, ResultType, Algorithm>(vdata, row_count, input_vector, result_validity,
		                                                          results);
		break;

	case LogicalTypeId::UHUGEINT:
		hash_fixed_type_generic<uhugeint_t, ResultType, Algorithm>(vdata, row_count, input_vector, result_validity,
		                                                           results);
		break;

	case LogicalTypeId::USMALLINT:
		hash_fixed_type_generic<uint16_t, ResultType, Algorithm>(vdata, row_count, input_vector, result_validity,
		                                                         results);
		break;

	case LogicalTypeId::UINTEGER:
		hash_fixed_type_generic<uint32_t, ResultType, Algorithm>(vdata, row_count, input_vector, result_validity,
		                                                         results);
		break;

	case LogicalTypeId::INTEGER:
		hash_fixed_type_generic<int32_t, ResultType, Algorithm>(vdata, row_count, input_vector, result_validity,
		                                                        results);
		break;

	case LogicalTypeId::BIGINT:
		hash_fixed_type_generic<int64_t, ResultType, Algorithm>(vdata, row_count, input_vector, result_validity,
		                                                        results);
		break;

	case LogicalTypeId::UBIGINT:
		hash_fixed_type_generic<uint64_t, ResultType, Algorithm>(vdata, row_count, input_vector, result_validity,
		                                                         results);
		break;

	case LogicalTypeId::SMALLINT:
		hash_fixed_type_generic<int16_t, ResultType, Algorithm>(vdata, row_count, input_vector, result_validity,
		                                                        results);
		break;

	case LogicalTypeId::UTINYINT:
		hash_fixed_type_generic<uint8_t, ResultType, Algorithm>(vdata, row_count, input_vector, result_validity,
		                                                        results);
		break;

	case LogicalTypeId::TINYINT:
		hash_fixed_type_generic<int8_t, ResultType, Algorithm>(vdata, row_count, input_vector, result_validity,
		                                                       results);
		break;

	case LogicalTypeId::FLOAT:
		hash_fixed_type_generic<float, ResultType, Algorithm>(vdata, row_count, input_vector, result_validity, results);
		break;

	case LogicalTypeId::DOUBLE:
		hash_fixed_type_generic<double, ResultType, Algorithm>(vdata, row_count, input_vector, result_validity,
		                                                       results);
		break;

	case LogicalTypeId::DATE:
		hash_fixed_type_generic<uint32_t, ResultType, Algorithm>(vdata, row_count, input_vector, result_validity,
		                                                         results);
		break;

	case LogicalTypeId::TIME:
		hash_fixed_type_generic<uint64_t, ResultType, Algorithm>(vdata, row_count, input_vector, result_validity,
		                                                         results);
		break;

	default:
		throw NotImplementedException("Unsupported type for XXH hash: " + LogicalType(type_id).ToString());
	}

	// Optimize for single-row results
	if (row_count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

template <typename ResultType, HashAlgorithm Algorithm>
inline void hashfunc_generic_with_seed(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input_vector = args.data[0];
	auto &seed_vector = args.data[1];
	const auto row_count = args.size();

	// Early return for empty chunks
	if (row_count == 0) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		return;
	}

	UnifiedVectorFormat input_vdata;
	input_vector.ToUnifiedFormat(row_count, input_vdata);

	UnifiedVectorFormat seed_vdata;
	seed_vector.ToUnifiedFormat(row_count, seed_vdata);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_validity = FlatVector::Validity(result);
	auto results = FlatVector::GetData<ResultType>(result);

	const auto type_id = input_vector.GetType().id();

	switch (type_id) {
	case LogicalTypeId::BLOB:
	case LogicalTypeId::VARCHAR: {
		auto inputs = FlatVector::GetData<string_t>(input_vector);

		auto seeds = FlatVector::GetData<hash_seed_type_t<Algorithm>>(seed_vector);
		for (idx_t i = 0; i < row_count; i++) {
			if (!input_vdata.validity.RowIsValid(i) || !seed_vdata.validity.RowIsValid(i)) {
				result_validity.SetInvalid(i);
				continue;
			}
			const auto &str = inputs[input_vdata.sel->get_index(i)];

			const auto seed_value = seeds[seed_vdata.sel->get_index(i)];

			if constexpr (Algorithm == HashAlgorithm::XXH32) {
				// 32-bit hash using XXH32
				results[i] = XXH32(str.GetData(), str.GetSize(), seed_value);
			} else if constexpr (Algorithm == HashAlgorithm::XXH64) {
				// 64-bit hash using XXH64
				results[i] = XXH64(str.GetData(), str.GetSize(), seed_value);
			} else if constexpr (Algorithm == HashAlgorithm::XXH3_64) {
				results[i] = XXH3_64bits_withSeed(str.GetData(), str.GetSize(), seed_value);
			} else if constexpr (Algorithm == HashAlgorithm::RAPIDHASH) {
				results[i] = rapidhash_withSeed(str.GetData(), str.GetSize(), seed_value);
				// } else if constexpr (Algorithm == HashAlgorithm::RAPIDHASH_MICRO) {
				// 	results[i] = rapidhashMicro_withSeed(str.GetData(), str.GetSize(), seed_value);
				// } else if constexpr (Algorithm == HashAlgorithm::RAPIDHASH_NANO) {
				// 	results[i] = rapidhashNano_withSeed(str.GetData(), str.GetSize(), seed_value);
			} else if constexpr (Algorithm == HashAlgorithm::MURMURHASH3_32) {
				// 32-bit hash using MurmurHash3
				MurmurHash3_x86_32(str.GetData(), str.GetSize(), seed_value, &results[i]);
			} else if constexpr (Algorithm == HashAlgorithm::MURMURHASH3_128) {
				// 128-bit hash using MurmurHash3
				MurmurHash3_x86_128(str.GetData(), str.GetSize(), seed_value, &results[i]);
			} else if constexpr (Algorithm == HashAlgorithm::MURMURHASH3_X64_128) {
				// 128-bit hash using MurmurHash3 x64
				MurmurHash3_x64_128(str.GetData(), str.GetSize(), seed_value, &results[i]);
			} else if constexpr (Algorithm == HashAlgorithm::XXH3_128) {
				// 128-bit hash
				XXH128_hash_t hash128 = XXH3_128bits_withSeed(str.GetData(), str.GetSize(), seed_value);
				results[i] = uhugeint_t {hash128.low64, hash128.high64};
			}
		}
		break;
	}

	case LogicalTypeId::HUGEINT:
		hash_fixed_type_generic_with_seed<hugeint_t, ResultType, Algorithm>(
		    input_vdata, row_count, input_vector, seed_vdata, seed_vector, result_validity, results);
		break;

	case LogicalTypeId::UHUGEINT:
		hash_fixed_type_generic_with_seed<uhugeint_t, ResultType, Algorithm>(
		    input_vdata, row_count, input_vector, seed_vdata, seed_vector, result_validity, results);
		break;

	case LogicalTypeId::USMALLINT:
		hash_fixed_type_generic_with_seed<uint16_t, ResultType, Algorithm>(
		    input_vdata, row_count, input_vector, seed_vdata, seed_vector, result_validity, results);
		break;

	case LogicalTypeId::UINTEGER:
		hash_fixed_type_generic_with_seed<uint32_t, ResultType, Algorithm>(
		    input_vdata, row_count, input_vector, seed_vdata, seed_vector, result_validity, results);
		break;

	case LogicalTypeId::INTEGER:
		hash_fixed_type_generic_with_seed<int32_t, ResultType, Algorithm>(
		    input_vdata, row_count, input_vector, seed_vdata, seed_vector, result_validity, results);
		break;

	case LogicalTypeId::BIGINT:
		hash_fixed_type_generic_with_seed<int64_t, ResultType, Algorithm>(
		    input_vdata, row_count, input_vector, seed_vdata, seed_vector, result_validity, results);
		break;

	case LogicalTypeId::UBIGINT:
		hash_fixed_type_generic_with_seed<uint64_t, ResultType, Algorithm>(
		    input_vdata, row_count, input_vector, seed_vdata, seed_vector, result_validity, results);
		break;

	case LogicalTypeId::SMALLINT:
		hash_fixed_type_generic_with_seed<int16_t, ResultType, Algorithm>(
		    input_vdata, row_count, input_vector, seed_vdata, seed_vector, result_validity, results);
		break;

	case LogicalTypeId::UTINYINT:
		hash_fixed_type_generic_with_seed<uint8_t, ResultType, Algorithm>(
		    input_vdata, row_count, input_vector, seed_vdata, seed_vector, result_validity, results);
		break;

	case LogicalTypeId::TINYINT:
		hash_fixed_type_generic_with_seed<int8_t, ResultType, Algorithm>(
		    input_vdata, row_count, input_vector, seed_vdata, seed_vector, result_validity, results);
		break;

	case LogicalTypeId::FLOAT:
		hash_fixed_type_generic_with_seed<float, ResultType, Algorithm>(
		    input_vdata, row_count, input_vector, seed_vdata, seed_vector, result_validity, results);
		break;

	case LogicalTypeId::DOUBLE:
		hash_fixed_type_generic_with_seed<double, ResultType, Algorithm>(
		    input_vdata, row_count, input_vector, seed_vdata, seed_vector, result_validity, results);
		break;

	case LogicalTypeId::DATE:
		hash_fixed_type_generic_with_seed<uint32_t, ResultType, Algorithm>(
		    input_vdata, row_count, input_vector, seed_vdata, seed_vector, result_validity, results);
		break;

	case LogicalTypeId::TIME:
		hash_fixed_type_generic_with_seed<uint64_t, ResultType, Algorithm>(
		    input_vdata, row_count, input_vector, seed_vdata, seed_vector, result_validity, results);
		break;

	default:
		throw NotImplementedException("Unsupported type for XXH hash: " + LogicalType(type_id).ToString());
	}

	// Optimize for single-row results
	if (row_count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

// 32-bit hash function using XXH32
inline void hashfunc_XXH32(DataChunk &args, ExpressionState &state, Vector &result) {
	hashfunc_generic<uint32_t, HashAlgorithm::XXH32>(args, state, result);
}

inline void hashfunc_XXH32_with_seed(DataChunk &args, ExpressionState &state, Vector &result) {
	hashfunc_generic_with_seed<uint32_t, HashAlgorithm::XXH32>(args, state, result);
}

// 64-bit hash function using XXH64
inline void hashfunc_XXH64(DataChunk &args, ExpressionState &state, Vector &result) {
	hashfunc_generic<uint64_t, HashAlgorithm::XXH64>(args, state, result);
}

inline void hashfunc_XXH64_with_seed(DataChunk &args, ExpressionState &state, Vector &result) {
	hashfunc_generic_with_seed<uint64_t, HashAlgorithm::XXH64>(args, state, result);
}

// Your existing 64-bit hash function using XXH3 (now uses the generic template)
inline void hashfunc_XXH3_64(DataChunk &args, ExpressionState &state, Vector &result) {
	hashfunc_generic<uint64_t, HashAlgorithm::XXH3_64>(args, state, result);
}

// 64-bit hash function using XXH3 with seed
inline void hashfunc_XXH3_64_with_seed(DataChunk &args, ExpressionState &state, Vector &result) {
	hashfunc_generic_with_seed<uint64_t, HashAlgorithm::XXH3_64>(args, state, result);
}

// 128-bit hash function using uhugeint_t
inline void hashfunc_XXH3_128(DataChunk &args, ExpressionState &state, Vector &result) {
	hashfunc_generic<uhugeint_t, HashAlgorithm::XXH3_128>(args, state, result);
}

// 128-bit hash function using XXH3 with seed
inline void hashfunc_XXH3_128_with_seed(DataChunk &args, ExpressionState &state, Vector &result) {
	hashfunc_generic_with_seed<uhugeint_t, HashAlgorithm::XXH3_128>(args, state, result);
}

inline void hashfunc_rapidhash(DataChunk &args, ExpressionState &state, Vector &result) {
	hashfunc_generic<uint64_t, HashAlgorithm::RAPIDHASH>(args, state, result);
}

inline void hashfunc_rapidhash_with_seed(DataChunk &args, ExpressionState &state, Vector &result) {
	hashfunc_generic_with_seed<uint64_t, HashAlgorithm::RAPIDHASH>(args, state, result);
}

// inline void hashfunc_rapidhashMicro(DataChunk &args, ExpressionState &state, Vector &result) {
// 	hashfunc_generic<uint64_t, HashAlgorithm::RAPIDHASH_MICRO>(args, state, result);
// }

// inline void hashfunc_rapidhashMicro_with_seed(DataChunk &args, ExpressionState &state, Vector &result) {
// 	hashfunc_generic_with_seed<uint64_t, HashAlgorithm::RAPIDHASH_MICRO>(args, state, result);
// }

// inline void hashfunc_rapidhashNano(DataChunk &args, ExpressionState &state, Vector &result) {
// 	hashfunc_generic<uint64_t, HashAlgorithm::RAPIDHASH_NANO>(args, state, result);
// }

// inline void hashfunc_rapidhashNano_with_seed(DataChunk &args, ExpressionState &state, Vector &result) {
// 	hashfunc_generic_with_seed<uint64_t, HashAlgorithm::RAPIDHASH_NANO>(args, state, result);
// }

inline void hashfunc_MurmurHash3_32(DataChunk &args, ExpressionState &state, Vector &result) {
	hashfunc_generic<uint32_t, HashAlgorithm::MURMURHASH3_32>(args, state, result);
}

inline void hashfunc_MurmurHash3_32_with_seed(DataChunk &args, ExpressionState &state, Vector &result) {
	hashfunc_generic_with_seed<uint32_t, HashAlgorithm::MURMURHASH3_32>(args, state, result);
}

inline void hashfunc_MurmurHash3_128(DataChunk &args, ExpressionState &state, Vector &result) {
	hashfunc_generic<uhugeint_t, HashAlgorithm::MURMURHASH3_128>(args, state, result);
}

inline void hashfunc_MurmurHash3_128_with_seed(DataChunk &args, ExpressionState &state, Vector &result) {
	hashfunc_generic_with_seed<uhugeint_t, HashAlgorithm::MURMURHASH3_128>(args, state, result);
}

inline void hashfunc_MurmurHash3_X64_128(DataChunk &args, ExpressionState &state, Vector &result) {
	hashfunc_generic<uhugeint_t, HashAlgorithm::MURMURHASH3_X64_128>(args, state, result);
}

inline void hashfunc_MurmurHash3_X64_128_with_seed(DataChunk &args, ExpressionState &state, Vector &result) {
	hashfunc_generic_with_seed<uhugeint_t, HashAlgorithm::MURMURHASH3_X64_128>(args, state, result);
}

} // namespace

static void LoadInternal(ExtensionLoader &loader) {

	// XXH32 - 32-bit xxHash
	ScalarFunctionSet xxh32_set("xxh32");
	xxh32_set.AddFunction(ScalarFunction({LogicalType::ANY}, LogicalType::UINTEGER, hashfunc_XXH32));
	xxh32_set.AddFunction(
	    ScalarFunction({LogicalType::ANY, LogicalType::UINTEGER}, LogicalType::UINTEGER, hashfunc_XXH32_with_seed));
	CreateScalarFunctionInfo xxh32_info(xxh32_set);
	xxh32_info.descriptions.push_back(
	    {/* parameter_types */ {LogicalType::ANY},
	     /* parameter_names */ {"value"},
	     /* description */ "Computes a 32-bit xxHash (XXH32) non-cryptographic hash of the input",
	     /* examples */ {"xxh32('hello')"},
	     /* categories */ {"hash"}});
	xxh32_info.descriptions.push_back(
	    {/* parameter_types */ {LogicalType::ANY, LogicalType::UINTEGER},
	     /* parameter_names */ {"value", "seed"},
	     /* description */ "Computes a 32-bit xxHash (XXH32) non-cryptographic hash of the input with a seed",
	     /* examples */ {"xxh32('hello', 42)"},
	     /* categories */ {"hash"}});
	loader.RegisterFunction(xxh32_info);

	// XXH64 - 64-bit xxHash
	ScalarFunctionSet xxh64_set("xxh64");
	xxh64_set.AddFunction(ScalarFunction({LogicalType::ANY}, LogicalType::UBIGINT, hashfunc_XXH64));
	xxh64_set.AddFunction(
	    ScalarFunction({LogicalType::ANY, LogicalType::UBIGINT}, LogicalType::UBIGINT, hashfunc_XXH64_with_seed));
	CreateScalarFunctionInfo xxh64_info(xxh64_set);
	xxh64_info.descriptions.push_back(
	    {/* parameter_types */ {LogicalType::ANY},
	     /* parameter_names */ {"value"},
	     /* description */ "Computes a 64-bit xxHash (XXH64) non-cryptographic hash of the input",
	     /* examples */ {"xxh64('hello')"},
	     /* categories */ {"hash"}});
	xxh64_info.descriptions.push_back(
	    {/* parameter_types */ {LogicalType::ANY, LogicalType::UBIGINT},
	     /* parameter_names */ {"value", "seed"},
	     /* description */ "Computes a 64-bit xxHash (XXH64) non-cryptographic hash of the input with a seed",
	     /* examples */ {"xxh64('hello', 42)"},
	     /* categories */ {"hash"}});
	loader.RegisterFunction(xxh64_info);

	// XXH3_64 - 64-bit xxHash3 (faster than XXH64 for short inputs)
	ScalarFunctionSet xxh3_64_set("xxh3_64");
	xxh3_64_set.AddFunction(ScalarFunction({LogicalType::ANY}, LogicalType::UBIGINT, hashfunc_XXH3_64));
	xxh3_64_set.AddFunction(
	    ScalarFunction({LogicalType::ANY, LogicalType::UBIGINT}, LogicalType::UBIGINT, hashfunc_XXH3_64_with_seed));
	CreateScalarFunctionInfo xxh3_64_info(xxh3_64_set);
	xxh3_64_info.descriptions.push_back(
	    {/* parameter_types */ {LogicalType::ANY},
	     /* parameter_names */ {"value"},
	     /* description */
	     "Computes a 64-bit xxHash3 (XXH3_64) non-cryptographic hash of the input. Faster than XXH64 for short inputs",
	     /* examples */ {"xxh3_64('hello')"},
	     /* categories */ {"hash"}});
	xxh3_64_info.descriptions.push_back(
	    {/* parameter_types */ {LogicalType::ANY, LogicalType::UBIGINT},
	     /* parameter_names */ {"value", "seed"},
	     /* description */ "Computes a 64-bit xxHash3 (XXH3_64) non-cryptographic hash of the input with a seed",
	     /* examples */ {"xxh3_64('hello', 42)"},
	     /* categories */ {"hash"}});
	loader.RegisterFunction(xxh3_64_info);

	// XXH3_128 - 128-bit xxHash3
	ScalarFunctionSet xxh3_128_set("xxh3_128");
	xxh3_128_set.AddFunction(ScalarFunction({LogicalType::ANY}, LogicalType::UHUGEINT, hashfunc_XXH3_128));
	xxh3_128_set.AddFunction(
	    ScalarFunction({LogicalType::ANY, LogicalType::UBIGINT}, LogicalType::UHUGEINT, hashfunc_XXH3_128_with_seed));
	CreateScalarFunctionInfo xxh3_128_info(xxh3_128_set);
	xxh3_128_info.descriptions.push_back(
	    {/* parameter_types */ {LogicalType::ANY},
	     /* parameter_names */ {"value"},
	     /* description */ "Computes a 128-bit xxHash3 (XXH3_128) non-cryptographic hash of the input",
	     /* examples */ {"xxh3_128('hello')"},
	     /* categories */ {"hash"}});
	xxh3_128_info.descriptions.push_back(
	    {/* parameter_types */ {LogicalType::ANY, LogicalType::UBIGINT},
	     /* parameter_names */ {"value", "seed"},
	     /* description */ "Computes a 128-bit xxHash3 (XXH3_128) non-cryptographic hash of the input with a seed",
	     /* examples */ {"xxh3_128('hello', 42)"},
	     /* categories */ {"hash"}});
	loader.RegisterFunction(xxh3_128_info);

	// RapidHash - fast 64-bit hash
	ScalarFunctionSet rapidhash_set("rapidhash");
	rapidhash_set.AddFunction(ScalarFunction({LogicalType::ANY}, LogicalType::UBIGINT, hashfunc_rapidhash));
	rapidhash_set.AddFunction(
	    ScalarFunction({LogicalType::ANY, LogicalType::UBIGINT}, LogicalType::UBIGINT, hashfunc_rapidhash_with_seed));
	CreateScalarFunctionInfo rapidhash_info(rapidhash_set);
	rapidhash_info.descriptions.push_back(
	    {/* parameter_types */ {LogicalType::ANY},
	     /* parameter_names */ {"value"},
	     /* description */
	     "Computes a 64-bit RapidHash non-cryptographic hash of the input. Very fast for all input sizes",
	     /* examples */ {"rapidhash('hello')"},
	     /* categories */ {"hash"}});
	rapidhash_info.descriptions.push_back(
	    {/* parameter_types */ {LogicalType::ANY, LogicalType::UBIGINT},
	     /* parameter_names */ {"value", "seed"},
	     /* description */ "Computes a 64-bit RapidHash non-cryptographic hash of the input with a seed",
	     /* examples */ {"rapidhash('hello', 42)"},
	     /* categories */ {"hash"}});
	loader.RegisterFunction(rapidhash_info);

	// MurmurHash3 32-bit
	ScalarFunctionSet murmurhash3_32_set("murmurhash3_32");
	murmurhash3_32_set.AddFunction(ScalarFunction({LogicalType::ANY}, LogicalType::UINTEGER, hashfunc_MurmurHash3_32));
	murmurhash3_32_set.AddFunction(ScalarFunction({LogicalType::ANY, LogicalType::UINTEGER}, LogicalType::UINTEGER,
	                                              hashfunc_MurmurHash3_32_with_seed));
	CreateScalarFunctionInfo murmurhash3_32_info(murmurhash3_32_set);
	murmurhash3_32_info.descriptions.push_back(
	    {/* parameter_types */ {LogicalType::ANY},
	     /* parameter_names */ {"value"},
	     /* description */ "Computes a 32-bit MurmurHash3 non-cryptographic hash of the input",
	     /* examples */ {"murmurhash3_32('hello')"},
	     /* categories */ {"hash"}});
	murmurhash3_32_info.descriptions.push_back(
	    {/* parameter_types */ {LogicalType::ANY, LogicalType::UINTEGER},
	     /* parameter_names */ {"value", "seed"},
	     /* description */ "Computes a 32-bit MurmurHash3 non-cryptographic hash of the input with a seed",
	     /* examples */ {"murmurhash3_32('hello', 42)"},
	     /* categories */ {"hash"}});
	loader.RegisterFunction(murmurhash3_32_info);

	// MurmurHash3 128-bit (x86 variant)
	ScalarFunctionSet murmurhash3_128_set("murmurhash3_128");
	murmurhash3_128_set.AddFunction(
	    ScalarFunction({LogicalType::ANY}, LogicalType::UHUGEINT, hashfunc_MurmurHash3_128));
	murmurhash3_128_set.AddFunction(ScalarFunction({LogicalType::ANY, LogicalType::UINTEGER}, LogicalType::UHUGEINT,
	                                               hashfunc_MurmurHash3_128_with_seed));
	CreateScalarFunctionInfo murmurhash3_128_info(murmurhash3_128_set);
	murmurhash3_128_info.descriptions.push_back(
	    {/* parameter_types */ {LogicalType::ANY},
	     /* parameter_names */ {"value"},
	     /* description */ "Computes a 128-bit MurmurHash3 (x86 variant) non-cryptographic hash of the input",
	     /* examples */ {"murmurhash3_128('hello')"},
	     /* categories */ {"hash"}});
	murmurhash3_128_info.descriptions.push_back(
	    {/* parameter_types */ {LogicalType::ANY, LogicalType::UINTEGER},
	     /* parameter_names */ {"value", "seed"},
	     /* description */ "Computes a 128-bit MurmurHash3 (x86 variant) non-cryptographic hash of the input with a seed",
	     /* examples */ {"murmurhash3_128('hello', 42)"},
	     /* categories */ {"hash"}});
	loader.RegisterFunction(murmurhash3_128_info);

	// MurmurHash3 128-bit (x64 variant - optimized for 64-bit platforms)
	ScalarFunctionSet murmurhash3_x64_128_set("murmurhash3_x64_128");
	murmurhash3_x64_128_set.AddFunction(
	    ScalarFunction({LogicalType::ANY}, LogicalType::UHUGEINT, hashfunc_MurmurHash3_X64_128));
	murmurhash3_x64_128_set.AddFunction(ScalarFunction({LogicalType::ANY, LogicalType::UINTEGER}, LogicalType::UHUGEINT,
	                                                   hashfunc_MurmurHash3_X64_128_with_seed));
	CreateScalarFunctionInfo murmurhash3_x64_128_info(murmurhash3_x64_128_set);
	murmurhash3_x64_128_info.descriptions.push_back(
	    {/* parameter_types */ {LogicalType::ANY},
	     /* parameter_names */ {"value"},
	     /* description */
	     "Computes a 128-bit MurmurHash3 (x64 variant) non-cryptographic hash of the input. Optimized for 64-bit "
	     "platforms",
	     /* examples */ {"murmurhash3_x64_128('hello')"},
	     /* categories */ {"hash"}});
	murmurhash3_x64_128_info.descriptions.push_back(
	    {/* parameter_types */ {LogicalType::ANY, LogicalType::UINTEGER},
	     /* parameter_names */ {"value", "seed"},
	     /* description */
	     "Computes a 128-bit MurmurHash3 (x64 variant) non-cryptographic hash of the input with a seed",
	     /* examples */ {"murmurhash3_x64_128('hello', 42)"},
	     /* categories */ {"hash"}});
	loader.RegisterFunction(murmurhash3_x64_128_info);

	QueryFarmSendTelemetry(loader, "hashfuncs", "2025120402");
}

void HashfuncsExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string HashfuncsExtension::Name() {
	return "hashfuncs";
}

std::string HashfuncsExtension::Version() const {
	return "2025120402";
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(hashfuncs, loader) {
	duckdb::LoadInternal(loader);
}
}
