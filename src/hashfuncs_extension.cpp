#include "hashfuncs_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/common/types/string_type.hpp"
#include "xxhash.h"
#include "rapidhash.h"
#include "MurmurHash3.h"

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

		if constexpr (Algorithm == HashAlgorithm::XXH32) {
			// 32-bit hash using XXH32
			results[i] = XXH32(&inputs[i], sizeof(TargetType), seeds[i]);
		} else if constexpr (Algorithm == HashAlgorithm::XXH64) {
			// 64-bit hash using XXH64
			results[i] = XXH64(&inputs[i], sizeof(TargetType), seeds[i]);
		} else if constexpr (Algorithm == HashAlgorithm::XXH3_64) {
			// 64-bit hash using XXH3
			results[i] = XXH3_64bits_withSeed(&inputs[i], sizeof(TargetType), seeds[i]);
		} else if constexpr (Algorithm == HashAlgorithm::RAPIDHASH) {
			// 64-bit hash using RapidHash
			results[i] = rapidhash_withSeed(&inputs[i], sizeof(TargetType), seeds[i]);
			// } else if constexpr (Algorithm == HashAlgorithm::RAPIDHASH_MICRO) {
			// 	// 64-bit hash using RapidHash Micro
			// 	results[i] = rapidhashMicro_withSeed(&inputs[i], sizeof(TargetType), seeds[i]);
			// } else if constexpr (Algorithm == HashAlgorithm::RAPIDHASH_NANO) {
			// 	// 64-bit hash using RapidHash Nano
			// 	results[i] = rapidhashNano_withSeed(&inputs[i], sizeof(TargetType), seeds[i]);
		} else if constexpr (Algorithm == HashAlgorithm::MURMURHASH3_32) {
			// 32-bit hash using MurmurHash3
			MurmurHash3_x86_32(&inputs[i], sizeof(TargetType), seeds[i], &results[i]);
		} else if constexpr (Algorithm == HashAlgorithm::MURMURHASH3_128) {
			// 128-bit hash using MurmurHash3
			MurmurHash3_x86_128(&inputs[i], sizeof(TargetType), seeds[i], &results[i]);
		} else if constexpr (Algorithm == HashAlgorithm::MURMURHASH3_X64_128) {
			// 128-bit hash using MurmurHash3 x64
			MurmurHash3_x64_128(&inputs[i], sizeof(TargetType), seeds[i], &results[i]);
		} else if constexpr (Algorithm == HashAlgorithm::XXH3_128) {
			// 128-bit hash
			XXH128_hash_t hash128 = XXH3_128bits_withSeed(&inputs[i], sizeof(TargetType), seeds[i]);
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

		if constexpr (Algorithm == HashAlgorithm::XXH32) {
			// 32-bit hash using XXH32
			results[i] = XXH32(&inputs[i], sizeof(TargetType), 0);
		} else if constexpr (Algorithm == HashAlgorithm::XXH64) {
			// 64-bit hash using XXH64
			results[i] = XXH64(&inputs[i], sizeof(TargetType), 0);
		} else if constexpr (Algorithm == HashAlgorithm::XXH3_64) {
			// 64-bit hash using XXH3
			results[i] = XXH3_64bits(&inputs[i], sizeof(TargetType));
		} else if constexpr (Algorithm == HashAlgorithm::RAPIDHASH) {
			// 64-bit hash using RapidHash
			results[i] = rapidhash(&inputs[i], sizeof(TargetType));
			// } else if constexpr (Algorithm == HashAlgorithm::RAPIDHASH_MICRO) {
			// 	// 64-bit hash using RapidHash Micro
			// 	results[i] = rapidhashMicro(&inputs[i], sizeof(TargetType));
			// } else if constexpr (Algorithm == HashAlgorithm::RAPIDHASH_NANO) {
			// 	// 64-bit hash using RapidHash Nano
			// 	results[i] = rapidhashNano(&inputs[i], sizeof(TargetType));
		} else if constexpr (Algorithm == HashAlgorithm::MURMURHASH3_32) {
			// 32-bit hash using MurmurHash3
			MurmurHash3_x86_32(&inputs[i], sizeof(TargetType), 0, &results[i]);
		} else if constexpr (Algorithm == HashAlgorithm::MURMURHASH3_128) {
			// 128-bit hash using MurmurHash3
			MurmurHash3_x86_128(&inputs[i], sizeof(TargetType), 0, &results[i]);
		} else if constexpr (Algorithm == HashAlgorithm::MURMURHASH3_X64_128) {
			// 128-bit hash using MurmurHash3 x64
			MurmurHash3_x64_128(&inputs[i], sizeof(TargetType), 0, &results[i]);
		} else if constexpr (Algorithm == HashAlgorithm::XXH3_128) {
			// 128-bit hash
			XXH128_hash_t hash128 = XXH3_128bits(&inputs[i], sizeof(TargetType));
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
			const auto &str = inputs[i];

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
			const auto &str = inputs[i];

			if constexpr (Algorithm == HashAlgorithm::XXH32) {
				// 32-bit hash using XXH32
				results[i] = XXH32(str.GetData(), str.GetSize(), seeds[i]);
			} else if constexpr (Algorithm == HashAlgorithm::XXH64) {
				// 64-bit hash using XXH64
				results[i] = XXH64(str.GetData(), str.GetSize(), seeds[i]);
			} else if constexpr (Algorithm == HashAlgorithm::XXH3_64) {
				results[i] = XXH3_64bits_withSeed(str.GetData(), str.GetSize(), seeds[i]);
			} else if constexpr (Algorithm == HashAlgorithm::RAPIDHASH) {
				results[i] = rapidhash_withSeed(str.GetData(), str.GetSize(), seeds[i]);
				// } else if constexpr (Algorithm == HashAlgorithm::RAPIDHASH_MICRO) {
				// 	results[i] = rapidhashMicro_withSeed(str.GetData(), str.GetSize(), seeds[i]);
				// } else if constexpr (Algorithm == HashAlgorithm::RAPIDHASH_NANO) {
				// 	results[i] = rapidhashNano_withSeed(str.GetData(), str.GetSize(), seeds[i]);
			} else if constexpr (Algorithm == HashAlgorithm::MURMURHASH3_32) {
				// 32-bit hash using MurmurHash3
				MurmurHash3_x86_32(str.GetData(), str.GetSize(), seeds[i], &results[i]);
			} else if constexpr (Algorithm == HashAlgorithm::MURMURHASH3_128) {
				// 128-bit hash using MurmurHash3
				MurmurHash3_x86_128(str.GetData(), str.GetSize(), seeds[i], &results[i]);
			} else if constexpr (Algorithm == HashAlgorithm::MURMURHASH3_X64_128) {
				// 128-bit hash using MurmurHash3 x64
				MurmurHash3_x64_128(str.GetData(), str.GetSize(), seeds[i], &results[i]);
			} else if constexpr (Algorithm == HashAlgorithm::XXH3_128) {
				// 128-bit hash
				XXH128_hash_t hash128 = XXH3_128bits_withSeed(str.GetData(), str.GetSize(), seeds[i]);
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

static void LoadInternal(DatabaseInstance &instance) {

	auto xxh32_set = ScalarFunctionSet("xxh32");
	xxh32_set.AddFunction(ScalarFunction("xxh32", {LogicalType::ANY}, LogicalType::UINTEGER, hashfunc_XXH32));
	xxh32_set.AddFunction(ScalarFunction("xxh32", {LogicalType::ANY, LogicalType::UINTEGER}, LogicalType::UINTEGER,
	                                     hashfunc_XXH32_with_seed));
	ExtensionUtil::RegisterFunction(instance, xxh32_set);

	auto xxh64_set = ScalarFunctionSet("xxh64");
	xxh64_set.AddFunction(ScalarFunction("xxh64", {LogicalType::ANY}, LogicalType::UBIGINT, hashfunc_XXH64));
	xxh64_set.AddFunction(ScalarFunction("xxh64", {LogicalType::ANY, LogicalType::UBIGINT}, LogicalType::UBIGINT,
	                                     hashfunc_XXH64_with_seed));
	ExtensionUtil::RegisterFunction(instance, xxh64_set);

	auto xxh3_64_set = ScalarFunctionSet("xxh3_64");
	xxh3_64_set.AddFunction(ScalarFunction("xxh3_64", {LogicalType::ANY}, LogicalType::UBIGINT, hashfunc_XXH3_64));
	xxh3_64_set.AddFunction(ScalarFunction("xxh3_64", {LogicalType::ANY, LogicalType::UBIGINT}, LogicalType::UBIGINT,
	                                       hashfunc_XXH3_64_with_seed));
	ExtensionUtil::RegisterFunction(instance, xxh3_64_set);

	auto xxh3_128_set = ScalarFunctionSet("xxh3_128");
	xxh3_128_set.AddFunction(ScalarFunction("xxh3_128", {LogicalType::ANY}, LogicalType::UHUGEINT, hashfunc_XXH3_128));
	xxh3_128_set.AddFunction(ScalarFunction("xxh3_128", {LogicalType::ANY, LogicalType::UHUGEINT},
	                                        LogicalType::UHUGEINT, hashfunc_XXH3_128_with_seed));
	ExtensionUtil::RegisterFunction(instance, xxh3_128_set);

	auto rapidhash_set = ScalarFunctionSet("rapidhash");
	rapidhash_set.AddFunction(
	    ScalarFunction("rapidhash", {LogicalType::ANY}, LogicalType::UBIGINT, hashfunc_rapidhash));
	rapidhash_set.AddFunction(ScalarFunction("rapidhash", {LogicalType::ANY, LogicalType::UBIGINT},
	                                         LogicalType::UBIGINT, hashfunc_rapidhash_with_seed));
	ExtensionUtil::RegisterFunction(instance, rapidhash_set);

	// auto rapidhash_micro_set = ScalarFunctionSet("rapidhash_micro");
	// rapidhash_micro_set.AddFunction(
	//     ScalarFunction("rapidhash_micro", {LogicalType::ANY}, LogicalType::UBIGINT, hashfunc_rapidhashMicro));
	// rapidhash_micro_set.AddFunction(ScalarFunction("rapidhash_micro", {LogicalType::ANY, LogicalType::UBIGINT},
	//                                                LogicalType::UBIGINT, hashfunc_rapidhashMicro_with_seed));
	// ExtensionUtil::RegisterFunction(instance, rapidhash_micro_set);

	// auto rapidhash_nano_set = ScalarFunctionSet("rapidhash_nano");
	// rapidhash_nano_set.AddFunction(
	//     ScalarFunction("rapidhash_nano", {LogicalType::ANY}, LogicalType::UBIGINT, hashfunc_rapidhashNano));
	// rapidhash_nano_set.AddFunction(ScalarFunction("rapidhash_nano", {LogicalType::ANY, LogicalType::UBIGINT},
	//                                               LogicalType::UBIGINT, hashfunc_rapidhashNano_with_seed));
	// ExtensionUtil::RegisterFunction(instance, rapidhash_nano_set);

	auto murmurhash3_32_set = ScalarFunctionSet("murmurhash3_32");
	murmurhash3_32_set.AddFunction(
	    ScalarFunction("murmurhash3_32", {LogicalType::ANY}, LogicalType::UINTEGER, hashfunc_MurmurHash3_32));
	murmurhash3_32_set.AddFunction(ScalarFunction("murmurhash3_32", {LogicalType::ANY, LogicalType::UINTEGER},
	                                              LogicalType::UINTEGER, hashfunc_MurmurHash3_32_with_seed));
	ExtensionUtil::RegisterFunction(instance, murmurhash3_32_set);

	auto murmurhash3_128_set = ScalarFunctionSet("murmurhash3_128");
	murmurhash3_128_set.AddFunction(
	    ScalarFunction("murmurhash3_128", {LogicalType::ANY}, LogicalType::UHUGEINT, hashfunc_MurmurHash3_128));
	murmurhash3_128_set.AddFunction(ScalarFunction("murmurhash3_128", {LogicalType::ANY, LogicalType::UHUGEINT},
	                                               LogicalType::UHUGEINT, hashfunc_MurmurHash3_128_with_seed));
	ExtensionUtil::RegisterFunction(instance, murmurhash3_128_set);

	auto murmurhash3_x64_128_set = ScalarFunctionSet("murmurhash3_x64_128");
	murmurhash3_x64_128_set.AddFunction(
	    ScalarFunction("murmurhash3_x64_128", {LogicalType::ANY}, LogicalType::UHUGEINT, hashfunc_MurmurHash3_X64_128));
	murmurhash3_x64_128_set.AddFunction(ScalarFunction("murmurhash3_x64_128", {LogicalType::ANY, LogicalType::UHUGEINT},
	                                                   LogicalType::UHUGEINT, hashfunc_MurmurHash3_X64_128_with_seed));
	ExtensionUtil::RegisterFunction(instance, murmurhash3_x64_128_set);
}

void HashfuncsExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string HashfuncsExtension::Name() {
	return "hashfuncs";
}

std::string HashfuncsExtension::Version() const {
#ifdef EXT_VERSION_Hashfuncs
	return EXT_VERSION_Hashfuncs;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void hashfuncs_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::HashfuncsExtension>();
}

DUCKDB_EXTENSION_API const char *hashfuncs_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}
