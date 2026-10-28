#include "vendored_hashes.hpp"

#include "third_party/farmhash/farmhash.h"
#include "third_party/cityhash102/city.h"

extern "C" {
int hashfuncs_siphash_1_3(const void *in, const size_t inlen, const void *k, uint8_t *out, const size_t outlen);
int hashfuncs_siphash_2_4(const void *in, const size_t inlen, const void *k, uint8_t *out, const size_t outlen);
}

namespace duckdb {
namespace hashfuncs {

uint64_t FarmFingerprint64(const char *data, size_t size) {
	return NAMESPACE_FOR_HASH_FUNCTIONS::Fingerprint64(data, size);
}

uint64_t CityHash64V102(const char *data, size_t size) {
	return CityHash_v1_0_2::CityHash64(data, size);
}

static uint64_t ReadLittleEndian64(const uint8_t bytes[8]) {
	uint64_t result = 0;
	for (int i = 7; i >= 0; i--) {
		result = (result << 8) | bytes[i];
	}
	return result;
}

uint64_t SipHash13(const void *data, size_t size, const uint8_t key[16]) {
	uint8_t out[8];
	hashfuncs_siphash_1_3(data, size, key, out, sizeof(out));
	return ReadLittleEndian64(out);
}

uint64_t SipHash24(const void *data, size_t size, const uint8_t key[16]) {
	uint8_t out[8];
	hashfuncs_siphash_2_4(data, size, key, out, sizeof(out));
	return ReadLittleEndian64(out);
}

} // namespace hashfuncs
} // namespace duckdb
