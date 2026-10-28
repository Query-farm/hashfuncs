#pragma once

#include <cstddef>
#include <cstdint>

// Wrappers around the vendored hash implementations in src/third_party that are compiled in their own translation
// units (FarmHash, CityHash, SipHash, CRC-32C), so those headers' macros and typedefs stay out of the extension code.
// The header-only wyhash and komihash are included directly by hashfuncs_extension.cpp.
namespace duckdb {
namespace hashfuncs {

//! FarmHash Fingerprint64, which BigQuery's FARM_FINGERPRINT returns as a signed INT64.
uint64_t FarmFingerprint64(const char *data, size_t size);

//! CityHash64 from CityHash v1.0.2, the version ClickHouse's cityHash64 uses.
uint64_t CityHash64V102(const char *data, size_t size);

//! SipHash with a 16-byte key, returning the 8 output bytes read as a little-endian integer.
uint64_t SipHash13(const void *data, size_t size, const uint8_t key[16]);
uint64_t SipHash24(const void *data, size_t size, const uint8_t key[16]);

//! Extends a CRC-32C (Castagnoli) checksum: Crc32cExtend(Crc32cExtend(0, a), b) == Crc32cExtend(0, a || b).
uint32_t Crc32cExtend(uint32_t crc, const void *data, size_t size);

} // namespace hashfuncs
} // namespace duckdb
