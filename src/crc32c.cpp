#include "vendored_hashes.hpp"

#include <array>
#include <cstring>

#if (defined(__aarch64__) || defined(_M_ARM64)) && defined(__BYTE_ORDER__) &&                                          \
    __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__ && (defined(__GNUC__) || defined(__clang__))
#define HASHFUNCS_CRC32C_ARM 1
#include <arm_acle.h>
#if defined(__linux__)
#include <sys/auxv.h>
#ifndef HWCAP_CRC32
#define HWCAP_CRC32 (1 << 7)
#endif
#endif
#elif (defined(__x86_64__) || defined(_M_X64)) && (defined(__GNUC__) || defined(__clang__))
#define HASHFUNCS_CRC32C_X86 1
#include <nmmintrin.h>
#endif

namespace duckdb {
namespace hashfuncs {

// CRC-32C (Castagnoli), reflected polynomial 0x82F63B78. Uses the CPU's CRC32C instructions when it has them
// (ARMv8 CRC extension, x86 SSE4.2), detected once at load time, and software slicing-by-8 tables otherwise.
static constexpr uint32_t CRC32C_POLYNOMIAL = 0x82F63B78;

using Crc32cTables = std::array<std::array<uint32_t, 256>, 8>;

static Crc32cTables MakeCrc32cTables() {
	Crc32cTables tables {};
	for (uint32_t byte = 0; byte < 256; byte++) {
		uint32_t crc = byte;
		for (int bit = 0; bit < 8; bit++) {
			crc = (crc >> 1) ^ ((crc & 1) ? CRC32C_POLYNOMIAL : 0);
		}
		tables[0][byte] = crc;
	}
	for (uint32_t byte = 0; byte < 256; byte++) {
		for (size_t table = 1; table < 8; table++) {
			const uint32_t previous = tables[table - 1][byte];
			tables[table][byte] = (previous >> 8) ^ tables[0][previous & 0xFF];
		}
	}
	return tables;
}

static const Crc32cTables CRC32C_TABLES = MakeCrc32cTables();

static uint32_t Crc32cSoftware(uint32_t crc, const uint8_t *bytes, size_t size) {
	while (size >= 8) {
		// Combine eight bytes per step, reading them in little-endian order on every platform.
		const uint32_t low =
		    crc ^ (uint32_t(bytes[0]) | uint32_t(bytes[1]) << 8 | uint32_t(bytes[2]) << 16 | uint32_t(bytes[3]) << 24);
		crc = CRC32C_TABLES[7][low & 0xFF] ^ CRC32C_TABLES[6][(low >> 8) & 0xFF] ^
		      CRC32C_TABLES[5][(low >> 16) & 0xFF] ^ CRC32C_TABLES[4][low >> 24] ^ CRC32C_TABLES[3][bytes[4]] ^
		      CRC32C_TABLES[2][bytes[5]] ^ CRC32C_TABLES[1][bytes[6]] ^ CRC32C_TABLES[0][bytes[7]];
		bytes += 8;
		size -= 8;
	}
	while (size > 0) {
		crc = (crc >> 8) ^ CRC32C_TABLES[0][(crc ^ *bytes) & 0xFF];
		bytes++;
		size--;
	}
	return crc;
}

#if defined(HASHFUNCS_CRC32C_ARM)
#if defined(__clang__)
#define HASHFUNCS_CRC_TARGET __attribute__((target("crc")))
#else
#define HASHFUNCS_CRC_TARGET __attribute__((target("+crc")))
#endif
HASHFUNCS_CRC_TARGET static uint32_t Crc32cHardware(uint32_t crc, const uint8_t *bytes, size_t size) {
	while (size >= 8) {
		uint64_t word;
		memcpy(&word, bytes, sizeof(word));
		crc = __crc32cd(crc, word);
		bytes += 8;
		size -= 8;
	}
	if (size >= 4) {
		uint32_t word;
		memcpy(&word, bytes, sizeof(word));
		crc = __crc32cw(crc, word);
		bytes += 4;
		size -= 4;
	}
	while (size > 0) {
		crc = __crc32cb(crc, *bytes++);
		size--;
	}
	return crc;
}
static bool DetectHardwareCrc32c() {
#if defined(__APPLE__)
	return true; // every Apple arm64 CPU has the CRC extension
#elif defined(__linux__)
	return (getauxval(AT_HWCAP) & HWCAP_CRC32) != 0;
#elif defined(__ARM_FEATURE_CRC32)
	return true;
#else
	return false;
#endif
}
#elif defined(HASHFUNCS_CRC32C_X86)
__attribute__((target("sse4.2"))) static uint32_t Crc32cHardware(uint32_t crc, const uint8_t *bytes, size_t size) {
	uint64_t crc64 = crc;
	while (size >= 8) {
		uint64_t word;
		memcpy(&word, bytes, sizeof(word));
		crc64 = _mm_crc32_u64(crc64, word);
		bytes += 8;
		size -= 8;
	}
	crc = uint32_t(crc64);
	while (size > 0) {
		crc = _mm_crc32_u8(crc, *bytes++);
		size--;
	}
	return crc;
}
static bool DetectHardwareCrc32c() {
	// This runs from a static initializer, which may execute before the runtime has filled in the CPU model.
	__builtin_cpu_init();
	return __builtin_cpu_supports("sse4.2");
}
#else
static uint32_t Crc32cHardware(uint32_t crc, const uint8_t *bytes, size_t size) {
	return Crc32cSoftware(crc, bytes, size);
}
static bool DetectHardwareCrc32c() {
	return false;
}
#endif

static const bool HAS_HARDWARE_CRC32C = DetectHardwareCrc32c();

uint32_t Crc32cExtend(uint32_t crc, const void *data, size_t size) {
	auto bytes = static_cast<const uint8_t *>(data);
	crc = ~crc;
	crc = HAS_HARDWARE_CRC32C ? Crc32cHardware(crc, bytes, size) : Crc32cSoftware(crc, bytes, size);
	return ~crc;
}

} // namespace hashfuncs
} // namespace duckdb
