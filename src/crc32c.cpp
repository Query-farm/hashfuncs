#include "vendored_hashes.hpp"

#include <array>

namespace duckdb {
namespace hashfuncs {

// CRC-32C (Castagnoli), reflected polynomial 0x82F63B78, computed in software with slicing-by-8 tables.
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

uint32_t Crc32cExtend(uint32_t crc, const void *data, size_t size) {
	auto bytes = static_cast<const uint8_t *>(data);
	crc = ~crc;
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
	return ~crc;
}

} // namespace hashfuncs
} // namespace duckdb
