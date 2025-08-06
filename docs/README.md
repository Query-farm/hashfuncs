# Hashfuncs (hash functions) Extension for DuckDB

The **Hashfuncs** extension, developed by **[Query.Farm](https://query.farm)**, adds non-cryptographic hash functions to DuckDB.

## Installation

**`hashfuncs` is a [DuckDB Community Extension](https://github.com/duckdb/community-extensions).**

You can now use this by using this SQL:

```sql
install hashfuncs from community;
load hashfuncs;
```

## What is hashing?

Hashing is a process that transforms input data of any size into a fixed-size output, which is typically a hash value or hash code. Hash functions are designed to be fast, deterministic (same input always produces the same output), and distribute hash values uniformly across the output space.

Non cryptographic hash functions are commonly used for:

- **Database indexing**: Creating efficient lookup structures

- **Partitioning data**: Distributing data across multiple nodes or buckets

- **Caching**: Creating cache keys

- **Bloom filters**: Probabilistic data structures for membership testing

## Available Hash Functions

This extension provides multiple high-performance non-cryptographic hash algorithms, each optimized for different use cases:

### xxHash Family

**xxHash** is an extremely fast non-cryptographic hash algorithm, reaching speeds close to RAM limits.

#### `xxh32(data [, seed])`
- **Returns**: `UINTEGER` (32-bit unsigned integer)
- **Seed type**: `UINTEGER` (optional)
- **Description**: 32-bit version of xxHash, good for hash tables and checksums

```sql
SELECT xxh32('hello world');
┌──────────────────────┐
│ xxh32('hello world') │
│        uint32        │
├──────────────────────┤
│      3468387874      │
│    (3.47 billion)    │
└──────────────────────┘

SELECT xxh32('hello world', 42);
┌──────────────────────────┐
│ xxh32('hello world', 42) │
│          uint32          │
├──────────────────────────┤
│        4225033588        │
│      (4.23 billion)      │
└──────────────────────────┘
```

#### `xxh64(data [, seed])`
- **Returns**: `UBIGINT` (64-bit unsigned integer)
- **Seed type**: `UBIGINT` (optional)
- **Description**: 64-bit version of xxHash, recommended for most applications

```sql
SELECT xxh64('hello world');
┌──────────────────────┐
│ xxh64('hello world') │
│        uint64        │
├──────────────────────┤
│ 5020219685658847592  │
│  (5.02 quintillion)  │
└──────────────────────┘

SELECT xxh64('hello world', 12345);
┌─────────────────────────────┐
│ xxh64('hello world', 12345) │
│           uint64            │
├─────────────────────────────┤
│    15771590491225725957     │
└─────────────────────────────┘
```

#### `xxh3_64(data [, seed])`
- **Returns**: `UBIGINT` (64-bit unsigned integer)
- **Seed type**: `UBIGINT` (optional)
- **Description**: Latest xxHash algorithm, optimized for modern CPUs

```sql
SELECT xxh3_64('hello world');
┌────────────────────────┐
│ xxh3_64('hello world') │
│         uint64         │
├────────────────────────┤
│  15296390279056496779  │
└────────────────────────┘

SELECT xxh3_64('hello world', 999);
┌─────────────────────────────┐
│ xxh3_64('hello world', 999) │
│           uint64            │
├─────────────────────────────┤
│     3002856137354040482     │
│     (3.00 quintillion)      │
└─────────────────────────────┘
```

#### `xxh3_128(data [, seed])`
- **Returns**: `UHUGEINT` (128-bit unsigned integer)
- **Seed type**: `UBIGINT` (optional)
- **Description**: 128-bit version providing larger hash space, reducing collision probability

```sql
SELECT xxh3_128('hello world');
┌─────────────────────────────────────────┐
│         xxh3_128('hello world')         │
│                 uint128                 │
├─────────────────────────────────────────┤
│ 225447084758876380551077147957698971904 │
└─────────────────────────────────────────┘

SELECT xxh3_128('hello world', 777);
┌─────────────────────────────────────────┐
│      xxh3_128('hello world', 777)       │
│                 uint128                 │
├─────────────────────────────────────────┤
│ 283192007746380917896797379546610829141 │
└─────────────────────────────────────────┘
```

### RapidHash Family

**RapidHash** is designed for exceptional speed while maintaining good hash quality.

#### `rapidhash(data [, seed])`
- **Returns**: `UBIGINT` (64-bit unsigned integer)
- **Seed type**: `UBIGINT` (optional)
- **Description**: Main RapidHash algorithm, optimized for speed and quality

```sql
SELECT rapidhash('hello world');
┌──────────────────────────┐
│ rapidhash('hello world') │
│          uint64          │
├──────────────────────────┤
│   3397907815814400320    │
│    (3.40 quintillion)    │
└──────────────────────────┘

SELECT rapidhash('hello world', 2023);
┌────────────────────────────────┐
│ rapidhash('hello world', 2023) │
│             uint64             │
├────────────────────────────────┤
│      11789095433300219990      │
└────────────────────────────────┘
```

### MurmurHash3 Family

**MurmurHash3** is a well-established non-cryptographic hash function known for good distribution and performance.

#### `murmurhash3_32(data [, seed])`
- **Returns**: `UINTEGER` (32-bit unsigned integer)
- **Seed type**: `UINTEGER` (optional)
- **Description**: 32-bit MurmurHash3, widely used and tested

```sql
SELECT murmurhash3_32('hello world');
┌───────────────────────────────┐
│ murmurhash3_32('hello world') │
│            uint32             │
├───────────────────────────────┤
│          1586663183           │
│        (1.59 billion)         │
└───────────────────────────────┘

SELECT murmurhash3_32('hello world', 123);
┌────────────────────────────────────┐
│ murmurhash3_32('hello world', 123) │
│               uint32               │
├────────────────────────────────────┤
│             679062093              │
│          (679.06 million)          │
└────────────────────────────────────┘
```

#### `murmurhash3_128(data [, seed])`
- **Returns**: `UHUGEINT` (128-bit unsigned integer)
- **Seed type**: `UINTEGER` (optional)
- **Description**: 128-bit MurmurHash3 for x86 platforms

```sql
SELECT murmurhash3_128('hello world');
┌─────────────────────────────────────────┐
│     murmurhash3_128('hello world')      │
│                 uint128                 │
├─────────────────────────────────────────┤
│ 206095855024402301784664199839047883400 │
└─────────────────────────────────────────┘
```

#### `murmurhash3_x64_128(data [, seed])`
- **Returns**: `UHUGEINT` (128-bit unsigned integer)
- **Seed type**: `UINTEGER` (optional)
- **Description**: 128-bit MurmurHash3 optimized for x64 platforms

```sql
SELECT murmurhash3_x64_128('hello world');
┌─────────────────────────────────────────┐
│   murmurhash3_x64_128('hello world')    │
│                 uint128                 │
├─────────────────────────────────────────┤
│ 228083453807047072434243676435732455694 │
└─────────────────────────────────────────┘
```

## Supported Data Types

All hash functions support the following DuckDB data types:

- **String types**: `VARCHAR`, `BLOB`
- **Integer types**: `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, `HUGEINT`
- **Unsigned integer types**: `UTINYINT`, `USMALLINT`, `UINTEGER`, `UBIGINT`, `UHUGEINT`
- **Floating point types**: `FLOAT`, `DOUBLE`
- **Date/time types**: `DATE`, `TIME`

## Performance Characteristics

| Algorithm | Speed | Quality | Output Size | Best Use Case |
|-----------|-------|---------|-------------|---------------|
| `xxh32` | Very Fast | Good | 32-bit | Legacy systems, hash tables |
| `xxh64` | Very Fast | Very Good | 64-bit | General purpose hashing |
| `xxh3_64` | Fastest | Excellent | 64-bit | Modern applications |
| `xxh3_128` | Fast | Excellent | 128-bit | When collision resistance is critical |
| `rapidhash` | Extremely Fast | Good | 64-bit | High-throughput applications |
| `rapidhash_micro` | Extremely Fast | Good | 64-bit | Small data, high frequency |
| `rapidhash_nano` | Fastest | Fair | 64-bit | Tiny data, maximum speed |
| `murmurhash3_32` | Fast | Very Good | 32-bit | Distributed systems, Bloom filters |
| `murmurhash3_128` | Fast | Very Good | 128-bit | UUID generation, partitioning |
| `murmurhash3_x64_128` | Fast | Very Good | 128-bit | 64-bit optimized partitioning |

## Usage Examples

### Basic Hashing

```sql
-- Hash various data types
SELECT xxh64(42);                    -- Integer
SELECT xxh64('Hello, World!');       -- String
SELECT xxh64('2023-12-01'::DATE);    -- Date
SELECT xxh64(3.14159::FLOAT);        -- Float
```

### Data Partitioning

```sql
-- Distribute data across 10 partitions
SELECT
    customer_id,
    xxh64(customer_id) % 10 as partition_id
FROM customers;
```

### Creating Consistent Hash Keys

```sql
-- Create cache keys from multiple columns
SELECT
    user_id,
    product_id,
    xxh64(CONCAT(user_id, ':', product_id)) as cache_key
FROM user_purchases;
```

### Data Integrity Verification

```sql
-- Create checksums for data verification
SELECT
    file_name,
    file_content,
    xxh3_128(file_content) as checksum
FROM file_storage;
```

### Using Seeds for Different Hash Spaces

```sql
-- Create different hash values for the same data
SELECT
    data,
    xxh64(data, 0) as hash_space_0,
    xxh64(data, 1) as hash_space_1,
    xxh64(data, 2) as hash_space_2
FROM my_table;
```

### Bloom Filter Implementation

```sql
-- Generate multiple hash values for Bloom filter
WITH bloom_hashes AS (
    SELECT
        item,
        murmurhash3_32(item, 0) % 1000000 as hash1,
        murmurhash3_32(item, 1) % 1000000 as hash2,
        murmurhash3_32(item, 2) % 1000000 as hash3
    FROM items
)
SELECT * FROM bloom_hashes;
```

### Load Balancing

```sql
-- Distribute requests across servers
SELECT
    request_id,
    xxh3_64(request_id) % 5 as server_id
FROM incoming_requests;
```

## Algorithm Selection Guide

**For general-purpose hashing**: Use `xxh3_64` - it provides the best balance of speed and quality for modern applications.

**For maximum speed**: Use `rapidhash` or `rapidhash_nano` when you need the absolute fastest hashing.

**For legacy compatibility**: Use `murmurhash3_32` if you need compatibility with existing systems using MurmurHash.

**For high collision resistance**: Use `xxh3_128` or `murmurhash3_x64_128` when you need larger hash spaces.

**For small data**: Use `rapidhash_micro` or `rapidhash_nano` for very small inputs.

## Performance Tips

1. **Choose appropriate output size**: Use 32-bit hashes only when memory is constrained; 64-bit hashes provide better collision resistance.

2. **Use seeds wisely**: Seeds allow you to create independent hash functions from the same algorithm.

3. **Consider your data distribution**: Some algorithms perform better with certain types of input data.

4. **Benchmark for your use case**: Performance can vary based on your specific data patterns and hardware.

## Limitations

- These are **non-cryptographic** hash functions - do not use them for security-sensitive applications like password hashing or digital signatures
- Hash collisions are possible (but rare with good algorithms and appropriate output sizes)
- Performance characteristics may vary based on input data patterns and hardware architecture

## License

MIT Licensed
