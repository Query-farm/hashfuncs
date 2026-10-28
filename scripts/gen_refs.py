"""Generate test/sql/hash_interop.test and test/sql/hash_algorithms.test from reference implementations.

Expected values come from the real systems, never from this extension:
  * Spark spark-sql                  -> spark_hash, spark_xxhash64
  * kafka-clients (jshell)           -> kafka_murmur2, kafka_partition (BuiltInPartitioner.partitionForKey)
  * ClickHouse (clickhouse-local)    -> clickhouse_cityhash64, and sipHash64 for comparison with siphash24
  * hash4j (jshell)                  -> wyhash (final 4), komihash (5.x), farm_fingerprint (farmHashNa)
  * java.util.zip.CRC32C (jshell)    -> crc32c
  * Rust core SipHash test vectors   -> siphash13, siphash24
BigQuery's documented FARM_FINGERPRINT examples are written into the test directly.

Requirements: Python 3.9+, Java 17+ (jshell), Docker (pulls clickhouse/clickhouse-server), and a <refs-dir> holding
  spark-4.2.1-bin-hadoop3/   https://archive.apache.org/dist/spark/spark-4.2.1/
  kafka-clients-4.4.0.jar, slf4j-api-2.0.17.jar, hash4j-0.31.0.jar   from Maven Central
  rust_sip.rs                rust-lang/rust library/coretests/tests/hash/sip.rs

Usage: python3 -I scripts/gen_refs.py <refs-dir> test/sql
"""
import json
import os
import re
import subprocess
import sys

refs, out_dir = sys.argv[1:3]
SPARK_SQL = os.path.join(refs, "spark-4.2.1-bin-hadoop3/bin/spark-sql")
KAFKA_JAR = os.path.join(refs, "kafka-clients-4.4.0.jar")
HASH4J_JAR = os.path.join(refs, "hash4j-0.31.0.jar")
SLF4J_JAR = os.path.join(refs, "slf4j-api-2.0.17.jar")
RUST_SIP = os.path.join(refs, "rust_sip.rs")

# Strings hashed by every string reference: (DuckDB SQL, Spark SQL, ClickHouse SQL, Python value).
LONG = "0123456789" * 30


def plain(value):
    return ("'" + value + "'", "'" + value + "'", "'" + value + "'", value)


STRINGS = [plain(v) for v in ["", "a", "ab", "abc", "abcd", "hello", "hello world",
                              "The quick brown fox jumps over the lazy dog", "ünïcödé ✓"]]
STRINGS.append(("repeat('0123456789', 30)", "repeat('0123456789', 30)", "repeat('0123456789', 30)", LONG))


def java_literal(value):
    return '"' + "".join(c if 32 <= ord(c) < 127 and c not in '"\\' else "\\u%04x" % ord(c) for c in value) + '"'


# ---------------------------------------------------------------------------------------------------------------
# Spark: (Spark arguments, DuckDB arguments)
SPARK_CASES = [
    ("'Spark', array(123), 2", "'Spark', [123], 2"),
    ("true", "true"),
    ("false", "false"),
    ("cast(-7 as tinyint)", "(-7)::TINYINT"),
    ("cast(-300 as smallint)", "(-300)::SMALLINT"),
    ("42", "42"),
    ("-2147483648", "(-2147483648)::INTEGER"),
    ("cast(42 as bigint)", "42::BIGINT"),
    ("cast(-1 as bigint)", "(-1)::BIGINT"),
    ("9223372036854775807L", "9223372036854775807::BIGINT"),
    ("cast(1.5 as float)", "1.5::FLOAT"),
    ("cast('-0.0' as float)", "'-0.0'::FLOAT"),
    ("cast('NaN' as float)", "'NaN'::FLOAT"),
    ("3.25d", "3.25::DOUBLE"),
    ("cast('-0.0' as double)", "'-0.0'::DOUBLE"),
    ("cast('NaN' as double)", "'NaN'::DOUBLE"),
    ("cast(1.5 as decimal(4,2))", "1.5::DECIMAL(4,2)"),
    ("cast(123.45 as decimal(5,2))", "123.45::DECIMAL(5,2)"),
    ("cast(-123.45 as decimal(18,2))", "(-123.45)::DECIMAL(18,2)"),
    ("cast(12345678901234567890.12 as decimal(38,2))", "12345678901234567890.12::DECIMAL(38,2)"),
    ("cast(-1 as decimal(20,0))", "(-1)::DECIMAL(20,0)"),
    ("cast(128 as decimal(20,0))", "128::DECIMAL(20,0)"),
    ("cast(-129 as decimal(25,0))", "(-129)::DECIMAL(25,0)"),
    ("cast(0 as decimal(30,5))", "0::DECIMAL(30,5)"),
    ("date'2024-01-02'", "DATE '2024-01-02'"),
    ("date'1960-05-06'", "DATE '1960-05-06'"),
    ("timestamp_ntz'2024-01-02 03:04:05.123456'", "TIMESTAMP '2024-01-02 03:04:05.123456'"),
    ("timestamp_ntz'2024-01-02 03:04:05.123456'", "TIMESTAMP_NS '2024-01-02 03:04:05.123456'"),
    ("timestamp_ntz'1960-01-01 00:00:00'", "TIMESTAMP '1960-01-01'"),
    ("timestamp'2024-01-02 03:04:05.123456+00:00'", "TIMESTAMPTZ '2024-01-02 03:04:05.123456+00'"),
    ("X'DEADBEEF01'", "'\\xDE\\xAD\\xBE\\xEF\\x01'::BLOB"),
    ("X'FF'", "'\\xFF'::BLOB"),
    ("array(1, 2, null, 3)", "[1, 2, NULL, 3]"),
    ("array(1, 2, 3)", "[1, 2, 3]::INTEGER[3]"),
    ("array('a', 'bc')", "['a', 'bc']"),
    ("array(array(1, 2), array())", "[[1, 2], []]"),
    ("struct(1, 'a', cast(null as int))", "(1, 'a', NULL::INTEGER)"),
    ("named_struct('x', 1.5d, 'y', array(true, false))", "{'x': 1.5::DOUBLE, 'y': [true, false]}"),
    ("array(cast(1.10 as decimal(10,2)), cast(-2.20 as decimal(10,2)))",
     "[1.10::DECIMAL(10,2), (-2.20)::DECIMAL(10,2)]"),
    ("null", "NULL"),
    ("cast(null as string), 'a'", "NULL::VARCHAR, 'a'"),
    ("'a', 1, 2.5d, date'2024-01-02'", "'a', 1, 2.5::DOUBLE, DATE '2024-01-02'"),
    ("'happy'", "'happy'::mood"),
] + [(spark, duck) for duck, spark, _, _ in STRINGS]


def run_spark():
    statements = ["SET spark.sql.session.timeZone=UTC;"]
    for i, (spark_args, _) in enumerate(SPARK_CASES):
        statements.append(f"SELECT {i}, hash({spark_args}), xxhash64({spark_args});")
    path = "/tmp/hashfuncs_spark_refs.sql"
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(statements) + "\n")
    proc = subprocess.run([SPARK_SQL, "-S", "-f", path], capture_output=True, text=True, check=True)
    results = {}
    for line in proc.stdout.splitlines():
        parts = line.split("\t")
        if len(parts) == 3 and parts[0].isdigit():
            results[int(parts[0])] = (int(parts[1]), int(parts[2]))
    assert len(results) == len(SPARK_CASES), (len(results), proc.stderr[-2000:])
    return results


# ---------------------------------------------------------------------------------------------------------------
# Java references (Kafka, hash4j, CRC32C) through jshell.
PARTITION_COUNTS = [1, 3, 7, 12, 100]
SEEDS = [0, 42]


def run_java():
    lines = [
        "import java.nio.charset.StandardCharsets;",
        "import org.apache.kafka.common.utils.Utils;",
        "import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;",
        "import com.dynatrace.hash4j.hashing.Hashing;",
        "import java.util.zip.CRC32C;",
    ]
    for i, (_, _, _, value) in enumerate(STRINGS):
        lines.append(f"byte[] b{i} = {java_literal(value)}.getBytes(StandardCharsets.UTF_8);")
        lines.append(f'System.out.println("murmur2|{i}|" + Utils.murmur2(b{i}));')
        for n in PARTITION_COUNTS:
            lines.append(f'System.out.println("partition|{i}|{n}|" + BuiltInPartitioner.partitionForKey(b{i}, {n}));')
        for seed in SEEDS:
            lines.append(f'System.out.println("wyhash|{i}|{seed}|" + '
                         f'Long.toUnsignedString(Hashing.wyhashFinal4({seed}L).hashBytesToLong(b{i})));')
            lines.append(f'System.out.println("komihash|{i}|{seed}|" + '
                         f'Long.toUnsignedString(Hashing.komihash5_0({seed}L).hashBytesToLong(b{i})));')
        lines.append(f'System.out.println("farmhash|{i}|" + Hashing.farmHashNa().hashBytesToLong(b{i}));')
        lines.append(f"CRC32C c{i} = new CRC32C(); c{i}.update(b{i});")
        lines.append(f'System.out.println("crc32c|{i}|" + c{i}.getValue());')
    lines.append("/exit")
    path = "/tmp/hashfuncs_java_refs.jsh"
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    proc = subprocess.run(["jshell", "-R-Dfile.encoding=UTF-8", "--class-path", f"{KAFKA_JAR}:{HASH4J_JAR}:{SLF4J_JAR}", path],
                          capture_output=True, text=True, check=True)
    results = {}
    for line in proc.stdout.splitlines():
        parts = line.strip().split("|")
        if parts[0] in ("murmur2", "partition", "wyhash", "komihash", "farmhash", "crc32c"):
            results[tuple(parts[:-1])] = int(parts[-1])
    expected = len(STRINGS) * (1 + len(PARTITION_COUNTS) + 2 * len(SEEDS) + 2)
    assert len(results) == expected, (len(results), expected, proc.stdout[-2000:], proc.stderr[-2000:])
    return results


def run_clickhouse():
    selects = [f"SELECT {i}, cityHash64({ch}), sipHash64({ch})" for i, (_, _, ch, _) in enumerate(STRINGS)]
    query = " UNION ALL ".join(selects) + " ORDER BY 1 FORMAT TSV"
    proc = subprocess.run(["docker", "run", "--rm", "clickhouse/clickhouse-server:latest", "clickhouse-local", "-q",
                           query], capture_output=True, text=True, check=True)
    results = {}
    for line in proc.stdout.splitlines():
        i, city, sip = line.split("\t")
        results[int(i)] = (int(city), int(sip))
    assert len(results) == len(STRINGS), proc.stderr[-2000:]
    return results


def rust_sip_vectors(name):
    text = open(RUST_SIP).read()
    body = text[text.index(f"fn {name}()"):]
    body = body[body.index("= [") + 3:]
    body = body[:body.index("\n    ];")]
    rows = re.findall(r"\[((?:0x[0-9a-f]{2},?\s*){8})\]", body)
    vectors = []
    for row in rows:
        bytes_ = [int(x, 16) for x in re.findall(r"0x([0-9a-f]{2})", row)]
        vectors.append(int.from_bytes(bytes(bytes_), "little"))
    assert len(vectors) == 64
    return vectors


# ---------------------------------------------------------------------------------------------------------------
spark = run_spark()
java = run_java()
clickhouse = run_clickhouse()


def header(name, description):
    return [f"# name: test/sql/{name}", f"# description: {description}", "# group: [sql]", "",
            "# Generated by scripts/gen_refs.py from reference implementations; do not edit expected values by hand.", "",
            "require hashfuncs", ""]


interop = header("hash_interop.test", "functions that match other systems return the same values those systems do")
interop += ["# BigQuery's documented FARM_FINGERPRINT example.", "query III",
            "SELECT farm_fingerprint('1footrue'), farm_fingerprint('2applefalse'), farm_fingerprint('3true');",
            "----", "-1541654101129638711\t2794438866806483259\t-4880158226897771312", ""]
interop += ["# FarmHash Fingerprint64 as computed by hash4j's farmHashNa, as VARCHAR and as BLOB."]
for i, (duck, _, _, _) in enumerate(STRINGS):
    value = java[("farmhash", str(i))]
    interop += ["query II", f"SELECT farm_fingerprint({duck}), farm_fingerprint(encode({duck}));", "----",
                f"{value}\t{value}", ""]

interop += ["# Spark 4.2.1: SELECT hash(...), xxhash64(...).", "statement ok",
            "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');", ""]
for i, (spark_args, duck_args) in enumerate(SPARK_CASES):
    h, x = spark[i]
    interop += [f"# Spark: hash({spark_args})", "query II",
                f"SELECT spark_hash({duck_args}), spark_xxhash64({duck_args});", "----", f"{h}\t{x}", ""]

interop += ["# Kafka 4.4.0: Utils.murmur2 and BuiltInPartitioner.partitionForKey."]
for i, (duck, _, _, _) in enumerate(STRINGS):
    m = java[("murmur2", str(i))]
    parts = [java[("partition", str(i), str(n))] for n in PARTITION_COUNTS]
    calls = ", ".join(f"kafka_partition({duck}, {n})" for n in PARTITION_COUNTS)
    interop += ["query I" + "I" * (1 + len(PARTITION_COUNTS)),
                f"SELECT kafka_murmur2({duck}), kafka_murmur2(encode({duck})), {calls};", "----",
                "\t".join(str(v) for v in [m, m] + parts), ""]

interop += ["# ClickHouse: cityHash64 of a String."]
for i, (duck, _, _, _) in enumerate(STRINGS):
    city, _ = clickhouse[i]
    interop += ["query II", f"SELECT clickhouse_cityhash64({duck}), clickhouse_cityhash64(encode({duck}));", "----",
                f"{city}\t{city}", ""]

algorithms = header("hash_algorithms.test", "wyhash, komihash, SipHash, CRC-32C and FNV-1a match reference values")
algorithms += ["# wyhash final 4 and komihash 5, as computed by hash4j."]
for i, (duck, _, _, _) in enumerate(STRINGS):
    values = []
    calls = []
    for seed in SEEDS:
        calls += [f"wyhash({duck}, {seed})", f"komihash({duck}, {seed})"]
        values += [java[("wyhash", str(i), str(seed))], java[("komihash", str(i), str(seed))]]
    calls += [f"wyhash({duck})", f"komihash({duck})"]
    values += [java[("wyhash", str(i), "0")], java[("komihash", str(i), "0")]]
    algorithms += ["query " + "I" * len(calls), "SELECT " + ", ".join(calls) + ";", "----",
                   "\t".join(str(v) for v in values), ""]

algorithms += ["# CRC-32C as computed by java.util.zip.CRC32C, including the standard check value.", "query I",
               "SELECT crc32c('123456789');", "----", "3808858755", ""]
for i, (duck, _, _, _) in enumerate(STRINGS):
    algorithms += ["query I", f"SELECT crc32c({duck});", "----", str(java[("crc32c", str(i))]), ""]

# Rust's vectors hash bytes 0x00, 0x01, ... (n bytes) with key bytes 00..0f.
key = str(int("0f0e0d0c0b0a09080706050403020100", 16)) + "::UHUGEINT"
for name, function in (("test_siphash_1_3", "siphash13"), ("test_siphash_2_4", "siphash24")):
    vectors = rust_sip_vectors(name)
    algorithms += [f"# {function}: Rust core test vectors ({name}), key bytes 00..0f."]
    for n in (0, 1, 7, 8, 15, 16, 31, 63):
        data = "unhex('" + "".join("%02x" % b for b in range(n)) + "')"
        algorithms += ["query I", f"SELECT {function}({data}, {key});", "----", str(vectors[n]), ""]

algorithms += ["# With no key, siphash24 matches ClickHouse's sipHash64 of a String."]
for i, (duck, _, _, _) in enumerate(STRINGS):
    _, sip = clickhouse[i]
    algorithms += ["query I", f"SELECT siphash24({duck});", "----", str(sip), ""]

with open(os.path.join(out_dir, "hash_interop.test"), "w", encoding="utf-8") as f:
    f.write("\n".join(interop))
with open(os.path.join(out_dir, "hash_algorithms.test"), "w", encoding="utf-8") as f:
    f.write("\n".join(algorithms))
print("spark cases:", len(SPARK_CASES), "strings:", len(STRINGS))
print("clickhouse sipHash64 equals reference SipHash-2-4 with zero key: see hash_algorithms.test")
