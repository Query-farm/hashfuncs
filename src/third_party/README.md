# Third-party hash implementations

Vendored unmodified, so the extension builds the same way on every platform (including WASM) without extra
vcpkg ports. Each directory keeps its upstream license.

| Directory | Upstream | Version | License |
|---|---|---|---|
| `wyhash` | https://github.com/wangyi-fudan/wyhash `wyhash.h` | ea3b25e1 (final version 4, November 2022, with the original default secret; matches hash4j's `wyhashFinal4`. Versions 4.2+ changed the default secret and so every hash) | Unlicense |
| `komihash` | https://github.com/avaneev/komihash `komihash.h` | 5.34 (50a15150) | MIT |
| `siphash` | https://github.com/veorq/SipHash `siphash.c`, `siphash.h` | 32d06760 | CC0 |
| `farmhash` | https://github.com/google/farmhash `src/farmhash.cc`, `src/farmhash.h` | 9d99331e | MIT |
| `cityhash102` | https://github.com/ClickHouse/ClickHouse `contrib/cityhash102` (CityHash v1.0.2, as ClickHouse ships it) | ba4072f0 | MIT |

`src/siphash_1_3.c` and `src/siphash_2_4.c` compile the SipHash reference implementation twice, once per round count.

Local modification: `cityhash102/city.h` drops ClickHouse's defaulted `operator<=>` on `uint128`, which needs C++20
and is not used here (the extension builds as C++17). The hashing code is unchanged.
