<p align="center">
  <a href="https://query.farm">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://query.farm/media-kit/logo/wordmark-dark.svg">
      <img alt="Query.Farm" src="https://query.farm/media-kit/logo/wordmark-light.svg" height="64">
    </picture>
  </a>
</p>

# Hashfuncs (hash functions) Extension for DuckDB

[![DuckDB](https://img.shields.io/badge/DuckDB-community_extension-fdf1e0?logo=duckdb&logoColor=fff000)](https://duckdb.org/community_extensions/extensions/hashfuncs.html)
[![v1.5 build](https://github.com/Query-farm/hashfuncs/actions/workflows/MainDistributionPipeline.yml/badge.svg?branch=v1.5)](https://github.com/Query-farm/hashfuncs/actions/workflows/MainDistributionPipeline.yml?query=branch%3Av1.5)

This `hashfuncs` extension adds functions for computing hash values from data.

## Documentation

Full documentation, including installation, usage, the function reference, and cookbook examples, is available at:

**[https://query.farm/products/extensions/hashfuncs](https://query.farm/products/extensions/hashfuncs)**

## Installation

```sql
install hashfuncs from community;
load hashfuncs;
```

## October 7, 2026 compatibility fixes (v1.5 source)

The VARCHAR/BLOB xxh3_128_hex overloads now emit canonical high64-then-low64 hex, with or without a seed. For hello, the corrected digest is b5e9c1ad071b3e7fc779cfaa5e523818. Older affected builds (including 0dec806) emitted c779cfaa5e523818b5e9c1ad071b3e7f. Swap the two 16-character halves of previously stored affected hex digests once when migrating.

Numeric xxh3_128 outputs are deliberately unchanged to preserve stored keys. Their historical UHUGEINT half ordering is not canonical: swap the halves of their zero-padded 32-character hex for interchange. Community packages may lag this source fix; verify the hello digest before removing any old workaround.
