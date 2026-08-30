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
