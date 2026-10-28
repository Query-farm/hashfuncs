cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME hashfuncs)
set(CMAKE_CXX_STANDARD 17)
# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
find_package(xxHash CONFIG REQUIRED)
find_package(murmurhash CONFIG REQUIRED)
find_path(RAPIDHASH_INCLUDE_DIRS "rapidhash.h")

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include
${RAPIDHASH_INCLUDE_DIRS})

set(EXTENSION_SOURCES src/hashfuncs_extension.cpp
src/hash_interop.cpp
src/vendored_hashes.cpp
src/crc32c.cpp
src/siphash_1_3.c
src/siphash_2_4.c
src/third_party/farmhash/farmhash.cc
src/third_party/cityhash102/city.cc
src/query_farm_telemetry.cpp)

# FarmHash defaults to the namespace `util`; give it a private one so a static build bundling another FarmHash can't
# collide with it.
set_source_files_properties(src/third_party/farmhash/farmhash.cc src/vendored_hashes.cpp PROPERTIES
  COMPILE_DEFINITIONS NAMESPACE_FOR_HASH_FUNCTIONS=hashfuncs_farmhash)
# FarmHash uses __builtin_expect unless told not to; MSVC doesn't have it.
if(MSVC)
  set_property(SOURCE src/third_party/farmhash/farmhash.cc APPEND PROPERTY COMPILE_DEFINITIONS FARMHASH_NO_BUILTIN_EXPECT)
endif()

# CityHash v1.0.2 includes its own headers with angle brackets; keep its config.h off the global include path.
set_source_files_properties(src/third_party/cityhash102/city.cc PROPERTIES INCLUDE_DIRECTORIES
  ${CMAKE_CURRENT_SOURCE_DIR}/src/third_party/cityhash102)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL in both the static library as the loadable extension
target_link_libraries(${EXTENSION_NAME} xxHash::xxhash murmurhash::murmurhash)
target_link_libraries(${LOADABLE_EXTENSION_NAME} xxHash::xxhash murmurhash::murmurhash)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
