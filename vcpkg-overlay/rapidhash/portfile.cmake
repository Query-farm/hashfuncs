vcpkg_download_distfile(ARCHIVE
  URLS "https://github.com/Nicoshev/rapidhash/archive/refs/tags/rapidhash_v${VERSION}.tar.gz"
  FILENAME "rapidhash-v${VERSION}.tar.gz"
  SHA512 2f389411f8cffa91b02a6b91baf00ba11a888b312c9b93885aef783628f0d383ca83a2ed85a9f4e37c62ca8464620c582b3d5a581609c7eba619cfa4f9050b53
)

vcpkg_extract_source_archive(SOURCE_PATH
  ARCHIVE "${ARCHIVE}"
)

file(COPY "${SOURCE_PATH}/rapidhash.h" DESTINATION ${CURRENT_PACKAGES_DIR}/include)

file(INSTALL "${SOURCE_PATH}/LICENSE" DESTINATION "${CURRENT_PACKAGES_DIR}/share/${PORT}" RENAME copyright)
