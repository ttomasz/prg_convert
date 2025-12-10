# Changelog

## [v0.6.0] - 2025-12-10

### Fixed

- fixed model/schema 2021 reading coordinates in reversed order

### Added

- added cli flag to save geoparquet with geometry either in epsg:2180 crs (original) or epsg:4326 (transformed)

## [v0.5.0] - 2025-11-23

### Added

- prg_convert can now read content of ZIP file directly so you don't need to decompress. It determines which files in the archive to read based on their extensions and `--schema-version` parameter. Schema version 2012 reads files with xml extension, while version 2021 reads file with gml extension.

## [v0.4.0] - 2025-11-19

### Added

- option `--schema-version 2021` is now available, you can convert files in the new schema. Note that these files require to download additional dictionary with administrative unit names, see README.md for details

### Changed

- changed schema of output data to match both PRG models 2012 and 2021. Field `wazny_od` has been renamed to `wazny_od_lub_data_nadania`, field `status` is nullable now

## [v0.3.0] - 2025-11-12

### Fixed

- fixed trying to write lokalny_id as UUID type, there was a bug in the code which caused a crash when writing CSV, but even after fixing it it turned out it's not well supported by clients so we'll use regular strings

### Changed

- changed default zstd compression level from 16 to 11
- field lokalny_id will be included in compression (previously it was always uncompressed)

### Added

- added more cli options for geoparquet output format settings like compression type, level, etc

## [v0.2.0] - 2025-11-11

### Added

- added zstd compression to geoparquet

### Changed

- adjusted types of timestamp fields from TimestampSeconds to a better recognized TimestampMilliseconds
- adjusted type of lokalny_id to Uuid
- set max row_group_size in geoparquet writer to match batch_size

## [v0.1.0] - 2025-11-10

Initial release
