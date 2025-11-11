# Changelog

## [v0.2.0] - 2025-11-11

### Added

- added zstd compression to geoparquet

### Changed

- adjusted types of timestamp fields from TimestampSeconds to a better recognized TimestampMilliseconds
- adjusted type of lokalny_id to Uuid
- set max row_group_size in geoparquet writer to match batch_size

## [v0.1.0] - 2025-11-10

Initial release
