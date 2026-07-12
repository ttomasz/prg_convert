use std::borrow::Cow;
use std::sync::Arc;

use anyhow::Context;
use arrow::array::ArrayBuilder;
use arrow::array::Date32Builder;
use arrow::array::Float64Builder;
use arrow::array::RecordBatch;
use arrow::array::StringBuilder;
use arrow::array::TimestampMillisecondBuilder;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::TimeUnit;
use chrono::Duration;
use chrono::MappedLocalTime;
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use chrono::TimeZone;
use chrono_tz::Europe::Warsaw;
#[cfg(feature = "cli")]
use geoarrow::datatypes::Crs;
#[cfg(feature = "cli")]
use geoarrow::datatypes::PointType;
use std::sync::LazyLock;

use proj4rs::Proj;

use crate::CoordOrder;

pub const EPOCH_DATE: NaiveDate = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

pub static SCHEMA_CSV: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("przestrzen_nazw", DataType::Utf8, false),
        Field::new("lokalny_id", DataType::Utf8, false),
        Field::new(
            "wersja_id",
            DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC"))),
            false,
        ),
        Field::new(
            "poczatek_wersji_obiektu",
            DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC"))),
            true,
        ),
        Field::new("wazny_od_lub_data_nadania", DataType::Date32, true),
        Field::new("wazny_do", DataType::Date32, true),
        Field::new("teryt_wojewodztwo", DataType::Utf8, true),
        Field::new("wojewodztwo", DataType::Utf8, false),
        Field::new("teryt_powiat", DataType::Utf8, true),
        Field::new("powiat", DataType::Utf8, false),
        Field::new("teryt_gmina", DataType::Utf8, true),
        Field::new("gmina", DataType::Utf8, false),
        Field::new("teryt_miejscowosc", DataType::Utf8, true),
        Field::new("miejscowosc", DataType::Utf8, false),
        Field::new("czesc_miejscowosci", DataType::Utf8, true),
        Field::new("teryt_ulica", DataType::Utf8, true),
        Field::new("ulica", DataType::Utf8, true),
        Field::new("numer_porzadkowy", DataType::Utf8, false),
        Field::new("kod_pocztowy", DataType::Utf8, true),
        Field::new("status", DataType::Utf8, true),
        Field::new("x_epsg_2180", DataType::Float64, true),
        Field::new("y_epsg_2180", DataType::Float64, true),
        Field::new("dlugosc_geograficzna", DataType::Float64, true),
        Field::new("szerokosc_geograficzna", DataType::Float64, true),
    ]))
});
#[cfg(feature = "cli")]
pub static CRS_2180: LazyLock<Crs> = LazyLock::new(|| {
    Crs::from_projjson(serde_json::from_str(include_str!("crs/epsg2180.json")).unwrap())
});
#[cfg(feature = "cli")]
pub static CRS_4326: LazyLock<Crs> = LazyLock::new(|| {
    Crs::from_projjson(serde_json::from_str(include_str!("crs/epsg4326.json")).unwrap())
});
pub static EPSG_2180: LazyLock<Proj> = LazyLock::new(|| Proj::from_epsg_code(2180).unwrap());
pub static EPSG_4326: LazyLock<Proj> = LazyLock::new(|| Proj::from_epsg_code(4326).unwrap());

#[cfg(feature = "cli")]
pub fn get_geoparquet_schema(geoarrow_geom_type: PointType) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("przestrzen_nazw", DataType::Utf8, false),
        Field::new("lokalny_id", DataType::Utf8, false),
        Field::new(
            "wersja_id",
            DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC"))),
            false,
        ),
        Field::new(
            "poczatek_wersji_obiektu",
            DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC"))),
            true,
        ),
        Field::new("wazny_od_lub_data_nadania", DataType::Date32, true),
        Field::new("wazny_do", DataType::Date32, true),
        Field::new("teryt_wojewodztwo", DataType::Utf8, true),
        Field::new("wojewodztwo", DataType::Utf8, false),
        Field::new("teryt_powiat", DataType::Utf8, true),
        Field::new("powiat", DataType::Utf8, false),
        Field::new("teryt_gmina", DataType::Utf8, true),
        Field::new("gmina", DataType::Utf8, false),
        Field::new("teryt_miejscowosc", DataType::Utf8, true),
        Field::new("miejscowosc", DataType::Utf8, false),
        Field::new("czesc_miejscowosci", DataType::Utf8, true),
        Field::new("teryt_ulica", DataType::Utf8, true),
        Field::new("ulica", DataType::Utf8, true),
        Field::new("numer_porzadkowy", DataType::Utf8, false),
        Field::new("kod_pocztowy", DataType::Utf8, true),
        Field::new("status", DataType::Utf8, true),
        Field::new("dlugosc_geograficzna", DataType::Float64, true),
        Field::new("szerokosc_geograficzna", DataType::Float64, true),
        geoarrow_geom_type.to_field("geometry", true),
    ]))
}

/// Owns the arrow column builders for one canonical (`SCHEMA_CSV`-shaped)
/// batch. Shared by both schema parsers so the column set, order, and
/// null-padding are defined in one place, next to `SCHEMA_CSV`.
pub(crate) struct CanonicalBuilders {
    pub(crate) uuid: StringBuilder,
    pub(crate) id_namespace: StringBuilder,
    pub(crate) version: TimestampMillisecondBuilder,
    pub(crate) lifecycle_start_date: TimestampMillisecondBuilder,
    pub(crate) valid_since_date: Date32Builder,
    pub(crate) valid_to_date: Date32Builder,
    pub(crate) voivodeship: StringBuilder,
    pub(crate) county: StringBuilder,
    pub(crate) municipality: StringBuilder,
    pub(crate) city: StringBuilder,
    pub(crate) city_part: StringBuilder,
    pub(crate) street: StringBuilder,
    pub(crate) house_number: StringBuilder,
    pub(crate) postcode: StringBuilder,
    pub(crate) status: StringBuilder,
    pub(crate) x_epsg_2180: Float64Builder,
    pub(crate) y_epsg_2180: Float64Builder,
    pub(crate) longitude: Float64Builder,
    pub(crate) latitude: Float64Builder,
    pub(crate) voivodeship_teryt_id: StringBuilder,
    pub(crate) county_teryt_id: StringBuilder,
    pub(crate) municipality_teryt_id: StringBuilder,
    pub(crate) city_teryt_id: StringBuilder,
    pub(crate) street_teryt_id: StringBuilder,
}

impl CanonicalBuilders {
    pub(crate) fn with_capacity(batch_size: usize) -> Self {
        Self {
            id_namespace: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            uuid: StringBuilder::with_capacity(batch_size, 36 * batch_size),
            version: TimestampMillisecondBuilder::with_capacity(batch_size)
                .with_timezone(Arc::from("UTC")),
            lifecycle_start_date: TimestampMillisecondBuilder::with_capacity(batch_size)
                .with_timezone(Arc::from("UTC")),
            valid_since_date: Date32Builder::with_capacity(batch_size),
            valid_to_date: Date32Builder::with_capacity(batch_size),
            voivodeship: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            county: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            municipality: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            city: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            city_part: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            street: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            house_number: StringBuilder::with_capacity(batch_size, 6 * batch_size),
            postcode: StringBuilder::with_capacity(batch_size, 6 * batch_size),
            status: StringBuilder::with_capacity(batch_size, 10 * batch_size),
            x_epsg_2180: Float64Builder::with_capacity(batch_size),
            y_epsg_2180: Float64Builder::with_capacity(batch_size),
            longitude: Float64Builder::with_capacity(batch_size),
            latitude: Float64Builder::with_capacity(batch_size),
            voivodeship_teryt_id: StringBuilder::with_capacity(batch_size, 54 * batch_size),
            county_teryt_id: StringBuilder::with_capacity(batch_size, 54 * batch_size),
            municipality_teryt_id: StringBuilder::with_capacity(batch_size, 54 * batch_size),
            city_teryt_id: StringBuilder::with_capacity(batch_size, 62 * batch_size),
            street_teryt_id: StringBuilder::with_capacity(batch_size, 91 * batch_size),
        }
    }

    /// Finish all builders into a batch matching `SCHEMA_CSV`'s column order.
    pub(crate) fn build_record_batch(&mut self) -> RecordBatch {
        RecordBatch::try_new(
            SCHEMA_CSV.clone(),
            vec![
                Arc::new(self.id_namespace.finish()),
                Arc::new(self.uuid.finish()),
                Arc::new(self.version.finish()),
                Arc::new(self.lifecycle_start_date.finish()),
                Arc::new(self.valid_since_date.finish()),
                Arc::new(self.valid_to_date.finish()),
                Arc::new(self.voivodeship_teryt_id.finish()),
                Arc::new(self.voivodeship.finish()),
                Arc::new(self.county_teryt_id.finish()),
                Arc::new(self.county.finish()),
                Arc::new(self.municipality_teryt_id.finish()),
                Arc::new(self.municipality.finish()),
                Arc::new(self.city_teryt_id.finish()),
                Arc::new(self.city.finish()),
                Arc::new(self.city_part.finish()),
                Arc::new(self.street_teryt_id.finish()),
                Arc::new(self.street.finish()),
                Arc::new(self.house_number.finish()),
                Arc::new(self.postcode.finish()),
                Arc::new(self.status.finish()),
                Arc::new(self.x_epsg_2180.finish()),
                Arc::new(self.y_epsg_2180.finish()),
                Arc::new(self.longitude.finish()),
                Arc::new(self.latitude.finish()),
            ],
        )
        .expect("Failed to create RecordBatch")
    }

    /// Called at the end of each parsed address record: `uuid` is appended for
    /// every record, so any other column that fell one row behind it gets a
    /// null, keeping all builders the same length.
    pub(crate) fn pad_short_columns(&mut self) {
        let buffer_length = self.uuid.len();
        if self.id_namespace.len() < buffer_length {
            self.id_namespace.append_null();
        }
        if self.version.len() < buffer_length {
            self.version.append_null();
        }
        if self.lifecycle_start_date.len() < buffer_length {
            self.lifecycle_start_date.append_null();
        }
        if self.valid_since_date.len() < buffer_length {
            self.valid_since_date.append_null();
        }
        if self.valid_to_date.len() < buffer_length {
            self.valid_to_date.append_null();
        }
        if self.voivodeship.len() < buffer_length {
            self.voivodeship.append_null();
        }
        if self.county.len() < buffer_length {
            self.county.append_null();
        }
        if self.municipality.len() < buffer_length {
            self.municipality.append_null();
        }
        if self.city.len() < buffer_length {
            self.city.append_null();
        }
        if self.city_part.len() < buffer_length {
            self.city_part.append_null();
        }
        if self.street.len() < buffer_length {
            self.street.append_null();
        }
        if self.house_number.len() < buffer_length {
            self.house_number.append_null();
        }
        if self.postcode.len() < buffer_length {
            self.postcode.append_null();
        }
        if self.status.len() < buffer_length {
            self.status.append_null();
        }
        if self.longitude.len() < buffer_length {
            self.longitude.append_null();
        }
        if self.latitude.len() < buffer_length {
            self.latitude.append_null();
        }
        if self.voivodeship_teryt_id.len() < buffer_length {
            self.voivodeship_teryt_id.append_null();
        }
        if self.county_teryt_id.len() < buffer_length {
            self.county_teryt_id.append_null();
        }
        if self.municipality_teryt_id.len() < buffer_length {
            self.municipality_teryt_id.append_null();
        }
        if self.city_teryt_id.len() < buffer_length {
            self.city_teryt_id.append_null();
        }
        if self.street_teryt_id.len() < buffer_length {
            self.street_teryt_id.append_null();
        }
        if self.x_epsg_2180.len() < buffer_length {
            self.x_epsg_2180.append_null();
        }
        if self.y_epsg_2180.len() < buffer_length {
            self.y_epsg_2180.append_null();
        }
    }
}

pub fn get_attribute<'a>(
    event_start: &'a quick_xml::events::BytesStart<'_>,
    attribute: &'a [u8],
) -> Cow<'a, str> {
    event_start
        .attributes()
        .find(|a| a.as_ref().expect("Could not parse attribute.").key.as_ref() == attribute)
        .expect("Could not find attribute.")
        .expect("Could not parse attribute.")
        .decode_and_unescape_value(event_start.decoder())
        .expect("Could not decode attribute value.")
}

pub fn str_append_value_or_null(builder: &mut StringBuilder, value: &str) {
    if value.is_empty() {
        builder.append_null();
    } else {
        builder.append_value(value);
    }
}

pub fn option_append_value_or_null(builder: &mut StringBuilder, value: Option<String>) {
    match value {
        None => {
            builder.append_null();
        }
        Some(s) => {
            builder.append_value(s);
        }
    }
}

pub struct PointCoords {
    pub x4326: f64,
    pub y4326: f64,
    pub x2180: f64,
    pub y2180: f64,
}

pub fn parse_gml_pos(
    text_trimmed: &str,
    coordinate_order: CoordOrder,
) -> anyhow::Result<Option<PointCoords>> {
    let coords: Vec<&str> = text_trimmed.split_whitespace().collect();
    if coords.len() == 2 {
        let (x, y) = match coordinate_order {
            CoordOrder::XY => (coords[0], coords[1]),
            CoordOrder::YX => (coords[1], coords[0]),
        };
        let y2180 = y
            .parse::<f64>()
            .with_context(|| format!("Could not parse y2180 out of: `{}`", text_trimmed))?;
        let x2180 = x
            .parse::<f64>()
            .with_context(|| format!("Could not parse x2180 out of: `{}`", text_trimmed))?;
        if x2180.is_nan() || y2180.is_nan() {
            Ok(None)
        } else {
            let mut p = (x2180.clone(), y2180.clone());
            proj4rs::transform::transform(&EPSG_2180, &EPSG_4326, &mut p).with_context(|| {
                format!(
                    "Failed to transform coordinates `{:?}` from EPSG:2180 to EPSG:4326",
                    p
                )
            })?;
            let lon = p.0.to_degrees();
            let lat = p.1.to_degrees();
            Ok(Some(PointCoords {
                x4326: lon,
                y4326: lat,
                x2180: x2180,
                y2180: y2180,
            }))
        }
    } else {
        anyhow::bail!(
            "Warning: could not parse coordinates in gml:pos: `{}`.",
            text_trimmed
        );
    }
}

/// Interpret a naive datetime as `Europe/Warsaw` wall-clock time and return the
/// corresponding UTC instant as epoch milliseconds.
///
/// Uses the IANA tz database so winter (CET, +01:00) and summer (CEST, +02:00)
/// are handled correctly. On an ambiguous local time (the autumn DST overlap)
/// the earlier of the two instants is chosen. A non-existent local time (the
/// spring-forward gap) is resolved by shifting forward past the gap, matching
/// PostgreSQL / java.time semantics: 02:30 in a 02:00 -> 03:00 jump becomes
/// the same instant as 03:30 CEST.
pub fn warsaw_naive_to_utc_millis(naive: NaiveDateTime) -> anyhow::Result<i64> {
    match Warsaw.from_local_datetime(&naive) {
        MappedLocalTime::Single(dt) => Ok(dt.timestamp_millis()),
        MappedLocalTime::Ambiguous(earliest, _) => Ok(earliest.timestamp_millis()),
        // every spring-forward gap in Warsaw's tz history is one hour wide,
        // so shifting by an hour always lands on a mappable wall-clock time
        MappedLocalTime::None => Warsaw
            .from_local_datetime(&(naive + Duration::hours(1)))
            .earliest()
            .map(|dt| dt.timestamp_millis())
            .with_context(|| {
                format!(
                    "Local time `{}` cannot be mapped to a Europe/Warsaw instant.",
                    naive
                )
            }),
    }
}

#[test]
fn test_warsaw_naive_to_utc_millis_summer() {
    // 2025-10-14 14:04:04 Warsaw is CEST (+02:00) -> 12:04:04 UTC
    let naive = chrono::NaiveDate::from_ymd_opt(2025, 10, 14)
        .unwrap()
        .and_hms_opt(14, 4, 4)
        .unwrap();
    let expected = chrono::DateTime::parse_from_rfc3339("2025-10-14T12:04:04Z")
        .unwrap()
        .timestamp()
        * 1000;
    assert_eq!(warsaw_naive_to_utc_millis(naive).unwrap(), expected);
}

#[test]
fn test_warsaw_naive_to_utc_millis_winter() {
    // 2025-11-06 15:01:26 Warsaw is CET (+01:00) -> 14:01:26 UTC
    let naive = chrono::NaiveDate::from_ymd_opt(2025, 11, 6)
        .unwrap()
        .and_hms_opt(15, 1, 26)
        .unwrap();
    let expected = chrono::DateTime::parse_from_rfc3339("2025-11-06T14:01:26Z")
        .unwrap()
        .timestamp()
        * 1000;
    assert_eq!(warsaw_naive_to_utc_millis(naive).unwrap(), expected);
}

#[test]
fn test_warsaw_naive_to_utc_millis_spring_gap() {
    // 2025-03-30 02:30:00 does not exist in Warsaw (clocks jump 02:00 -> 03:00
    // that night). Resolved by shifting past the gap: same instant as
    // 03:30 CEST (+02:00) -> 01:30 UTC.
    let naive = chrono::NaiveDate::from_ymd_opt(2025, 3, 30)
        .unwrap()
        .and_hms_opt(2, 30, 0)
        .unwrap();
    let expected = chrono::DateTime::parse_from_rfc3339("2025-03-30T01:30:00Z")
        .unwrap()
        .timestamp_millis();
    assert_eq!(warsaw_naive_to_utc_millis(naive).unwrap(), expected);
}

#[test]
fn test_warsaw_naive_to_utc_millis_autumn_overlap() {
    // 2024-10-27 02:30:00 occurs twice in Warsaw (clocks fall back
    // 03:00 -> 02:00 that night). Policy: pick the earlier instant,
    // i.e. CEST (+02:00) -> 00:30 UTC, not CET (+01:00) -> 01:30 UTC.
    let naive = chrono::NaiveDate::from_ymd_opt(2024, 10, 27)
        .unwrap()
        .and_hms_opt(2, 30, 0)
        .unwrap();
    let expected = chrono::DateTime::parse_from_rfc3339("2024-10-27T00:30:00Z")
        .unwrap()
        .timestamp_millis();
    assert_eq!(warsaw_naive_to_utc_millis(naive).unwrap(), expected);
}

#[test]
fn test_get_attribute_returns_value() {
    let xml = r#"<root attr="hello" key="value"/>"#;
    let mut reader = quick_xml::Reader::from_str(xml);
    reader.config_mut().expand_empty_elements = true;
    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf).unwrap() {
            quick_xml::events::Event::Start(e) => {
                assert_eq!(get_attribute(&e, b"attr"), Cow::from("hello"));
                assert_eq!(get_attribute(&e, b"key"), Cow::from("value"));
                break;
            }
            _ => {}
        }
    }
}

#[test]
fn test_parse_gml_pos_empty() {
    let gml_pos = "";
    let coords = parse_gml_pos(gml_pos, CoordOrder::XY);
    assert!(coords.is_err());
}

#[test]
fn test_parse_gml_pos_1() {
    let gml_pos = "0.0";
    let coords = parse_gml_pos(gml_pos, CoordOrder::XY);
    assert!(coords.is_err());
}

#[test]
fn test_parse_gml_pos_3() {
    let gml_pos = "0.0 1.1 2.2";
    let coords = parse_gml_pos(gml_pos, CoordOrder::XY);
    assert!(coords.is_err());
}

#[test]
fn test_parse_gml_pos_nan() {
    let gml_pos = "NaN NaN";
    let coords = parse_gml_pos(gml_pos, CoordOrder::XY).expect("NaN should have been parsed.");
    assert!(coords.is_none());
}

#[test]
fn test_parse_gml_pos_xy() {
    let gml_pos = "216691.39 505645.69";
    let coords = parse_gml_pos(gml_pos, CoordOrder::XY).unwrap().unwrap();
    assert!((coords.x2180 - 216691.39).abs() <= 0.01);
    assert!((coords.y2180 - 505645.69).abs() <= 0.01);
    assert!((coords.x4326 - 14.8391033).abs() <= 0.000001);
    assert!((coords.y4326 - 52.343422).abs() <= 0.000001);
}

#[test]
fn test_parse_gml_pos_yx() {
    let gml_pos = "505645.69 216691.39";
    let coords = parse_gml_pos(gml_pos, CoordOrder::YX).unwrap().unwrap();
    assert!((coords.x2180 - 216691.39).abs() <= 0.01);
    assert!((coords.y2180 - 505645.69).abs() <= 0.01);
    assert!((coords.x4326 - 14.8391033).abs() <= 0.000001);
    assert!((coords.y4326 - 52.343422).abs() <= 0.000001);
}
