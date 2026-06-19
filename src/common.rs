use std::borrow::Cow;
use std::sync::Arc;

use anyhow::Context;
use arrow::array::StringBuilder;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::TimeUnit;
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
/// the earlier of the two instants is chosen; a non-existent local time (the
/// spring-forward gap) returns an error.
pub fn warsaw_naive_to_utc_millis(naive: NaiveDateTime) -> anyhow::Result<i64> {
    let dt = Warsaw
        .from_local_datetime(&naive)
        .earliest()
        .with_context(|| {
            format!(
                "Local time `{}` does not exist in Europe/Warsaw (DST spring-forward gap).",
                naive
            )
        })?;
    Ok(dt.timestamp() * 1000)
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
