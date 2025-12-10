use std::borrow::Cow;
use std::sync::Arc;

use anyhow::Context;
use arrow::array::StringBuilder;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::TimeUnit;
use chrono::NaiveDate;
use geoarrow::datatypes::Crs;
use geoarrow::datatypes::PointType;
use once_cell::sync::Lazy;
use proj4rs::Proj;

pub const EPOCH_DATE: NaiveDate = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

pub static SCHEMA_CSV: Lazy<Arc<Schema>> = Lazy::new(|| {
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
const PROJJSON_EPSG_2180: Lazy<serde_json::Value> = Lazy::new(|| {
    serde_json::from_str(
        r#"
    {
    "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
    "type": "ProjectedCRS",
    "name": "ETRF2000-PL / CS92",
    "base_crs": {
        "name": "ETRF2000-PL",
        "datum": {
        "type": "GeodeticReferenceFrame",
        "name": "ETRF2000 Poland",
        "ellipsoid": {
            "name": "GRS 1980",
            "semi_major_axis": 6378137,
            "inverse_flattening": 298.257222101
        }
        },
        "coordinate_system": {
        "subtype": "ellipsoidal",
        "axis": [
            {
            "name": "Geodetic latitude",
            "abbreviation": "Lat",
            "direction": "north",
            "unit": "degree"
            },
            {
            "name": "Geodetic longitude",
            "abbreviation": "Lon",
            "direction": "east",
            "unit": "degree"
            }
        ]
        },
        "id": {
        "authority": "EPSG",
        "code": 9702
        }
    },
    "conversion": {
        "name": "Poland CS92",
        "method": {
        "name": "Transverse Mercator",
        "id": {
            "authority": "EPSG",
            "code": 9807
        }
        },
        "parameters": [
        {
            "name": "Latitude of natural origin",
            "value": 0,
            "unit": "degree",
            "id": {
            "authority": "EPSG",
            "code": 8801
            }
        },
        {
            "name": "Longitude of natural origin",
            "value": 19,
            "unit": "degree",
            "id": {
            "authority": "EPSG",
            "code": 8802
            }
        },
        {
            "name": "Scale factor at natural origin",
            "value": 0.9993,
            "unit": "unity",
            "id": {
            "authority": "EPSG",
            "code": 8805
            }
        },
        {
            "name": "False easting",
            "value": 500000,
            "unit": "metre",
            "id": {
            "authority": "EPSG",
            "code": 8806
            }
        },
        {
            "name": "False northing",
            "value": -5300000,
            "unit": "metre",
            "id": {
            "authority": "EPSG",
            "code": 8807
            }
        }
        ]
    },
    "coordinate_system": {
        "subtype": "Cartesian",
        "axis": [
        {
            "name": "Northing",
            "abbreviation": "x",
            "direction": "north",
            "unit": "metre"
        },
        {
            "name": "Easting",
            "abbreviation": "y",
            "direction": "east",
            "unit": "metre"
        }
        ]
    },
    "scope": "Topographic mapping (medium and small scale).",
    "area": "Poland - onshore and offshore.",
    "bbox": {
        "south_latitude": 49,
        "west_longitude": 14.14,
        "north_latitude": 55.93,
        "east_longitude": 24.15
    },
    "id": {
        "authority": "EPSG",
        "code": 2180
    }
    }
"#,
    )
    .unwrap()
});
const PROJJSON_EPSG_4326: Lazy<serde_json::Value> = Lazy::new(|| {
    serde_json::from_str(
        r#"
    {
    "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
    "type": "GeographicCRS",
    "name": "WGS 84",
    "datum_ensemble": {
        "name": "World Geodetic System 1984 ensemble",
        "members": [
        {
            "name": "World Geodetic System 1984 (Transit)",
            "id": {
            "authority": "EPSG",
            "code": 1166
            }
        },
        {
            "name": "World Geodetic System 1984 (G730)",
            "id": {
            "authority": "EPSG",
            "code": 1152
            }
        },
        {
            "name": "World Geodetic System 1984 (G873)",
            "id": {
            "authority": "EPSG",
            "code": 1153
            }
        },
        {
            "name": "World Geodetic System 1984 (G1150)",
            "id": {
            "authority": "EPSG",
            "code": 1154
            }
        },
        {
            "name": "World Geodetic System 1984 (G1674)",
            "id": {
            "authority": "EPSG",
            "code": 1155
            }
        },
        {
            "name": "World Geodetic System 1984 (G1762)",
            "id": {
            "authority": "EPSG",
            "code": 1156
            }
        },
        {
            "name": "World Geodetic System 1984 (G2139)",
            "id": {
            "authority": "EPSG",
            "code": 1309
            }
        }
        ],
        "ellipsoid": {
        "name": "WGS 84",
        "semi_major_axis": 6378137,
        "inverse_flattening": 298.257223563
        },
        "accuracy": "2.0",
        "id": {
        "authority": "EPSG",
        "code": 6326
        }
    },
    "coordinate_system": {
        "subtype": "ellipsoidal",
        "axis": [
        {
            "name": "Geodetic latitude",
            "abbreviation": "Lat",
            "direction": "north",
            "unit": "degree"
        },
        {
            "name": "Geodetic longitude",
            "abbreviation": "Lon",
            "direction": "east",
            "unit": "degree"
        }
        ]
    },
    "scope": "Horizontal component of 3D system.",
    "area": "World.",
    "bbox": {
        "south_latitude": -90,
        "west_longitude": -180,
        "north_latitude": 90,
        "east_longitude": 180
    },
    "id": {
        "authority": "EPSG",
        "code": 4326
    }
    }
"#,
    )
    .unwrap()
});
pub const CRS_4326: Lazy<Crs> = Lazy::new(|| Crs::from_projjson(PROJJSON_EPSG_4326.clone()));
pub const CRS_2180: Lazy<Crs> = Lazy::new(|| Crs::from_projjson(PROJJSON_EPSG_2180.clone()));
pub const EPSG_2180: Lazy<Proj> = Lazy::new(|| Proj::from_epsg_code(2180).unwrap());
pub const EPSG_4326: Lazy<Proj> = Lazy::new(|| Proj::from_epsg_code(4326).unwrap());

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

pub fn parse_gml_pos(text_trimmed: &str) -> anyhow::Result<Option<PointCoords>> {
    let coords: Vec<&str> = text_trimmed.split_whitespace().collect();
    if coords.len() == 2 {
        let y2180 = coords[0]
            .parse::<f64>()
            .with_context(|| format!("Could not parse y2180 out of: `{}`", text_trimmed))?;
        let x2180 = coords[1]
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
    let coords = parse_gml_pos(gml_pos);
    assert!(coords.is_err());
}

#[test]
fn test_parse_gml_pos_1() {
    let gml_pos = "0.0";
    let coords = parse_gml_pos(gml_pos);
    assert!(coords.is_err());
}

#[test]
fn test_parse_gml_pos_3() {
    let gml_pos = "0.0 1.1 2.2";
    let coords = parse_gml_pos(gml_pos);
    assert!(coords.is_err());
}

#[test]
fn test_parse_gml_pos_nan() {
    let gml_pos = "NaN NaN";
    let coords = parse_gml_pos(gml_pos).expect("NaN should have been parsed.");
    assert!(coords.is_none());
}
