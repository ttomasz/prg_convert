use std::collections::HashMap;

use once_cell::sync::Lazy;

static STREET_TYPE: Lazy<HashMap<&str, &str>> = Lazy::new(|| {
    let mut mapping = HashMap::new();
    mapping.insert("1", ""); // originally: ulica, which is default type and doesn't require to be provided
    mapping.insert("3", "plac");
    mapping.insert("11", "osiedle");
    mapping.insert("6", "rondo");
    mapping.insert("2", "aleja");
    mapping.insert("4", "skwer");
    mapping.insert("5", "bulwar");
    mapping.insert("7", "park");
    mapping.insert("8", "rynek");
    mapping.insert("9", "szosa");
    mapping.insert("10", "droga");
    mapping.insert("12", "ogród");
    mapping.insert("13", "wyspa");
    mapping.insert("14", "wybrzeże");
    mapping.insert("15", ""); //originally: innyLiniowy, which is catch-all term for any linear type
    mapping.insert("16", ""); // originally: innyPowierzchniowy, which is catch-all term for any area type
    mapping
});

/// Concatenates parts of the name and the street type.
/// If name contains shorthand type then we don't replace it with the full type name.
pub fn construct_full_name_from_parts(part1: &str, part2: Option<&str>, typ: &str) -> String {
    let str_typ = STREET_TYPE.get(typ).cloned().unwrap_or_default();
    let prefix = match typ {
        "3" => {
            if part1.to_lowercase().starts_with(str_typ) || part1.to_lowercase().starts_with("pl.")
            {
                ""
            } else {
                str_typ
            }
        }
        "11" => {
            if part1.to_lowercase().starts_with(str_typ) || part1.to_lowercase().starts_with("os.")
            {
                ""
            } else {
                str_typ
            }
        }
        "6" => {
            if part1.to_lowercase().starts_with(str_typ)
                || part1.to_lowercase().starts_with("rondo")
            {
                ""
            } else {
                str_typ
            }
        }
        "2" => {
            if part1.to_lowercase().starts_with(str_typ) || part1.to_lowercase().starts_with("al.")
            {
                ""
            } else {
                str_typ
            }
        }
        _ => {
            if part1.to_lowercase().starts_with(str_typ) {
                ""
            } else {
                str_typ
            }
        }
    };
    let name_parts = [prefix, part2.unwrap_or_default(), part1];
    let non_empty_parts: Vec<String> = name_parts
        .into_iter()
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    non_empty_parts.join(" ")
}

#[test]
fn name_from_part1() {
    let typ = "1";
    let part1 = "Test";
    let part2 = None;
    let expected_name = "Test";
    let name = construct_full_name_from_parts(part1, part2, typ);
    assert_eq!(name, expected_name);
}

#[test]
fn name_from_part1_part2() {
    let typ = "1";
    let part1 = "Test";
    let part2 = Some("Test2");
    let expected_name = "Test2 Test";
    let name = construct_full_name_from_parts(part1, part2, typ);
    assert_eq!(name, expected_name);
}

#[test]
fn name_from_part1_typ_3() {
    let typ = "3";
    let part1 = "Test";
    let part2 = None;
    let expected_name = "plac Test";
    let name = construct_full_name_from_parts(part1, part2, typ);
    assert_eq!(name, expected_name);
}

#[test]
fn name_from_part1_part2_typ_3() {
    let typ = "3";
    let part1 = "Test";
    let part2 = Some("Test2");
    let expected_name = "plac Test2 Test";
    let name = construct_full_name_from_parts(part1, part2, typ);
    assert_eq!(name, expected_name);
}

#[test]
fn name_from_part1_typ_3_prefix() {
    let typ = "3";
    let part1 = "plac Test";
    let part2 = None;
    let expected_name = "plac Test";
    let name = construct_full_name_from_parts(part1, part2, typ);
    assert_eq!(name, expected_name);
}

#[test]
fn name_from_part1_typ_3_prefix_short() {
    let typ = "3";
    let part1 = "pl. Test";
    let part2 = None;
    let expected_name = "pl. Test";
    let name = construct_full_name_from_parts(part1, part2, typ);
    assert_eq!(name, expected_name);
}
