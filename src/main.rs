use anyhow::{bail, Result};
use geojson::Feature;

fn main() -> Result<()> {
    let boundary = load_boundary("data/boundary.geojson")?;

    let mut output = Vec::new();
    for (polygon, zone) in popgetter::clip_zones(boundary)? {
        let mut feature = Feature {
            bbox: None,
            geometry: Some(geojson::Geometry::new(geojson::Value::from(&polygon))),
            id: None,
            properties: None,
            foreign_members: None,
        };
        feature.set_property("id", zone.id);
        feature.set_property("cars_0", zone.cars_0);
        feature.set_property("cars_1", zone.cars_1);
        feature.set_property("cars_2", zone.cars_2);
        feature.set_property("cars_3", zone.cars_3);
        output.push(feature);
    }

    let out = geojson::GeoJson::from(output.into_iter().collect::<geojson::FeatureCollection>());
    std::fs::write("out.geojson", out.to_string())?;

    Ok(())
}

fn load_boundary(path: &str) -> Result<geo::Polygon<f64>> {
    let mut gj: geojson::FeatureCollection = std::fs::read_to_string(path)?.parse()?;
    if gj.features.len() != 1 {
        bail!("{path} doesn't have exactly 1 feature");
    }
    let value = gj.features.pop().unwrap().geometry.unwrap().value;
    match value {
        geojson::Value::Polygon(_) => Ok(value.try_into()?),
        _ => bail!("wrong type"),
    }
}
