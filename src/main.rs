use std::time::Instant;

use anyhow::{bail, Result};
use geo::Intersects;
use geojson::Feature;
use topojson::{to_geojson, TopoJson};

fn main() -> Result<()> {
    let boundary = load_boundary("data/boundary.geojson")?;

    let gj = load_geojson()?;

    let start = Instant::now();
    let mut output = Vec::new();
    for gj_feature in gj {
        // TODO Can we avoid the clone and just use the geometry?
        let geom: geo::Geometry<f64> = gj_feature.clone().try_into()?;
        if boundary.intersects(&geom) {
            output.push(gj_feature);
        }
    }
    println!(
        "Filtering took {:?}. {} results",
        start.elapsed(),
        output.len()
    );

    let out = geojson::GeoJson::from(output.into_iter().collect::<geojson::FeatureCollection>());
    std::fs::write("out.geojson", out.to_string())?;

    Ok(())
}

fn load_geojson() -> Result<Vec<Feature>> {
    let mut start = Instant::now();
    let topojson_str = std::fs::read_to_string("data/uk_oa.topojson")?;
    println!("Reading file took {:?}", start.elapsed());

    start = Instant::now();
    let topo = topojson_str.parse::<TopoJson>()?;
    println!("Parsing topojson took {:?}", start.elapsed());

    start = Instant::now();
    let fc = match topo {
        TopoJson::Topology(t) => to_geojson(&t, "OA_2011_Pop20")?,
        _ => bail!("Unexpected topojson contents"),
    };
    println!("Converting to geojson took {:?}", start.elapsed());

    Ok(fc.features)
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
