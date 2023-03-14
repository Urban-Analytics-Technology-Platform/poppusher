use std::time::Instant;

use anyhow::{bail, Result};
use geo::Intersects;
use geojson::Feature;
use serde::{Deserialize, Serialize};
use topojson::{to_geojson, TopoJson};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CensusZone {
    // England: OA11CD
    pub id: String,

    // (England-only for now)
    // 0 cars or vars per household. See https://www.ons.gov.uk/datasets/TS045/editions/2021/versions/3 for details.
    pub cars_0: u16,
    pub cars_1: u16,
    pub cars_2: u16,
    // 3 or more cars or vans per household
    pub cars_3: u16,
}

impl CensusZone {
    // Assumes "3 or more" just means 3
    pub fn total_cars(&self) -> u16 {
        self.cars_1 + 2 * self.cars_2 + 3 * self.cars_3
    }
}

/// Clips existing TopoJSON files to the given boundary. All polygons are in WGS84.
pub fn clip_zones(boundary: geo::Polygon<f64>) -> Result<Vec<(geo::Polygon<f64>, CensusZone)>> {
    let gj = load_all_zones_as_geojson()?;

    let start = Instant::now();
    let mut output = Vec::new();
    for gj_feature in gj {
        let geom: geo::Geometry<f64> = gj_feature.clone().try_into()?;
        if boundary.intersects(&geom) {
            let polygon = match geom {
                geo::Geometry::Polygon(p) => p,
                // TODO What're these, and what should we do with them?
                geo::Geometry::MultiPolygon(mut mp) => mp.0.remove(0),
                _ => bail!("Unexpected geometry type for {:?}", gj_feature.properties),
            };
            let census_zone = CensusZone {
                id: gj_feature
                    .property("ID")
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .to_string(),
                cars_0: gj_feature
                    .property("cars_0")
                    .unwrap()
                    .as_u64()
                    .unwrap()
                    .try_into()?,
                cars_1: gj_feature
                    .property("cars_1")
                    .unwrap()
                    .as_u64()
                    .unwrap()
                    .try_into()?,
                cars_2: gj_feature
                    .property("cars_2")
                    .unwrap()
                    .as_u64()
                    .unwrap()
                    .try_into()?,
                cars_3: gj_feature
                    .property("cars_3")
                    .unwrap()
                    .as_u64()
                    .unwrap()
                    .try_into()?,
            };
            output.push((polygon, census_zone));
        }
    }
    println!(
        "Filtering took {:?}. {} results",
        start.elapsed(),
        output.len()
    );

    Ok(output)
}

fn load_all_zones_as_geojson() -> Result<Vec<Feature>> {
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
