use anyhow::Ok;
use polars::prelude::*;

use popgetter::{getter::Getter, uk::NorthernIreland};

// Bool to write merged if required as one dataframe though large dataframe as duplicate geometry
// for each age/sex category
const WRITE_MERGED: bool = false;

fn write_df(file_name: &str, df: &mut DataFrame) -> anyhow::Result<()> {
    let mut file = std::fs::File::create(file_name)?;
    CsvWriter::new(&mut file).finish(df)?;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let ni = NorthernIreland;

    // Census data
    let mut pop = ni.population().await?;
    println!("Population:");
    println!("{}", pop);
    write_df("data/pop.csv", &mut pop)?;

    // Geojson
    let mut geojson_df = ni.geojson_dataframe().await?;
    println!("Geojson as dataframe:");
    println!("{}", geojson_df);
    write_df("data/geo.csv", &mut geojson_df)?;

    // Merge
    if WRITE_MERGED {
        let mut merged = pop.join(
            &geojson_df,
            ["Census 2021 Data Zone Code"],
            ["DZ2021_cd"],
            JoinType::Left.into(),
        )?;
        println!("Merged:");
        println!("{}", merged);

        // Write to file
        write_df("data/merged.csv", &mut merged)?;
    }
    Ok(())
}
