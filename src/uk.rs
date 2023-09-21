use std::io::{Cursor, Read, Write};

use async_trait::async_trait;
use geojson::FeatureCollection;
use polars::prelude::{CsvReader, DataFrame, SerReader};

use crate::getter::Getter;

// TODO: consider whether to add data/cache fields here.
#[derive(Default, Debug)]
pub struct NorthernIreland;

#[async_trait]
impl Getter for NorthernIreland {
    async fn population(&self) -> anyhow::Result<DataFrame> {
        let url =
            "https://build.nisra.gov.uk/en/custom/table.csv?d=PEOPLE&v=DZ21&v=UR_SEX&v=AGE_SYOA_85";
        let data: Vec<u8> = reqwest::get(url).await?.text().await?.bytes().collect();
        Ok(CsvReader::new(Cursor::new(data))
            .has_header(true)
            .finish()?)
    }
    async fn geojson(&self) -> anyhow::Result<FeatureCollection> {
        let url = "https://www.nisra.gov.uk/sites/nisra.gov.uk/files/publications/geography-dz2021-geojson.zip";
        let mut tmpfile = tempfile::tempfile()?;
        tmpfile.write_all(&reqwest::get(url).await?.bytes().await?)?;
        let mut zip = zip::ZipArchive::new(tmpfile)?;
        let mut file = zip.by_name("DZ2021.geojson")?;
        let mut buffer = String::from("");
        file.read_to_string(&mut buffer)?;
        Ok(buffer.parse()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_northern_ireland() {
        let ni = NorthernIreland;
        let pop = ni.population().await.unwrap();
        println!("{}", pop);
        let geojson = ni.geojson().await.unwrap();
        println!("{}", geojson);
    }
}
