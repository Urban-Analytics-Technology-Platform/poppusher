use std::io::{Cursor, Read, Write};

extern crate zip;
use async_trait::async_trait;
use geojson::FeatureCollection;
use polars::{prelude::*, series::Series};
use zip::ZipArchive;

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
        let mut zip = ZipArchive::new(tmpfile)?;
        let mut file = zip.by_name("DZ2021.geojson")?;
        let mut buffer = String::from("");
        file.read_to_string(&mut buffer)?;
        Ok(buffer.parse()?)
    }
    async fn geojson_dataframe(&self) -> anyhow::Result<DataFrame> {
        let geojson = self.geojson().await.unwrap();
        let v = geojson.features.iter().fold(
            vec![Vec::<String>::new(), Vec::<String>::new()],
            |mut acc, feature| {
                acc[0].push(
                    feature
                        .properties
                        .as_ref()
                        .unwrap()
                        .get("DZ2021_cd")
                        .unwrap()
                        .to_string()
                        .replace('"', ""),
                );
                acc[1].push(feature.geometry.as_ref().unwrap().to_string());
                acc
            },
        );
        Ok(DataFrame::new(vec![
            Series::new("DZ2021_cd", v[0].clone()),
            Series::new("geometry", v[1].clone()),
        ])?)
    }
}
