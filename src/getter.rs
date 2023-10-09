use async_trait::async_trait;
use geojson::FeatureCollection;
use polars::prelude::DataFrame;

/// Gets population and GIS data implemented on a given country.
// TODO: refine methods and types
#[async_trait]
pub trait Getter {
    /// Gets population data.
    async fn population(&self) -> anyhow::Result<DataFrame>;
    /// Gets GeoJSON data.
    async fn geojson(&self) -> anyhow::Result<FeatureCollection>;
    /// Gets GeoJSON data as a dataframe.
    async fn geojson_dataframe(&self) -> anyhow::Result<DataFrame>;
}
