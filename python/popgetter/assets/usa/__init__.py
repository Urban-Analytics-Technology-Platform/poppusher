
from popgetter.assets.country import Country
from popgetter.metadata import (
    CountryMetadata,
    DataPublisher,
    GeometryMetadata,
    MetricMetadata,
    SourceDataRelease,
    metadata_to_dataframe,
)
from popgetter.utils import add_metadata, markdown_from_plot
import geopandas as gpd
from popgetter.cloud_outputs import GeometryOutput, MetricsOutput
import pandas as pd
from typing import ClassVar
from dagster import asset

from .census_tasks import get_geom_ids_table_for_summary, generate_variable_dictionary
from datetime import date

from icecream import ic

from .config import ACS_METADATA, SUMMARY_LEVELS


class USA(Country):
    country_metadata: ClassVar[CountryMetadata] =  CountryMetadata(
            name_short_en="United States",
            name_official="United States of America",
            iso3="USA",
            iso2="US",
            iso3166_2="US",
    )    
    geo_levels: ClassVar[list[str]] = list("tracts")
    required_tables: list[str] | None = None 

    def _country_metadata(self, _context) -> CountryMetadata:
        return self.country_metadata

    def _data_publisher(
        self, _context, country_metadata: CountryMetadata
    ) -> DataPublisher:
        return DataPublisher(
            name="United States Census Bureau",
            url="https://www.census.gov/programs-surveys/acs",
            description=(
                """
                The United States Census Bureau, officially the Bureau of the Census, 
                is a principal agency of the U.S. Federal Statistical System, responsible 
                for producing data about the American people and economy.
                """

            ),
            countries_of_interest=[country_metadata.id],
        )
    
    def _catalog():
        pass
    

    def _geometry(
        self, context
    ) -> list[tuple[GeometryMetadata, gpd.GeoDataFrame, pd.DataFrame]]:
        geometries_to_return = []
        for year, metadata in ACS_METADATA.items():
            for geo_level, url in metadata["geoms"].items():
                geometry_metadata = GeometryMetadata(
                    validity_period_start=date(year, 1, 1),
                    validity_period_end=date(year, 1, 1),
                    level=geo_level,
                    # TODO: what should hxl_tag be?
                    hxl_tag=geo_level
                )

                region_geometries_raw: gpd.GeoDataFrame = gpd.read_file(url)
                
                # TODO: loop over the 
                # fiveYear is a superset of oneYear names, so use fiveYear
                region_geometries_names = get_geom_ids_table_for_summary(year, "fiveYear")
                

                region_geometries_raw = region_geometries_raw.dissolve(
                    by="GEOID"
                ).reset_index()

                context.log.debug(ic(region_geometries_raw.head()))
                region_geometries = region_geometries_raw.rename(
                    columns={"GEOID": "GEO_ID"}
                ).loc[:, ["geometry", "GEO_ID"]]
                
                region_names = (
                    region_geometries_names.rename(
                        columns={
                            "GEOID": "GEO_ID",
                            "NAME": "eng",
                            
                        }
                    )
                    .loc[
                        # Subset to row in geoms file
                        region_geometries_names["GEO_ID"].isin(region_geometries["GEOID"]),
                        ["GEO_ID", "eng"]
                    ]
                    .drop_duplicates()
                )


                geometries_to_return.append(
                    GeometryOutput(
                        metadata=geometry_metadata,
                        gdf=region_geometries,
                        names_df=region_names,
                    )
                )
        
        return geometries_to_return
        

    def _source_data_releases(
        self, _context, geometry, data_publisher
    ) -> dict[str, SourceDataRelease]:
        source_data_releases = {}
        for geo_metadata, _, _ in geometry:
            source_data_release: SourceDataRelease = SourceDataRelease(
                name="ACS 2019 5 year",
                # https://www.nisra.gov.uk/publications/census-2021-outputs-prospectus:
                # 9.30 am on 21 February 2023 for DZ and SDZ and District Electoral Areas
                date_published=date(2019, 1, 1),
                reference_period_start=date(2014, 1, 1),
                reference_period_end=date(2019, 1, 1),
                collection_period_start=CENSUS_COLLECTION_DATE,
                collection_period_end=CENSUS_COLLECTION_DATE,
                expect_next_update=date(2020, 1, 1),
                url="https://www.census.gov/programs-surveys/acs",
                data_publisher_id=data_publisher.id,
                description="""
                    The American Community Survey (ACS) helps local officials, 
                    community leaders, and businesses understand the changes 
                    taking place in their communities. It is the premier source 
                    for detailed population and housing information about our nation.
                """,
                geometry_metadata_id=geo_metadata.id,
            )
            source_data_releases[geo_metadata.level] = source_data_release
        return source_data_releases
    
    def _census_tables():
        pass
    
    def _derived_metrics():
        pass
        
    def _source_metric_metadata():
        pass

# @asset
# def num(context) -> int:
#     return 1

# Assets
usa = USA()
country_metadata = usa.create_country_metadata()
data_publisher = usa.create_data_publisher()
geometry = usa.create_geometry()