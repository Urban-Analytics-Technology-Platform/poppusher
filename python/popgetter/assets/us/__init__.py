import popgetter
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




class US(Country):
    key_prefix: ClassVar[str] = "us"
    geo_levels: ClassVar[list[str]] = list("tracts")
    required_tables: list[str] | None = None 

    def _country_metadata(self, _context) -> CountryMetadata:
        return CountryMetadata(
            name_short_en="United States",
            name_official="United States of America",
            iso3="USA",
            iso2="US",
            iso3166_2="US",
        )

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

     def _geometry(
        self, context
    ) -> list[tuple[GeometryMetadata, gpd.GeoDataFrame, pd.DataFrame]]:
        geometry_metadta = GeometryMetadata(
            validity_period_start=CENSUS_COLLECTION_DATE,
            validity_period_end=CENSUS_COLLECTION_DATE,
            level="tracts",
            hxl_tag="tracts"
        )


        gp.read_
        

        

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
