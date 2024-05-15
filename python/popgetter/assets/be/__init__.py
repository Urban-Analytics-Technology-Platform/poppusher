#!/usr/bin/python3
from __future__ import annotations

from . import (
    census_derived,  # noqa: F401
    census_geometry,  # noqa: F401
    census_tables,  # noqa: F401
)


# @asset(key_prefix=asset_prefix)
# def get_population_details_per_municipality(context):
#     """
#     Downloads the population breakdown data per Municipality Sector and returns a DataFrame.

#     If the data has already been downloaded, it is not downloaded again and the
#     DataFrame is loaded from the cache.

#     returns a DataFrame with one row per, with the number of people satisfying a
#     unique combination of age, sex, civic (marital) status, per municipality.
#     """
#     output_dir = Path("population") / "demographic_breakdown"

#     # Population
#     # "Population by place of residence, nationality, marital status, age and sex"
#     # https://statbel.fgov.be/en/open-data/population-place-residence-nationality-marital-status-age-and-sex-13

#     # Data (excel)
#     # "https://statbel.fgov.be/sites/default/files/files/opendata/bevolking%20naar%20woonplaats%2C%20nationaliteit%20burgelijke%20staat%20%2C%20leeftijd%20en%20geslacht/TF_SOC_POP_STRUCT_2023.xlsx"
#     # Data (zipped txt)
#     pop_data_url = "https://statbel.fgov.be/sites/default/files/files/opendata/bevolking%20naar%20woonplaats%2C%20nationaliteit%20burgelijke%20staat%20%2C%20leeftijd%20en%20geslacht/TF_SOC_POP_STRUCT_2023.zip"

#     zip_file_contents = "TF_SOC_POP_STRUCT_2023.txt"

#     url = f"zip://{zip_file_contents}::{pop_data_url}"

#     text_file = get_path_to_cache(url, output_dir, "rt")

#     with text_file.open() as f:
#         population_df = pd.read_csv(f, sep="|", encoding="utf-8-sig")

#     population_df.index = population_df.index.astype(str)

#     context.add_output_metadata(
#         metadata={
#             "num_records": len(population_df),  # Metadata can be any key-value pair
#             "columns": MetadataValue.md(
#                 "\n".join([f"- '`{col}`'" for col in population_df.columns.to_list()])
#             ),
#             "preview": MetadataValue.md(population_df.head().to_markdown()),
#         }
#     )

#     return population_df


# @asset(key_prefix=asset_prefix)
# def get_population_by_statistical_sector(context):
#     """
#     Downloads the population data per Statistical Sector and returns a DataFrame.

#     If the data has already been downloaded, it is not downloaded again and the
#     DataFrame is loaded from the cache.

#     returns a DataFrame with one row per Statistical Sector, with the total number
#     of people per sector.
#     """
#     output_dir = Path("population") / "per_sector"

#     # Population
#     # Population by Statistical sector
#     # https://statbel.fgov.be/en/open-data/population-statistical-sector-10

#     pop_data_url = "https://statbel.fgov.be/sites/default/files/files/opendata/bevolking/sectoren/OPENDATA_SECTOREN_2022.zip"

#     # The column descriptions from https://statbel.fgov.be/sites/default/files/files/opendata/bevolking/sectoren/Columns%20description2020.xlsx
#     # are incorrect and do not match the data. The correct column descriptions are taken from the data file itself.

#     # | Naam/Nom            |
#     # | ------------------- |
#     # | CD_REFNIS           |
#     # | CD_SECTOR           |
#     # | TOTAL               |
#     # | DT_STRT_SECTOR      |
#     # | DT_STOP_SECTOR      |
#     # | OPPERVLAKKTE IN HM² |
#     # | TX_DESCR_SECTOR_NL  |
#     # | TX_DESCR_SECTOR_FR  |
#     # | TX_DESCR_NL         |
#     # | TX_DESCR_FR         |

#     zip_file_contents = "OPENDATA_SECTOREN_2022.txt"
#     url = f"zip://{zip_file_contents}::{pop_data_url}"

#     text_file = get_path_to_cache(url, output_dir, "rt")

#     with text_file.open() as f:
#         population_df = pd.read_csv(f, sep="|", encoding="utf-8-sig")

#     population_df.index = population_df.index.astype(str)
#     population_df["CD_REFNIS"] = population_df["CD_REFNIS"].astype(str)

#     context.add_output_metadata(
#         metadata={
#             "num_records": len(population_df),  # Metadata can be any key-value pair
#             "columns": MetadataValue.md(
#                 "\n".join([f"- '`{col}`'" for col in population_df.columns.to_list()])
#             ),
#             "preview": MetadataValue.md(population_df.head().to_markdown()),
#         }
#     )

#     return population_df


# def aggregate_population_details_per_municipalities(
#     pop_per_municipality_df: pd.DataFrame, output_dir: Path
# ) -> pd.DataFrame:
#     """
#     Aggregates a DataFrame of the population details per Statistical Sector to Municipalities.

#     The `pop_per_municipality_df` is assumed to be produced by `get_population_details_per_municipality()`.

#     Also saves the result to a CSV file in the output_dir

#     returns a DataFrame with one row per Municipality, with the total number of people

#     TODO: Explore the other ways of aggregating this data.
#     """
#     # Columns detail take from https://statbel.fgov.be/sites/default/files/files/opendata/bevolking%20naar%20woonplaats%2C%20nationaliteit%20burgelijke%20staat%20%2C%20leeftijd%20en%20geslacht/Columns%20description.xlsx

#     # | Naam/Nom             | Description                     |
#     # | -------------------- | ------------------------------- |
#     # | CD_MUNTY_REFNIS      | Refnis code of the municipality |
#     # | TX_MUNTY_DESCR_NL    | Municipality name in Dutch      |
#     # | TX_MUNTY_DESCR_FR    | Municipality name in French     |
#     # | CD_DSTR_REFNIS       | Refnis code of the district     |
#     # | TX_ADM_DSTR_DESCR_NL | District name in Dutch          |
#     # | TX_ADM_DSTR_DESCR_FR | District name in French         |
#     # | CD_PROV_REFNIS       | Refnis code of the province     |
#     # | TX_PROV_DESCR_NL     | Province name in Dutch          |
#     # | TX_PROV_DESCR_FR     | Province name in French         |
#     # | CD_RGN_REFNIS        | Refnis code of the region       |
#     # | TX_RGN_DESCR_NL      | Region name in Dutch            |
#     # | TX_RGN_DESCR_FR      | Region name in French           |
#     # | CD_SEX               | Gender                          |
#     # | CD_NATLTY            | Nationality code                |
#     # | TX_NATLTY_FR         | Nationality in French           |
#     # | TX_NATLTY_NL         | Nationality in Dutch            |
#     # | CD_CIV_STS           | Civil status code               |
#     # | TX_CIV_STS_FR        | Civil status in French          |
#     # | TX_CIV_STS_NL        | Civil status in Dutch           |
#     # | CD_AGE               | Age                             |
#     # | MS_POPULATION        | Number of individuals           |
#     # | CD_YEAR              | Reference year                  |

#     # Drop all the columns we don't need

#     # TODO there are many different ways top aggregate this data. For now we just take the sum of the population for each municipality
#     pop_per_municipality_df = pop_per_municipality_df[["CD_REFNIS", "MS_POPULATION"]]

#     munty_df = pop_per_municipality_df.groupby(by="CD_REFNIS").sum()
#     munty_df.to_csv(output_dir / "municipalities.csv", sep="|")
#     munty_df.index = munty_df.index.astype(str)

#     return munty_df


# @asset(
#     key_prefix=asset_prefix,
# )
# def get_car_per_sector(context):
#     """
#     Downloads the number of cars per Statistical Sector and returns a DataFrame.

#     If the data has already been downloaded, it is not downloaded again and the
#     DataFrame is loaded from the cache.

#     returns a DataFrame with one row per Statistical Sector, with the total number
#     of cars per sector.
#     """
#     output_dir = Path("car_ownership")

#     # Number of cars by Statistical sector
#     # https://statbel.fgov.be/en/open-data/number-cars-statistical-sector
#     cars_url = "https://statbel.fgov.be/sites/default/files/files/opendata/Aantal%20wagens%20per%20statistische%20sector/TF_CAR_HH_SECTOR.zip"

#     # Column names (inferred as the website links for the wrong column descriptions)

#     # | Column Name       |
#     # | ----------------- |
#     # | CD_YEAR           |
#     # | CD_REFNIS         |
#     # | TX_MUNTY_DESCR_FR |
#     # | TX_MUNTY_DESCR_NL |
#     # | TX_MUNTY_DESCR_DE |
#     # | TX_MUNTY_DESCR_EN |
#     # | cd_sector         |
#     # | total_huisH       |
#     # | total_wagens      |

#     zip_file_contents = "TF_CAR_HH_SECTOR.txt"
#     url = f"zip://{zip_file_contents}::{cars_url}"

#     text_file = get_path_to_cache(url, output_dir, "rt")

#     with text_file.open() as f:
#         car_per_sector_df = pd.read_csv(f, sep="|", encoding="utf-8-sig")

#     car_per_sector_df.index = car_per_sector_df.index.astype(str)

#     context.add_output_metadata(
#         metadata={
#             "num_records": len(car_per_sector_df),  # Metadata can be any key-value pair
#             "columns": MetadataValue.md(
#                 "\n".join(
#                     [f"- '`{col}`'" for col in car_per_sector_df.columns.to_list()]
#                 )
#             ),
#             "preview": MetadataValue.md(car_per_sector_df.head().to_markdown()),
#         }
#     )

#     return car_per_sector_df


# @asset(
#     key_prefix=asset_prefix,
# )
# def get_car_ownership_by_housetype(context):
#     """
#     Downloads the number of cars per household type by municipality and returns a DataFrame.

#     If the data has already been downloaded, it is not downloaded again and the
#     DataFrame is loaded from the cache.

#     returns a DataFrame with one row per Municipality, with the total number
#     of cars per household type.
#     """
#     output_dir = Path("car_ownership")

#     # Number of cars per household type by municipality
#     # https://statbel.fgov.be/en/open-data/number-cars-household-type-municipality
#     car_per_household_url = "https://statbel.fgov.be/sites/default/files/files/opendata/Aantal%20wagens%20volgens%20huishoudtype%20per%20gemeente/TF_CAR_HHTYPE_MUNTY.zip"

#     # Column Description from https://statbel.fgov.be/sites/default/files/files/opendata/Aantal%20wagens%20volgens%20huishoudtype%20per%20gemeente/Columns%20description_TF_CAR_HHTYPE_MUNTY.xlsx

#     # | Omschrijving                | Description                     |
#     # | --------------------------- | ------------------------------- |
#     # | Referentie jaar             | Reference year                  |
#     # | Refnis-code van de gemeente | Refnis code of the municipality |
#     # | Naam van de gemeente in FR  | Name of the municipality in FR  |
#     # | Naam van de gemeente in NL  | Name of the municipality in NL  |
#     # | Huishoud type               | Household type                  |
#     # | Aantal huishoudens          | Number of households            |
#     # | Aantal wagens               | Number of cars                  |

#     # where "Household type" is one of:

#     # | Detail                                   |
#     # | ---------------------------------------- |
#     # | 1\. People living alone                  |
#     # | 2\. Married couples with no children     |
#     # | 3\. Married couples with children        |
#     # | 4\. Not-married couples with no children |
#     # | 5\. Non-married couples with children    |
#     # | 6\. Single parents                       |
#     # | 7\. Other types of household             |
#     # | 8\. Collective households                |

#     zip_file_contents = "TF_CAR_HHTYPE_MUNTY.txt"
#     url = f"zip://{zip_file_contents}::{car_per_household_url}"

#     text_file = get_path_to_cache(url, output_dir, "rt")

#     with text_file.open() as f:
#         car_per_household_df = pd.read_csv(f, sep="|", encoding="utf-8-sig")

#     car_per_household_df.index = car_per_household_df.index.astype(str)

#     context.add_output_metadata(
#         metadata={
#             "num_records": len(
#                 car_per_household_df
#             ),  # Metadata can be any key-value pair
#             "columns": MetadataValue.md(
#                 "\n".join(
#                     [f"- '`{col}`'" for col in car_per_household_df.columns.to_list()]
#                 )
#             ),
#             "preview": MetadataValue.md(car_per_household_df.head().to_markdown()),
#         }
#     )

#     return car_per_household_df


# @asset(
#     key_prefix=asset_prefix,
#     ins={
#         "get_geometries": AssetIn(key_prefix=asset_prefix),
#         "get_population_by_statistical_sector": AssetIn(key_prefix=asset_prefix),
#     },
# )
# def sector_populations(context, get_geometries, get_population_by_statistical_sector):
#     """
#     Returns a GeoDataFrame of the Statistical Sectors joined with the population per sector.
#     """
#     # Population
#     pop_gdf = get_geometries.merge(
#         get_population_by_statistical_sector,
#         right_on="CD_REFNIS",
#         left_on="cd_munty_refnis",
#         how="inner",
#     )

#     # Plot and convert the image to Markdown to preview it within Dagster
#     # Yes we do pass the `plt` object to the markdown_from_plot function and not the `ax` object
#     ax = pop_gdf.plot(column="TOTAL", legend=True, scheme="quantiles")
#     ax.set_title("Populations per sector in Belgium")
#     md_plot = markdown_from_plot(plt)

#     context.add_output_metadata(
#         metadata={
#             "num_records": len(pop_gdf),  # Metadata can be any key-value pair
#             "columns": MetadataValue.md(
#                 "\n".join([f"- '`{col}`'" for col in pop_gdf.columns.to_list()])
#             ),
#             "preview": MetadataValue.md(
#                 pop_gdf.loc[:, pop_gdf.columns != "geometry"].head().to_markdown()
#             ),
#             "plot": MetadataValue.md(md_plot),
#         }
#     )

#     return pop_gdf


# @asset(
#     key_prefix=asset_prefix,
#     ins={
#         "get_geometries": AssetIn(key_prefix=asset_prefix),
#         "get_car_per_sector": AssetIn(key_prefix=asset_prefix),
#     },
# )
# def sector_car_ownership(context, get_geometries, get_car_per_sector):
#     """
#     Returns a GeoDataFrame of the Statistical Sectors joined with the number of cars per sector.
#     """
#     # Vehicle Ownership
#     cars = get_car_per_sector
#     cars["CD_REFNIS"] = cars["CD_REFNIS"].astype(str)
#     cars_gdf = get_geometries.merge(
#         cars, right_on="CD_REFNIS", left_on="cd_munty_refnis", how="inner"
#     )

#     # Plot and convert the image to Markdown to preview it within Dagster
#     # Yes we do pass the `plt` object to the markdown_from_plot function and not the `ax` object
#     ax = cars_gdf.plot(column="total_wagens", legend=True, scheme="quantiles")
#     ax.set_title("Car ownership per sector in Belgium")
#     md_plot = markdown_from_plot(plt)

#     context.add_output_metadata(
#         metadata={
#             "num_records": len(cars_gdf),  # Metadata can be any key-value pair
#             "columns": MetadataValue.md(
#                 "\n".join([f"- '`{col}`'" for col in cars_gdf.columns.to_list()])
#             ),
#             "preview": MetadataValue.md(
#                 cars_gdf.loc[:, cars_gdf.columns != "geometry"].head().to_markdown()
#             ),
#             "plot": MetadataValue.md(md_plot),
#         }
#     )

#     return cars_gdf


# @asset(
#     key_prefix=asset_prefix,
#     ins={
#         "get_population_details_per_municipality": AssetIn(key_prefix=asset_prefix),
#     },
# )
# def pivot_population(
#     context,
#     get_population_details_per_municipality: pd.DataFrame,
# ):
#     # For brevity
#     pop: pd.DataFrame = get_population_details_per_municipality

#     # Check that the columns we need are present (currently failing on Windows) on CI for some unknown reason
#     assert "CD_REFNIS" in pop.columns
#     assert "CD_AGE" in pop.columns
#     assert "MS_POPULATION" in pop.columns
#     assert len(pop) > 0

#     # Drop all the columns we don't need
#     pop = pop[
#         [
#             "CD_REFNIS",  # keep
#             # "CD_DSTR_REFNIS",   # drop
#             # "CD_PROV_REFNIS",   # drop
#             # "CD_RGN_REFNIS",    # drop
#             "CD_SEX",  # keep
#             # "CD_NATLTY",        # drop
#             # "CD_CIV_STS",       # drop
#             "CD_AGE",  # keep
#             "MS_POPULATION",  # keep
#             # "CD_YEAR",          # drop
#         ]
#     ]

#     # Check that the columns we need are present (currently failing on Windows) on CI for some unknown reason
#     assert "CD_REFNIS" in pop.columns
#     assert "CD_AGE" in pop.columns
#     assert "MS_POPULATION" in pop.columns
#     assert len(pop) > 0

#     new_table: pd.DataFrame = pd.DataFrame()

#     # Using HXL tags for variable names (https://hxlstandard.org/standard/1-1final/dictionary/#tag_population)
#     columns: dict[str, pd.Series[bool]] = {
#         "population_children_age5_17": (pop["CD_AGE"] >= 5) & (pop["CD_AGE"] < 18),
#         "population_infants_age0_4": (pop["CD_AGE"] <= 4),
#         "population_children_age0_17": (pop["CD_AGE"] >= 0) & (pop["CD_AGE"] < 18),
#         "population_adults_f": (pop["CD_AGE"] > 18) & (pop["CD_SEX"] == "F"),
#         "population_adults_m": (pop["CD_AGE"] > 18) & (pop["CD_SEX"] == "M"),
#         "population_adults": (pop["CD_AGE"] > 18),
#         "population_ind": (pop["CD_AGE"] >= 0),
#     }

#     for col_name, filter in columns.items():
#         new_col_def = {col_name: pd.NamedAgg(column="MS_POPULATION", aggfunc="sum")}
#         temp_table: pd.DataFrame = (
#             pop.loc[filter]
#             .groupby(by="CD_REFNIS", as_index=True)
#             .agg(
#                 func=None,
#                 **new_col_def,  # type: ignore TODO, don't know why pyright is complaining here
#             )
#         )

#         if len(new_table) == 0:
#             new_table = temp_table
#         else:
#             new_table = new_table.merge(
#                 temp_table, left_index=True, right_index=True, how="inner"
#             )

#     # table.set_index("CD_REFNIS", inplace=True, drop=False)

#     context.add_output_metadata(
#         metadata={
#             "num_records": len(new_table),  # Metadata can be any key-value pair
#             "columns": MetadataValue.md(
#                 "\n".join([f"- '`{col}`'" for col in new_table.columns.to_list()])
#             ),
#             "preview": MetadataValue.md(new_table.head().to_markdown()),
#         }
#     )

#     return new_table


# @asset(
#     key_prefix=asset_prefix,
#     ins={
#         "aggregate_sectors_to_municipalities": AssetIn(key_prefix=asset_prefix),
#         "pivot_population": AssetIn(key_prefix=asset_prefix),
#     },
# )
# def municipalities_populations(
#     context,
#     aggregate_sectors_to_municipalities,
#     pivot_population,
# ):
#     """
#     Returns a GeoDataFrame of the Municipalities joined with the population per municipality.
#     """
#     # Population
#     population = pivot_population
#     population.index = population.index.astype(str)

#     geom = aggregate_sectors_to_municipalities
#     pop_gdf = geom.merge(
#         population, right_on="CD_REFNIS", left_on="cd_munty_refnis", how="inner"
#     )

#     ax = pop_gdf.plot(column="population_ind", legend=True, scheme="quantiles")
#     ax.set_title("Population per Municipality in Belgium")
#     md_plot = markdown_from_plot(plt)

#     context.add_output_metadata(
#         metadata={
#             "num_records": len(pop_gdf),  # Metadata can be any key-value pair
#             "columns": MetadataValue.md(
#                 "\n".join([f"- '`{col}`'" for col in pop_gdf.columns.to_list()])
#             ),
#             "preview": MetadataValue.md(
#                 pop_gdf.loc[:, pop_gdf.columns != "geometry"].head().to_markdown()
#             ),
#             "plot": MetadataValue.md(md_plot),
#         }
#     )

#     return pop_gdf
