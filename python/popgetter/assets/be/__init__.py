#!/usr/bin/python3
# from __future__ import annotations
from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    MetadataValue,
    OpExecutionContext,
    asset,
)

# from dagster._core.execution.context.compute import AssetExecutionContext
from popgetter.utils import download_zipped_files, get_path_to_cache, markdown_from_plot

WORKING_DIR = Path("belgium")
asset_prefix = "be"


# Cannot annotate `context` parameter with type AssetExecutionContext. `context` must be annotated with AssetExecutionContext, OpExecutionContext, or left blank.


@asset(key_prefix=asset_prefix)
def get_geometries(context: OpExecutionContext) -> gpd.GeoDataFrame:
    # def get_geometries(context: AssetExecutionContext) -> gpd.GeoDataFrame:
    """
    Downloads the Statistical Sector for Belgium and returns a GeoDataFrame.

    If the data has already been downloaded, it is not downloaded again and
    the cached version is used to create the GeoDataFrame.
    """
    output_dir = WORKING_DIR / "statistical_sectors" / "geometries"

    # Administrative boundaries - aka "Statistical sectors 2023 (Areas)"
    #
    # User WebUI:
    # "https://statbel.fgov.be/en/open-data/statistical-sectors-2023"

    # URL of datafile
    statistical_sectors = "https://statbel.fgov.be/sites/default/files/files/opendata/Statistische%20sectoren/sh_statbel_statistical_sectors_3812_20230101.geojson.zip"

    # Column Descriptions from https://statbel.fgov.be/sites/default/files/files/opendata/Statistische%20sectoren/Columns%20description_0.xlsx

    # | Naam/Nom             | Description                                                                           |
    # | -------------------- | ------------------------------------------------------------------------------------- |
    # | cd_sector            | Statistical sector code on 01/01/2018                                                 |
    # | tx_sector_descr_nl   | Name of the statistical sector, Dutch version                                         |
    # | tx_sector_descr_fr   | Name of the statistical sector, French version                                        |
    # | tx_sector_descr_de   | Name of the statistical sector, German version                                        |
    # | cd_sub_munty         | Aggregation of statistical sectors that have the same first 6 positions in their code |
    # | tx_sub_munty_nl      | NIS6 name, Dutch version                                                              |
    # | tx_sub_munty_fr      | NIS6 name, French version                                                             |
    # | cd_munty_refnis      | NIS code of the municipality on 01/01/2018 as text                                    |
    # | tx_munty_descr_nl    | Name of the municipality, Dutch version                                               |
    # | tx_munty_descr_fr    | Name of the municipality, French version                                              |
    # | tx_munty_descr_de    | Name of the municipality, German version                                              |
    # | cd_dstr_refnis       | NIS Code of the district                                                              |
    # | tx_adm_dstr_descr_nl | Name of the district, Dutch version                                                   |
    # | tx_adm_dstr_descr_fr | Name of the district, French version                                                  |
    # | tx_adm_dstr_descr_de | Name of the district, German version                                                  |
    # | cd_prov_refnis       | NIS Code of the province                                                              |
    # | tx_prov_descr_nl     | Name of the province, Dutch version                                                   |
    # | tx_prov_descr_fr     | Name of the province, French version                                                  |
    # | tx_prov_descr_de     | Name of the province, German version                                                  |
    # | cd_rgn_refnis        | NIS Code of the region                                                                |
    # | tx_rgn_descr_nl      | Name of the region, Dutch version                                                     |
    # | tx_rgn_descr_fr      | Name of the region, French version                                                    |
    # | tx_rgn_descr_de      | Name of the region, German version                                                    |
    # | cd_country           | Code of the country                                                                   |
    # | cd_nuts1             | NUTS1 code, version 2016 used by Eurostat                                             |
    # | cd_nuts2             | NUTS2 code, version 2016 used by Eurostat                                             |
    # | cd_nuts3             | NUTS3 code, version 2016 used by Eurostat                                             |
    # | ms_area_ha           | Area of the statistical sector in hectares calculated in RS Lambert 2008)             |
    # | ms_perimeter_m       | Perimeter of the statistical sector in meters calculated in RS Lambert 2008)          |

    # zip_file_contents = "sh_statbel_statistical_sectors_3812_20230101.geojson"

    # # try:
    # url = f"zip://{zip_file_contents}::{statistical_sectors}"
    # local_path = get_path_to_cache(url, output_dir)

    # raise ValueError(
    #     f"{local_path}\n"
    #     f"{local_path.full_name}\n"
    #     f"{local_path.path}\n"
    #     f"{local_path.fs}\n"
    #     f"{local_path.fobjects}\n"
    #     f"{dir(local_path)}"
    # )

    # # except ValueError:
    # #     print("Skipping download of statistical sectors")

    try:
        download_zipped_files(statistical_sectors, output_dir)
    except ValueError:
        context.log.info(
            "File already stored locally. Skipping download of statistical sectors"
        )

    geojson_path = (
        output_dir
        / "sh_statbel_statistical_sectors_3812_20230101.geojson"
        / "sh_statbel_statistical_sectors_3812_20230101.geojson"
    )

    sectors_gdf = gpd.read_file(geojson_path)
    sectors_gdf.index = sectors_gdf.index.astype(str)

    # Plot and convert the image to Markdown to preview it within Dagster
    # Yes we do pass the `plt` object to the markdown_from_plot function and not the `ax` object
    ax = sectors_gdf.plot(color="green")
    ax.set_title("Sectors in Belgium")
    md_plot = markdown_from_plot(plt)

    context.add_output_metadata(
        metadata={
            "num_records": len(sectors_gdf),  # Metadata can be any key-value pair
            "columns": MetadataValue.md(
                "\n".join([f"- '`{col}`'" for col in sectors_gdf.columns.to_list()])
            ),
            "preview": MetadataValue.md(
                sectors_gdf.loc[:, sectors_gdf.columns != "geometry"]
                .head()
                .to_markdown()
            ),
            "plot": MetadataValue.md(md_plot),
        }
    )

    return sectors_gdf


@asset(
    key_prefix=asset_prefix,
    ins={
        "get_geometries": AssetIn(key_prefix=asset_prefix),
    },
)
def aggregate_sectors_to_municipalities(context: AssetExecutionContext, get_geometries):
    """
    Aggregates a GeoDataFrame of the Statistical Sectors to Municipalities.

    The `sectors_gdf` is assumed to be produced by `get_geometries()`.

    Also saves the result to a GeoPackage file in the output_dir
    returns a GeoDataFrame of the Municipalities.
    """
    output_dir = WORKING_DIR / "statistical_sectors"
    munty_gdf = get_geometries.dissolve(by="cd_munty_refnis")
    munty_gdf.to_file(output_dir / "municipalities.gpkg", driver="GPKG")
    munty_gdf.index = munty_gdf.index.astype(str)

    # Plot and convert the image to Markdown to preview it within Dagster
    # Yes we do pass the `plt` object to the markdown_from_plot function and not the `ax` object
    ax = munty_gdf.plot(color="green")
    ax.set_title("Municipalities in Belgium")
    md_plot = markdown_from_plot(plt)

    context.add_output_metadata(
        metadata={
            "num_records": len(munty_gdf),  # Metadata can be any key-value pair
            "columns": MetadataValue.md(
                "\n".join([f"- '`{col}`'" for col in munty_gdf.columns.to_list()])
            ),
            "preview": MetadataValue.md(
                munty_gdf.loc[:, munty_gdf.columns != "geometry"].head().to_markdown()
            ),
            "plot": MetadataValue.md(md_plot),
        }
    )

    return munty_gdf


@asset(key_prefix=asset_prefix)
def get_population_details_per_municipality(context: AssetExecutionContext):
    """
    Downloads the population breakdown data per Municipality Sector and returns a DataFrame.

    If the data has already been downloaded, it is not downloaded again and the
    DataFrame is loaded from the cache.

    returns a DataFrame with one row per, with the number of people satisfying a
    unique combination of age, sex, civic (marital) status, per municipality.
    """
    output_dir = Path("population") / "demographic_breakdown"

    # Population
    # "Population by place of residence, nationality, marital status, age and sex"
    # https://statbel.fgov.be/en/open-data/population-place-residence-nationality-marital-status-age-and-sex-13

    # Data (excel)
    # "https://statbel.fgov.be/sites/default/files/files/opendata/bevolking%20naar%20woonplaats%2C%20nationaliteit%20burgelijke%20staat%20%2C%20leeftijd%20en%20geslacht/TF_SOC_POP_STRUCT_2023.xlsx"
    # Data (zipped txt)
    pop_data_url = "https://statbel.fgov.be/sites/default/files/files/opendata/bevolking%20naar%20woonplaats%2C%20nationaliteit%20burgelijke%20staat%20%2C%20leeftijd%20en%20geslacht/TF_SOC_POP_STRUCT_2023.zip"

    zip_file_contents = "TF_SOC_POP_STRUCT_2023.txt"

    url = f"zip://{zip_file_contents}::{pop_data_url}"

    text_file = get_path_to_cache(url, output_dir, "rt")

    with text_file.open() as f:
        population_df = pd.read_csv(f, sep="|", encoding="utf-8-sig")

    population_df.index = population_df.index.astype(str)

    context.add_output_metadata(
        metadata={
            "num_records": len(population_df),  # Metadata can be any key-value pair
            "columns": MetadataValue.md(
                "\n".join([f"- '`{col}`'" for col in population_df.columns.to_list()])
            ),
            "preview": MetadataValue.md(population_df.head().to_markdown()),
        }
    )

    return population_df


@asset(key_prefix=asset_prefix)
def get_population_by_statistical_sector(context: AssetExecutionContext):
    """
    Downloads the population data per Statistical Sector and returns a DataFrame.

    If the data has already been downloaded, it is not downloaded again and the
    DataFrame is loaded from the cache.

    returns a DataFrame with one row per Statistical Sector, with the total number
    of people per sector.
    """
    output_dir = Path("population") / "per_sector"

    # Population
    # Population by Statistical sector
    # https://statbel.fgov.be/en/open-data/population-statistical-sector-10

    pop_data_url = "https://statbel.fgov.be/sites/default/files/files/opendata/bevolking/sectoren/OPENDATA_SECTOREN_2022.zip"

    # The column descriptions from https://statbel.fgov.be/sites/default/files/files/opendata/bevolking/sectoren/Columns%20description2020.xlsx
    # are incorrect and do not match the data. The correct column descriptions are taken from the data file itself.

    # | Naam/Nom            |
    # | ------------------- |
    # | CD_REFNIS           |
    # | CD_SECTOR           |
    # | TOTAL               |
    # | DT_STRT_SECTOR      |
    # | DT_STOP_SECTOR      |
    # | OPPERVLAKKTE IN HM² |
    # | TX_DESCR_SECTOR_NL  |
    # | TX_DESCR_SECTOR_FR  |
    # | TX_DESCR_NL         |
    # | TX_DESCR_FR         |

    zip_file_contents = "OPENDATA_SECTOREN_2022.txt"
    url = f"zip://{zip_file_contents}::{pop_data_url}"

    text_file = get_path_to_cache(url, output_dir, "rt")

    with text_file.open() as f:
        population_df = pd.read_csv(f, sep="|", encoding="utf-8-sig")

    population_df.index = population_df.index.astype(str)
    population_df["CD_REFNIS"] = population_df["CD_REFNIS"].astype(str)

    context.add_output_metadata(
        metadata={
            "num_records": len(population_df),  # Metadata can be any key-value pair
            "columns": MetadataValue.md(
                "\n".join([f"- '`{col}`'" for col in population_df.columns.to_list()])
            ),
            "preview": MetadataValue.md(population_df.head().to_markdown()),
        }
    )

    return population_df


def aggregate_population_details_per_municipalities(
    pop_per_municipality_df: pd.DataFrame, output_dir: str
) -> pd.DataFrame:
    """
    Aggregates a DataFrame of the population details per Statistical Sector to Municipalities.

    The `pop_per_municipality_df` is assumed to be produced by `get_population_details_per_municipality()`.

    Also saves the result to a CSV file in the output_dir

    returns a DataFrame with one row per Municipality, with the total number of people

    TODO: Explore the other ways of aggregating this data.
    """
    # Columns detail take from https://statbel.fgov.be/sites/default/files/files/opendata/bevolking%20naar%20woonplaats%2C%20nationaliteit%20burgelijke%20staat%20%2C%20leeftijd%20en%20geslacht/Columns%20description.xlsx

    # | Naam/Nom             | Description                     |
    # | -------------------- | ------------------------------- |
    # | CD_MUNTY_REFNIS      | Refnis code of the municipality |
    # | TX_MUNTY_DESCR_NL    | Municipality name in Dutch      |
    # | TX_MUNTY_DESCR_FR    | Municipality name in French     |
    # | CD_DSTR_REFNIS       | Refnis code of the district     |
    # | TX_ADM_DSTR_DESCR_NL | District name in Dutch          |
    # | TX_ADM_DSTR_DESCR_FR | District name in French         |
    # | CD_PROV_REFNIS       | Refnis code of the province     |
    # | TX_PROV_DESCR_NL     | Province name in Dutch          |
    # | TX_PROV_DESCR_FR     | Province name in French         |
    # | CD_RGN_REFNIS        | Refnis code of the region       |
    # | TX_RGN_DESCR_NL      | Region name in Dutch            |
    # | TX_RGN_DESCR_FR      | Region name in French           |
    # | CD_SEX               | Gender                          |
    # | CD_NATLTY            | Nationality code                |
    # | TX_NATLTY_FR         | Nationality in French           |
    # | TX_NATLTY_NL         | Nationality in Dutch            |
    # | CD_CIV_STS           | Civil status code               |
    # | TX_CIV_STS_FR        | Civil status in French          |
    # | TX_CIV_STS_NL        | Civil status in Dutch           |
    # | CD_AGE               | Age                             |
    # | MS_POPULATION        | Number of individuals           |
    # | CD_YEAR              | Reference year                  |

    # Drop all the columns we don't need

    # TODO there are many different ways top aggregate this data. For now we just take the sum of the population for each municipality
    pop_per_municipality_df = pop_per_municipality_df[["CD_REFNIS", "MS_POPULATION"]]

    munty_df = pop_per_municipality_df.groupby(by="CD_REFNIS").sum()
    munty_df.to_csv(output_dir / "municipalities.csv", sep="|")
    munty_df.index = munty_df.index.astype(str)

    return munty_df


@asset(
    key_prefix=asset_prefix,
)
def get_car_per_sector(context: AssetExecutionContext):
    """
    Downloads the number of cars per Statistical Sector and returns a DataFrame.

    If the data has already been downloaded, it is not downloaded again and the
    DataFrame is loaded from the cache.

    returns a DataFrame with one row per Statistical Sector, with the total number
    of cars per sector.
    """
    output_dir = Path("car_ownership")

    # Number of cars by Statistical sector
    # https://statbel.fgov.be/en/open-data/number-cars-statistical-sector
    cars_url = "https://statbel.fgov.be/sites/default/files/files/opendata/Aantal%20wagens%20per%20statistische%20sector/TF_CAR_HH_SECTOR.zip"

    # Column names (inferred as the website links for the wrong column descriptions)

    # | Column Name       |
    # | ----------------- |
    # | CD_YEAR           |
    # | CD_REFNIS         |
    # | TX_MUNTY_DESCR_FR |
    # | TX_MUNTY_DESCR_NL |
    # | TX_MUNTY_DESCR_DE |
    # | TX_MUNTY_DESCR_EN |
    # | cd_sector         |
    # | total_huisH       |
    # | total_wagens      |

    zip_file_contents = "TF_CAR_HH_SECTOR.txt"
    url = f"zip://{zip_file_contents}::{cars_url}"

    text_file = get_path_to_cache(url, output_dir, "rt")

    with text_file.open() as f:
        car_per_sector_df = pd.read_csv(f, sep="|", encoding="utf-8-sig")

    car_per_sector_df.index = car_per_sector_df.index.astype(str)

    context.add_output_metadata(
        metadata={
            "num_records": len(car_per_sector_df),  # Metadata can be any key-value pair
            "columns": MetadataValue.md(
                "\n".join(
                    [f"- '`{col}`'" for col in car_per_sector_df.columns.to_list()]
                )
            ),
            "preview": MetadataValue.md(car_per_sector_df.head().to_markdown()),
        }
    )

    return car_per_sector_df


@asset(
    key_prefix=asset_prefix,
)
def get_car_ownership_by_housetype(context: AssetExecutionContext):
    """
    Downloads the number of cars per household type by municipality and returns a DataFrame.

    If the data has already been downloaded, it is not downloaded again and the
    DataFrame is loaded from the cache.

    returns a DataFrame with one row per Municipality, with the total number
    of cars per household type.
    """
    output_dir = Path("car_ownership")

    # Number of cars per household type by municipality
    # https://statbel.fgov.be/en/open-data/number-cars-household-type-municipality
    car_per_household_url = "https://statbel.fgov.be/sites/default/files/files/opendata/Aantal%20wagens%20volgens%20huishoudtype%20per%20gemeente/TF_CAR_HHTYPE_MUNTY.zip"

    # Column Description from https://statbel.fgov.be/sites/default/files/files/opendata/Aantal%20wagens%20volgens%20huishoudtype%20per%20gemeente/Columns%20description_TF_CAR_HHTYPE_MUNTY.xlsx

    # | Omschrijving                | Description                     |
    # | --------------------------- | ------------------------------- |
    # | Referentie jaar             | Reference year                  |
    # | Refnis-code van de gemeente | Refnis code of the municipality |
    # | Naam van de gemeente in FR  | Name of the municipality in FR  |
    # | Naam van de gemeente in NL  | Name of the municipality in NL  |
    # | Huishoud type               | Household type                  |
    # | Aantal huishoudens          | Number of households            |
    # | Aantal wagens               | Number of cars                  |

    # where "Household type" is one of:

    # | Detail                                   |
    # | ---------------------------------------- |
    # | 1\. People living alone                  |
    # | 2\. Married couples with no children     |
    # | 3\. Married couples with children        |
    # | 4\. Not-married couples with no children |
    # | 5\. Non-married couples with children    |
    # | 6\. Single parents                       |
    # | 7\. Other types of household             |
    # | 8\. Collective households                |

    zip_file_contents = "TF_CAR_HHTYPE_MUNTY.txt"
    url = f"zip://{zip_file_contents}::{car_per_household_url}"

    text_file = get_path_to_cache(url, output_dir, "rt")

    with text_file.open() as f:
        car_per_household_df = pd.read_csv(f, sep="|", encoding="utf-8-sig")

    car_per_household_df.index = car_per_household_df.index.astype(str)

    context.add_output_metadata(
        metadata={
            "num_records": len(
                car_per_household_df
            ),  # Metadata can be any key-value pair
            "columns": MetadataValue.md(
                "\n".join(
                    [f"- '`{col}`'" for col in car_per_household_df.columns.to_list()]
                )
            ),
            "preview": MetadataValue.md(car_per_household_df.head().to_markdown()),
        }
    )

    return car_per_household_df


@asset(
    key_prefix=asset_prefix,
    ins={
        "get_geometries": AssetIn(key_prefix=asset_prefix),
        "get_population_by_statistical_sector": AssetIn(key_prefix=asset_prefix),
    },
)
def sector_populations(
    context: AssetExecutionContext, get_geometries, get_population_by_statistical_sector
):
    """
    Returns a GeoDataFrame of the Statistical Sectors joined with the population per sector.
    """
    # Population
    pop_gdf = get_geometries.merge(
        get_population_by_statistical_sector,
        right_on="CD_REFNIS",
        left_on="cd_munty_refnis",
        how="inner",
    )

    # Plot and convert the image to Markdown to preview it within Dagster
    # Yes we do pass the `plt` object to the markdown_from_plot function and not the `ax` object
    ax = pop_gdf.plot(column="TOTAL", legend=True, scheme="quantiles")
    ax.set_title("Populations per sector in Belgium")
    md_plot = markdown_from_plot(plt)

    context.add_output_metadata(
        metadata={
            "num_records": len(pop_gdf),  # Metadata can be any key-value pair
            "columns": MetadataValue.md(
                "\n".join([f"- '`{col}`'" for col in pop_gdf.columns.to_list()])
            ),
            "preview": MetadataValue.md(
                pop_gdf.loc[:, pop_gdf.columns != "geometry"].head().to_markdown()
            ),
            "plot": MetadataValue.md(md_plot),
        }
    )

    return pop_gdf


@asset(
    key_prefix=asset_prefix,
    ins={
        "get_geometries": AssetIn(key_prefix=asset_prefix),
        "get_car_per_sector": AssetIn(key_prefix=asset_prefix),
    },
)
def sector_car_ownership(
    context: AssetExecutionContext, get_geometries, get_car_per_sector
):
    """
    Returns a GeoDataFrame of the Statistical Sectors joined with the number of cars per sector.
    """
    # Vehicle Ownership
    cars = get_car_per_sector
    cars["CD_REFNIS"] = cars["CD_REFNIS"].astype(str)
    cars_gdf = get_geometries.merge(
        cars, right_on="CD_REFNIS", left_on="cd_munty_refnis", how="inner"
    )

    # Plot and convert the image to Markdown to preview it within Dagster
    # Yes we do pass the `plt` object to the markdown_from_plot function and not the `ax` object
    ax = cars_gdf.plot(column="total_wagens", legend=True, scheme="quantiles")
    ax.set_title("Car ownership per sector in Belgium")
    md_plot = markdown_from_plot(plt)

    context.add_output_metadata(
        metadata={
            "num_records": len(cars_gdf),  # Metadata can be any key-value pair
            "columns": MetadataValue.md(
                "\n".join([f"- '`{col}`'" for col in cars_gdf.columns.to_list()])
            ),
            "preview": MetadataValue.md(
                cars_gdf.loc[:, cars_gdf.columns != "geometry"].head().to_markdown()
            ),
            "plot": MetadataValue.md(md_plot),
        }
    )

    return cars_gdf


@asset(
    key_prefix=asset_prefix,
    ins={
        "get_population_details_per_municipality": AssetIn(key_prefix=asset_prefix),
    },
)
def pivot_population(
    context: AssetExecutionContext,
    get_population_details_per_municipality: pd.DataFrame,
):
    # For brevity
    pop = get_population_details_per_municipality

    # Drop all the columns we don't need
    pop = pop[
        [
            "CD_REFNIS",  # keep
            # "CD_DSTR_REFNIS",   # drop
            # "CD_PROV_REFNIS",   # drop
            # "CD_RGN_REFNIS",    # drop
            "CD_SEX",  # keep
            # "CD_NATLTY",        # drop
            # "CD_CIV_STS",       # drop
            "CD_AGE",  # keep
            "MS_POPULATION",  # keep
            # "CD_YEAR",          # drop
        ]
    ]

    table = None

    # Using HXL tags for variable names (https://hxlstandard.org/standard/1-1final/dictionary/#tag_population)
    columns = {
        "population_children_age5_17": (pop["CD_AGE"] >= 5) & (pop["CD_AGE"] < 18),
        "population_infants_age0_4": (pop["CD_AGE"] <= 4),
        "population_children_age0_17": (pop["CD_AGE"] >= 0) & (pop["CD_AGE"] < 18),
        "population_adults_f": (pop["CD_AGE"] > 18) & (pop["CD_SEX"] == "F"),
        "population_adults_m": (pop["CD_AGE"] > 18) & (pop["CD_SEX"] == "M"),
        "population_adults": (pop["CD_AGE"] > 18),
        "population_ind": (pop["CD_AGE"] >= 0),
    }

    for col_name, filter in columns.items():
        temp_table = (
            pop.loc[filter]
            .groupby(by=["CD_REFNIS"], as_index=True)
            .agg(
                **{
                    col_name: pd.NamedAgg(column="MS_POPULATION", aggfunc="sum"),
                }
            )
        )

        if table is None:
            table = temp_table
        else:
            table = table.merge(
                temp_table, left_index=True, right_index=True, how="inner"
            )

    # table.set_index("CD_REFNIS", inplace=True, drop=False)

    context.add_output_metadata(
        metadata={
            "num_records": len(table),  # Metadata can be any key-value pair
            "columns": MetadataValue.md(
                "\n".join([f"- '`{col}`'" for col in table.columns.to_list()])
            ),
            "preview": MetadataValue.md(table.head().to_markdown()),
        }
    )

    return table


@asset(
    key_prefix=asset_prefix,
    ins={
        "aggregate_sectors_to_municipalities": AssetIn(key_prefix=asset_prefix),
        "pivot_population": AssetIn(key_prefix=asset_prefix),
    },
)
def municipalities_populations(
    context: AssetExecutionContext,
    aggregate_sectors_to_municipalities,
    pivot_population,
):
    """
    Returns a GeoDataFrame of the Municipalities joined with the population per municipality.
    """
    # Population
    population = pivot_population
    population.index = population.index.astype(str)

    geom = aggregate_sectors_to_municipalities
    pop_gdf = geom.merge(
        population, right_on="CD_REFNIS", left_on="cd_munty_refnis", how="inner"
    )

    ax = pop_gdf.plot(column="population_ind", legend=True, scheme="quantiles")
    ax.set_title("Population per Municipality in Belgium")
    md_plot = markdown_from_plot(plt)

    context.add_output_metadata(
        metadata={
            "num_records": len(pop_gdf),  # Metadata can be any key-value pair
            "columns": MetadataValue.md(
                "\n".join([f"- '`{col}`'" for col in pop_gdf.columns.to_list()])
            ),
            "preview": MetadataValue.md(
                pop_gdf.loc[:, pop_gdf.columns != "geometry"].head().to_markdown()
            ),
            "plot": MetadataValue.md(md_plot),
        }
    )

    return pop_gdf
