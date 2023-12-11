from __future__ import annotations

import os
import tempfile
import urllib
from pathlib import Path

import docker
import geopandas as gp
import pandas as pd
from dagster import (
    AssetOut,
    DynamicPartitionsDefinition,
    asset,
    multi_asset,
)

from .config import ACS_METADATA, SUMMARY_LEVELS

year = 2019
summary_level = "fiveYear"


@asset(key_prefix="us2", name="non_unique_name")
def non_unique_name():
    return "USA"


# @asset(key_prefix="us", name="non_unique_name_2")
# def non_unique_name_2(unique_name):
#     return f"country name is: '{unique_name}'"

raw_table_files_partition = DynamicPartitionsDefinition(name="raw_table_files")

geo_dir = tempfile.mkdtemp()
#
# tractDir = os.path.join(workdir,"tracts")
# blockGroupDir = os.path.join(workdir,"block_groups")
# countyDir = os.path.join(workdir,"counties")
#
# os.mkdir(blockGroupDir)
# os.mkdir(tractDir)
# os.mkdir(countyDir)


def get_summary_table(table_name: str, year: int, summary_level: str):
    base = ACS_METADATA[year]["base"]
    summary_file_dir = base + ACS_METADATA[year][summary_level]["tables"]
    data = pd.read_csv(f"{summary_file_dir}/{table_name}", sep="|")
    return data


def extract_values_at_specified_levels(df: pd.DataFrame, geoids: pd.DataFrame):
    joined = pd.merge(
        df,
        geoids[["DADSID", "SUMLEVEL"]],
        left_on="GEO_ID",
        right_on="DADSID",
        how="left",
    )
    result = {}

    for level, id in SUMMARY_LEVELS.items():
        result[level] = joined[joined["SUMLEVEL"] == id].drop(
            ["DADSID", "SUMLEVEL"], axis=1
        )
    return result


def merge_parquet_files(file_names):
    result = pd.DataFrame()
    for batch in tqdm(batched(file_names, 20)):
        newDFS = [
            select_estimates(pd.read_parquet(file).set_index("GEO_ID"))
            for file in batch
        ]
        result = pd.concat([result] + newDFS, axis=1)
    return result


@asset
def generate_variable_dictionary():
    metadata = ACS_METADATA[year]
    base = metadata["base"]
    shells = metadata[summary_level]["shells"]
    raw = pd.read_csv(base + shells, encoding="latin")
    result = (
        []
    )  # pd.DataFrame(columns=["tableID","uniqueID", "universe","tableName", "variableName", "variableExtedndedName"])

    universe = ""
    tableName = ""
    path = []
    previousWasEdge = True
    for index, row in raw.iterrows():
        if (type(row["Table ID"]) == str and len(row["Table ID"].strip()) == 0) or type(
            row["Table ID"]
        ) == float:
            # path=[]
            # previousWasEdge = True
            continue

        stub = row["Stub"]

        if row[["UniqueID"]].isna().all():
            if "Universe" in stub:
                universe = stub.split("Universe:")[1].strip()
            else:
                tableName = stub
        else:
            if ":" in stub:
                if previousWasEdge:
                    path.append(stub.replace(":", ""))
                else:
                    path.pop()
                    path.append(stub.replace(":", ""))
            else:
                previousWasEdge = False
            extendedName = "|".join(path)
            if ":" not in stub:
                extendedName = extendedName + "|" + stub
            result.append(
                {
                    "tableID": row["Table ID"],
                    "uniqueID": row["UniqueID"],
                    "universe": universe,
                    "variableName": stub,
                    "variableExtendedName": extendedName,
                }
            )

    return pd.DataFrame.from_records(result)


@multi_asset(
    outs={
        "tract_files": AssetOut(),
        "blockGroup_files": AssetOut(),
        "county_files": AssetOut(),
    },
    partitions_def=raw_table_files_partition,
)
def aws_table_files(context, geometry_ids, summary_table_names):
    base = ACS_METADATA[year]["base"]
    summary_file_dir = base + ACS_METADATA[year][summary_level]["tables"]

    context.log.info("Trying to get partition name")
    table = context.asset_partition_key_for_output()
    context.log.info("table is ", table)

    data = get_summary_table(table, year, summary_level)

    context.log.info("got summary table")
    values = extract_values_at_specified_levels(data, geometry_ids)
    context.log.info("extracted values ", values)
    # values['tract'].to_parquet(os.path.join(tractDir,table.replace(".dat",".parquet")))
    # values['county'].to_parquet(os.path.join(countyDir,table.replace(".dat",".parquet")))
    # values['block_group'].to_parquet(os.path.join(blockGroupDir,table.replace(".dat",".parquet")))

    return values["tract"], values["county"], values["block_group"]


# @asset
# def merge_parquet_files(tract_parquet_files, blockGroup_parquet_files, county_parquet_files):
#     # merge_parquet_files([os.path.join(countyDir,file) for file in os.listdir(countyDir)]).to_parquet(f"county_{year}_{summary_level}.parquet"
#     merge_parquet_files([os.path.join(tract_parquet_files,file) for file in os.listdir(tractDir)]).to_parquet(f"tracts_{year}_{summary_level}.parquet")
#     # merge_parquet_files([os.path.join(blockGroupDir,file) for file in os.listdir(blockGroupDir)]).to_parquet(f"block_groups_{year}_{summary_level}.parquet")


#
# @asset
# def cartographic_file()
#     metadata = ACS_METADATA[year]
#     url = metadata['geors'][admin_level]
#     if(work_dir == None):
#         work_dir = os.tmpdir()
#     local_dir = os.path.join(work_dir, admin_level+".zip")
#     urllib.request.urlretrieve(url, local_dir)
#     return local_dir
#
@asset
def geometry_ids():
    path = ACS_METADATA[year]["base"] + ACS_METADATA[year][summary_level]["geoIds"]
    sep = ACS_METADATA[year]["geoIdsSep"] if "geoIdsSep" in ACS_METADATA[year] else ","
    table = pd.read_csv(path, encoding="latin", sep=sep)
    return table


#
#
@asset
def summary_table_names(context):
    metadata = ACS_METADATA[year]
    base = metadata["base"]
    table_path = base + metadata[summary_level]["tables"]

    table = pd.read_html(table_path)[0]
    filtered = table[table["Name"].str.startswith("acs", na=False)]

    partition = context.instance.get_dynamic_partitions("raw_table_files")

    # [context.instance.delete_dynamic_partition('raw_table_files', part) for part in parts_to_del]

    context.instance.add_dynamic_partitions("raw_table_files", list(filtered["Name"]))

    # for name in filtered["Name"]
    #     raw_table_files_partition.build_add_request(name)

    return list(filtered["Name"])


@asset()
def raw_cartography_file():
    metadata = ACS_METADATA[year]
    url = metadata["geoms"]["tracts"]
    local_dir = os.path.join(geo_dir, "tracts" + ".zip")
    urllib.request.urlretrieve(url, local_dir)
    return local_dir


@asset()
def cartography_in_cloud_formats(raw_cartography_file):
    path = raw_cartography_file
    data = gp.read_file(f"zip://{path}")
    parquet_path = path.replace(".zip", ".parquet")
    flatgeobuff_path = path.replace(".zip", ".flatgeobuff")
    geojson_seq_path = path.replace(".zip", ".geojsonseq")

    data.to_parquet(parquet_path)
    data.to_file(flatgeobuff_path, driver="FlatGeobuf")
    data.to_file(geojson_seq_path, driver="GeoJSONSeq")
    return {
        "parquet_path": parquet_path,
        "flatgeobuff_path": flatgeobuff_path,
        "geojson_seq_path": geojson_seq_path,
    }


@asset()
def generate_pmtiles(context, cartography_in_cloud_formats):
    client = docker.from_env()
    mount_folder = Path(
        cartography_in_cloud_formats["geojson_seq_path"]
    ).parent.resolve()

    container = client.containers.run(
        "stuartlynn/tippecanoe:latest",
        "tippecanoe -o tracts.pmtiles tracts.geojsonseq",
        volumes={mount_folder: {"bind": "/app", "mode": "rw"}},
        detach=True,
        remove=True,
    )

    output = container.attach(stdout=True, stream=True, logs=True)
    for line in output:
        context.log.info(line)
