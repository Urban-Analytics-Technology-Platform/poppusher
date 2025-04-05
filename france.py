#!/usr/bin/python3
# No Python dependencies, but requires external commands: wget, 7zr, unzip, ogr2ogr

import csv
from pathlib import Path
import os
import subprocess


def run(args):
    print(">", " ".join(args))
    subprocess.run(args, check=True)


# Produces working_dir/zones.geojson in WGS84, with only one property, "iris_code"
def get_geometries(working_dir):
    # 2023 IRIS areas for all of France, from https://geoservices.ign.fr/irisge
    # TODO Probably need to use 2021, to match up with the census spreadsheet (https://wxs.ign.fr/0aezh2n2288zcsb0gyx38xfs/telechargement/prepackage/IRIS-GE-TERRITOIRES-PACK_2019$IRIS-GE_2-0__SHP_LAMB93_FXX_2019-01-01/file/IRIS-GE_2-0__SHP_LAMB93_FXX_2019-01-01.7z)
    run(
        [
            "wget",
            "https://wxs.ign.fr/0aezh2n2288zcsb0gyx38xfs/telechargement/prepackage/IRIS-GE-TERRITOIRES-PACK_2023-01-03$IRIS-GE_3-0__SHP__FRA_2023-01-01/file/IRIS-GE_3-0__SHP__FRA_2023-01-01.7z",
            "-O",
            str(working_dir / "shapefiles.7z"),
        ]
    )
    run(["7zr", "x", str(working_dir / "shapefiles.7z"), "-o" + str(working_dir)])

    # There are shapefiles for habitats, activities, and various, per
    # https://fr.wikipedia.org/wiki/%C3%8Elots_regroup%C3%A9s_pour_l%27information_statistique.
    # Convert the habitats to GeoJSON, only keeping one ID property.
    run(
        [
            "ogr2ogr",
            "-f",
            "GeoJSON",
            str(working_dir / "zones.geojson"),
            "-t_srs",
            "EPSG:4326",
            str(
                working_dir
                / "IRIS-GE_3-0__SHP__FRA_2023-01-01"
                / "IRIS-GE"
                / "1_DONNEES_LIVRAISON_2023-06-00072"
                # TODO Not sure what the other directories are -- just used the largest shapefile
                / "IRIS-GE_3-0_SHP_LAMB93_FXX-ED2023-01-01"
                / "IRIS.shp"
            ),
            "-sql",
            "SELECT CODE_IRIS as iris_code FROM IRIS",
        ]
    )


# Produces working_dir/population.csv with an iris_code column matching the GeoJSON and a total_population column.
def get_2019_census(working_dir):
    # From https://www.insee.fr/fr/statistiques/6543200
    run(
        [
            "wget",
            "https://www.insee.fr/fr/statistiques/fichier/6543200/base-ic-evol-struct-pop-2019_csv.zip",
            "-O",
            str(working_dir / "2019_csv.zip"),
        ]
    )
    run(["unzip", str(working_dir / "2019_csv.zip"), "-d", str(working_dir)])

    # See https://www.insee.fr/fr/statistiques/6543200#dictionnaire
    # There are three groups of age buckets, but the most specific is: 0-2,
    # 3-5, 6-10, 11-17, 18-24, 25-39, 40-54, 55-64, 65-79, 80+
    # Also broken down by some profession types and male/female
    with open(working_dir / "base-ic-evol-struct-pop-2019.CSV") as inFile:
        with open(working_dir / "population.csv", "w") as outFile:
            writer = csv.DictWriter(
                outFile, fieldnames=["iris_code", "total_population"]
            )
            writer.writeheader()
            for row in csv.DictReader(inFile, delimiter=";"):
                writer.writerow(
                    {"iris_code": row["IRIS"], "total_population": row["P19_POP"]}
                )


if __name__ == "__main__":
    WORKING_DIR = Path(__file__).parent / "data" / "france"
    os.makedirs(WORKING_DIR, exist_ok=True)
    # Not idempotent -- manually delete the intermediate directory between
    # runs, or manually comment out some steps to skip

    get_geometries(WORKING_DIR)
    get_2019_census(WORKING_DIR)
