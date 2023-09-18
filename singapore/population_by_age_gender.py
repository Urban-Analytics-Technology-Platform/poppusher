"""
population_by_age_gender.py
---------------------------
Script to download Singapore 'subzone' geometries as well as population data
(broken down by subzone, age range, and gender).

Subzones are the smallest unit of geographical division in Singapore.
"""

import requests
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt

import re
import json
import hashlib
from pathlib import Path

# --- File retrieval and caching mechanisms ------------------------------------

def sha256_file(path):
    """
    Returns the SHA256 hash of a file, iterating in chunks of 4KB to avoid
    using excessive memory.
    """
    sha256 = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256.update(chunk)
    return sha256.hexdigest()

def get_file(url, cache_path=None, expected_sha256=None):
    """
    If `cache_path` is None, this simply downloads a file from a URL and
    returns the contents as a string.

    If `cache_path` is provided, this attempts to load the file from the cache
    first. If the file does not exist in the cache, it is downloaded and then
    stored in the cache. Note that `expected_md5` must also be provided in this
    case (as it is used to check for file integrity).
    """
    if cache_path is None:
        resp = requests.get(url)
        return resp.text

    else:
        if expected_sha256 is None:
            raise ValueError("An expected SHA256 must be provided to use the cache.")

        # Convert to pathlib.Path and make parent folder
        cache_path = Path(cache_path) 
        if not cache_path.parent.exists():
            cache_path.parent.mkdir(parents=True)

        # Download if it doesn't exist
        if not cache_path.exists():
            resp = requests.get(url)
            with open(cache_path, "w") as f:
                f.write(resp.text)

        # Check SHA256 and return if it matches
        actual_sha256 = sha256_file(cache_path)
        if actual_sha256 != expected_sha256:
            raise ValueError(f"SHA256 mismatch for {cache_path}: "
                             f"expected {expected_sha256}, got {actual_sha256}")
        with open(cache_path, "r") as f:
            contents = f.read()
        return contents

# --- Actual interesting stuff -------------------------------------------------

def get_geometries():
    """
    Returns GeoJSON for the planning area and subzones in the 2019 URA Master
    Plan. See https://www.ura.gov.sg/Corporate/Planning/Master-Plan.

    The resulting GeoDataFrame has the EPSG:4326 coordinate system, and
    contains the following columns (in addition to the geometry):

     - subzone : str
           Name of the subzone in all-uppercase. (Subzones are the smallest
           unit of geographical division in Singapore.)
     - planning_area : str
           Name of the planning area in all-uppercase.
     - region : str
           Name of the region in all-uppercase. (Regions are the largest
           divisions)
    """
    cache_path = Path(__file__).parent / "data" / "sg-planning-area.geojson"

    # Get URL pointing to the GeoJSON file
    RESOURCE_ID = "84b62d90-c1b7-4ada-acfc-f5874b5fd945"
    resp = requests.get(f"https://data.gov.sg/api/action/resource_show?id={RESOURCE_ID}")
    url = resp.json()["result"]["url"]

    # Get the file
    expected_sha256 = "00aab6de51b0dd1d23a04db518ff702a1773408a905f760c05aee251e9dcd93b"
    gj = gpd.read_file(
        get_file(url, cache_path=cache_path, expected_sha256=expected_sha256),
        driver="GeoJSON"
    )

    # Clean up data
    rgx = re.compile(
        r"^.+" # Anything
        r"<th>SUBZONE_N</th>\s*<td>([^<]+)</td>\s*" # Subzone name
        r".+"  # Anything
        r"<th>PLN_AREA_N</th>\s*<td>([^<]+)</td>\s*" # Planning area name
        r".+"  # Anything
        r"<th>REGION_N</th>\s*<td>([^<]+)</td>\s*" # Region name
        r".+$" # Anything
    )
    gj[["subzone", "planning_area", "region"]] = gj["Description"].str.extract(rgx)
    gj = gj.drop(columns=["Description"])
    # The data are already in the correct CRS
    assert gj.crs == "EPSG:4326"

    return gj

def get_population():
    """
    Returns a Pandas dataframe containing the population data for each planning
    area and subzone in Singapore, further split by gender and age range. Data
    are taken from the 2020 Census. See
    https://tablebuilder.singstat.gov.sg/table/CT/17560.

    The resulting dataframe is in 'tidy' (long) format, with the following
    columns:

     - subzone : str
           Name of the subzone in all uppercase (this is done to match the
           capitalisation in the geometries).
     - gender : str
           One of 'Total', 'Females', or 'Males'.
     - age : tuple of Optional[int]
           Minimum and maximium ages in the given age range. The maximum age is
           None if the range is not bounded above. For example, 0-5 years old
           is represented as (0, 5), and 90+ years old is represented as (90,
           None).
     - population : int
           Population count.
    """
    cache_path = Path(__file__).parent / "data" / "sg-population.json"
    url = "https://tablebuilder.singstat.gov.sg/api/table/tabledata/17560"
    expected_sha256 = "a52c3a1b217e0ddcd48a74fc80d9ad8168013ca3d9d28336df2dc66478e886c0"

    population = json.loads(get_file(url, cache_path=cache_path, expected_sha256=expected_sha256))

    # Extract only subzone-level data (some rows contain region-level data)
    pop = population["Data"]
    subzone_records = [row for row in pop["row"] if re.match(r"^\d+\.\d+$", row["rowNo"])]

    # Convert each nested JSON into a flat dictionary
    def parse_age(age):
        if age == "Total":
            return (0, None)
        elif age == "90 & Over":
            return (90, None)
        else:
            return tuple(int(x) for x in age.split(" - "))
    def clean(subzone):
        # Uppercase this to match the capitalisation in the geometries
        subzone_name = subzone["rowText"].upper()
        entries = []
        for col1 in subzone["columns"]:
            gender = col1["key"]
            for col2 in col1["columns"]:
                age_min, age_max = parse_age(col2["key"])
                val = col2["value"]
                entries.append({
                    "subzone": subzone_name,
                    "gender": gender,
                    "age_min": age_min,
                    "age_max": age_max,
                    "population": 0 if val == "-" else int(val)
                })
        return entries

    # Convert to dataframe
    return pd.DataFrame.from_records(
        entry for subzone_record in subzone_records for entry in clean(subzone_record)
    )

if __name__ == "__main__":
    geometries = get_geometries()
    print(geometries)

    population = get_population()
    print(population)

    # Demo plot
    total_population = population.query(
        "gender == 'Total' and age_min == 0 and age_max.isnull()"
    )
    print(total_population)
    geometries_with_total_pop = geometries.merge(total_population, on="subzone")
    ax = geometries_with_total_pop.plot(column="population", legend=True)
    ax.set_title("Total population per subzone")
    plt.show()
