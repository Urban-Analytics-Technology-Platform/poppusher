import pandas as pd 
import urllib.request
import tempfile
import os 
from functools import reduce
from tqdm import tqdm
from more_itertools import batched
import geopandas as gp
import subprocess
import docker
from pathlib import Path
import os
import tempfile

SUMMARY_LEVELS={
    "tract": 140 ,
    "block_group":150,
    "county":50
}

ACS_METADATA={
    # 2018 needs additional logic for joining together geometry files
    # 2018:{
    #     base:"https://www2.census.gov/programs-surveys/acs/summary_file/2018",
    #     type:'table',
    #     geoms:{
    #         tract:,
    #         block:"",
    #         county:"https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_county_500k.zip":
    #     }
    #     oneYear:{
    #         tables:"prototype/1YRData/",
    #         geoIds:"https://www2.census.gov/programs-surveys/acs/summary_file/2018/prototype/20181YRGeos.csv",
    #         shells:"prototype/ACS2018_Table_Shells.xlsx"
    #     },
    #     fiveYear:{
    #         tables:"prototype/5YRData/",
    #         geoIds:"https://www2.census.gov/programs-surveys/acs/summary_file/2018/prototype/20185YRGeos.csv",
    #         shells:"prototype/ACS2018_Table_Shells.xlsx"
    #     }
    # },
    2019:{
        "base" : "https://www2.census.gov/programs-surveys/acs/summary_file/2019/",
        "type":'table',
        "geoms":{
            "tract": "https://www2.census.gov/geo/tiger/GENZ2019/shp/cb_2019_us_tract_500k.zip",
            "block_group":"https://www2.census.gov/geo/tiger/GENZ2019/shp/cb_2019_us_bg_500k.zip",
            "county": "https://www2.census.gov/geo/tiger/GENZ2019/shp/cb_2019_us_county_500k.zip"
        },
        "oneYear":{
            "tables":"prototype/1YRData/",
            "geoIds":"prototype/Geos20191YR.csv",
            "shells":"prototype/ACS2019_Table_Shells.csv"
        },
        "fiveYear":{
            "tables":"prototype/5YRData/",
            "geoIds":"prototype/Geos20195YR.csv",
            "shells":"prototype/ACS2019_Table_Shells.csv"
        },
        "geoIdCol": "DADSID"
    },
    # Note 1 year esimates are not avaliable because of covid. More details of the exeprimental estimates 
    # are here : https://www.census.gov/programs-surveys/acs/technical-documentation/table-and-geography-changes/2020/1-year.html
    2020:{
        "base":"https://www2.census.gov/programs-surveys/acs/summary_file/2020/",
        "type":'table',
        "geoms":{
            "tract":"https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_tract_500k.zip",
            "block_group":"https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_bg_500k.zip",
            "county":"https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_county_500k.zip"
        },
        "shells":"prototype/ACS2020_Table_Shells.csv",
        "oneYear":None,
        "fiveYear":{
            "shells":"prototype/ACS2020_Table_Shells.csv",
            "tables":"prototype/5YRData/",
            "geoIds":"prototype/Geos20205YR.csv",
        },
        "geoIdCol": "DADSID"
    },
    2021:{
        "base":"https://www2.census.gov/programs-surveys/acs/summary_file/2021/table-based-SF/",
        "type":'table',
        "geoIdsSep":"|",
        "geoms":{
            "tract":"https://www2.census.gov/geo/tiger/GENZ2021/shp/cb_2021_us_tract_500k.zip",
            "block_group":"https://www2.census.gov/geo/tiger/GENZ2021/shp/cb_2021_us_bg_500k.zip",
            "county":"https://www2.census.gov/geo/tiger/GENZ2021/shp/cb_2021_us_county_500k.zip"
        },
        "oneYear":{
            "tables":"data/1YRData/",
            "geoIds":"documentation/Geos20211YR.txt",
            "shells":"documentation/ACS20211YR_Table_Shells.txt"
        },
        "fiveYear":{
            "tables":"data/5YRData/",
            "geoIds":"documentation/Geos20215YR.txt",
            "shells":"documentation/ACS20215YR_Table_Shells.txt"
        },
        "geoIdCol": "GEO_ID"
    }
}

def generate_variable_dictionary(year:int, summary_level:str):
    metadata = ACS_METADATA[year]
    base = metadata["base"]
    shells = metadata[summary_level]["shells"]

    # Config for each year
    if int(year) == 2019:
        raw= pd.read_csv(base+shells, encoding="latin")
        unique_id_col_name = "UniqueID"
    elif int(year) == 2020:
        raw= pd.read_csv(base+shells, encoding="latin")
        unique_id_col_name = "Unique ID"
    elif int(year) == 2021:
        raw= pd.read_csv(base+shells, sep="|")
        unique_id_col_name = "Unique ID"
        raw = raw.rename(columns={"Label": "Stub"})
    else:
        raise ValueError(f"generate_variable_dictionary() not implemented for year: {year}")

    result = [] # pd.DataFrame(columns=["tableID",unique_id_col_name, "universe","tableName", "variableName", "variableExtedndedName"])
    universe =""
    tableName = ""
    path =[]
    previousWasEdge = True 
    for (index,row) in raw.iterrows():
        if(( type(row["Table ID"]) == str and len(row["Table ID"].strip())==0) or type(row["Table ID"]) == float):
            # path=[]
            # previousWasEdge = True 
            continue

        stub = row["Stub"]

        if (row[[unique_id_col_name]].isna().all()):
            if("Universe" in stub):
                universe = stub.split("Universe:")[1].strip()
            else:
                tableName=stub
        else:
            if (":" in stub):
                if(previousWasEdge):
                    path.append(stub.replace(":",""))
                else:
                    path.pop()
                    path.append(stub.replace(":",""))
            else:
                previousWasEdge = False 
            extendedName = "|".join(path) 
            if(":" not in stub):
                extendedName = extendedName + "|"+stub
            result.append({"tableID": row["Table ID"], "uniqueID":row[unique_id_col_name], "universe":universe, "variableName":stub, "variableExtendedName": extendedName})
    
                
                

    return pd.DataFrame.from_records(result)


def download_cartography_file(year: int, admin_level: str, work_dir: str| None = None):
    metadata = ACS_METADATA[year]
    url = metadata['geoms'][admin_level]
    if(work_dir == None):
        work_dir = tempfile.mkdtemp()
    local_dir = os.path.join(work_dir, admin_level+".zip")
    urllib.request.urlretrieve(url, local_dir)
    return local_dir

def convert_cartography_file_to_formats(path: str):
    data=  gp.read_file(f"zip://{path}")
    data.to_parquet(path.replace(".zip",".parquet"))
    data.to_file(path.replace(".zip",".flatgeobuff"), driver="FlatGeobuf")
    data.to_file(path.replace(".zip", ".geojsonseq"), driver="GeoJSONSeq")

def generate_pmtiles(path:str):
    client = docker.from_env()
    mount_folder = Path(path).resolve()
    container =client.containers.run("stuartlynn/tippecanoe:latest",
                          "tippecanoe -o tracts.pmtiles tracts.geojsonseq",
                          volumes={mount_folder: {"bind":"/app","mode":"rw"} },
                          detach=True,
                          remove=True)

    output = container.attach(stdout=True, stream=True, logs=True)
    for line in output:
        print(line)

""""
    Return the fips codes for states as two digit zero padded strings.
    Exclude state id which are placeholders for future potential states. 
"""
def state_fips():
    return [ f'{n:02d}' for n in  range(1,56) if n not in [3,7,14,43,52]]

"""
    Convert a data table to parquet and output 
"""
def convert_to_parquet(files: list[str], metrics: [list[str]]): # type: ignore
    pass
    # variableDescs = get_variable_definitions()
    # for metric in metrics:
    #      print(variableDescs[metric])
    
    # for file in files:
    #     data = pd.read_csv(file, dtype={"GEO_ID":str})
    #     data.drop(["state","county","tract"],axis=1).to_parquet(file.replace(".csv",".parquet"))

"""
    get the names of each summary file for a given year and summary level
"""
def get_summary_table_file_names(year:int, summary_level:str="fiveYear"):
    metadata = ACS_METADATA[year]
    base = metadata['base']
    table_path =  base + metadata[summary_level]['tables'] 
    
    table = pd.read_html(table_path)[0]
    filtered = table[table['Name'].str.startswith("acs",na=False)]
    return list(filtered["Name"])

"""
    Get the geometry identifier table for a given year and summary_level
"""
def get_geom_ids_table_for_summary(year:int, summary_level:str):
    path = ACS_METADATA[year]["base"] + ACS_METADATA[year][summary_level]['geoIds']
    sep = ACS_METADATA[year]["geoIdsSep"] if "geoIdsSep" in ACS_METADATA[year] else ","
    table = pd.read_csv(path, encoding='latin', sep=sep, low_memory=False)
    return table

"""
    Extract variables for the geographies we are interested in from the summary table
"""
def extract_values_at_specified_levels(
        df: pd.DataFrame, geoids: pd.DataFrame, geo_ids_col: str = "DADSID"
    ):
    joined = pd.merge(df,geoids[[geo_ids_col,"SUMLEVEL"]], left_on="GEO_ID", right_on=geo_ids_col, how='left')
    result = {}

    for (level, id) in SUMMARY_LEVELS.items():
        result[level]=(
            joined[joined['SUMLEVEL']==id]
            .drop(
                ["SUMLEVEL"]
                + ([geo_ids_col] if geo_ids_col != "GEO_ID" else []), axis=1)
        )
    return result         
        

"""
    Download and process a specific summary table 
"""
def get_summary_table(table_name: str, year:int, summary_level:str):
    base = ACS_METADATA[year]["base"]
    summary_file_dir = base + ACS_METADATA[year][summary_level]['tables']
    data = pd.read_csv(f"{summary_file_dir}/{table_name}", sep="|")
    return data

def select_estimates(df):
    estimate_columns = [ col for col in df.columns if col[0]=="B" and col.split("_")[1][0]=="E"]
    return df[estimate_columns]

def select_errors(df):
    error_columns = [ col for col in df.columns if col[0]=="B" and col.split("_")[1][0]=="M"]
    return df[error_columns]


def merge_parquet_files(file_names):
    result=pd.DataFrame()
    for batch in tqdm(batched(file_names,20)):
        newDFS = [select_estimates(pd.read_parquet(file).set_index("GEO_ID")) for file in batch]
        result = pd.concat([result] + newDFS  ,axis=1)
    return result

"""
    Generate the metirc parquet files for a given year and summary level 
"""
def process_year_summary_level(year:int, summary_level:str ="fiveYear"):
    workdir = tempfile.mkdtemp()
    tractDir = os.path.join(workdir,"tract")
    blockGroupDir = os.path.join(workdir,"block_groups")
    countyDir = os.path.join(workdir,"counties")
    os.mkdir(blockGroupDir)
    os.mkdir(tractDir)
    os.mkdir(countyDir)

    
    table_names = get_summary_table_file_names(year,summary_level)
    geoids = get_geom_ids_table_for_summary(year,summary_level)
    print("temp dir is ", workdir)
    for table in tqdm(table_names):
        df = get_summary_table(table, year,summary_level)
        values = extract_values_at_specified_levels(df, geoids)
        values['tract'].to_parquet(os.path.join(tractDir,table.replace(".dat",".parquet")))
        values['county'].to_parquet(os.path.join(countyDir,table.replace(".dat",".parquet")))
        values['block_group'].to_parquet(os.path.join(blockGroupDir,table.replace(".dat",".parquet")))

    merge_parquet_files([os.path.join(countyDir,file) for file in os.listdir(countyDir)]).to_parquet(f"county_{year}_{summary_level}.parquet")
    merge_parquet_files([os.path.join(tractDir,file) for file in os.listdir(tractDir)]).to_parquet(f"tract_{year}_{summary_level}.parquet")
    merge_parquet_files([os.path.join(blockGroupDir,file) for file in os.listdir(blockGroupDir)]).to_parquet(f"block_groups_{year}_{summary_level}.parquet")

if __name__ == "__main__":
    process_year_summary_level(2019,"fiveYear")
# """
#     Old code for getting the metrics from the Census API
# """
# def get_census_merics(years:list[int], admin_level:str, metrics:list[str]):
#     import requests
#     import json
#     import csv

#     metrics = get_variable_definitions();
#     metrics = [k for k in metrics.keys() if k[0]=='D']
#     print(metrics)
    
#     for year in years:
#         allValues =[]
#         for state in state_fips():
#             print(f"Downloading {year} for {state}")
#             api_call = f'https://api.census.gov/data/{year}/acs/acs5/profile?get=GEO_ID,{",".join(metrics)}&for={admin_level}:*&in=state:{state}&in=county:*'

#             print(api_call)
#             try:
#                 r = requests.get(api_call)
#                 r.raise_for_status

#                 data = r.json()
#                 if(len(allValues) > 0):
#                     allValues = allValues + data[1:]
#                 else:
#                     allValues = data
#             except requests.exceptions.HTTPError as errh:
#                 print(errh)
                           

#         with open(f'acs5_{year}_by_tract.csv','w',newline='') as csvfile:
#             csv.writer(csvfile).writerows(allValues)
#     print("Got metric")
