from dagster import asset 

import pandas as pd 
from .config import ACS_METADATA,SUMMARY_LEVELS
import tempfile
import os 

year = 2019
summary_level = "fiveYear"



def get_summary_table(table_name: str, year:int, summary_level:str):
    base = ACS_METADATA[year]["base"]
    summary_file_dir = base + ACS_METADATA[year][summary_level]['tables']
    data = pd.read_csv(f"{summary_file_dir}/{table_name}", sep="|")
    return data


def extract_values_at_specified_levels(df: pd.DataFrame, geoids: pd.DataFrame):
    joined = pd.merge(df,geoids[["DADSID","SUMLEVEL"]], left_on="GEO_ID", right_on="DADSID", how='left')
    result = {}

    for (level, id) in SUMMARY_LEVELS.items():
        result[level]=joined[joined['SUMLEVEL']==id].drop(["DADSID","SUMLEVEL"],axis=1)
    return result         

def merge_parquet_files(file_names):
    result=pd.DataFrame()
    for batch in tqdm(batched(file_names,20)):
        newDFS = [select_estimates(pd.read_parquet(file).set_index("GEO_ID")) for file in batch]
        result = pd.concat([result] + newDFS  ,axis=1)
    return result


@asset 
def generate_variable_dictionary():
    metadata = ACS_METADATA[year]
    base = metadata["base"]
    shells = metadata[summary_level]["shells"]
    raw= pd.read_csv(base+shells, encoding="latin")    
    result = [] # pd.DataFrame(columns=["tableID","uniqueID", "universe","tableName", "variableName", "variableExtedndedName"])

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

        if (row[["UniqueID"]].isna().all()):
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
            result.append({"tableID": row["Table ID"], "uniqueID":row["UniqueID"], "universe":universe, "variableName":stub, "variableExtendedName": extendedName})
    
    return pd.DataFrame.from_records(result)



@asset 
def aws_table_files(context, summary_table_names, geometry_ids):
    base = ACS_METADATA[year]["base"]
    summary_file_dir = base + ACS_METADATA[year][summary_level]['tables']

    workdir = tempfile.mkdtemp()

    tractDir = os.path.join(workdir,"tracts")
    blockGroupDir = os.path.join(workdir,"block_groups")
    countyDir = os.path.join(workdir,"counties")

    os.mkdir(blockGroupDir)
    os.mkdir(tractDir)
    os.mkdir(countyDir)

    for index, table in enumerate(summary_table_names):
        context.log.info(f"Downloading {table} {index} of {len(summary_table_names)}" )
        data = get_summary_table(table, year, summary_level) 
        values = extract_values_at_specified_levels(data, geometry_ids)
        values['tract'].to_parquet(os.path.join(tractDir,table.replace(".dat",".parquet")))
        values['county'].to_parquet(os.path.join(countyDir,table.replace(".dat",".parquet")))
        values['block_group'].to_parquet(os.path.join(blockGroupDir,table.replace(".dat",".parquet")))
    return {"tract": tractDir, "bockGroup": blockGroupDir, "county":countyDir}


@asset
def merge_parquet_files(aws_table_files):
    merge_parquet_files([os.path.join(countyDir,file) for file in os.listdir(countyDir)]).to_parquet(f"county_{year}_{summary_level}.parquet")
    merge_parquet_files([os.path.join(tractDir,file) for file in os.listdir(tractDir)]).to_parquet(f"tracts_{year}_{summary_level}.parquet")
    merge_parquet_files([os.path.join(blockGroupDir,file) for file in os.listdir(blockGroupDir)]).to_parquet(f"block_groups_{year}_{summary_level}.parquet")

    


        
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
    path = ACS_METADATA[year]["base"] + ACS_METADATA[year][summary_level]['geoIds']
    sep = ACS_METADATA[year]["geoIdsSep"] if "geoIdsSep" in ACS_METADATA[year] else ","
    table = pd.read_csv(path, encoding='latin', sep=sep)
    return table
#
#
@asset 
def summary_table_names():
    metadata = ACS_METADATA[year]
    base = metadata['base']
    table_path =  base + metadata[summary_level]['tables'] 
    
    table = pd.read_html(table_path)[0]
    filtered = table[table['Name'].str.startswith("acs",na=False)]
    return list(filtered["Name"])
