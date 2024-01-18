from popgetter.metadata import *

SUMMARY_LEVELS={
    "tract": 140 ,
    "block_group":150,
    "county":50
}

ACS_METADATA={
    ## 2018 needs additional logic for joining together geometry files
    ## 2018:{
    ##     base:"https://www2.census.gov/programs-surveys/acs/summary_file/2018",
    ##     type:'table',
    ##     geoms:{
    ##         tract:,
    ##         block:"",
    ##         county:"https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_county_500k.zip":
    ##     }
    ##     oneYear:{
    ##         tables:"prototype/1YRData/",
    ##         geoIds:"https://www2.census.gov/programs-surveys/acs/summary_file/2018/prototype/20181YRGeos.csv",
    ##         shells:"prototype/ACS2018_Table_Shells.xlsx"
    ##     },
    ##     fiveYear:{
    ##         tables:"prototype/5YRData/",
    ##         geoIds:"https://www2.census.gov/programs-surveys/acs/summary_file/2018/prototype/20185YRGeos.csv",
    ##         shells:"prototype/ACS2018_Table_Shells.xlsx"
    ##     }
    ## },
    2019:{
        "base" : "https://www2.census.gov/programs-surveys/acs/summary_file/2019/",
        "type":'table',
        "geoms":{
            "tracts": "https://www2.census.gov/geo/tiger/GENZ2019/shp/cb_2019_us_tract_500k.zip",
            "blockGroups":"https://www2.census.gov/geo/tiger/GENZ2019/shp/cb_2019_us_bg_500k.zip",
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
        }
    },
    # Note 1 year esimates are not avaliable because of covid. More details of the exeprimental estimates 
    # are here : https://www.census.gov/programs-surveys/acs/technical-documentation/table-and-geography-changes/2020/1-year.html
    2020:{
        "base":"https://www2.census.gov/programs-surveys/acs/summary_file/2020/",
        "type":'table',
        "geoms":{
            "tracts":"https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_tract_500k.zip",
            "blockGroups":"https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_bg_500k.zip",
            "county":"https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_county_500k.zip"
        },
        "shells":"prototype/ACS2020_Table_Shells.csv",
        "oneYear":None,
        "fiveYear":{
            "shells":"prototype/ACS2020_Table_Shells.csv",
            "tables":"prototype/5YRData/",
            "geoIds":"prototype/Geos20205YR.csv",
        }
    },
    2021:{
        "base":"https://www2.census.gov/programs-surveys/acs/summary_file/2021/table-based-SF/",
        "type":'table',
        "geoIdsSep":"|",
        "geoms":{
            "tracts":"https://www2.census.gov/geo/tiger/GENZ2021/shp/cb_2021_us_tract_500k.zip",
            "blockGroups":"https://www2.census.gov/geo/tiger/GENZ2021/shp/cb_2021_us_bg_500k.zip",
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
        }
     }
}


US_Meta = CountryMetadata(
   name_short_en="USA",
   name_official="United States of America",
   iso3="USA",
   iso2="US" 
)

US_Census_Bureau_Meta = DataPublisher(
    name= "United States Census Bureau",
    url="https://www.census.gov/",
    description="The United States Census Bureau, officially the Bureau of the Census, is a principal agency of the U.S. Federal Statistical System, responsible for producing data about the American people and economy.",  
    countries_of_interest=[US_Meta]
)


