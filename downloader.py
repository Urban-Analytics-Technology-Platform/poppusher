import xml.etree.ElementTree as ET
import subprocess

def download_from_wfs(wfs_url, output_file):
    """
    Downloads data from a WFS (`wfs_url`) and saves it to a file (`output_file`). The `ogr2ogr` command line tool is used to workaround the feature count limit that can be imposed by the server. (See https://gdal.org/drivers/vector/wfs.html#request-paging for details.)

    Parameters
    ----------
    wfs_url : str
        The URL of the WFS.
    output_file : str
        The name of the output file.
    """
    template = r"""
        <OGRWFSDataSource>
            <URL>CHANGE_ME</URL>
            <PagingAllowed>ON</PagingAllowed>
            <PageSize>1000</PageSize>
        </OGRWFSDataSource>
    """

    print("Writing OGR Virtual Format file")
    root = ET.fromstring(template)
    root.find("URL").text = wfs_url

    with open(f"{output_file}.xml", "w") as f:
        f.write(ET.tostring(root).decode())

    print("Running ogr2ogr")
    # subprocess.run(["ogrinfo", "-ro", f"{output_file}.xml"])
    subprocess.run(["ogr2ogr", "-f", "GeoJSON", f"{output_file}.geojson", f"{output_file}.xml"])

    print("Done")



if __name__ == "__main__":

    oa_wfs_url = "https://dservices1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/services/Output_Areas_Dec_2021_Boundaries_Generalised_Clipped_EW_BGC_/WFSServer?service=wfs",
    layer_name = "Output_Areas_Dec_2021_Boundaries_Generalised_Clipped_EW"

    print(f"URL: {url}")
    print(f"Layer: {layer_name}")

    download_from_wfs(oa_wfs_url, layer_name)
