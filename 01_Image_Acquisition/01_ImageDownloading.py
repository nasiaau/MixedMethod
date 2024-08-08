import asf_search as asf
import geopandas as gpd
from shapely.geometry import box
from datetime import date
import time,sys
import json


def do_download(kdprov):
    print('########################################################')
    print('Downloading for' ,kdprov)
    start_time=time.time()
    data_provinsi='/data/ksa/00_Data_Input/provinsi.gpkg'
    gpd_provinsi=gpd.read_file(data_provinsi)
    prov_=gpd_provinsi.query('provno==@kdprov')
    bounds = prov_.total_bounds
    gdf_bounds = gpd.GeoSeries([box(*bounds)])
    wkt_aoi = gdf_bounds.to_wkt().values.tolist()[0]
    results = asf.search(
        platform= asf.PLATFORM.SENTINEL1A,
        processingLevel=[asf.PRODUCT_TYPE.GRD_HD],
        start = date(2021, 1, 1),
        end = date(2023, 12, 31),
        intersectsWith = wkt_aoi
        )
    print(f'Total Images Found: {len(results)}')
    metadata = results.geojson()
    json_object = json.dumps(metadata)
    print('Writing the metadata.......')
    with open(f'/data/ksa/01_Image_Acquisition/04_Json_Raw_Download/{kdprov}_metadata_ASF.json', 'w') as f:
        f.write(json_object)
    with open('config.txt', 'r') as file:
        token = file.read().rstrip()
    session=asf.ASFSession().auth_with_token(token)
    results.download(
         path = '/data/ksa/01_Image_Acquisition/01_Raw_Image',
         session = session,
         processes = 50)
    print('Begin downloading at ',time.time())
    print('Finished at ',time.time())
    print("--- %s seconds ---" % (time.time() - start_time))
    print('########################################################')
    
def main():
    kdprov=sys.argv[1]
    do_download(kdprov)

if __name__ == "__main__":
    main()

        