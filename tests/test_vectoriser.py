from pathlib import Path

import boto3
import geopandas
import xarray as xr
from shapely.geometry import Point

from dea_vectoriser import vector_wos
from dea_vectoriser.cli import vector_convert
from dea_vectoriser.utils import load_document_from_s3
from dea_vectoriser.vectorise import save_vector_to_s3


def test_create_vectors(sample_data, tmp_path):
    sample_tiff = list(sample_data.glob('**/*.tif'))[0]

    gpd = vector_wos.vectorise_wos(sample_tiff)

    filename = str(tmp_path / "testfilename.shp")
    gpd.to_file(filename)

    assert Path(filename).exists()


def test_convert_from_s3(samples_on_s3, sample_data, mocker):
    sample_tiff = list(sample_data.glob('**/*.tif'))[0]

    sample_xarray = xr.open_rasterio(sample_tiff)
    mocker.patch('dea_vectoriser.vector_wos.xr.open_rasterio', return_value=sample_xarray)

    stac_url = [obj for obj in samples_on_s3 if obj.endswith('json')][0]
    stac_document = load_document_from_s3(stac_url)
    destination = 's3://second-bucket/'
    output_format = 'GPKG'
    vector_convert(stac_document, destination, output_format)

    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket='second-bucket')
    assert len(response['Contents']) == 1
    assert response['Contents'][0]['Key'] == '097/075/1998/08/17/ga_ls_wo_3_097075_1998-08-17_final_water.gpkg'


def test_save_vector_to_s3(s3):
    d = {'col1': ['name1', 'name2'], 'geometry': [Point(1, 2), Point(2, 1)]}

    gdf = geopandas.GeoDataFrame(d, crs="EPSG:4326")

    dest_prefix = 's3://first-bucket/part/more/stuff'
    filename = 'example_filename'

    save_vector_to_s3(gdf, dest_prefix=dest_prefix, filename=filename, output_format='GPKG')

    client = boto3.client('s3')
    response = client.list_objects_v2(Bucket='first-bucket')
    assert response['Contents'][0]['Key'] == 'part/more/stuff/example_filename.gpkg'
#    response = client.head_object(Bucket='first-bucket', Key=)
#    assert response
