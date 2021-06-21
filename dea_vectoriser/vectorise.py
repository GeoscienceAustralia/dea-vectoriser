from pathlib import Path
from tempfile import TemporaryDirectory

import geopandas
import geopandas as gp
import rasterio.features
import xarray as xr
from shapely.geometry import shape

# Mapping from Name: File extension
from dea_vectoriser.utils import LOG, url_to_bucket_and_key, upload_directory

OUTPUT_FORMATS = {
    'Shapefile': '.shp',
    'GeoJSON': '.json',
    'GPKG': '.gpkg'
}


def vectorise_data(data_array: xr.DataArray, transform, crs, label='Label'):
    """Return a vector representation of the input raster.

    Input
    data_array: an xarray.DataArray with boolean values (1,0) with 1 or True equal to the areas that will be turned
                into vectors
    label: default 'Label', String, the data label that will be added to each geometry in geodataframe

    Output
    Geodataframe containing shapely geometries with data type label in a series called attribute"""

    vector = rasterio.features.shapes(
        data_array.data.astype('float32'),
        mask=data_array.data.astype('float32') == 1,  # this defines which part of array becomes polygons
        transform=transform)

    # rasterio.features.shapes outputs tuples. we only want the polygon coordinate portions of the tuples
    vectored_data = list(vector)  # put tuple output in list

    # Extract the polygon coordinates from the list
    polygons = [polygon for polygon, value in vectored_data]

    # create a list with the data label type
    labels = [label for _ in polygons]

    # Convert polygon coordinates into polygon shapes
    polygons = [shape(polygon) for polygon in polygons]

    # Create a geopandas dataframe populated with the polygon shapes
    data_gdf = gp.GeoDataFrame(data={'attribute': labels},
                               geometry=polygons,
                               crs=crs)
    return data_gdf


def save_vector_to_s3(
        vector_data: geopandas.GeoDataFrame, dest_prefix: str, filename: str, output_format='GPKG') -> str:
    """Save a GeoPandas Vector to an AWS S3 Object

    :param vector_data: Vector data to serialise to S3
    :param dest_prefix: An S3 URL prefix. Eg: 's3://my-bucket/prefix/paths
    :param filename: Filename without an extension
    :param output_format: Vector format to create

    :return: string URL of written S3 Object. (Some formats may write multiple objects)
    """
    LOG.debug(f'Saving vector output to path: {dest_prefix}, filename: {filename}')

    # Append the correct file extension to the filename
    filename = filename + OUTPUT_FORMATS[output_format]

    bucket, key_prefix = url_to_bucket_and_key(dest_prefix)
    LOG.debug(f'Saving Vector output into Bucket: {bucket} with Prefix: {key_prefix}')

    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        LOG.debug(f'Writing Vector data to local file: {tmpdir / filename}')
        vector_data.to_file(tmpdir / filename, driver=output_format)

        LOG.debug(f'Uploading {tmpdir} to Bucket: {bucket} Prefix: {key_prefix}')
        upload_directory(tmpdir, bucket, key_prefix)
    return f"s3://{bucket}/{key_prefix}/{filename}"
