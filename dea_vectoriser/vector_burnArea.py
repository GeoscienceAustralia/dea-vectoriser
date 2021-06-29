
import geopandas as gp
import pandas as pd
import xarray as xr
from fiona.crs import from_epsg
from scipy import ndimage
from typing import Tuple

import geopandas as gp
import rasterio.features
import xarray as xr
from pathlib import Path
from shapely.geometry import shape

from dea_vectoriser.vectorise import vectorise_data

def load_burn_data(url) -> xr.Dataset:
    """Open a GeoTIFF info an in memory DataArray
    with DataArray labled as given name"""
    geotiff_burn = xr.open_rasterio(url)
    burn_dataset = geotiff_burn.to_dataset('band')
    return burn_dataset

def generate_likely_burn_rasters(brun_dataset: xr.Dataset) -> Tuple[xr.DataArray, xr.DataArray, xr.DataArray, xr.DataArray]:
    """Convert in memory delta Normalized Burn Ratio raster to vector format.
  as an interum, take dNBR classify into:
  0: unburnt everything less than +0.1 (not vectorise this class)
  1: very low probability of burn >= +0.1
  2: low probability of burn >= +0.26
  3: high probability of burn >= +0.66
  4: Very High probability of burn >= +1.3
  
  Output:
        Tuple[verylowprob, lowprob_burnt, highprob_burnt, veryhigh_prob]
    """
    #create binary array for low prob burn
    verylowprob_burnt =( brun_dataset[1] >= 0.1 )*1
    
    lowprob_burnt =( brun_dataset[1] >= 0.27) *1 

    highprob_burnt = (brun_dataset[1] >= 0.66) *1
    
    veryhighprob_burnt = (brun_dataset[1] >= 1.3) *1
    
    brun_dataset = brun_dataset[1]
    
    # erode then dilate all binary arrays by 2 itterations
    erroded_VeryLowProb = xr.DataArray(ndimage.binary_erosion(verylowprob_burnt, iterations=2).astype(brun_dataset.dtype),
                                 coords=brun_dataset.coords)
    dilated_VeryLowProb = xr.DataArray(ndimage.binary_dilation(erroded_VeryLowProb, iterations=2).astype(brun_dataset.dtype),
                                 coords=brun_dataset.coords)
    
    erroded_LowProb = xr.DataArray(ndimage.binary_erosion(lowprob_burnt, iterations=2).astype(brun_dataset.dtype),
                                 coords=brun_dataset.coords)
    dilated_LowProb = xr.DataArray(ndimage.binary_dilation(erroded_LowProb, iterations=2).astype(brun_dataset.dtype),
                                 coords=brun_dataset.coords)
    
    erroded_highprob = xr.DataArray(ndimage.binary_erosion(highprob_burnt, iterations=2).astype(brun_dataset.dtype),
                                 coords=brun_dataset.coords)
    dilated_highprob = xr.DataArray(ndimage.binary_dilation(erroded_highprob, iterations=2).astype(brun_dataset.dtype),
                                 coords=brun_dataset.coords)
    
    erroded_veryhighprob = xr.DataArray(ndimage.binary_erosion(veryhighprob_burnt, iterations=2).astype(brun_dataset.dtype),
                                 coords=brun_dataset.coords)
    dilated_veryhighprob = xr.DataArray(ndimage.binary_dilation(erroded_veryhighprob, iterations=2).astype(brun_dataset.dtype),
                                 coords=brun_dataset.coords)

    return [dilated_VeryLowProb, dilated_LowProb, dilated_highprob, dilated_veryhighprob]


def vectorise_burn(url) -> gp.GeoDataFrame:
    """Load a delta Normalized Burn Ratio raster and convert to In Memory Vector"""
    raster = load_burn_data(url)
    #raster = xr.open_rasterio(url).to_dataset('band')
    
    dataset_crs = from_epsg(raster.crs[11:])
    dataset_transform = raster.transform
    # grab crs from input tiff
    
    #do the science to the input dataset
    verylowprob_burnt, lowprob_burnt, highprob_burnt, veryhighprob_burnt = generate_likely_burn_rasters(raster)

    # vectorise the arrays
    verylow_burntGPD = vectorise_data(verylowprob_burnt, dataset_transform, dataset_crs, label='very_low_probability_burn')
    low_burntGPD = vectorise_data(lowprob_burnt, dataset_transform, dataset_crs, label='low_probability_burn')
    high_burntGPD = vectorise_data(highprob_burnt, dataset_transform, dataset_crs, label='high_probability_burn')
    veryhigh_burntGPD = vectorise_data(veryhighprob_burnt, dataset_transform, dataset_crs, label='very_high_probability_burn')

    # Simplify

    # change to 'epsg:3577' prior to simplifiying to insure consistent results
    low_burntGPD = low_burntGPD.to_crs('epsg:3577')
    high_burntGPD = high_burntGPD.to_crs('epsg:3577')
    verylow_burntGPD = verylow_burntGPD.to_crs('epsg:3577')
    veryhigh_burntGPD = veryhigh_burntGPD.to_crs('epsg:3577')

    # Run simplification with 15 tolerance
    simplified_low_burnt = low_burntGPD.simplify(10)
    simplified_high_burnt = high_burntGPD.simplify(10)
    simplified_verylow_burnt = verylow_burntGPD.simplify(10)
    simplified_veryhigh_burnt = veryhigh_burntGPD.simplify(10)

    # Put simplified shapes in a dataframe
    simple_low_burntGPD = gp.GeoDataFrame(geometry=simplified_low_burnt,
                                      crs=from_epsg('3577'))

    simple_high_burntGPD = gp.GeoDataFrame(geometry=simplified_high_burnt,
                                            crs=from_epsg('3577'))
    
    simple_verylow_burntGPD = gp.GeoDataFrame(geometry=simplified_verylow_burnt,
                                      crs=from_epsg('3577'))

    simple_veryhigh_burntGPD = gp.GeoDataFrame(geometry=simplified_veryhigh_burnt,
                                            crs=from_epsg('3577'))

    # add attribute labels back in
    simple_low_burntGPD['attribute'] = low_burntGPD['attribute']

    simple_high_burntGPD['attribute'] = high_burntGPD['attribute']
    
    simple_verylow_burntGPD['attribute'] = verylow_burntGPD['attribute']

    simple_veryhigh_burntGPD['attribute'] = veryhigh_burntGPD['attribute']

    # Join layers together

    all_classes = gp.GeoDataFrame(pd.concat([simple_verylow_burntGPD, simple_low_burntGPD, simple_high_burntGPD, simple_veryhigh_burntGPD],
                                            ignore_index=True), crs=simple_low_burntGPD.crs)

    return all_classes





