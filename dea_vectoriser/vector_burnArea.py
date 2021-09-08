import geopandas as gp
import pandas as pd
import xarray as xr
from fiona.crs import from_epsg
from scipy import ndimage
from typing import Tuple

from skimage import morphology
from skimage.morphology import ball

import geopandas as gp
import rasterio.features
import xarray as xr
from pathlib import Path
from shapely.geometry import shape

from dea_vectoriser.vectorise import vectorise_data

def load_burn_data(url) -> xr.Dataset:
    """Open a GeoTIFF into an in memory DataArray
    with DataArray labelled as given name"""
    geotiff_burn = xr.open_rasterio(url)
    burn_dataset = geotiff_burn.to_dataset('band')
    return burn_dataset

def threshold_Delta_dataset(burn_dataset: xr.Dataset, threshold: float =0.5, greater: bool =True) -> xr.DataArray:
    """Apply a threshold to in memory continuous dataset 
    For now, conduct erosion and dilation.
  
    Input: xr.Dataset containing one dataarray which is to be thresholded
            threshold to be applied to xr.Dataset
            direction of threshold to be conducted. Default TRUE means comparison will be conducted 
            as greater or equal to threshold value.
    Output: a xr.DataArray containing 1,0 with 1 meeting the criteria of applied threshold
    """

    
    #create binary array for low agreement burn
    if greater == True:
    
        threshold_data = ( burn_dataset[1] >= threshold )*1
        
    else:
        threshold_data = ( burn_dataset[1] <= threshold )*1

            # erode then dilate binary array by 2 iterations
    dilated_data = xr.DataArray(morphology.binary_closing(threshold_data, morphology.disk(3)).astype(burn_dataset[1].dtype),
                                 coords=burn_dataset[1].coords)
    erroded_data = xr.DataArray(morphology.erosion(dilated_data, morphology.disk(3)).astype(burn_dataset[1].dtype),
                                 coords=burn_dataset[1].coords)
    dilated_data = xr.DataArray(ndimage.binary_dilation(erroded_data, morphology.disk(3)).astype(burn_dataset[1].dtype),
                                 coords=burn_dataset[1].coords)

    
    return dilated_data

def create_fmask_mask(fmask_dataset: xr.Dataset) -> xr.Dataset:
    '''Create a mask from fmask and then dilate and erode data. 
    Include values (2: cloud 3: shadow 4: snow 5: water) in fmask
    mask that are therefore not equal to 1 (1: valid). 
    
    Input: xr.Dataset containing one fmask data.
    Output: a xr.DataArray containing 1,0 with 1 meeting the criteria of applied threshold
    """'''
    
    #make binary mask based on fmask
    fmask_mask =  ( fmask_dataset != 1 )*1
    # erode then dilate binary array by 2 iterations
    dilated_data = xr.DataArray(morphology.binary_closing(fmask_mask[1], morphology.disk(3)).astype(fmask_dataset[1].dtype),
                                 coords=fmask_dataset[1].coords)
    erroded_data = xr.DataArray(morphology.erosion(dilated_data, morphology.disk(3)).astype(fmask_dataset[1].dtype),
                                 coords=fmask_dataset[1].coords)
    dilated_data = xr.DataArray(ndimage.binary_dilation(erroded_data, morphology.disk(3)).astype(fmask_dataset[1].dtype),
                                 coords=fmask_dataset[1].coords)
    
    return dilated_data


def generate_burn_agreement(BSI_dataset: xr.Dataset, NDVI_dataset: xr.Dataset, NBR_dataset: xr.Dataset,) -> Tuple[xr.DataArray, xr.DataArray, xr.DataArray]:
    """combine the three burn index models into an agreement burn map. 

    """
    BSI_burn = threshold_Delta_dataset(BSI_dataset, threshold=0.2, greater=True)
    NDVI_burn = threshold_Delta_dataset(NDVI_dataset, threshold=0.1, greater=True)
    NBR_burn = threshold_Delta_dataset(NBR_dataset, threshold=0.1, greater=True)
    
    combined_agreement = BSI_burn + NDVI_burn + NBR_burn
    #combine boolean arrays so that where two overlap value become 2, three overlap value becomes 3.
    
    lowagreement = (combined_agreement == 1) * 1
    modagreement = (combined_agreement == 2) * 1
    highagreement = (combined_agreement == 3) * 1
    
        
    return [lowagreement, modagreement, highagreement]

def generate_burn_area(BSI_dataset: xr.Dataset, NDVI_dataset: xr.Dataset, NBR_dataset: xr.Dataset,) -> xr.DataArray:
    """Take three simple models. Apply threshold to models and find where DNBR and at least one other model indicate burn area is likely.
    
    Input:  BSI_dataset: xr.Dataset, Differenced Bare Soil Index Dataset 
            NDVI_dataset: xr.Dataset, Differenced Normalised Difference Vegetation Dataset
            NBR_dataset: xr.Dataset, Differeced Normalised Burn Ration Dataset
            
    output: xr.Dataset, boolean values where burn is likely to have occured

    """
    BSI_burn = threshold_Delta_dataset(BSI_dataset, threshold=0.1, greater=True) * 1
    NDVI_burn = threshold_Delta_dataset(NDVI_dataset, threshold=0.1, greater=True) * 1
    NBR_burn = threshold_Delta_dataset(NBR_dataset, threshold=0.1, greater=True) * 1
    
    stacked_agreement = (NBR_burn * BSI_burn) + (NBR_burn * NDVI_burn)
    
    likely_burn = stacked_agreement >= 1
    #I know it's kinda redundant to threshold again put I want the output boolean for vectorisation
        
    return(likely_burn)

def simplify_vectors(burn_dataframe: gp.GeoDataFrame, tolerance:int=10)-> gp.GeoDataFrame:
# Simplify

    # change to 'epsg:3577' prior to simplifiying to insure consistent results
    burn_dataframe = burn_dataframe.to_crs('epsg:3577')

    # Run simplification with 10 tolerance
    simplified_burn_shapes = burn_dataframe.simplify(10)

    # Put simplified shapes in a dataframe
    simple_burnt_dataframe = gp.GeoDataFrame(geometry=simplified_burn_shapes,
                                      crs=from_epsg('3577'))

    # add attribute labels back in
    simple_burnt_dataframe['attribute'] = burn_dataframe['attribute']
    
    return(simple_burnt_dataframe)

def vectorise_burn(raster_urls) -> gp.GeoDataFrame:
    """Load from S3 dBSI, dNBR, dNDVI, and fmask rasters and
     produces two vector products. Add fmask mask to outputs.
    
    Burn_agreement finds agreement between the three burn models:
        High agreement: where three models agree
        Medium agreement: where two models agree
        Low agreement: Where only one model finds burn
        
    dNBRGPD: Burnt area defined only by delta Normalised Burn Ratio. Burn area is greater than 0.1 
    Rahman et al. 2018 found this a good threshold to define burn area using sentinel 2. 
    
    """
    
    BSI_raster = load_burn_data(raster_urls['delta_bsi_asset_url'])
    NDVI_raster = load_burn_data(raster_urls['delta_ndvi_asset_url'])
    NBR_raster = load_burn_data(raster_urls['delta_nbr_asset_url'])
    fmask_raster = load_burn_data(raster_urls['fmask_asset_url'])
    
    dataset_crs = from_epsg(BSI_raster.crs[11:])
    dataset_transform = BSI_raster.transform
    # grab crs from input tiff
    
#     # Extract date from the first file path. Assumes that the last four path elements are year/month/day/YYYYMMDDTHHMMSS
    year, month, day, time = str(raster_urls['delta_bsi_asset_url']).split('/')[-5:-1]
    time_hour =time[-6:-4]
    time_mins =time[-4:-2]
    obs_date = f'{year}-{month}-{day}T{time_hour}:{time_mins}:00:0Z'
#     obs_date = '2021-08-05T00:00:00:0Z'
    
    #do the science to the input dataset generate likely burn area 
    burn_area_dataset = generate_burn_area(BSI_raster, NDVI_raster, NBR_raster)

    #create mask to create highlight not-valid data
    fmask_mask = create_fmask_mask(fmask_raster)

    # vectorise the arrays
    burn_area_GPD = vectorise_data(burn_area_dataset, dataset_transform, dataset_crs, label='potential_burn') #unsure if should change this lable?
    fmaskGPD = vectorise_data(fmask_mask, dataset_transform, dataset_crs, label= 'not_analysed')

    #Do simplification here if desiered
#     burn_area_GPD = simplify_vectors(burn_area_GPD, tolerance=10)
#     fmaskGPD = simplify_vectors(fmaskGPD, tolerance=10)
    
#     Join layers together
    Burn_agreement = gp.GeoDataFrame(pd.concat([burn_area_GPD, fmaskGPD],
                                            ignore_index=True), crs=burn_area_GPD.crs)

    # add observation date as new attribute
    Burn_agreement['Observed_date'] = obs_date
    
    return(Burn_agreement)