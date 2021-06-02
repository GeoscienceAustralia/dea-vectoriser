from pathlib import Path

from datacube_vectoriser import vector_wos


def test_create_vectors():
    smol = 'Little_WOs_nrt_water_2021_02_08.tif'

    gpd = vector_wos.vectorise_wos_from_url(smol)
    # raster = vector_wos.load_data(smol)

    # vector_wos.generate_raster_layers(raster)
    # with tempdir() as tmpdir:
    filename = "testfilename.shp"
    gpd.to_file(filename)

    assert Path(filename).exists()
