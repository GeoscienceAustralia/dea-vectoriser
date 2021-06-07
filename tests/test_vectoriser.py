from pathlib import Path

from dea_vectoriser import vector_wos


def test_create_vectors(sample_data, tmp_path):
    sample_tiff = sample_data / "sample_raster.tif"

    gpd = vector_wos.vectorise_wos_from_url(sample_tiff)

    # vector_wos.generate_raster_layers(raster)
    # with tempdir() as tmpdir:
    filename = str(tmp_path / "testfilename.shp")
    gpd.to_file(filename)

    assert Path(filename).exists()
