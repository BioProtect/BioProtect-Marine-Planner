import glob
import subprocess
from subprocess import PIPE, CalledProcessError, Popen

from os import pardir, path, sep
from string import punctuation

import geopandas as gpd
import numpy as np
import rasterio
from mapbox import Uploader, errors
from osgeo import gdal, gdalconst
from pyproj import CRS
from rasterio.warp import Resampling, calculate_default_transform, reproject
from sqlalchemy import create_engine, exc

from .env import setup_environment

dir_path = path.dirname(path.realpath(__file__))
data_path = "/".join(dir_path.split('/')[:-1])


def replace_chars(text):
    out = text.translate(str.maketrans("", "", punctuation))
    out = out.replace("  ", "_").replace(" ", "_").lower()
    return out


def get_tif_list(tif_dir, file_type):
    """Get a list of all the tifs in a directory

    Keyword arguments:
    tif_dir -- the directory where the list of tifs is
    Return: a sorted list of dictionaries containing the filename and path
    """
    return sorted([{
        "label": ".".join(filename.split('/')[-1].split('.')[0:-1]),
        "path": filename
    } for filename in glob.glob((data_path+tif_dir) + "**/*." + file_type, recursive=True)], key=lambda i: i['label'])


def setup_sens_matrix():
    print('Setting up sensitivity matrix....')
    habitat_list = [item['label']
                    for item in
                    get_tif_list('/'+config['input_coral'], 'asc') +
                    get_tif_list('/'+config['input_fish'], 'asc')]
    sens_mat = config['sensmat']
    for habitat_name in habitat_list:
        sens_mat.loc[habitat_name] = sens_mat.loc['VME']
    return sens_mat


def normalize_nparray(nparray):
    out_array = np.ma.masked_invalid(nparray)
    logged = np.log(out_array + 1)
    min_val = logged.min()
    max_val = logged.max()
    return (logged-min_val)/(max_val - min_val)


def get_rasters_transform(rast, reprojection_crs):
    with rasterio.open('data/rasters/all_habitats.tif') as template:
        transform, width, height = calculate_default_transform(
            template.crs, reprojection_crs, template.width, template.height, *template.bounds)
        return {
            "transform": transform,
            "width": width,
            "height": height,
            "meta": template.meta.copy()
        }


def reproject_raster(file, output_folder, reprojection_crs=None):
    """
    Reproject an individual raster to the given crs

    Args:
        file (path): Path to the raster to reproject
        output_folder (path): Path to the folder to where new reprojected raster will be saved
        reprojection_crs (string, optional): string representation of crs to reproject to. Defaults to None.

    Returns:
        str: path of newly reprojected raster
    """
    nodata_val = 0
    filename = file.split('/')[-1].split('.')[0]
    output_file = output_folder + filename + '.tif'

    with rasterio.open(file, 'r+') as src:
        transform, width, height = calculate_default_transform(
            src.crs, reprojection_crs, src.width, src.height, *src.bounds
        )
        kwargs = src.meta.copy()
        kwargs.update({
            'crs': reprojection_crs,
            'transform': transform,
            'width': width,
            'height': height,
            'nodata': nodata_val
        })

        with rasterio.open(output_file, 'w', **kwargs) as dst:
            for i in range(1, src.count + 1):
                source = rasterio.band(src, i)
                reproject(source=source,
                          destination=rasterio.band(dst, i),
                          src_transform=src.transform,
                          src_crs=src.crs,
                          dst_transform=transform,
                          dst_crs=reprojection_crs,
                          resampling=Resampling.nearest,
                          dst_nodata=nodata_val)
    return output_file


def reproject_shape(filename, save_path, reproject):
    """
    reproject a shapefile from one CRS to another.

    Args:
        filename (shapefile): shapefile to be reprojected
        reproject (CRS): CRS from CRS.from_proj4() function
        save_path (string): location of where to save file

    Returns:
        string: returns the location of the newly reprojected shapefile
    """
    shapefile = gpd.read_file(filename)
    shapefile = shapefile.to_crs(reproject)
    shapefile.to_file(save_path)
    return save_path


def reproject_and_normalise_upload(raster_name, data, reprojection_crs, wgs84, crop_file):
    # reproject to wgs84 if needs be
    raster_info = data.meta.copy()
    if '4326' not in raster_info['crs'].to_string():
        wgs84_rast = reproject_raster(raster_data=data,
                                      output_folder='data/tmp/',
                                      reprojection_crs=wgs84)
        with rasterio.open(wgs84_rast, 'r+') as src:
            rast_data = src
    else:
        rast_data = data

    out_file = 'data/uploaded_rasters/' + raster_name.lower()
    normalized_name = 'data/tmp/' + raster_name
    template_info = get_rasters_transform(rast='data/rasters/all_habitats.tif',
                                          reprojection_crs=reprojection_crs)
    nodata_val = template_info['meta']['nodata']
    # normalise
    with rasterio.open(normalized_name, "w", **template_info['meta']) as dst:
        for i in range(1, rast_data.count + 1):
            band = np.ma.masked_values(
                rast_data.read(i, masked=True), nodata_val)
            source = normalize_nparray(band)
            reproject(width=template_info['width'],
                      height=template_info['height'],
                      source=source,
                      src_transform=rast_data.transform,
                      src_crs=rast_data.meta['crs'],
                      destination=rasterio.band(dst, i),
                      dst_transform=template_info['transform'],
                      dst_crs=reprojection_crs,
                      resampling=Resampling.nearest)

    project_raster(rast1=normalized_name,
                   template_file='data/rasters/all_habitats.tif',
                   output_file=normalized_name.lower())

    return normalized_name.lower()


def reproject_raster_to_all_habs(tmp_file, data, meta, out_file):
    src_crs = meta.get('crs')
    if src_crs is None:
        src_crs = config['wgs84_str']
    template_info = get_rasters_transform(rast='data/rasters/all_habitats.tif',
                                          reprojection_crs=config['jose_crs_str'])

    with rasterio.open(tmp_file, "w", **template_info['meta']) as dst:
        reproject(width=template_info['width'],
                  height=template_info['height'],
                  source=data,
                  destination=rasterio.band(dst, 1),
                  src_transform=meta['transform'],
                  src_crs=src_crs,
                  dst_transform=template_info['transform'],
                  dst_crs=config['jose_crs_str'],
                  resampling=Resampling.nearest,
                  nodata=meta['nodata'])

    project_raster(rast1=tmp_file,
                   template_file='data/rasters/all_habitats.tif',
                   output_file=out_file)


def project_raster(rast1, template_file, output_file):
    inputfile = rast1
    input = gdal.Open(rast1, gdalconst.GA_ReadOnly)
    input_proj = input.GetProjection()
    input_trans = input.GetGeoTransform()

    reference = gdal.Open(template_file, gdalconst.GA_ReadOnly)
    reference_proj = reference.GetProjection()
    reference_trans = reference.GetGeoTransform()
    band_reference = reference.GetRasterBand(1)
    x = reference.RasterXSize
    y = reference.RasterYSize

    driver = gdal.GetDriverByName('GTiff')
    output = driver.Create(output_file, x, y, 1, band_reference.DataType)
    output.SetGeoTransform(reference_trans)
    output.SetProjection(reference_proj)

    gdal.ReprojectImage(input, output, input_proj,
                        reference_proj, gdalconst.GRA_Bilinear)


def psql_str():
    return " |  psql -h " + config["host"] + " -p " + config["port"] + \
        " -U " + config['user'] + " -d " + config['database']


def create_colormap(min, max):
    colormap = [(124, 202, 247, 255),
                (122, 201, 247, 255),
                (121, 200, 247, 255),
                (120, 199, 247, 255),
                (119, 198, 247, 255),
                (117, 197, 247, 255),
                (116, 195, 247, 255),
                (115, 194, 248, 255),
                (113, 193, 248, 255),
                (112, 192, 248, 255),
                (111, 191, 248, 255),
                (110, 190, 248, 255),
                (108, 188, 248, 255),
                (107, 187, 248, 255),
                (106, 186, 249, 255),
                (104, 185, 249, 255),
                (103, 184, 249, 255),
                (102, 183, 249, 255),
                (101, 181, 249, 255),
                (99, 180, 249, 255),
                (98, 179, 249, 255),
                (97, 178, 250, 255),
                (96, 177, 250, 255),
                (94, 176, 250, 255),
                (93, 174, 250, 255),
                (92, 173, 250, 255),
                (90, 172, 250, 255),
                (89, 171, 250, 255),
                (88, 170, 251, 255),
                (87, 169, 251, 255),
                (85, 167, 251, 255),
                (84, 166, 251, 255),
                (83, 165, 251, 255),
                (81, 164, 251, 255),
                (80, 163, 251, 255),
                (79, 162, 252, 255),
                (78, 160, 252, 255),
                (76, 159, 252, 255),
                (75, 158, 252, 255),
                (74, 157, 252, 255),
                (73, 156, 252, 255),
                (71, 155, 252, 255),
                (70, 153, 253, 255),
                (69, 152, 253, 255),
                (67, 151, 253, 255),
                (66, 150, 253, 255),
                (65, 149, 253, 255),
                (64, 148, 253, 255),
                (62, 146, 254, 255),
                (61, 145, 254, 255),
                (60, 144, 254, 255),
                (58, 143, 254, 255),
                (57, 142, 254, 255),
                (56, 141, 254, 255),
                (55, 139, 254, 255),
                (53, 138, 255, 255),
                (52, 137, 255, 255),
                (51, 136, 255, 255),
                (50, 135, 255, 255),
                (48, 134, 255, 255),
                (47, 132, 255, 255),
                (46, 131, 255, 255),
                (44, 130, 255, 255),
                (43, 129, 255, 255),
                (44, 129, 254, 255),
                (48, 131, 251, 255),
                (51, 133, 248, 255),
                (54, 135, 245, 255),
                (58, 137, 241, 255),
                (61, 139, 238, 255),
                (64, 141, 235, 255),
                (68, 143, 232, 255),
                (71, 145, 228, 255),
                (74, 147, 225, 255),
                (78, 149, 222, 255),
                (81, 151, 219, 255),
                (84, 153, 216, 255),
                (88, 155, 212, 255),
                (91, 157, 209, 255),
                (95, 159, 206, 255),
                (98, 161, 203, 255),
                (101, 163, 200, 255),
                (105, 165, 196, 255),
                (108, 167, 193, 255),
                (111, 169, 190, 255),
                (115, 171, 187, 255),
                (118, 173, 183, 255),
                (121, 175, 180, 255),
                (125, 177, 177, 255),
                (128, 179, 174, 255),
                (132, 181, 171, 255),
                (135, 183, 167, 255),
                (138, 185, 164, 255),
                (142, 188, 161, 255),
                (145, 190, 158, 255),
                (148, 192, 155, 255),
                (152, 194, 151, 255),
                (155, 196, 148, 255),
                (158, 198, 145, 255),
                (162, 200, 142, 255),
                (165, 202, 139, 255),
                (168, 204, 135, 255),
                (172, 206, 132, 255),
                (175, 208, 129, 255),
                (179, 210, 126, 255),
                (182, 212, 122, 255),
                (185, 214, 119, 255),
                (189, 216, 116, 255),
                (192, 218, 113, 255),
                (195, 220, 110, 255),
                (199, 222, 106, 255),
                (202, 224, 103, 255),
                (205, 226, 100, 255),
                (209, 228, 97, 255),
                (212, 230, 94, 255),
                (215, 232, 90, 255),
                (219, 234, 87, 255),
                (222, 236, 84, 255),
                (226, 238, 81, 255),
                (229, 240, 77, 255),
                (232, 242, 74, 255),
                (236, 244, 71, 255),
                (239, 246, 68, 255),
                (242, 248, 65, 255),
                (246, 250, 61, 255),
                (249, 252, 58, 255),
                (252, 254, 55, 255),
                (255, 255, 52, 255),
                (255, 254, 53, 255),
                (255, 252, 54, 255),
                (255, 250, 55, 255),
                (255, 248, 56, 255),
                (255, 246, 57, 255),
                (255, 244, 59, 255),
                (255, 241, 60, 255),
                (255, 239, 61, 255),
                (255, 237, 62, 255),
                (255, 235, 63, 255),
                (255, 233, 64, 255),
                (255, 231, 65, 255),
                (255, 229, 66, 255),
                (255, 227, 68, 255),
                (255, 225, 69, 255),
                (255, 223, 70, 255),
                (255, 221, 71, 255),
                (255, 219, 72, 255),
                (255, 217, 73, 255),
                (255, 215, 74, 255),
                (255, 213, 76, 255),
                (255, 211, 77, 255),
                (255, 209, 78, 255),
                (255, 207, 79, 255),
                (255, 205, 80, 255),
                (255, 203, 81, 255),
                (255, 201, 82, 255),
                (255, 199, 83, 255),
                (255, 196, 85, 255),
                (255, 194, 86, 255),
                (255, 192, 87, 255),
                (255, 190, 88, 255),
                (255, 188, 89, 255),
                (255, 186, 90, 255),
                (255, 184, 91, 255),
                (255, 182, 93, 255),
                (255, 180, 94, 255),
                (255, 178, 95, 255),
                (255, 176, 96, 255),
                (255, 174, 97, 255),
                (255, 172, 98, 255),
                (255, 170, 99, 255),
                (255, 168, 100, 255),
                (255, 166, 102, 255),
                (255, 164, 103, 255),
                (255, 162, 104, 255),
                (255, 160, 105, 255),
                (255, 158, 106, 255),
                (255, 156, 107, 255),
                (255, 154, 108, 255),
                (255, 152, 109, 255),
                (255, 149, 111, 255),
                (255, 147, 112, 255),
                (255, 145, 113, 255),
                (255, 143, 114, 255),
                (255, 141, 115, 255),
                (255, 139, 116, 255),
                (255, 137, 117, 255),
                (255, 135, 119, 255),
                (255, 133, 120, 255),
                (255, 131, 121, 255),
                (255, 129, 122, 255),
                (255, 127, 123, 255),
                (255, 125, 123, 255),
                (255, 124, 121, 255),
                (255, 122, 119, 255),
                (255, 120, 117, 255),
                (255, 119, 115, 255),
                (255, 117, 113, 255),
                (255, 116, 111, 255),
                (255, 114, 109, 255),
                (255, 112, 107, 255),
                (255, 111, 105, 255),
                (255, 109, 103, 255),
                (255, 108, 101, 255),
                (255, 106, 99, 255),
                (255, 104, 97, 255),
                (255, 103, 95, 255),
                (255, 101, 93, 255),
                (255, 100, 92, 255),
                (255, 98, 90, 255),
                (255, 96, 88, 255),
                (255, 95, 86, 255),
                (255, 93, 84, 255),
                (255, 92, 82, 255),
                (255, 90, 80, 255),
                (255, 88, 78, 255),
                (255, 87, 76, 255),
                (255, 85, 74, 255),
                (255, 84, 72, 255),
                (255, 82, 70, 255),
                (255, 81, 68, 255),
                (255, 79, 66, 255),
                (255, 77, 64, 255),
                (255, 76, 62, 255),
                (255, 74, 60, 255),
                (255, 73, 58, 255),
                (255, 71, 56, 255),
                (255, 69, 55, 255),
                (255, 68, 53, 255),
                (255, 66, 51, 255),
                (255, 65, 49, 255),
                (255, 63, 47, 255),
                (255, 61, 45, 255),
                (255, 60, 43, 255),
                (255, 58, 41, 255),
                (255, 57, 39, 255),
                (255, 55, 37, 255),
                (255, 53, 35, 255),
                (255, 52, 33, 255),
                (255, 50, 31, 255),
                (255, 49, 29, 255),
                (255, 47, 27, 255),
                (255, 45, 25, 255),
                (255, 44, 23, 255),
                (255, 42, 21, 255),
                (255, 41, 19, 255),
                (255, 39, 18, 255),
                (255, 37, 16, 255),
                (255, 36, 14, 255),
                (255, 34, 12, 255),
                (255, 33, 10, 255),
                (255, 31, 8, 255),
                (255, 30, 6, 255),
                (255, 28, 4, 255),
                (255, 26, 2, 255),
                (255, 25, 0, 255)]
    raster_colormap = {}
    for val in range(min, max):
        raster_colormap[val] = colormap[val]
    return raster_colormap


def get_layer_cumul_imp(eco_layer, strs_layer, senstivity_val):
    """
    DRY function.
    This is the essense of the cumulative impact function
    get the data for our eco system layers
    get the data for our pressure/stressor layers (should settle on one)
    multiple the eco layer by the pressure layer by the senstivity matrix score

    Keyword arguments:
    eco_layer -- the eco system layer
    strs_layer -- the pressure layer
    senstivity_val -- The sensitivity matrix score for the above 2 layers - how the pressure affects the eco system
    Return: updated matrix of cumulative impacts
    """

    eco = np.around(eco_layer, decimals=4)
    strs = np.around(strs_layer, decimals=4)
    return (np.multiply(eco, strs)) * senstivity_val


def cumul_impact(ecosys_list, sens_mat, stressors_list, nodata_val):
    print('Running cumulative impact function.....')
    cumul_impact = None
    meta = None
    for eco in ecosys_list:
        # Check if the eco system component exists in the sensitivity matrix.
        try:
            eco_row = sens_mat.loc[eco['label']]
        except ValueError as e:
            continue
        except KeyError as e:
            continue

        with rasterio.open(eco['path'], "r+") as ecosrc:
            if meta is None:
                meta = ecosrc.meta.copy()
                meta.update(nodata=0)
            eco_data = ecosrc.read(1)

        for strs in stressors_list:
            try:
                sens_score = eco_row.loc[strs['label']]
            except ValueError as e:
                continue
            except KeyError as e:
                continue
            if sens_score == 0:
                continue
            else:
                with rasterio.open(strs['path'], 'r+') as strsrc:
                    strs_data = strsrc.read(1)
                    multi = get_layer_cumul_imp(eco_data,
                                                strs_data,
                                                sens_score)
                    if cumul_impact is None:
                        cumul_impact = multi
                    else:
                        cumul_impact = np.add(cumul_impact, multi)
    return [cumul_impact, meta]


def uploadRasterToMapbox(filename, _name):
    service = Uploader(access_token=config['mbat'])
    formatted_file = change_to_8bit(filename)
    upload_resp = service.upload(formatted_file, _name)
    if 'id' in upload_resp.json().keys():
        return upload_resp.json()['id']
    else:
        print("Failed to get an upload ID from Mapbox")


def colorise_raster(file_name, outfile_name):
    with rasterio.Env():
        with rasterio.open(file_name) as src:
            shade = src.read(1)
            meta = src.meta

        colors = create_colormap(shade.min(), shade.max())
        with rasterio.open(outfile_name, 'w', **meta) as dst:
            dst.write(shade, indexes=1)
            dst.write_colormap(1, colors)
            cmap = dst.colormap(1)
    return outfile_name


def change_to_8bit(filename):
    mapbox_projection = reproject_raster(file=filename,
                                         output_folder='data/mapbox/',
                                         reprojection_crs='EPSG:3857')
    gdal_file = "data/mapbox/gdal.tif"
    outfile = "data/mapbox/mapbox.tif"
    subprocess.call(["gdal_translate",
                     "-of", "GTiff",
                     "-co", "COMPRESS=LZW",
                     "-co", "TILED=YES",
                     "-ot", "Byte",
                     "-scale", mapbox_projection, gdal_file])
    colorise_raster(gdal_file, outfile)
    return outfile


config = setup_environment()

wgs84 = CRS.from_proj4(config.get("wgs84_str"))
JOSE_CRS = CRS.from_proj4(config.get("jose_crs_str"))
WGS84_SHP = reproject_shape(
    filename=data_path + sep + 'data/shapefiles/ATLAS_CaseStudy_areas.shp',
    save_path=data_path + sep + 'data/shapefiles/case_study',
    reproject=config.get("wgs84_str")
)

engine = create_engine('postgresql+psycopg2://' +
                       config["user"] + ':' +
                       config["pass"]+'@' +
                       config["host"]+':' +
                       config["port"]+'/' +
                       config["database"])
