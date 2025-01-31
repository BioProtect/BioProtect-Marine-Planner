import asyncio
import ctypes
import datetime
import fnmatch
import glob
import io
import json
import logging
import os
import platform
import re
import shlex
import shutil
import signal
import subprocess
import sys
import time
import traceback
import urllib
import uuid
import webbrowser
import xml.etree.ElementTree as ET
import zipfile
from collections import OrderedDict
from subprocess import PIPE, CalledProcessError, Popen
from threading import Thread
from urllib import request
from urllib.parse import urlparse

import aiopg
import colorama
import geopandas as gpd
import numpy as np
import pandas as pd
import psutil
import psycopg2
import rasterio
import requests
import tornado.options
from classes.db_config import DBConfig
from classes.folder_path_config import get_folder_path_config
from classes.postgis_class import get_pg
from colorama import Back, Fore, Style
from functions.utils import (create_cost_from_impact, cumul_impact,
                             get_tif_list, pad_dict, project_raster, psql_str,
                             replace_chars, reproject_and_normalise_upload,
                             reproject_raster, reproject_raster_to_all_habs,
                             reproject_shape, wgs84)
from mapbox import Uploader, errors
from osgeo import ogr
from psycopg2 import sql
from rasterio.io import MemoryFile
from services.file_service import (add_parameter_to_file,
                                   check_zipped_shapefile, create_zipfile,
                                   delete_all_files,
                                   delete_records_in_text_file,
                                   delete_zipped_shapefile, get_dict_value,
                                   get_files_in_folder,
                                   get_key_values_from_file,
                                   get_output_filename, read_file, unzip_file,
                                   unzip_shapefile, update_file_parameters,
                                   write_df_to_file, write_to_file, zip_folder)
from services.project_service import (get_project_data,
                                      get_projects_for_feature,
                                      get_safe_project_name, set_folder_paths,
                                      write_csv)
from services.run_command_service import run_command
from services.service_error import ServicesError, raise_error
from services.user_service import (
    get_notifications_data, dismiss_notification, get_users, reset_notifications)
from handlers.base_handler import BaseHandler
from handlers.user_handler import UserHandler
from handlers.project_handler import ProjectHandler
from handlers.feature_handler import FeatureHandler
from handlers.websocket_handler import WebSocketHandler
from handlers.planning_unit_handler import PlanningUnitHandler
from sqlalchemy import create_engine, exc
from tornado import concurrent, gen, httpclient, queues
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.iostream import StreamClosedError
from tornado.log import LogFormatter
from tornado.platform.asyncio import AnyThreadEventLoopPolicy
from tornado.process import Subprocess
from tornado.web import HTTPError, StaticFileHandler
from tornado.websocket import WebSocketClosedError
from tornado.escape import json_decode
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from passlib.hash import bcrypt
import jwt
import os
from datetime import datetime, timezone, timedelta


####################################################################################################################################################################################################################################################################
# constant declarations
####################################################################################################################################################################################################################################################################

# SECURITY SETTINGS
PERMITTED_METHODS = ["getServerData", "testTornado",
                     "getProjectsWithGrids", "getAtlasLayers"]
"""REST services that do not need authentication/authorisation."""
ROLE_UNAUTHORISED_METHODS = {
    "ReadOnly": ["createProject", "upgradeProject", "getCountries", "createPlanningUnitGrid", "uploadTilesetToMapBox", "uploadFileToFolder", "uploadFile", "importPlanningUnitGrid", "createFeaturePreprocessingFileFromImport", "importFeatures", "updatePUFile", "updateSpecFile", "getMarxanLog", "PreprocessFeature", "preprocessPlanningUnits", "preprocessProtectedAreas", "runMarxan", "stopProcess", "testRoleAuthorisation", "getRunLogs", "clearRunLogs", "updateWDPA", "unzipShapefile", "getShapefileFieldnames", "runGapAnalysis", "importGBIFData", "deleteGapAnalysis", "shutdown", "addParameter", "resetDatabase", "cleanup", "exportProject", "importProject", 'updateCosts', 'deleteCost', 'runSQLFile'],
    "User": ["testRoleAuthorisation", "clearRunLogs", "updateWDPA", "shutdown", "addParameter", "resetDatabase", "cleanup", 'runSQLFile'],
    "Admin": []
}
"""Dict that controls access to REST services using role-based authentication. Add REST services that you want to lock down to specific roles - a class added to an array will make that method unavailable for that role"""
SERVER_VERSION = "v1.0.7"
GUEST_USERNAME = "guest"
NOT_AUTHENTICATED_ERROR = "Request could not be authenticated. No secure cookie found."
NO_REFERER_ERROR = "The request header does not specify a referer and this is required for CORS access."
# MAPBOX_USER = "blishten"
MAPBOX_USER = "craicerjack"
"""The default name for the Mapbox user account to store Vector tiles"""
# file prefixes
SOLUTION_FILE_PREFIX = "output_r"
MISSING_VALUES_FILE_PREFIX = "output_mv"
# export settings
EXPORT_F_SHP_FOLDER = "f_shps"
"""The name of the folder where feature shapefiles are exported to during a project export."""
EXPORT_PU_SHP_FOLDER = "pu_shps"
"""The name of the folder where planning grid shapefiles are exported to during a project export."""
EXPORT_F_METADATA = 'features.csv'
"""The name of the file that contains the feature metadata data during a project export."""
EXPORT_PU_METADATA = 'planning_grid.csv'
"""The name of the file that contains the planning grid metadata data during a project export."""
# gbif constants
GBIF_API_ROOT = "https://api.gbif.org/v1/"
"""The GBIF API root url"""
GBIF_CONCURRENCY = 10
"""How many concurrent download processes to do for GBIF."""
GBIF_PAGE_SIZE = 300
"""The page size for occurrence records for GBIF requests"""
GBIF_POINT_BUFFER_RADIUS = 1000
"""The radius in meters to buffer all lat/lng coordinates for GBIF occurrence data"""
GBIF_OCCURRENCE_LIMIT = 200000
"""From the GBIF docs here: https://www.gbif.org/developer/occurrence#search"""
UNIFORM_COST_NAME = "Equal area"
"""The name of the cost profile that is equal area."""
DOCS_ROOT = "https://docs.marxanweb.org/"
"""The url for the documentation root."""
SHUTDOWN_EVENT = tornado.locks.Event()
"""A Tornado event to allow it to exit gracefully."""
PING_INTERVAL = 30000
"""Interval between regular pings to keep a connection alive when using websockets."""
SHOW_START_LOG = True
"""To disable the start logging from unit tests."""
LOGGING_LEVEL = logging.INFO
"""Tornado logging level that controls what is logged to the console - options are logging.INFO, logging.DEBUG, logging.WARNING, logging.ERROR, logging.CRITICAL. All SQL statements can be logged by setting this to logging.DEBUG."""

# pdoc3 dict to whitelist private members for the documentation
__pdoc__ = {}
privateMembers = ['getGeometryType', 'add_parameter_to_file', 'check_zipped_shapefile', 'cleanup', 'clone_project', 'create_user', 'create_zipfile', 'delete_all_files', 'delete_archive_files', '_deleteFeature',  'delete_records_in_text_file', 'del_tileset', 'delete_zipped_shapefile', 'dismiss_notification',  'finish_feature_import', '_getAllProjects', 'get_dict_value', 'get_files_in_folder',   'get_key_value', 'get_keys', 'get_marxan_log', 'get_notifications_data', 'get_output_filename', 'get_pu_grids', 'get_project_data', 'get_projects_for_feature', 'get_projects_for_user', 'get_run_logs',
                  'get_safe_project_name', 'get_species_data', 'get_unique_feature_name', 'get_user_data', 'get_users', 'get_users_data', 'normalize_dataframe', 'pad_dict', '_preprocessProtectedAreas', 'puid_array_to_df', 'raise_error', 'read_file', '_reprocessProtectedAreas', 'reset_notifications', 'run_command', '_setCORS', 'set_folder_paths', 'set_global_vars', 'unzip_file', 'unzip_shapefile', 'update_dataframe', 'update_file_parameters', 'update_run_log', 'update_species_file', '_uploadTileset', 'upload_tileset_to_mapbox', 'validate_args', 'write_csv', 'write_to_file', 'write_df_to_file', 'zip_folder']

for m in privateMembers:
    __pdoc__[m] = True


def log_server_info():
    """Logs server-related information."""
    log(f"Server {SERVER_VERSION} listening on port {
        db_config.SERVER_PORT} ..", Fore.GREEN)
    log(pad_dict("Operating system:", platform.system()))
    log(pad_dict("Tornado version:", tornado.version))
    log(pad_dict("Permitted domains:", ",".join(
        project_paths.PERMITTED_DOMAINS)))
    log(pad_dict("SSL certificate file:",
        project_paths.CERTFILE if project_paths.CERTFILE != "None" else "None"))
    log(pad_dict("Private key file:",
        project_paths.KEYFILE if project_paths.KEYFILE != "None" else "None"))
    log(pad_dict("Database:", db_config.CONNECTION_STRING))


def logClientInfo():
    """Logs information about the Marxan client."""

    global FRONTEND_BUILD_FOLDER
    global MARXAN_CLIENT_VERSION

    parent_folder = os.path.abspath(os.path.join(
        project_paths.PROJECT_FOLDER, os.pardir)) + os.sep
    package_json_path = os.path.join(parent_folder, "frontend/package.json")
    FRONTEND_BUILD_FOLDER = os.path.join(parent_folder, "frontend/build")
    MARXAN_CLIENT_VERSION = "Not installed"

    # Check if package.json exists and retrieve the version if it does
    if os.path.exists(package_json_path):
        with open(package_json_path) as f:
            MARXAN_CLIENT_VERSION = json.load(f).get('version', 'Unknown')
    log(f"frontend {MARXAN_CLIENT_VERSION} installed", Fore.GREEN)


def log_other_info():
    # get the database version
    GDAL_ENV_VAR = os.environ.get('GDAL_DATA', "Not set")
    # Determine if SSL is enabled based on the presence of CERTFILE
    if project_paths.CERTFILE is not None:
        log(pad_dict("SSL certificate file:", project_paths.CERTFILE))
        protocol = "https://"
    else:
        log(pad_dict("SSL certificate file:", "None"))
        protocol = "http://"

    # Construct the test URL
    host_part = "<host>"
    port_part = f":{
        db_config.SERVER_PORT}" if db_config.SERVER_PORT != '80' else ""
    test_path = "/server/testTornado"
    test_url = f"{protocol}{host_part}{port_part}{test_path}"

    log(pad_dict("PostgreSQL:", DB_V_POSTGRES))
    log(pad_dict("PostGIS:", DB_V_POSTGIS))
    log(pad_dict("Planning grid limit:", project_paths.PLANNING_GRID_UNITS_LIMIT))
    log(pad_dict("Disable security:", project_paths.DISABLE_SECURITY))
    log(pad_dict("Disable file logging:", project_paths.DISABLE_FILE_LOGGING))
    log(pad_dict("Python executable:", sys.executable))
    log(pad_dict("ogr2ogr executable:", db_config.OGR2OGR_EXECUTABLE))
    log(pad_dict("GDAL_DATA path:", GDAL_ENV_VAR))
    log(pad_dict("Marxan executable:", db_config.MARXAN_EXECUTABLE))
    log(f"To test server goto {test_url}", Fore.GREEN)
    log(db_config.STOP_CMD, Fore.RED)


####################################################################################################################################################################################################################################################################
# generic functions that dont belong to a class so can be called by subclasses of tornado.web.RequestHandler and tornado.websocket.WebSocketHandler equally - underscores are used so they dont mask the equivalent url endpoints
####################################################################################################################################################################################################################################################################
project_paths = None
db_config = None


async def set_global_vars():
    """set all of the global path variables"""
    global DB_V_POSTGRES
    global DB_V_POSTGIS
    global pg

    pg = await get_pg()

    results = await pg.execute("SELECT version(), PostGIS_Version();", return_format="Array")
    DB_V_POSTGRES = results[0]["version"]
    DB_V_POSTGIS = results[0]["postgis_version"]
    log_server_info()
    logClientInfo()
    # initialise colorama to be able to show log messages on windows in color
    colorama.init()
    # register numpy int64 with psycopg2
    psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)
    log_other_info()


def log(message, color=Fore.RESET):
    """Logs the string to the logging handlers using the passed colorama color

    Args:
        _str (string): The string to log
        _color (int): The color to use. The default is Fore.RESET.
    Returns:
        None
    """
    if SHOW_START_LOG:
        print(f"{color}{message}{Style.RESET_ALL}")
    if not project_paths.DISABLE_FILE_LOGGING:
        write_to_file(
            f"{project_paths.PROJECT_FOLDER}server.log", f"{message}\n", "a")


async def get_species_data(obj):
    """
    Retrieves species data for a project from the Marxan SPECNAME file as a DataFrame.
    Joins this data with the PostGIS database if the project is a Marxan Web project.
    Sets the resulting data on the `speciesData` attribute of the passed `obj`.

    Args:
        obj (BaseHandler): The request handler instance.

    Returns:
        None
    """
    # Load species data from the SPECNAME file
    specname_path = os.path.join(
        obj.folder_input, obj.projectData["files"]["SPECNAME"])
    df = file_data_to_df(specname_path)

    # Prepare DataFrame with `id` as the index
    output_df = df.set_index("id")
    output_df["unique_id"] = output_df.index

    # Fetch feature data from PostGIS
    feature_data = await pg.execute("SELECT * FROM marxan.get_features()", return_format="DataFrame")

    # Join PostGIS data with species data
    output_df = output_df.join(feature_data.set_index("unique_id"), how="left")

    # Rename columns to match client expectations
    output_df.rename(columns={
        "prop": "target_value",
        "unique_id": "id"
    }, inplace=True)

    # Convert target values from percentage (e.g., 0.17) to integer (e.g., 17)
    output_df["target_value"] = (output_df["target_value"] * 100).astype(int)

    # Assign the processed DataFrame to the `speciesData` attribute
    obj.speciesData = output_df


# get the information about which species have already been preprocessed
def file_data_to_df(file_name):
    """Reads a file and returns the data as a DataFrame

    Args:
        file_name (string): The name of the file to read.
    Returns:
        DataFrame: The data from the file.
    """
    return pd.read_csv(file_name, sep=None, engine='python') if os.path.exists(file_name) else pd.DataFrame()


async def get_pu_grids():
    """
    Retrieves the data for all planning grids.

    Returns:
        list[dict]: A list of dictionaries containing planning grid data.
    """
    query = """
        SELECT DISTINCT
            alias,
            feature_class_name,
            description,
            to_char(creation_date, 'DD/MM/YY HH24:MI:SS')::text AS creation_date,
            country_id,
            aoi_id,
            domain,
            _area,
            ST_AsText(envelope) AS envelope,
            pu.source,
            original_n AS country,
            created_by,
            tilesetid,
            planning_unit_count
        FROM marxan.metadata_planning_units pu
        LEFT OUTER JOIN marxan.gaul_2015_simplified_1km
        ON id_country = country_id
        ORDER BY alias;
    """
    return await pg.execute(query, return_format="Dict")


async def process_protected_areas(obj, planning_grid_name=None, folder=None):
    """
    Intersects planning grids with WDPA and processes the protected area intersections.

    Args:
        obj (BaseHandler): The request handler instance.
        planning_grid_name (str, optional): The name of the planning grid for preprocessing.
        folder (str, optional): The folder containing project data for reprocessing.

    Returns:
        list[str]: A list of reprocessed project folders (if folder is provided), otherwise None.
    """
    threshold = 0.5

    if folder:
        # Reprocess all projects in the specified folder
        project_folders = glob.glob(os.path.join(folder, "*/"))

        for project_folder in project_folders:
            tmp_obj = ExtendableObject()
            tmp_obj.project = "unimportant"
            tmp_obj.folder_project = os.path.join(
                os.path.dirname(project_folder), os.sep)

            # Load project metadata
            await get_project_data(pg, tmp_obj)

            # Get the planning grid name
            planning_grid_name = tmp_obj.projectData['metadata']['PLANNING_UNIT_NAME']

            # Notify client about preprocessing status
            obj.send_response({'status': "Preprocessing",
                              'info': f'Preprocessing {planning_grid_name}'})

            # Preprocess protected areas
            await process_protected_areas(obj, planning_grid_name=planning_grid_name, folder=os.path.join(tmp_obj.folder_project, 'input'))

        return project_folders

    elif planning_grid_name:
        # Preprocess for the given planning grid
        query = sql.SQL("""
            SELECT DISTINCT iucn_cat, grid.puid
            FROM marxan.wdpa, marxan.{} grid
            WHERE ST_Intersects(wdpa.geometry, grid.geometry)
              AND wdpaid IN (
                  SELECT wdpaid
                  FROM (
                      SELECT envelope
                      FROM marxan.metadata_planning_units
                      WHERE feature_class_name = %s
                  ) AS sub, marxan.wdpa
                  WHERE ST_Intersects(wdpa.geometry, envelope)
              )
            ORDER BY 1, 2
        """).format(sql.Identifier(planning_grid_name))

        intersection_data = await obj.executeQuery(
            query,
            data=[planning_grid_name],
            return_format="DataFrame"
        )

        # Save the intersection data to file
        output_file_path = os.path.join(
            folder, "protected_area_intersections.dat")
        intersection_data.to_csv(output_file_path, index=False)

    else:
        raise ValueError(
            "Either 'planning_grid_name' or 'folder' must be provided.")


# gets the marxan log after a run
def get_marxan_log(obj):
    """
    Retrieves the Marxan log from the log file after a run and sets it on the provided object.
    """
    log_file_path = os.path.join(obj.folder_output, "output_log.dat")
    obj.marxanLog = read_file(
        log_file_path) if os.path.exists(log_file_path) else ""


async def update_species_file(obj, interest_features, target_values, spf_values, create=False):
    """
    Updates or creates the SPECNAME file with the passed interest features.

    Args:
        obj (BaseHandler): The request handler instance.
        interest_features (str): A comma-separated string with the interest features.
        target_values (str): A comma-separated string with the corresponding interest feature targets.
        spf_values (str): A comma-separated string with the corresponding interest feature spf values.
        create (bool): If True, creates a new SPECNAME file. Default is False.

    Returns:
        None
    """
    # Parse input data
    ids = [int(s) for s in interest_features.split(",") if interest_features]
    props = [int(s) for s in target_values.split(",") if target_values]
    spfs = spf_values.split(",")

    removed_ids = []
    if not create:
        # Read existing SPECNAME data
        specname_path = os.path.join(
            obj.folder_input, obj.projectData["files"]["SPECNAME"])
        df = file_data_to_df(specname_path)

        # Determine removed IDs
        current_ids = [] if df.empty else df["id"].unique().tolist()
        removed_ids = list(set(current_ids) - set(ids))

        # Update related files to reflect removals
        if removed_ids:
            puvspr_filename = obj.projectData["files"]["PUVSPRNAME"]

            # Update PUVSPRNAME file
            puvspr_path = os.path.join(obj.folder_input, puvspr_filename)
            if os.path.exists(puvspr_path):
                delete_records_in_text_file(
                    puvspr_path, "species", removed_ids)

            # Update feature preprocessing file
            preprocessing_path = os.path.join(
                obj.folder_input, "feature_preprocessing.dat")
            if os.path.exists(preprocessing_path):
                delete_records_in_text_file(
                    preprocessing_path, "id", removed_ids)

    # Prepare records for the new or updated SPECNAME file
    records = [
        {"id": ids[i], "prop": str(props[i] / 100), "spf": spfs[i]}
        for i in range(len(ids)) if ids[i] not in removed_ids
    ]

    # Create a new DataFrame
    new_df = pd.DataFrame(records, columns=["id", "prop", "spf"])

    # Merge with existing data if updating
    if not create and not df.empty and set(df.columns) != {"id", "prop", "spf"}:
        df = df.drop(columns=["prop", "spf"], errors="ignore")
        new_df = pd.merge(df, new_df, on="id", how="outer").fillna("0")

    # Sort by ID and write to file
    if not new_df.empty:
        new_df = new_df.sort_values(by=["id"])

    await write_csv(obj, "SPECNAME", new_df)


def normalize_dataframe(df, column_to_normalize_by, puid_column_name, classes=None):
    """
    # sourcery skip: extract-method
    Converts a DataFrame with duplicate values into a normalized array.

    Args:
        df (pd.DataFrame): The DataFrame to normalize.
        column_to_normalize_by (str): The column in the DataFrame used to provide the headings for the normalized data (e.g., "Status" column produces 1,2,3).
        puid_column_name (str): The name of the planning grid unit ID column to create the array of values.
        classes (int, optional): Number of classes to classify the data into. Defaults to None.

    Returns:
        list: The normalized data from the DataFrame organized as a list of values (headings) each with a list of PUIDs, e.g., [32, 2374, 5867, 24967...].
    """
    if df.empty:
        return []

    if classes:
        # Calculate the range and bin size for classification
        min_value = df[column_to_normalize_by].min()
        max_value = df[column_to_normalize_by].max()

        # Handle case where all values in the column are the same
        num_classes = 1 if min_value == max_value else classes
        bin_size = (max_value + 1 - min_value) / num_classes

        # Initialize bins
        bins = [[min_value + bin_size * (i + 1), []]
                for i in range(num_classes)]

        # Classify rows into bins
        for idx, row in df.iterrows():
            bin_index = int(
                (row[column_to_normalize_by] - min_value) / bin_size)
            bins[bin_index][1].append(int(row[puid_column_name]))

        return bins, min_value, max_value

    # Group by unique values in the column and organize data
    groups = df.groupby(column_to_normalize_by)
    return [
        [group, group_df[puid_column_name].tolist()]
        for group, group_df in groups
        if group != 0
    ]


def validate_args(arguments, required_arguments):
    # sourcery skip: use-named-expression
    """
    Validates that all required arguments are present in the provided arguments dictionary.

    Args:
        arguments (dict): Dictionary of arguments (e.g., from a Tornado HTTP request).
        required_arguments (list[str]): List of required argument names.

    Returns:
        None

    Raises:
        ServicesError: If any required arguments are missing.
    """
    missing_arguments = [
        arg for arg in required_arguments if arg not in arguments]
    if missing_arguments:
        raise ServicesError(f"Missing input arguments: {
                            ', '.join(missing_arguments)}")


async def upload_tileset_to_mapbox(feature_class_name, mapbox_layer_name=None):
    """
    Exports a feature class to Mapbox as a new tileset.

    Args:
        feature_class_name (str): The name of the feature class to export, zip, and upload.
                                 Must exist in the PostGIS database.
        mapbox_layer_name (str, optional): The name of the Mapbox layer to be created (currently not used).

    Returns:
        str: The upload ID of the job, or "0" if the tileset already exists.
    """
    # Check if the tileset already exists
    url = f"https://api.mapbox.com/tilesets/v1/{MAPBOX_USER}.{
        feature_class_name}?access_token={project_paths.MBAT}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return "0"
    except requests.RequestException as e:
        # Log or handle the exception as needed
        print(f"Error checking tileset existence: {e}")
        return "0"

    # Export and zip the shapefile for uploading to Mapbox
    folder = project_paths.EXPORT_FOLDER
    await pg.exportToShapefile(folder, feature_class_name, tEpsgCode="EPSG:3857")
    zipfilename = create_zipfile(folder, feature_class_name)

    try:
        # Upload the tileset to Mapbox
        upload_id = upload_tileset(zipfilename, feature_class_name)
        return upload_id
    finally:
        # Clean up the temporary shapefile and zip file
        delete_zipped_shapefile(
            project_paths.EXPORT_FOLDER, f"{
                feature_class_name}.zip", feature_class_name
        )


def upload_tileset(filename, tileset_name):
    """
    Uploads a zip file to Mapbox as a new tileset using the Mapbox Uploads API.

    Args:
        filename (str): The full path of the zip file to upload.
        tileset_name (str): The name of the resulting tileset on Mapbox.

    Returns:
        str: The upload ID of the job.

    Raises:
        ServicesError: If the Mapbox Uploads API fails to return an upload ID.
    """
    # Initialize the Mapbox Uploader service
    service = Uploader(access_token=project_paths.MBAT)

    try:
        with open(filename, 'rb') as file:
            upload_response = service.upload(file, tileset_name)
            upload_data = upload_response.json()

            if 'id' in upload_data:
                return upload_data['id']
            else:
                raise ServicesError(
                    "Failed to retrieve an upload ID from Mapbox response.")
    except Exception as e:
        raise ServicesError(
            f"An error occurred during the upload process: {e}")


def del_tileset(tileset_id):
    """Deletes a tileset on Mapbox using the tilesets API.

    Args:
        tilesetid (string): The tileset to delete.
    Returns:
        None
    """
    url = f"https://api.mapbox.com/tilesets/v1/{MAPBOX_USER}.{
        tileset_id}?access_token={project_paths.MBAT}"
    response = requests.delete(url)
    if response.status_code != 204:
        raise ServicesError(f"Failed to delete tileset '{tileset_id}'. "f"Response: {
                            response.status_code} - {response.text}")


def get_unique_feature_name(prefix):
    # mapbox tileset ids are limited to 32 characters
    return prefix + uuid.uuid4().hex[:(32 - len(prefix))]


async def finish_feature_import(feature_class_name, name, description, source, user):
    """
    Finalizes the creation of a feature by adding a spatial index, setting up a primary key,
    and inserting a record into the metadata_interest_features table.

    Args:
        feature_class_name (str): The feature class to finish creating.
        name (str): The name of the feature class used as an alias in the metadata_interest_features table.
        description (str): The description for the feature class.
        source (str): The source for the feature.
        user (str): The user who created the feature.

    Returns:
        int: The ID of the feature created.

    Raises:
        ServicesError: If the feature already exists or other errors occur.
    """
    # get the Mapbox tilesetId
    tileset_id = f"{MAPBOX_USER}.{feature_class_name}"
    index_name = f"idx_{uuid.uuid4().hex}"

    # create an index on the geometry column
    await pg.execute(
        sql.SQL("CREATE INDEX {} ON marxan.{} USING GIST (geometry);")
        .format(sql.Identifier(index_name), sql.Identifier(feature_class_name))
    )

    # Add a primary key to the table
    try:
        await pg.execute(
            sql.SQL("ALTER TABLE marxan.{} DROP COLUMN IF EXISTS id, ogc_fid;")
            .format(sql.Identifier(feature_class_name))
        )
        await pg.execute(
            sql.SQL("ALTER TABLE marxan.{} ADD COLUMN id SERIAL PRIMARY KEY;")
            .format(sql.Identifier(feature_class_name))
        )
    except psycopg2.errors.InvalidTableDefinition as e:
        logging.warning(f"Primary key already exists for {
                        feature_class_name}: {e}")

    # Insert metadata for the feature
    try:
        geometry_type = await pg.getGeometryType(feature_class_name)

        if geometry_type != 'ST_Point':
            # Polygon layer: Calculate total area
            query = """
                INSERT INTO marxan.metadata_interest_features (
                    feature_class_name, alias, description, creation_date, _area, tilesetid, extent, source, created_by
                )
                SELECT %s, %s, %s, now(), sub._area, %s, sub.extent, %s, %s
                FROM (
                    SELECT ST_Area(ST_Transform(geom, 3410)) AS _area, box2d(geom) AS extent
                    FROM (
                        SELECT ST_Union(geometry) AS geom FROM marxan.{}
                    ) AS sub2
                ) AS sub
                RETURNING unique_id;
            """
        else:
            # Point layer: Calculate total amount
            query = """
                INSERT INTO marxan.metadata_interest_features (
                    feature_class_name, alias, description, creation_date, _area, tilesetid, extent, source, created_by
                )
                SELECT %s, %s, %s, now(), sub._area, %s, sub.extent, %s, %s
                FROM (
                    SELECT amount AS _area, box2d(combined) AS extent
                    FROM (
                        SELECT SUM(value) AS amount, ST_Collect(geometry) AS combined
                        FROM marxan.{}
                    ) AS sub2
                ) AS sub
                RETURNING unique_id;
            """

        feature_id = await pg.execute(
            sql.SQL(query).format(sql.Identifier(feature_class_name)),
            data=[feature_class_name, name,
                  description, tileset_id, source, user],
            return_format="Array"
        )

    except ServicesError as e:
        await pg.execute(sql.SQL("DROP TABLE IF EXISTS marxan.{};").format(sql.Identifier(feature_class_name)))
        if "Database integrity error" in e.args[0]:
            raise ServicesError(f"The feature '{name}' already exists.") from e
        raise ServicesError(e.args[0]) from e

    return feature_id[0]


def get_shapefile_fieldnames(shapefile):
    """
    Retrieves the field names from a shapefile.

    Args:
        shapefile (str): The full path to the shapefile (*.shp).

    Returns:
        list[str]: A list of the field names in the shapefile.

    Raises:
        ServicesError: If the shapefile does not exist or cannot be read.
    """
    # Ensure OGR exceptions are raised
    ogr.UseExceptions()

    try:
        # Open the shapefile
        data_source = ogr.Open(shapefile)
        if not data_source:
            raise ServicesError(
                f"Shapefile '{shapefile}' not found or could not be opened.")

        # Access the first layer
        layer = data_source.GetLayer(0)
        if not layer:
            raise ServicesError(f"No layers found in shapefile '{shapefile}'.")

        # Extract field names from the layer definition
        layer_definition = layer.GetLayerDefn()
        return [layer_definition.GetFieldDefn(i).GetName() for i in range(layer_definition.GetFieldCount())]

    except RuntimeError as e:
        raise ServicesError(f"Error reading shapefile '{
                            shapefile}': {e.args[0]}")


def _setCORS(obj):
    """Sets the CORS headers on the request to prevent CORS errors in the client.

    Args:
        obj (BaseHandler): The request handler instance.
    Returns:
        None
    Raises:
        ServicesError: If the request is not allowed to make cross-domain requests (based on the settings in the server.dat file).
    """
    # get the referer
    if "Referer" in list(obj.request.headers.keys()):
        # get the referer url, e.g. https://marxan-client-blishten.c9users.io/ or https://beta.biopama.org/marxan-client/build/
        referer = obj.request.headers.get("Referer")
        # get the origin
        parsed = urlparse(referer)
        origin = parsed.scheme + "://" + parsed.netloc
        # get the method
        method = obj.request.path.strip(
            "/").split("/")[-1] if obj.request.path else ""
        # check the origin is permitted either by being in the list of permitted domains or if the referer and host are on the same machine, i.e. not cross domain - OR if a permitted method is being called
        if (origin in project_paths.PERMITTED_DOMAINS) or (referer.find(obj.request.host_name) != -1) or (method in PERMITTED_METHODS):
            obj.set_header("Access-Control-Allow-Origin", origin)
            obj.set_header("Access-Control-Allow-Credentials", "true")
            obj.set_header("SameSite", "Lax")
        else:
            # , reason = "The origin '" + referer + "' does not have permission to access the service (CORS error)"
            raise HTTPError(403, "The origin '" + origin +
                            "' does not have permission to access the service (CORS error)")
    else:
        raise HTTPError(403, NO_REFERER_ERROR)


def get_run_logs():
    """Fetches the run logs and updates the number of completed runs for running projects.
    Returns:
        pd.DataFrame: The updated DataFrame with run logs.
    """
    # Load the data from the run log file
    df = file_data_to_df(os.path.join(
        project_paths.PROJECT_FOLDER, "runlog.dat"))
    if df.empty:
        return df  # Return immediately if the file is empty or missing

    def update_runs(row):
        """Helper function to update the 'runs' column for a single row."""
        # Construct the output folder path
        output_folder = os.path.join(
            project_paths.USERS_FOLDER,
            row['user'],
            row['project'],
            "output"
        )
        # Count the number of completed runs
        num_runs_completed = len(
            glob.glob(os.path.join(output_folder, "output_r*")))
        # Update the 'runs' value with the new count
        return f"{num_runs_completed}{row['runs'].split('/', 1)[-1]}"

    # Apply the update logic to rows where the status is 'Running'
    running_projects = df['status'] == 'Running'
    df.loc[running_projects, 'runs'] = df.loc[running_projects].apply(
        update_runs, axis=1)
    return df


def update_run_log(pid, start_time, runs_completed, runs_required, status):
    """
    Updates the run log with the details of a Marxan job when it has stopped for any reason.

    Args:
        pid (int): The process ID of the Marxan run.
        start_time (datetime): The time the run started.
        runs_completed (int): The number of runs that have completed.
        runs_required (int): The number of runs required.
        status (str): The status of the run (e.g., 'Stopped', 'Completed', 'Killed').

    Returns:
        str: The status of the run with the given pid.

    Raises:
        ServicesError: If unable to update the log file.
    """
    try:
        # Load the run log
        run_log_path = os.path.join(project_paths.PROJECT_FOLDER, "runlog.dat")
        run_log = get_run_logs()

        # Locate the index of the record to update
        record_index = run_log.loc[run_log['pid'] == pid].index[0]
    except (IndexError, FileNotFoundError, KeyError):
        raise ServicesError(f"Unable to update run log for pid {
                            pid} with status {status}.")
    else:
        # Update the record in place
        current_time = datetime.datetime.now()
        if start_time:
            run_log.at[record_index, 'endtime'] = current_time.strftime(
                "%d/%m/%y %H:%M:%S")
            run_log.at[record_index, 'runtime'] = f"{
                (current_time - start_time).seconds}s"

        if runs_completed is not None:
            run_log.at[record_index, 'runs'] = f"{
                runs_completed}/{runs_required}"

        # Update the status only if it is currently 'Running'
        if run_log.at[record_index, 'status'] == 'Running':
            run_log.at[record_index, 'status'] = status

        # Write the updated log back to the file
        run_log.to_csv(run_log_path, index=False, sep='\t')

        return run_log.at[record_index, 'status']


async def cleanup():
    """
    Performs maintenance tasks to remove orphaned tables, temporary tables,
    and outdated clumping projects from the server.

    Args:
        None

    Returns:
        None
    """
    # Database cleanup
    database_cleanup_queries = [
        "SELECT marxan.deletedissolvedwdpafeatureclasses()",
        "SELECT marxan.deleteorphanedfeatures()",
        "SELECT marxan.deletescratchfeatureclasses()"
    ]
    for query in database_cleanup_queries:
        await pg.execute(query)

    # File cleanup - Remove files older than 1 day in the clump folder
    clump_files = glob.glob(os.path.join(project_paths.CLUMP_FOLDER, "*"))
    one_day_ago = datetime.datetime.now() - datetime.timedelta(days=1)
    for file_path in clump_files:
        file_mod_time = datetime.datetime.fromtimestamp(
            os.path.getmtime(file_path))
        if file_mod_time < one_day_ago:
            os.remove(file_path)

    # Folder cleanup - Remove orphaned project folders
    users = get_users()
    for user in users:
        user_projects = glob.glob(os.path.join(
            project_paths.USERS_FOLDER, user, "*/"))
        for project_path in user_projects:
            # Remove project folder if it's empty
            if not os.listdir(project_path):
                shutil.rmtree(project_path)


####################################################################################################################################################################################################################################################################
# generic classes
####################################################################################################################################################################################################################################################################


class ServicesError(Exception):
    """Custom exception class for raising exceptions in this module.
    """

    def __init__(self, *args, **kwargs):
        super(ServicesError, self)


class ExtendableObject(object):
    """Custom class for allowing objects to be extended with new attributes.
    """
    pass

####################################################################################################################################################################################################################################################################
# subclass of Popen to allow registering callbacks when processes complete on Windows (tornado.process.Subprocess.set_exit_callback is not supported on Windows)
####################################################################################################################################################################################################################################################################


class BPSubProcess(Popen):
    """
    Subclass of Popen to allow registering callbacks when processes complete on Windows.
    This addresses the lack of `tornado.process.Subprocess.set_exit_callback` support on Windows.

    Args:
        See https://docs.python.org/3/library/subprocess.html#popen-constructor
    """

    def set_exit_callback_windows(self, callback, *args, **kwargs):
        """
        Registers a callback function on Windows by creating a separate thread
        to poll the process until it finishes.

        Args:
            callback (function): The function to call when the process completes.
            *args: Additional positional arguments for the callback.
            **kwargs: Additional keyword arguments for the callback.
        """
        # Create a thread to monitor the process and call the callback on completion
        self._thread = Thread(
            target=self.poll_completion,
            args=(callback, args, kwargs),
            daemon=True  # Ensures the thread doesn't block program exit
        )
        self._thread.start()

    def poll_completion(self, callback, args, kwargs):
        """
        Polls the subprocess to determine when it has finished.

        Args:
            callback (function): The function to call when the process completes.
            args (tuple): Positional arguments to pass to the callback.
            kwargs (dict): Keyword arguments to pass to the callback.
        """
        # Poll the process at regular intervals until it finishes
        while self.poll() is None:
            time.sleep(1)  # Sleep for 1 second to reduce CPU usage

        # Call the callback with the process return code and any additional arguments
        callback(self.returncode, *args, **kwargs)

        # Clean up the thread reference
        self._thread = None


####################################################################################################################################################################################################################################################################
# RequestHandler subclasses
####################################################################################################################################################################################################################################################################


class methodNotFound(BaseHandler):
    """
    REST HTTP handler invoked when the REST service method does not match any defined handlers.
    """

    def prepare(self):
        """
        Overrides the `prepare` method to handle cases where a requested method is not found.
        """
        print("Method not found")
        error_message = "The method is not supported or the parameters are incorrect on this server."

        if 'Upgrade' in self.request.headers:
            # Handle unsupported WebSocket method
            raise tornado.web.HTTPError(501, reason=error_message)
        else:
            # Handle unsupported GET/POST method
            raise_error(self, error_message)


class AuthHandler(BaseHandler):

    async def post(self):
        try:
            # comment:
            body = json_decode(self.request.body)
            username = body.get("user")
            pwd = body.get("pwd")

            if not username or not pwd:
                self.set_status(400)
                self.write({"message": "Username and password required"})
                return

            # Query user from PostgreSQL
            query = """
                SELECT id, username, password_hash, role, last_project, show_popup, basemap, use_feature_colours, report_units, refresh_tokens 
                FROM users WHERE username = $1
            """
            result = await pg.execute(query, [username], return_format="Dict")
            notifications = get_notifications_data(self)

            if not result:
                self.set_status(401)
                self.write({"message": "Unauthorized."})
                return

            user = result[0]

            # Verify password
            if not bcrypt.verify(pwd, user["password_hash"]):
                self.set_status(401)
                self.write({"message": "Unauthorized."})
                return

            # Remove expired refresh tokens
            now = datetime.now()
            valid_refresh_tokens = []
            for token in user["refresh_tokens"] or []:
                try:
                    decoded_token = jwt.decode(token, self.proj_paths.gis_config.get(
                        "refresh_token"), algorithms=["HS256"])
                    if datetime.fromtimestamp(decoded_token["exp"]) > now:
                        valid_refresh_tokens.append(token)
                except jwt.ExpiredSignatureError:
                    continue  # Ignore expired tokens
                except jwt.InvalidTokenError:
                    continue  # Ignore invalid tokens

            # Generate tokens
            access_token = jwt.encode({
                "UserInfo": {"username": user["username"], "role": user["role"] or ""},
                "exp": now + timedelta(seconds=10),
            }, project_paths.gis_config.get("access_token"),  algorithm="HS256")

            refresh_token = jwt.encode({
                "username": user["username"],
                "exp": now + timedelta(seconds=15),
            }, project_paths.gis_config.get("refresh_token"), algorithm="HS256")

            valid_refresh_tokens.append(refresh_token)

            # Update refresh tokens in the database
            update_query = "UPDATE users SET refresh_tokens = $1 WHERE id = $2"
            await pg.execute(update_query, [valid_refresh_tokens, user["id"]])

            # Set secure cookie for refresh token
            self.set_cookie("jwt", refresh_token, httponly=True,
                            secure=True, samesite="None")

            # Remove sensitive fields before sending user data
            user.pop("password_hash")
            user.pop("refresh_tokens")

            # Respond with access token and user data
            self.write({
                "userId": user['id'],
                "accessToken": access_token,
                "userData": user,
                # Send user data along with authentication
                "dismissedNotification": notifications
            })
        except ServicesError as e:
            raise_error(self, e.args[0])

        # end try


class getCountries(BaseHandler):
    """REST HTTP handler. Gets a list of countries. The required arguments in the request.arguments parameter are:

    Args:
        None
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "records": dict[]: The country records. Each dict contains the keys: iso3, name_iso31, has_marine
        }
    """

    async def get(self):
        try:
            content = await pg.execute("SELECT DISTINCT (t.name_iso31), t.iso3, CASE WHEN m.iso3 IS NULL THEN False ELSE True END has_marine FROM marxan.gaul_2015_simplified_1km t LEFT JOIN marxan.eez_simplified_1km m on t.iso3 = m.iso3 WHERE t.iso3 NOT LIKE '%|%' ORDER BY t.name_iso31;", return_format="Dict")
            self.send_response({'records': content})
        except ServicesError as e:
            raise_error(self, e.args[0])


class getPlanningUnitGrids(BaseHandler):
    """REST HTTP handler. Gets all of the planning grid data. The required arguments in the request.arguments parameter are:

    Args:
        None
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message,
            "planning_unit_grids": dict[]: The data for the planning grids. Each dict contains the keys: alias,aoi_id,country,country_id,created_by,creation_date,description,domain,envelope,feature_class_name,planning_unit_count,source,tilesetid,_area
        }
    """

    async def get(self):
        try:
            planningUnitGrids = await get_pu_grids()
            self.send_response(
                {'info': 'Planning unit grids retrieved',
                 'planning_unit_grids': planningUnitGrids})
        except ServicesError as e:
            raise_error(self, e.args[0])


class ImportPlanningUnitGrid(BaseHandler):
    """
    REST HTTP handler to import a zipped planning grid shapefile into PostGIS as a planning unit grid feature class.

    Required Arguments:
        filename (str): The name of the zipped shapefile to import (not the full path).
        name (str): The name of the planning grid to use as the alias in the metadata_planning_units table.
        description (str): The description for the planning grid.

    Returns:
        dict: Contains the feature_class_name, Mapbox uploadId, and alias for the imported feature class.
    """

    async def get(self):
        try:
            # Validate input arguments
            validate_args(self.request.arguments, [
                          'filename', 'name', 'description'])
            filename = self.get_argument('filename')
            name = self.get_argument('name')
            description = self.get_argument('description')
            user = self.get_current_user()

            # Unzip the shapefile asynchronously
            root_filename = await asyncio.get_running_loop().run_in_executor(
                None, unzip_shapefile, project_paths.IMPORT_FOLDER, filename
            )

            # Generate a unique feature class name
            feature_class_name = get_unique_feature_name("pu_")
            tileset_id = f"{MAPBOX_USER}.{feature_class_name}"
            shapefile_path = os.path.join(
                project_paths.IMPORT_FOLDER, f"{root_filename}.shp")

            try:
                # Validate the shapefile
                check_zipped_shapefile(shapefile_path)
                fieldnames = get_shapefile_fieldnames(shapefile_path)
                if "PUID" in fieldnames:
                    raise ServicesError(
                        "The field 'puid' in the shapefile must be lowercase.")

                # Insert metadata for the planning unit grid
                await pg.execute(
                    """
                    INSERT INTO marxan.metadata_planning_units(
                        feature_class_name, alias, description, creation_date, source, created_by, tilesetid
                    ) VALUES (%s, %s, %s, now(), 'Imported from shapefile', %s, %s);
                    """,
                    [feature_class_name, name, description, user, tileset_id]
                )

                # Import the shapefile into PostGIS
                await pg.importShapefile(project_paths.IMPORT_FOLDER, f"{root_filename}.shp", feature_class_name)

                # Validate and process the geometry
                await pg.isValid(feature_class_name)
                await pg.execute(
                    sql.SQL("ALTER TABLE marxan.{} ALTER COLUMN puid TYPE integer;")
                    .format(sql.Identifier(feature_class_name))
                )

                # Update metadata: envelope and planning unit count
                await pg.execute(
                    sql.SQL(
                        """
                        UPDATE marxan.metadata_planning_units
                        SET envelope = (
                            SELECT ST_Transform(ST_Envelope(ST_Collect(geometry)), 4326)
                            FROM marxan.{}
                        )
                        WHERE feature_class_name = %s;
                        """
                    ).format(sql.Identifier(feature_class_name)),
                    [feature_class_name]
                )
                await pg.execute(
                    sql.SQL(
                        """
                        UPDATE marxan.metadata_planning_units
                        SET planning_unit_count = (
                            SELECT COUNT(puid)
                            FROM marxan.{}
                        )
                        WHERE feature_class_name = %s;
                        """
                    ).format(sql.Identifier(feature_class_name)),
                    [feature_class_name]
                )

                # Upload the shapefile to Mapbox
                upload_id = upload_tileset(shapefile_path, feature_class_name)

            except ServicesError as e:
                # Handle specific errors related to the shapefile or constraints
                if all(keyword in e.args[0] for keyword in ['column', 'puid', 'does not exist']):
                    raise ServicesError(
                        "The field 'puid' does not exist in the shapefile.") from e
                if 'violates unique constraint' in e.args[0]:
                    raise ServicesError(f"The planning grid '{
                                        name}' already exists.") from e
                raise
            finally:
                # Cleanup: delete the shapefile and zip file
                await asyncio.get_running_loop().run_in_executor(
                    None, delete_zipped_shapefile, project_paths.IMPORT_FOLDER, filename, root_filename
                )

            # Prepare the response data
            response_data = {
                'feature_class_name': feature_class_name,
                'uploadId': upload_id,
                'alias': name
            }

            # Send the response
            self.send_response({
                'info': f"Planning grid '{name}' imported",
                **response_data
            })

        except ServicesError as e:
            raise_error(self, e.args[0])


class getAllSpeciesData(BaseHandler):
    """REST HTTP handler. Gets all species information from the PostGIS database. The required arguments in the request.arguments parameter are:

    Args:
        None
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message,
            "data": dict[]: A list of the features. Each dict contains the keys: id,feature_class_name,alias,description,area,extent,creation_date,tilesetid,source,created_by
        }
    """

    async def get(self):
        try:
            # get all the species data
            query = (
                "SELECT unique_id::integer AS id, feature_class_name, alias, description, "
                "_area AS area, extent, to_char(creation_date, 'DD/MM/YY HH24:MI:SS') AS creation_date, "
                "tilesetid, source, created_by "
                "FROM marxan.metadata_interest_features "
                "ORDER BY lower(alias);"
            )

            self.allSpeciesData = await pg.execute(query, return_format="DataFrame")
            # set the response
            self.send_response({"info": "All species data received",
                                "data": self.allSpeciesData.to_dict(orient="records")})
        except ServicesError as e:
            raise_error(self, e.args[0])


class getPlanningUnitsCostData(BaseHandler):
    """REST HTTP handler. Gets the planning units cost information from the PUNAME file. The required arguments in the request.arguments parameter are:

    Args:
        user (string): The name of the user.
        project (string): The name of the project.
    Returns:    
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "data": list[]: A list of puids that are in each of the 9 classes of cost (the cost data is classified into 9 classes),
            "min": The minimum cost value,
            "max": The maximum cost value
        }
    """

    async def get(self):
        try:
            # validate the input arguments
            print("/" * 100)
            validate_args(self.request.arguments, ['user', 'project'])
            set_folder_paths(self, self.request.arguments,
                             project_paths.USERS_FOLDER)
            # get the planning units cost information
            df = file_data_to_df(os.path.join(
                self.folder_input, self.projectData["files"]["PUNAME"]))
            # normalise the planning unit cost data to make the payload smaller
            data = normalize_dataframe(df, "cost", "id", 9)

            # set the response
            self.send_response({
                "data": data[0],
                'min': str(data[1]),
                'max': str(data[2])
            })
        except ServicesError as e:
            raise_error(self, e.args[0])


class updateCosts(BaseHandler):
    """REST HTTP handler. Updates a projects costs in the PUNAME file using the named cost profile. The required arguments in the request.arguments parameter are:

    Args:
        user (string): The name of the user.
        project (string): The name of the project.
        costname (string): The name of the cost profile to use (i.e. the *.cost file).
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message
        }
    """

    async def get(self):
        try:
            # validate the input arguments
            validate_args(self.request.arguments, [
                'user', 'project', 'costname'])
            # update the costs
            costname = self.get_argument("costname")
            cost_file_path = os.path.join(
                self.folder_input, f"{costname}.cost")
            puname_file_path = os.path.join(
                self.folder_input, self.projectData["files"]["PUNAME"])

            # Load the PUNAME file into a DataFrame
            puname_df = file_data_to_df(puname_file_path)

            if costname == UNIFORM_COST_NAME:
                # Apply a uniform cost of 1 to all entries
                puname_df['cost'] = 1
            else:
                # Verify the existence of the specified cost file
                if not os.path.exists(cost_file_path):
                    raise ServicesError(
                        f"The cost file '{costname}' does not exist.")
                # Load and merge cost data with PUNAME data
                cost_df = pd.read_csv(
                    cost_file_path, sep=None, engine='python')
                puname_df = cost_df.join(puname_df[['status']])

            # Update the input.dat file with the selected costname
            input_dat_path = os.path.join(self.folder_project, "input.dat")
            update_file_parameters(input_dat_path, {'COSTS': costname})

            # Save the updated PUNAME data
            await write_csv(self, "PUNAME", puname_df)

            # set the response
            self.send_response({"info": 'Costs updated'})
        except ServicesError as e:
            raise_error(self, e.args[0])


class deleteCost(BaseHandler):
    """REST HTTP handler. Deletes a cost profile. The required arguments in the request.arguments parameter are:

    Args:
        user (string): The name of the user.
        project (string): The name of the project.
        costname (string): The name of the cost profile to delete (i.e. the *.cost file).
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message
        }
    """

    def get(self):
        try:
            # validate the input arguments
            validate_args(self.request.arguments, [
                'user', 'project', 'costname'])
            # delete the cost
            costname = self.get_argument("costname")
            cost_file_path = os.path.join(
                self.folder_input, f"{costname}.cost")

            # Check if the cost file exists, and delete it if it does
            if not os.path.exists(cost_file_path):
                raise ServicesError(
                    f"The cost file '{costname}' does not exist.")

            os.remove(cost_file_path)
            # set the response
            self.send_response({"info": 'Cost deleted'})
        except ServicesError as e:
            raise_error(self, e.args[0])


class getMarxanLog(BaseHandler):
    """REST HTTP handler. Gets the Marxan log for the project. Currently not used. The required arguments in the request.arguments parameter are:

    Args:
        user (string): The name of the user.
        project (string): The name of the project.
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "log": The contents of the Marxan log
        }
    """

    def get(self):
        try:
            # validate the input arguments
            validate_args(self.request.arguments, ['user', 'project'])
            # get the log
            get_marxan_log(self)
            # set the response
            self.send_response({"log": self.marxanLog})
        except ServicesError as e:
            raise_error(self, e.args[0])


class GetSolution(BaseHandler):
    """
    REST HTTP handler to retrieve an individual solution.

    Required Arguments:
        user (str): The name of the user.
        project (str): The name of the project.
        solution (str): The solution ID to retrieve.

    Returns:
        dict: Contains:
            - "mv": List of data from the Marxan missing values file for the solution (output_mv*). Not returned for clumping projects.
            - "user": Name of the user.
            - "project": Name of the project.
            - "solution": The solution data retrieved.
        If an error occurs, the response includes an 'error' key with the error message.
    """

    def get(self):
        try:
            # Validate input arguments
            validate_args(self.request.arguments, [
                'user', 'project', 'solution'])
            user = self.get_argument("user")
            project = self.get_argument("project")
            solution_id = self.get_argument("solution")

            try:
                # Retrieve the solution file name
                file_name = get_output_filename(
                    os.path.join(self.folder_output, f"{
                                 SOLUTION_FILE_PREFIX}{int(solution_id):05d}")
                )
            except ServicesError as e:
                # Handle missing solution (likely a clumping project)
                self.solution = []
                if user != "_clumping":
                    raise ServicesError(f"Solution '{solution_id}' in project '{
                                        project}' no longer exists.") from e
            else:
                if os.path.exists(file_name):
                    # Load and normalize solution data
                    df = file_data_to_df(file_name)
                    self.solution = normalize_dataframe(
                        df, df.columns[1], df.columns[0])

            # Handle missing values file for non-clumping projects
            if user != '_clumping':
                mv_file_name = get_output_filename(
                    os.path.join(self.folder_output, f"{
                                 MISSING_VALUES_FILE_PREFIX}{int(solution_id):05d}")
                )
                solution_df = file_data_to_df(mv_file_name)
                self.missingValues = solution_df.to_dict(orient="split")[
                    "data"]

                # Send response with solution and missing values
                self.send_response({
                    'solution': self.solution,
                    'mv': self.missingValues,
                    'user': user,
                    'project': project
                })
            else:
                # Send response for clumping projects
                self.send_response({
                    'solution': self.solution,
                    'user': user,
                    'project': project
                })
        except ServicesError as e:
            # Handle and raise errors
            raise_error(self, e.args[0])


class getResults(BaseHandler):
    """REST HTTP handler. Gets the combined results for the project. This includes the Marxan log, the best solution, the output summary and summed solutions. The required arguments in the request.arguments parameter are:

    Args:
        user (string): The name of the user.
        project (string): The name of the project.
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message,
            "log": The Marxan log for the run,
            "mvbest": list[]: A list of records from the Marxan output_mvbest file,
            "summary": list[]: A list of records from the Marxan output_sum file,
            "ssoln": list[]: A list of records from the Marxan output_ssoln file
        }
    """

    def get(self):
        try:
            # validate the input arguments
            validate_args(self.request.arguments, ['user', 'project'])
            # get the log
            get_marxan_log(self)
            # get the best solution
            self.bestSolution = file_data_to_df(
                get_output_filename(self.folder_output, "output_mvbest"))
            # get the output sum
            self.outputSummary = file_data_to_df(
                get_output_filename(self.folder_output + "output_sum"))
            # get the summed solution
            summed_sol_df = file_data_to_df(
                get_output_filename(self.folder_output, "output_ssoln"))
            self.summedSolution = normalize_dataframe(
                summed_sol_df, "number", "planning_unit")
            # set the response
            self.send_response({'info': 'Results loaded', 'log': self.marxanLog,
                                'mvbest': self.bestSolution.to_dict(orient="split")["data"],
                                'summary': self.outputSummary.to_dict(orient="split")["data"],
                                'ssoln': self.summedSolution})
        except (ServicesError):
            self.send_response({'info': 'No results available'})


class getServerData(BaseHandler):
    """REST HTTP handler. Gets the server configuration data from the server.dat file as an abject. The required arguments in the request.arguments parameter are:

    Args:
        None
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):
    """

    def get(self):
        try:
            # get the number of processors
            # get the virtual memory
            memory_gb = psutil.virtual_memory().total / (1024 ** 3)  # Convert bytes to GB
            memory = f"{memory_gb:.1f} Gb"

            # Update server data with system information
            self.server_data = {
                "RAM": memory,
                "PROCESSOR_COUNT": psutil.cpu_count(),
                "DATABASE_VERSION_POSTGIS": DB_V_POSTGRES,
                "DATABASE_VERSION_POSTGRESQL": DB_V_POSTGIS,
                "SYSTEM": platform.system(),
                "NODE": platform.node(),
                "RELEASE": platform.release(),
                "VERSION": platform.version(),
                "MACHINE": platform.machine(),
                "PROCESSOR": platform.processor(),
                "SERVER_VERSION": SERVER_VERSION,
                "MARXAN_CLIENT_VERSION": MARXAN_CLIENT_VERSION,
                "SERVER_NAME": db_config.SERVER_NAME,
                "SERVER_DESCRIPTION": db_config.SERVER_DESCRIPTION,
                "SERVER_PORT": db_config.SERVER_PORT,
                "ENABLE_RESET": project_paths.ENABLE_RESET,
                "PERMITTED_DOMAINS": project_paths.PERMITTED_DOMAINS,
                "CERTFILE": project_paths.CERTFILE,
                "KEYFILE": project_paths.KEYFILE,
                "PLANNING_GRID_UNITS_LIMIT": project_paths.PLANNING_GRID_UNITS_LIMIT,
                "DISABLE_SECURITY": project_paths.DISABLE_SECURITY,
                "DISABLE_FILE_LOGGING": project_paths.DISABLE_FILE_LOGGING,
                "WDPA_VERSION": project_paths.WDPA_VERSION,
                "DISK_FREE_SPACE": memory,
            }

            # get any shutdown timeouts if they have been set
            shutdownTime = read_file(project_paths.PROJECT_FOLDER + "shutdown.dat") if (
                os.path.exists(project_paths.PROJECT_FOLDER + "shutdown.dat")) else None
            if shutdownTime:
                self.server_data.update({'SHUTDOWNTIME': shutdownTime})
            # set the response
            self.send_response(
                {'info': 'Server data loaded', 'serverData': self.server_data})
        except ServicesError as e:
            raise_error(self, e.args[0])


class updateSpecFile(BaseHandler):
    """REST HTTP handler. Updates the SPECNAME file with the posted data. The required arguments in the request.arguments parameter are:

    Args:
        user (string): The name of the user.
        project (string): The name of the project.
        interest_features (string): A comma-separated string with the interest features.
        target_values (string): A comma-separated string with the corresponding interest feature targets.
        spf_values (string): A comma-separated string with the corresponding interest feature spf values.
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message
        }
    """

    async def post(self):
        try:
            # validate the input arguments
            validate_args(self.request.arguments, [
                'user', 'project', 'interest_features', 'spf_values', 'target_values'])
            # update the spec.dat file and other related files
            await update_species_file(self, self.get_argument("interest_features"), self.get_argument("target_values"), self.get_argument("spf_values"))
            # set the response
            self.send_response({'info': "spec.dat file updated"})
        except ServicesError as e:
            raise_error(self, e.args[0])


class UpdatePUFile(BaseHandler):
    """
    REST HTTP handler. Updates the pu.dat file with the posted data.

    Required Arguments:
        user (str): The name of the user.
        project (str): The name of the project.
        status1_ids (list[int]): Array of planning grid units that have a status of 1.
        status2_ids (list[int]): Array of planning grid units that have a status of 2.
        status3_ids (list[int]): Array of planning grid units that have a status of 3.

    Returns:
        dict: Contains an "info" key with an informational message.
        If an error occurs, the response includes an 'error' key with the error message.
    """

    @staticmethod
    def create_status_dataframe(puid_array, pu_status):
        """
        Helper function to create a DataFrame for planning units and their statuses.

        Args:
            puid_array (list[int]): Array of planning unit IDs.
            pu_status (int): Status to assign to all IDs.

        Returns:
            pd.DataFrame: DataFrame with columns 'id' and 'status_new'.
        """
        return pd.DataFrame({
            'id': [int(puid) for puid in puid_array],
            'status_new': [pu_status] * len(puid_array)
        }, dtype='int64')

    @staticmethod
    def get_int_array_from_arg(arguments, arg_name):
        """
        Extracts an array of integers from the specified argument.

        Args:
            arguments (dict): Dictionary of request arguments.
            arg_name (str): Name of the argument to extract.

        Returns:
            list[int]: List of integers from the argument value.
        """
        return [
            int(s) for s in arguments.get(arg_name, [b""])[0].decode("utf-8").split(",")
        ] if arg_name in arguments else []

    async def post(self):
        try:
            # Validate input arguments
            validate_args(self.request.arguments, ['user', 'project'])

            # Get IDs for the different statuses
            status1_ids = self.get_int_array_from_arg(
                self.request.arguments, "status1")
            status2_ids = self.get_int_array_from_arg(
                self.request.arguments, "status2")
            status3_ids = self.get_int_array_from_arg(
                self.request.arguments, "status3")

            # Create DataFrames for each status group
            status1 = self.create_status_dataframe(status1_ids, 1)
            status2 = self.create_status_dataframe(status2_ids, 2)
            status3 = self.create_status_dataframe(status3_ids, 3)

            # Read the data from the PUNAME file
            pu_file_path = os.path.join(
                self.folder_input, self.projectData["files"]["PUNAME"]
            )
            df = file_data_to_df(pu_file_path)

            # Reset the status for all planning units
            df['status'] = 0

            # Combine status DataFrames and merge with the original
            status_updates = pd.concat([status1, status2, status3])
            df = df.merge(status_updates, on='id', how='left')

            # Update the status column
            df['status'] = df['status_new'].fillna(df['status']).astype('int')

            # Drop the intermediate column and ensure data types
            df = df.drop(columns=['status_new'])
            df = df.astype({'id': 'int64', 'cost': 'int64', 'status': 'int64'})

            # Sort the DataFrame by 'id'
            df = df.sort_values(by='id')

            # Write the updated DataFrame back to the file
            await write_csv(self, "PUNAME", df)

            # Send response
            self.send_response({'info': "pu.dat file updated"})
        except ServicesError as e:
            raise_error(self, e.args[0])


class getPUData(BaseHandler):
    """REST HTTP handler. Gets the data for a planning unit including a set of features if there are some. The required arguments in the request.arguments parameter are:

    Args:
        user (string): The name of the user.
        project (string): The name of the project.
        puid (string): The planning unit id to get the data for.
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message,
            "data": dict containing the keys: features (the features within the planning unit), pu_data (the planning unit data)
        }
    """

    async def get(self):
        try:
            # validate the input arguments
            validate_args(self.request.arguments, [
                'user', 'project', 'puid'])
            # get the planning unit data
            pu_df = file_data_to_df(os.path.join(
                self.folder_input, self.projectData["files"]["PUNAME"]))
            pu_data = pu_df.loc[pu_df['id'] == int(
                self.get_argument('puid'))].iloc[0]
            # get a set of feature IDs from the puvspr file
            df = file_data_to_df(os.path.join(
                self.folder_input, self.projectData["files"]["PUVSPRNAME"]))

            if not df.empty:
                features = df.loc[df['pu'] == int(self.get_argument('puid'))]
            else:
                features = pd.DataFrame()
            # set the response
            self.send_response({"info": 'Planning unit data returned', 'data': {
                               'features': features.to_dict(orient="records"), 'pu_data': pu_data.to_dict()}})
        except ServicesError as e:
            raise_error(self, e.args[0])


# not currently used
class createFeaturePreprocessingFileFromImport(BaseHandler):
    """REST HTTP handler. Used to populate the feature_preprocessing.dat file from an imported PUVSPR file. The required arguments in the request.arguments parameter are:

    Args:
        user (string): The name of the user.
        project (string): The name of the project.
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message
        }
    """

    async def get(self):
        try:
            # validate the input arguments
            validate_args(self.request.arguments, ['user', 'project'])
            # run the internal routine
            puvspr_path = os.path.join(
                self.folder_input, self.projectData["files"]["PUVSPRNAME"])
            df = file_data_to_df(puvspr_path)

            if df.empty:
                raise ServicesError(
                    "There are no records in the puvspr.dat file.")

            # Calculate statistics: sum and count for each species
            summary = df.pivot_table(
                index='species',
                aggfunc={'amount': ['sum', 'count']}
            ).reset_index()

            # Flatten the pivot table and rename columns
            summary.columns = ['species', 'pu_area', 'pu_count']
            summary['id'] = summary['species']

            # Reorder and clean up columns
            summary = summary[['id', 'pu_area', 'pu_count']]

            # Save the processed data to the feature_preprocessing.dat file
            feature_preprocessing_path = os.path.join(
                self.folder_input, "feature_preprocessing.dat")
            summary.to_csv(feature_preprocessing_path, index=False)

            # set the response
            self.send_response(
                {'info': "feature_preprocessing.dat file populated"})
        except ServicesError as e:
            raise_error(self, e.args[0])


class addParameter(BaseHandler):
    """REST HTTP handler. Creates a new parameter in a *.dat file, either the user (user.dat), project (project.dat) or server (server.dat), by iterating through all the files and adding the key/value if it doesnt already exist. The required arguments in the request.arguments parameter are:
    Args:
        type (string): The type of configuration file to add the parameter to. One of server, user or project.
        key (string): The key to create/update.
        value (string): The value to set.
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {"info": Informational message}
    """

    def get(self):
        try:
            # validate the input arguments - the type parameter is one of {'user','project'}
            validate_args(self.request.arguments, [
                'type', 'key', 'value'])
            # add the parameter
            results = add_parameter_to_file(self.get_argument('type'),
                                            self.get_argument('key'),
                                            self.get_argument('value'),
                                            project_paths.USERS_FOLDER,
                                            project_paths.PROJECT_FOLDER)
            # set the response
            self.send_response({'info': results})
        except ServicesError as e:
            raise_error(self, e.args[0])


class listProjectsForPlanningGrid(BaseHandler):
    """REST HTTP handler. Gets a list of all of the projects that a planning grid is used in. The required arguments in the request.arguments parameter are:

    Args:
        feature_class_id (string): The planning grid feature oid.
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message,
            "projects": dict[]: A list of the projects that the feature is in. Each dict contains the keys: user, name
        }
    """

    def get(self):
        try:
            # validate the input arguments
            validate_args(self.request.arguments, ['feature_class_name'])
            # get the projects which contain the planning grid
            projects = get_projects_for_planning_grid(
                self.get_argument('feature_class_name'))
            # set the response for uploading to mapbox
            self.send_response(
                {'info': "Projects info returned", "projects": projects})
        except ServicesError as e:
            raise_error(self, e.args[0])


class uploadTilesetToMapBox(BaseHandler):
    """REST HTTP handler. Uploads a feature class with the passed feature class name to MapBox as a tileset using the MapBox Uploads API. The required arguments in the request.arguments parameter are:

    Args:
        feature_class_name (string): The name of the feature class to upload.
        mapbox_layer_name (string): The name of the destination Mapbox tileset.
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message,
            "uploadid": The Mapbox tileset upload id
        }
    """

    async def get(self):
        try:
            # validate the input arguments
            validate_args(self.request.arguments, [
                'feature_class_name', 'mapbox_layer_name'])
            uploadId = await upload_tileset_to_mapbox(self.get_argument('feature_class_name'), self.get_argument('mapbox_layer_name'))
            # set the response for uploading to mapbox
            self.send_response({'info': "Tileset '" + self.get_argument(
                'feature_class_name') + "' uploading", 'uploadid': uploadId})
        except ServicesError as e:
            raise_error(self, e.args[0])


class uploadFileToFolder(BaseHandler):
    """REST HTTP handler. Uploads a file to a specific folder within the Marxan root folder. The required arguments in the request.arguments parameter are:

    Args:
        files(bytes): The file data to upload.
        filename (string): The name of the file to be uploaded.
        destFolder (string): The folder path on the server to upload the file to relative to the project_paths.PROJECT_FOLDER, e.g. export.
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message,
            "file": The name of the file that was uploaded
        }
    """

    def post(self):
        try:
            # validate the input arguments
            validate_args(self.request.arguments, [
                'filename', 'destFolder'])
            # write the file to the server
            write_to_file(project_paths.PROJECT_FOLDER + self.get_argument('destFolder') + os.sep +
                          self.get_argument('filename'), self.request.files['value'][0].body, 'wb')
            # set the response
            self.send_response({'info': "File '" + self.get_argument('filename') +
                                "' uploaded", 'file': self.get_argument('filename')})
        except ServicesError as e:
            raise_error(self, e.args[0])


class uploadFile(BaseHandler):
    """REST HTTP handler. Uploads a file to the Marxan users project folder. The required arguments in the request.arguments parameter are:

    Args:
        user (string): The name of the user.
        project (string): The name of the project.
        files(bytes): The file data to upload.
        filename (string): The name of the file to be uploaded.
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message
        }
    """

    def post(self):
        try:
            # validate the input arguments
            validate_args(self.request.arguments, [
                'user', 'project', 'filename'])
            # write the file to the server
            write_to_file(self.folder_project + self.get_argument('filename'),
                          self.request.files['value'][0].body, 'wb')
            # set the response
            self.send_response({'info': "File '" + self.get_argument('filename') +
                                "' uploaded", 'file': self.get_argument('filename')})
        except ServicesError as e:
            raise_error(self, e.args[0])


class unzipShapefile(BaseHandler):
    """REST HTTP handler. Unzips an already uploaded shapefile and returns the rootname. The required arguments in the request.arguments parameter are:

    Args:
        filename (string): The name of the zip file that will be unzipped in the project_paths.IMPORT_FOLDER.
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message,
            "rootfilename": The name of the shapefile unzipped (minus the .shp extension)
        }
    """

    async def get(self):
        try:
            # validate the input arguments
            validate_args(self.request.arguments, ['filename'])
            # write the file to the server
            rootfilename = await IOLoop.current().run_in_executor(None, unzip_shapefile, project_paths.IMPORT_FOLDER, self.get_argument('filename'))
            # set the response
            self.send_response({'info': "File '" + self.get_argument(
                'filename') + "' unzipped", 'rootfilename': rootfilename})
        except ServicesError as e:
            raise_error(self, e.args[0])


class getShapefileFieldnames(BaseHandler):
    """REST HTTP handler. Gets a field list from a shapefile. The required arguments in the request.arguments parameter are:

    Args:
        filename (string): The name of the shapefile (minus the *.shp extension).
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message,
            "fieldnames": string[]: A list of the field names
        }
    """

    def get(self):
        ogr.UseExceptions()
        try:
            # validate the input arguments
            validate_args(self.request.arguments, ['filename'])
            # get the field list
            shapefile = project_paths.IMPORT_FOLDER + \
                self.get_argument('filename')
            data_source = ogr.Open(shapefile)
            if not data_source:
                raise ServicesError(f"Shapefile '{shapefile}' not found")

            layer = data_source.GetLayer(0)
            layer_definition = layer.GetLayerDefn()
            fields = [layer_definition.GetFieldDefn(x).GetName(
            ) for x in range(layer_definition.GetFieldCount())]

            # set the response
            self.send_response(
                {'info': "Field list returned", 'fieldnames': fields})
        except ServicesError as e:
            raise_error(self, e.args[0])


class deleteShapefile(BaseHandler):
    """REST HTTP handler. Deletes a zipped shapefile and its unzipped files (if present). The required arguments in the request.arguments parameter are:

    Args:
        zipfile (string): The name of the zipped shapfile.
        shapefile (string): The root name of the shapefile - this will be used to match the unzipped files in the folder and delete them.
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message
        }
    """

    def get(self):
        try:
            # validate the input arguments
            validate_args(self.request.arguments, [
                'zipfile', 'shapefile'])
            delete_zipped_shapefile(project_paths.IMPORT_FOLDER, self.get_argument(
                'zipfile'), self.get_argument('shapefile')[:-4])
            # set the response
            self.send_response({'info': "Shapefile deleted"})
        except ServicesError as e:
            raise_error(self, e.args[0])


class stopProcess(BaseHandler):
    """REST HTTP handler. Kills a running process - this is either a Marxan run or a PostGIS query. The required arguments in the request.arguments parameter are:

    Args:
        pid (string): The process identifier.
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message
        }
    Raises:
        ServicesError: If the process does not exist.
    """

    async def get(self):
        try:
            # validate the input arguments
            validate_args(self.request.arguments, ['pid'])
            # get the pid from the pid request parameter - this will be an identifier (m=marxan run, q=query) followed by the pid, e.g. m1234 is a marxan run process with a pid of 1234
            pid = self.get_argument('pid')[1:]
            try:
                # if the process is a marxan run, then update the run log
                if (self.get_argument('pid')[:1] == 'm'):
                    # to distinguish between a process killed by the user and by the OS, we need to update the runlog.dat file to set this process as stopped and not killed
                    update_run_log(int(pid), None, None, None, 'Stopped')
                    # now kill the process
                    os.kill(int(pid), signal.SIGKILL)
                else:
                    # cancel the query
                    await pg.execute("SELECT pg_cancel_backend(%s);", [pid])
            except OSError:
                raise ServicesError("The pid does not exist")
            except PermissionError:
                raise ServicesError(
                    "Unable to stop process: PermissionDenied")
            else:
                self.send_response({'info': "pid '" + pid + "' terminated"})
        except ServicesError as e:
            raise_error(self, e.args[0])


class getRunLogs(BaseHandler):
    """REST HTTP handler. Gets the run log. The required arguments in the request.arguments parameter are:

    Args:
        None
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message,
            "data": dict[]: A list of runs. Each dict contains the keys: endtime,pid,project,runs,runtime,starttime,status,user
        }
    """

    def get(self):
        try:
            runlog = get_run_logs()
            self.send_response({'info': "Run log returned",
                                'data': runlog.to_dict(orient="records")})
        except ServicesError as e:
            raise_error(self, e.args[0])


class clearRunLogs(BaseHandler):
    """REST HTTP handler. Clears the run log. The required arguments in the request.arguments parameter are:

    Args:
        None
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message
        }
    """

    def get(self):
        try:
            runlog = get_run_logs()
            runlog.loc[runlog['pid'] == -
                       1].to_csv(project_paths.PROJECT_FOLDER + "runlog.dat", index=False, sep='\t')
            self.send_response({'info': "Run log cleared"})
        except ServicesError as e:
            raise_error(self, e.args[0])


class dismissNotification(BaseHandler):
    """REST HTTP handler. Appends the notificationid in the users "notifications.dat" to dismiss the notification. The required arguments in the request.arguments parameter are:

    Args:
        notificationid (string): The id of the notification to dismiss.
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message
        }
    """

    def get(self):
        try:
            # validate the input arguments
            validate_args(self.request.arguments, ['notificationid'])
            # dismiss the notification
            dismiss_notification(self, self.get_argument('notificationid'))
            self.send_response({'info': "Notification dismissed"})
        except ServicesError as e:
            raise_error(self, e.args[0])


class resetNotifications(BaseHandler):
    """REST HTTP handler. Resets all notification for the currently authenticated user by clearing the "notifications.dat". The required arguments in the request.arguments parameter are:

    Args:
        None
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message
        }
    """

    def get(self):
        try:
            # reset the notification
            reset_notifications(self)
            self.send_response({'info': "Notifications reset"})
        except ServicesError as e:
            raise_error(self, e.args[0])


class deleteGapAnalysis(BaseHandler):
    """REST HTTP handler. Deletes a pre-cooked gap analysis, for example when the features in a project change. The required arguments in the request.arguments parameter are:

    Args:
        user (string): The name of the user.
        project (string): The name of the project in which to delete the gap analysis.
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message
        }
    """

    async def get(self):
        try:
            validate_args(self.request.arguments, ['user', 'project'])
            # delete the gap analysis
            project_name = get_safe_project_name(self.get_argument("project"))
            table_name = "gap_" + \
                self.get_argument("user") + "_" + project_name
            await pg.execute(sql.SQL("DROP TABLE IF EXISTS marxan.{};").format(sql.Identifier(table_name.lower())))
            self.send_response({'info': "Gap analysis deleted"})
        except ServicesError as e:
            raise_error(self, e.args[0])


class testRoleAuthorisation(BaseHandler):
    """REST HTTP handler. For testing role access to servivces. The required arguments in the request.arguments parameter are:

    Args:
        None
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message
        }
    """

    def get(self):
        self.send_response({'info': "Service successful"})


class runSQLFile(BaseHandler):
    """REST HTTP handler. Runs an already uploaded sql script - only called from client applications. The required arguments in the request.arguments parameter are:

    Args:
        filename (string): The name of the sql file to run.
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message
        }
    """

    async def get(self):
        try:
            validate_args(self.request.arguments, ['filename'])
            # check the SQL file exists
            if not os.path.exists(project_paths.PROJECT_FOLDER + self.get_argument("filename")):
                raise ServicesError(
                    "File '" + self.get_argument("filename") + "' does not exist")
            # see if suppressOutput is set
            suppressOutput = True if 'suppressOutput' in self.request.arguments else False
            # set the command
            cmd = 'sudo -u postgres psql -f ' + project_paths.PROJECT_FOLDER + self.get_argument(
                "filename") + ' postgresql://' + db_config.DATABASE_USER + ':' + db_config.DATABASE_PASSWORD + '@localhost:5432/marxanserver'
            # run the command
            result = await run_command(cmd, suppressOutput)
            self.send_response({'info': result})
        except ServicesError as e:
            raise_error(self, e.args[0])


class cleanup(BaseHandler):
    """REST HTTP handler. Cleans up the database and clumping files. The required arguments in the request.arguments parameter are:

    Args:
        None
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message
        }
    """

    async def get(self):
        try:
            await cleanup()
            self.send_response({'info': "Cleanup succesful"})
        except ServicesError as e:
            raise_error(self, e.args[0])


class shutdown(BaseHandler):
    """REST HTTP handler. Shuts down the marxan-server and computer after a period of time - currently only on Unix. The required arguments in the request.arguments parameter are:

    Args:
        delay (string): The delay in minutes after which the server will be shutdown.
    Returns:
        None
    """

    async def get(self):
        try:
            if platform.system() != "Windows":
                validate_args(self.request.arguments, ['delay'])
                minutes = int(self.get_argument("delay"))
                # this wont be sent until the await returns
                self.send_response({'info': "Shutting down"})
                # if we shutdown is postponed, write the shutdown file
                if (minutes != 0):
                    # write the shutdown file with the time in UTC isoformat
                    write_to_file(project_paths.PROJECT_FOLDER + "shutdown.dat", (datetime.datetime.now(
                        timezone.utc) + timedelta(minutes/1440)).isoformat())
                # wait for so many minutes
                await asyncio.sleep(minutes * 60)
                logging.warning("marxan-server stopping due to shutdown event")
                # delete the shutdown file
                if (os.path.exists(project_paths.PROJECT_FOLDER + "shutdown.dat")):
                    logging.warning("Deleting the shutdown file")
                    os.remove(project_paths.PROJECT_FOLDER + "shutdown.dat")
                # shutdown the os
                logging.warning("marxan-server stopped")
                os.system('sudo shutdown now')
        except ServicesError as e:
            raise_error(self, e.args[0])


class testTornado(BaseHandler):
    """REST HTTP handler. Tests tornado is working properly. The required arguments in the request.arguments parameter are:

    Args:
        None
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message
        }
    """

    def get(self):
        self.send_response({'info': "Tornado running"})

####################################################################################################################################################################################################################################################################
# WebSocketHandler subclasses
####################################################################################################################################################################################################################################################################


class runMarxan(WebSocketHandler):
    """REST WebSocket Handler. Starts a Marxan run on the server and streams back the output through WebSocket messages. The required arguments in the request.arguments parameter are:

    Args:
        user (string): The name of the user.
        project (string): The name of the project to run.
    Returns:
        WebSocket dict messages with one or more of the following keys (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "user": The name of the user,
            "project": The name of the project that is running,
            "info": Contains the Marxan streaming log (Unix only),
            "elapsedtime": The elapsed time in seconds of the run,
            "pid": The process id of the Marxan subprocess,
            "status": One of Preprocessing, pid, RunningMarxan or Finished
        }
    """

    async def open(self):
        """Authenticate and authorise the request and start the run if it is not already running.
        """
        try:
            await super().open({'info': "Running Marxan.."})
        except ServicesError:
            pass
        else:
            user = self.get_argument("user")
            project = self.get_argument("project")
            # see if the project is already running - if it is then return an error
            df = file_data_to_df(
                project_paths.PROJECT_FOLDER + "runlog.dat")
            filtered_df = df.loc[
                (df['status'] == 'Running') &
                (df['user'] == user) &
                (df['project'] == project)
            ]
            if not filtered_df.empty:
                self.close({'error': "The project is already running."})
            else:
                # set the current folder to the project folder so files can be found in the input.dat file
                if (os.path.exists(self.folder_project)):
                    os.chdir(self.folder_project)
                    # delete all of the current output files
                    delete_all_files(self.folder_output)
                    # run marxan
                    # the "exec " in front allows you to get the pid of the child process, i.e. marxan, and therefore to be able to kill the process using os.kill(pid, signal.SIGTERM) instead of the tornado process - see here: https://stackoverflow.com/questions/4789837/how-to-terminate-a-python-subprocess-launched-with-shell-true/4791612#4791612
                    try:
                        if platform.system() != "Windows":
                            # in Unix operating systems, the log is streamed from stdout to a Tornado STREAM
                            self.marxanProcess = Subprocess(
                                [db_config.MARXAN_EXECUTABLE], stdout=Subprocess.STREAM, stdin=PIPE)
                            # add a callback when the process finishes
                            self.marxanProcess.set_exit_callback(
                                self.finishOutput)
                        else:
                            # custom class as the Subprocess.STREAM option does not work on Windows - see here: https://www.tornadoweb.org/en/stable/process.html?highlight=Subprocess#tornado.process.Subprocess
                            self.marxanProcess = BPSubProcess(
                                [db_config.MARXAN_EXECUTABLE], stdout=PIPE, stdin=PIPE)
                            # to ensure that the child process is stopped when it ends on windows
                            self.marxanProcess.stdout.close()
                            # add a callback when the process finishes on windows
                            self.marxanProcess.set_exit_callback_windows(
                                self.finishOutput)
                        # make sure that the marxan process will end by sending ENTER to the stdin
                        self.marxanProcess.stdin.write('\n'.encode("utf-8"))
                        self.marxanProcess.stdin.close()
                    except (WindowsError) as e:  # pylint:disable=undefined-variable
                        if (e.winerror == 1260):
                            self.close({'error': "The executable '" + db_config.MARXAN_EXECUTABLE +
                                        "' is blocked by group policy. For more information, contact your system administrator."})
                    else:  # no errors
                        # get the number of runs that were in the input.dat file

                        if not hasattr(self, "projectData"):
                            await get_project_data(pg, self)

                        # Extract the NUMREPS value from runParameters
                        num_reps = next(
                            (int(param['value']) for param in self.projectData['runParameters']
                             if param['key'] == 'NUMREPS'),
                            None
                        )

                        if num_reps is None:
                            raise ValueError(
                                "NUMREPS parameter not found in runParameters.")

                        self.numRunsRequired = num_reps
                        # log the run to the run log file
                        if (self.user != '_clumping'):  # dont log any clumping runs
                            self.logRun()
                        # return the pid so that the process can be stopped - prefix with an 'm' indicating that the pid is for a marxan run
                        self.send_response(
                            {'pid': 'm' + str(self.marxanProcess.pid), 'status': 'pid'})
                        # callback on the next I/O loop
                        IOLoop.current().spawn_callback(self.stream_marxan_output)
                else:  # project does not exist
                    self.close({'error': "Project '" + self.get_argument("project") + "' does not exist",
                                'project': self.get_argument("project"), 'user': self.get_argument("user")})

    async def stream_marxan_output(self):
        """Called on the first IOLoop callback and then streams the marxan output back to the client through WebSocket messages. Not currently supported on Windows.
        """
        if platform.system() != "Windows":
            try:
                while True:
                    # read from the stdout stream
                    line = await self.marxanProcess.stdout.read_bytes(1024, partial=True)
                    self.send_response({'info': line.decode(
                        "utf-8"), 'status': 'RunningMarxan', 'pid': 'm' + str(self.marxanProcess.pid)})
            except (StreamClosedError):
                pass
        else:
            try:
                self.send_response(
                    {'info': "Streaming log not currently supported on Windows\n", 'status': 'RunningMarxan'})
                # while True: #on Windows this is a blocking function
                # #read from the stdout file object
                # line = self.marxanProcess.stdout.readline()
                # self.send_response({'info': line, 'status':'RunningMarxan'})
                # #bit of a hack to see when it has finished running
                # if line.find("Press return to exit") > -1:
                # #make sure that the marxan process will end by sending a new line character to the process in windows
                # self.marxanProcess.communicate(input='\n')
                # break

            except (BufferError):
                log("BufferError")
                pass
            except (StreamClosedError):
                log("StreamClosedError")
                pass

    def logRun(self):
        """Writes the details of the started marxan job to the "runlog.dat" file as a single line.
        """
        # get the user name
        self.user = self.get_argument('user')
        self.project = self.get_argument('project')
        # create the data record - pid, user, project, starttime, endtime, runtime, runs (e.g. 3/10), status = running, completed, stopped (by user), killed (by OS)
        record = [str(self.marxanProcess.pid), self.user, self.project, datetime.datetime.now(
        ).strftime("%d/%m/%y %H:%M:%S"), '', '', '0/' + str(self.numRunsRequired), 'Running']
        # add the tab separators
        recordLine = "\t".join(record)
        # append the record to the run log file
        write_to_file(project_paths.PROJECT_FOLDER + "runlog.dat",
                      recordLine + "\n", "a")

    def finishOutput(self, returnCode):
        """Finishes writing the output of a stream, writes the run log and closes the web socket connection. This is called when the run finishes or when it is stopped or killer by the operating system.

        Args:
            returnCode (string): Passed into the function.
        Returns:
            None
        """
        try:
            # close the output stream
            self.marxanProcess.stdout.close()
            if (self.user != '_clumping'):  # dont log clumping runs
                # get the number of runs completed
                numRunsCompleted = len(
                    glob.glob(self.folder_output + "output_r*"))
                # write the response depending on if the run completed or not
                if (numRunsCompleted == self.numRunsRequired):
                    update_run_log(self.marxanProcess.pid, self.startTime,
                                   numRunsCompleted, self.numRunsRequired, 'Completed')
                    self.close({'info': 'Run completed',
                                'project': self.project, 'user': self.user})
                else:  # if the user stopped it then the run log should already have a status of Stopped
                    actualStatus = update_run_log(
                        self.marxanProcess.pid, self.startTime, numRunsCompleted, self.numRunsRequired, 'Killed')
                    if (actualStatus == 'Stopped'):
                        self.close({'error': 'Run stopped by ' + self.user,
                                    'project': self.project, 'user': self.user})
                    else:
                        self.close({'error': 'Run stopped by operating system',
                                    'project': self.project, 'user': self.user})
            else:
                self.close({'info': 'Run completed',
                            'project': self.project, 'user': self.user})
        except ServicesError as e:
            self.close({'error': e.args[0]})


class importFeatures(WebSocketHandler):
    """REST WebSocket Handler. Imports a set of features from an unzipped shapefile. This can either be a single feature class or multiple. Sends an error if the feature(s) already exist(s). The required arguments in the request.arguments parameter are:

    Args:
        shapefile (string): The name of shapefile to import (minus the *.shp extension).
        name (string): Optional. If specified then this is the name of the single feature class that will be imported. If omitted then the import is for multiple features.
        description (string): Optional. A description for the imported feature class.
        splitfield (string): Optional. The name of the field to use to split the features in the shapefile into separate feature classes. The separate feature classes will have a name derived from the values in this field.
    Returns:
        WebSocket dict messages with one or more of the following keys (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Contains detailed progress statements on the import process,
            "elapsedtime": The elapsed time in seconds of the run,
            "status": One of Preprocessing, pid, FeatureCreated or Finished,
            "id": The oid of the feature created,
            "feature_class_name": The name of the feature class created,
            "uploadId": The Mapbox tileset upload id (for a single feature),
            "uploadIds": string[]: The Mapbox tileset upload ids (for multiple feature)
        }
    """

    async def open(self):
        try:
            await super().open({'info': "Importing features.."})
        except ServicesError:  # authentication/authorisation error
            pass
        else:
            # validate the input arguments
            validate_args(self.request.arguments, ['shapefile'])
            # initiate the mapbox upload ids array
            uploadIds = []
            # get the name of the shapefile that has already been unzipped on the server
            shapefile = self.get_argument('shapefile')
            # if a name is passed then this is a single feature class
            if "name" in list(self.request.arguments.keys()):
                name = self.get_argument('name')
            else:
                name = None
            try:
                # get a scratch name for the import
                scratch_name = get_unique_feature_name("scratch_")
                # first, import the shapefile into a PostGIS feature class in EPSG:4326
                await pg.importShapefile(project_paths.IMPORT_FOLDER, shapefile, scratch_name)
                # check the geometry
                self.send_response(
                    {'status': 'Preprocessing', 'info': "Checking the geometry.."})
                await pg.isValid(scratch_name)
                # get the feature names
                if name:  # single feature name
                    feature_names = [name]
                else:  # get the feature names from a field in the shapefile
                    splitfield = self.get_argument('splitfield')
                    features = await pg.execute(sql.SQL("SELECT {splitfield} FROM marxan.{scratchTable}").format(splitfield=sql.Identifier(splitfield), scratchTable=sql.Identifier(scratch_name)), return_format="DataFrame")
                    feature_names = list(set(features[splitfield].tolist()))
                    # if they are not unique then return an error
                    # if (len(feature_names) != len(set(feature_names))):
                    #     raise ServicesError("Feature names are not unique for the field '" + splitfield + "'")
                # split the imported feature class into separate feature classes
                for feature_name in feature_names:
                    # create the new feature class
                    if name:  # single feature name
                        feature_class_name = get_unique_feature_name("f_")
                        await pg.execute(sql.SQL("CREATE TABLE marxan.{feature_class_name} AS SELECT * FROM marxan.{scratchTable};").format(feature_class_name=sql.Identifier(feature_class_name), scratchTable=sql.Identifier(scratch_name)), [feature_name])
                        description = self.get_argument('description')
                    else:  # multiple feature names
                        feature_class_name = get_unique_feature_name("fs_")
                        await pg.execute(sql.SQL("CREATE TABLE marxan.{feature_class_name} AS SELECT * FROM marxan.{scratchTable} WHERE {splitField} = %s;").format(feature_class_name=sql.Identifier(feature_class_name), scratchTable=sql.Identifier(scratch_name), splitField=sql.Identifier(splitfield)), [feature_name])
                        description = "Imported from '" + shapefile + \
                            "' and split by '" + splitfield + "' field"
                    # add an index and a record in the metadata_interest_features table and start the upload to mapbox
                    geometryType = await pg.getGeometryType(feature_class_name)
                    source = "Imported shapefile" if (
                        geometryType != 'ST_Point') else "Imported shapefile (points)"

                    id = await finish_feature_import(feature_class_name, feature_name, description, source, self.get_current_user())
                    # start the upload to mapbox
                    uploadId = await upload_tileset_to_mapbox(feature_class_name, feature_class_name)

                    # append the uploadId to the uploadIds array
                    uploadIds.append(uploadId)
                    self.send_response({'id': id, 'feature_class_name': feature_class_name, 'uploadId': uploadId,
                                        'info': "Feature '" + feature_name + "' imported", 'status': 'FeatureCreated'})
                # complete
                self.close({'info': "Features imported",
                            'uploadIds': uploadIds})
            except (ServicesError) as e:
                if "already exists" in e.args[0]:
                    self.close({'error': "The feature '" + feature_name +
                                "' already exists", 'info': 'Failed to import features'})
                else:
                    self.close(
                        {'error': e.args[0], 'info': 'Failed to import features'})
            finally:
                # delete the scratch feature class
                await pg.execute(sql.SQL("DROP TABLE IF EXISTS marxan.{}").format(sql.Identifier(scratch_name)))

# imports an item from GBIF


class importGBIFData(WebSocketHandler):
    """REST WebSocket Handler. Imports an item using the GBIF API. The required arguments in the request.arguments parameter are:

    Args:
        taxonKey (string): The GBIF taxon key for the feature.
        scientificName (string): The GBIF scientific name for the feature.
    Returns:
        WebSocket dict messages with one or more of the following keys (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Contains detailed progress statements on the import process,
            "elapsedtime": The elapsed time in seconds of the run,
            "status": One of Preprocessing, pid, FeatureCreated or Finished,
            "id": The oid of the feature created,
            "feature_class_name": The name of the feature class created,
            "uploadId": The Mapbox tileset upload id
        }
    """
    @staticmethod
    def import_df(self, df, table_name):
        """
        Imports a DataFrame into a PostgreSQL table.

        This function creates or replaces the specified table in the 'marxan' schema
        with the contents of the given DataFrame. It uses a specific connection string
        and is not asynchronous.
        """
        db_url = (
            f"postgresql://{db_config.DATABASE_USER}:"
            f"{db_config.DATABASE_PASSWORD}@"
            f"{db_config.DATABASE_HOST}/"
            f"{db_config.DATABASE_NAME}"
        )
        engine = create_engine(db_url)
        # Import the DataFrame to the specified table
        df.to_sql(f"marxan.{table_name}", con=engine,
                  if_exists='replace', index=False)

    async def open(self):
        """Manages the GBIF import from downloading the data to importing into PostGIS.
        """
        try:
            await super().open({'info': "Importing features from GBIF.."})
        except ServicesError:  # authentication/authorisation error
            pass
        else:
            # validate the input arguments
            validate_args(self.request.arguments, [
                'taxonKey', 'scientificName'])
            try:
                taxonKey = self.get_argument('taxonKey')
                # get the occurrences using asynchronous parallel requests
                df = await self.getGBIFOccurrences(taxonKey)
                if (df.empty == False):
                    # get the feature class name
                    feature_class_name = "gbif_" + str(taxonKey)
                    # create the table if it doesnt already exists
                    await pg.execute(sql.SQL("DROP TABLE IF EXISTS marxan.{}").format(sql.Identifier(feature_class_name)))
                    await pg.execute(sql.SQL("CREATE TABLE marxan.{} (eventdate date, gbifid bigint, lng double precision, lat double precision, geometry geometry)").format(sql.Identifier(feature_class_name)))
                    # insert the records - this calls import_df which is blocking
                    await IOLoop.current().run_in_executor(None, self.import_df, df, feature_class_name)
                    # update the geometry field
                    await pg.execute(sql.SQL("UPDATE marxan.{} SET geometry=marxan.ST_SplitAtDateline(ST_Transform(ST_Buffer(ST_Transform(ST_SetSRID(ST_Point(lng, lat),4326),3410),%s),4326))").format(sql.Identifier(feature_class_name)), [GBIF_POINT_BUFFER_RADIUS])
                    # get the gbif vernacular name
                    feature_name = self.get_argument('scientificName')
                    vernacularNames = self.getVernacularNames(taxonKey)
                    description = self.getCommonName(vernacularNames)
                    # add an index and a record in the metadata_interest_features table and start the upload to mapbox

                    id = await finish_feature_import(feature_class_name, feature_name, description, "Imported from GBIF", self.get_current_user())
                    # start the upload to mapbox
                    uploadId = await upload_tileset_to_mapbox(feature_class_name, feature_class_name)

                    self.send_response({'id': id, 'feature_class_name': feature_class_name, 'uploadId': uploadId,
                                        'info': "Feature '" + feature_name + "' imported", 'status': 'FeatureCreated'})
                    # complete
                    self.close(
                        {'info': "Features imported", 'uploadId': uploadId})
                else:
                    raise ServicesError(
                        "No records for " + self.get_argument('scientificName'))

            except (ServicesError) as e:
                if "already exists" in e.args[0]:
                    self.close({'error': "The feature '" + feature_name +
                                "' already exists", 'info': 'Failed to import features'})
                else:
                    self.close(
                        {'error': e.args[0], 'info': 'Failed to import features'})

    # parallel asynchronous loading og gbif data
    async def getGBIFOccurrences(self, taxonKey):
        """Downloads the GBIF occurrence records from the API.

        Args:
            See the class definition.
        Returns:
             pd.DataFrame: The occurrence records as a dataframe. Each records contains the keys: eventDate,gbifID,lng,lat,geometry.
        """
        def getGBIFUrl(taxonKey, limit, offset=0):
            return GBIF_API_ROOT + "occurrence/search?taxonKey=" + str(taxonKey) + "&basisOfRecord=HUMAN_OBSERVATION&limit=" + str(limit) + "&hasCoordinate=true&offset=" + str(offset)

        # makes a call to gbif
        async def makeRequest(url):
            logging.debug(url)
            response = await httpclient.AsyncHTTPClient().fetch(url)
            return response.body.decode(errors="ignore")

        # fetches the url and tracks the progress
        async def fetch_url(current_url):
            if current_url in fetching:
                return
            fetching.add(current_url)
            response = await makeRequest(current_url)
            # get the response as a json object
            _json = json.loads(response)
            # get the lat longs
            data = [OrderedDict({'eventDate': item['eventDate'] if 'eventDate' in item.keys() else None, 'gbifID': item['gbifID'],
                                 'lng': item['decimalLongitude'], 'lat': item['decimalLatitude'], 'geometry': ''}) for item in _json['results']]
            # append them to the list
            latLongs.extend(data)
            fetched.add(current_url)

        # helper to request a specific url
        async def worker():
            async for url in q:
                if url is None:
                    return
                try:
                    # fetch the url
                    await fetch_url(url)
                except Exception as e:
                    log("Exception: %s %s" % (e, url))
                    dead.add(url)
                finally:
                    q.task_done()

        # initialise the lat/longs
        latLongs = []
        # get the number of occurrences
        _url = getGBIFUrl(taxonKey, 10)
        req = request.Request(_url)
        # get the response
        resp = request.urlopen(req)
        # parse the results as a json object
        results = json.loads(resp.read())
        numOccurrences = results['count']
        # error check
        if (numOccurrences > GBIF_OCCURRENCE_LIMIT):
            raise ServicesError(
                "Number of GBIF occurrence records is greater than " + str(GBIF_OCCURRENCE_LIMIT))
        # get the page count
        pageCount = int(numOccurrences//GBIF_PAGE_SIZE) + 1
        # get the urls to fetch
        urls = [getGBIFUrl(taxonKey, GBIF_PAGE_SIZE, (i * GBIF_PAGE_SIZE))
                for i in range(0, pageCount)]
        # create a queue for the urls to fetch
        q = queues.Queue()
        # initialise the sets to track progress
        fetching, fetched, dead = set(), set(), set()
        # add all the urls to the queue
        for _url in urls:
            await q.put(_url)
        # Start workers, then wait for the work queue to be empty.
        workers = gen.multi([worker() for _ in range(GBIF_CONCURRENCY)])
        await q.join()
        assert fetching == (fetched | dead)
        # Signal all the workers to exit.
        for _ in range(GBIF_CONCURRENCY):
            await q.put(None)
        await workers
        return pd.DataFrame(latLongs)

    def getVernacularNames(self, taxonKey):
        """Gets vernacular names for the GBIF taxon key.

        Args:
            See the class definition.
        Returns:
            See https://www.gbif.org/developer/species.
        """
        try:
            # build the url request
            url = GBIF_API_ROOT + "species/" + \
                str(taxonKey) + "/vernacularNames"
            # make the request
            req = request.Request(url)
            # get the response
            resp = request.urlopen(req)
            # parse the results as a json object
            results = json.loads(resp.read())
            return results['results']
        except (Exception) as e:
            log(e.args[0])

    def getCommonName(self, vernacularNames, language='eng'):
        """Gets common names for the vernacular names.

        Args:
            vernacularNames (string[]): See https://www.gbif.org/developer/species.
        Returns:
            See https://www.gbif.org/developer/species.
        """
        commonNames = [i['vernacularName']
                       for i in vernacularNames if i['language'] == language]
        if (len(commonNames) > 0):
            return commonNames[0]
        else:
            return 'No common name'


class createFeaturesFromWFS(WebSocketHandler):
    """REST WebSocket Handler. Creates a new feature (or set of features) from a WFS endpoint. Sends an error if the feature already exist. The required arguments in the request.arguments parameter are:

    Args:
        srs (string): The spatial reference system of the WFS service, e.g. 'EPSG:4326'.
        endpoint (string): The url endpoint to the WFS service.
        name (string): The name of the feature to be created.
        description (string): A description for the feature.
        featuretype (string): The layer name within the WFS service representing the feature class to import.
    Returns:
        WebSocket dict messages with one or more of the following keys (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Contains detailed progress statements on the import process,
            "elapsedtime": The elapsed time in seconds of the run,
            "status": One of Preprocessing, pid, FeatureCreated or Finished,
            "id": The oid of the feature created,
            "feature_class_name": The name of the feature class created,
            "uploadId": The Mapbox tileset upload id
        }
    """

    @staticmethod
    def get_gml(endpoint, featuretype):
        """Gets the gml data using the WFS endpoint and feature type

        Args:
            endpoint (string): The url of the WFS endpoint to get the GML data from.
            featuretype (string): The name of the feature class in the WFS service to get the GML data from.
        Returns:
            string: The gml as a text string.
        """
        response = requests.get(
            f"{endpoint}&request=getfeature&typeNames={featuretype}")
        return response.text

    async def open(self):
        try:
            await super().open({'info': "Importing features.."})
        except ServicesError:  # authentication/authorisation error
            pass
        else:
            # validate the input arguments
            validate_args(self.request.arguments, [
                'srs', 'endpoint', 'name', 'description', 'featuretype'])
            try:
                # get a unique feature class name for the import
                feature_class_name = get_unique_feature_name("f_")
                # get the WFS data as GML
                gml = await IOLoop.current().run_in_executor(None, self.get_gml, self.get_argument('endpoint'), self.get_argument('featuretype'))
                # write it to file
                write_to_file(
                    project_paths.IMPORT_FOLDER + feature_class_name + ".gml", gml)
                # import the GML into a PostGIS feature class in EPSG:4326
                await pg.importGml(project_paths.IMPORT_FOLDER, feature_class_name + ".gml", feature_class_name, sEpsgCode=self.get_argument('srs'))
                # check the geometry
                self.send_response(
                    {'status': 'Preprocessing', 'info': "Checking the geometry.."})
                await pg.isValid(feature_class_name)
                # add an index and a record in the metadata_interest_features table and start the upload to mapbox
                id = await finish_feature_import(feature_class_name, self.get_argument('name'), self.get_argument('description'), "imported from web service", self.get_current_user())
                # start the upload to mapbox
                uploadId = await upload_tileset_to_mapbox(feature_class_name, feature_class_name)

                self.send_response({'id': id, 'feature_class_name': feature_class_name, 'uploadId': uploadId,
                                    'info': "Feature '" + self.get_argument('name') + "' imported", 'status': 'FeatureCreated'})
                # complete
                self.close({'info': "Features imported", 'uploadId': uploadId})
            except (ServicesError) as e:
                if "already exists" in e.args[0]:
                    self.close({'error': "The feature '" + self.get_argument('name') +
                                "' already exists", 'info': 'Failed to import features'})
                else:
                    self.close(
                        {'error': e.args[0], 'info': 'Failed to import features'})
            finally:
                # delete the gml file
                if os.path.exists(project_paths.IMPORT_FOLDER + feature_class_name + ".gml"):
                    os.remove(project_paths.IMPORT_FOLDER +
                              feature_class_name + ".gml")
                # delete the gfs file
                if os.path.exists(project_paths.IMPORT_FOLDER + feature_class_name + ".gfs"):
                    os.remove(project_paths.IMPORT_FOLDER +
                              feature_class_name + ".gfs")


class exportProject(WebSocketHandler):
    """REST WebSocket Handler. Exports a project including all of the spatial data and metadata (not applicable to old version Marxan projects) to an *.mxw file. The required arguments in the request.arguments parameter are:

    Args:
        user (string): The name of the user.
        project (string): The name of the project to export.
    Returns:
        WebSocket dict messages with one or more of the following keys (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Contains detailed progress statements on the export process,
            "elapsedtime": The elapsed time in seconds of the export,
            "status": One of Preprocessing, pid, FeatureCreated or Finished,
            "id": The oid of the feature created,
            "feature_class_name": The name of the feature class created,
            "uploadId": The Mapbox tileset upload id
        }
    """

    async def open(self):
        try:
            await super().open({'info': "Exporting project.."})
        except ServicesError:  # authentication/authorisation error
            pass
        else:
            validate_args(self.request.arguments, ['user', 'project'])
            self.send_response(
                {'status': 'Preprocessing', 'info': "Copying project folder.."})
            # create a folder in the export folder to hold all the files
            exportFolder = project_paths.EXPORT_FOLDER + \
                self.get_argument('user') + "_" + self.get_argument('project')
            # remote the folder if it already exists
            if os.path.exists(exportFolder):
                shutil.rmtree(exportFolder)
            # copy the project folder
            shutil.copytree(self.folder_project, exportFolder)
            # FEATURES
            # get the species data from the spec.dat file and the PostGIS database
            await get_species_data(self)
            # get the feature class names that must be exported from postgis
            feature_class_names = self.speciesData['feature_class_name'].tolist(
            )
            # export all of the feature classes as shapefiles
            self.send_response(
                {'status': 'Preprocessing', 'info': "Exporting features.."})
            cmd = '"' + db_config.OGR2OGR_EXECUTABLE + '" -f "ESRI Shapefile" "' + exportFolder + os.sep + EXPORT_F_SHP_FOLDER + os.sep + '" PG:"host=' + db_config.DATABASE_HOST + \
                ' user=' + db_config.DATABASE_USER + ' dbname=' + db_config.DATABASE_NAME + ' password=' + \
                db_config.DATABASE_PASSWORD + ' ACTIVE_SCHEMA=marxan" ' + \
                " ".join(feature_class_names)
            await run_command(cmd)
            # export the features metadata
            self.send_response(
                {'status': 'Preprocessing', 'info': "Exporting feature metadata.."})
            escapedFeatureNames = "\'" + \
                "\',\'".join(feature_class_names) + "\'"
            cmd = '"' + db_config.OGR2OGR_EXECUTABLE + '" -f "CSV" "' + exportFolder + os.sep + EXPORT_F_METADATA + '" PG:"host=' + db_config.DATABASE_HOST + ' user=' + db_config.DATABASE_USER + ' dbname=' + db_config.DATABASE_NAME + ' password=' + db_config.DATABASE_PASSWORD + \
                ' ACTIVE_SCHEMA=marxan" -sql "SELECT oid, feature_class_name, alias, description FROM metadata_interest_features WHERE feature_class_name = ANY (ARRAY[' + \
                escapedFeatureNames + ']);" -lco SEPARATOR=TAB'
            await run_command(cmd)
            # PLANNING GRIDS
            # export the planning unit grid
            pu_name = self.projectData['metadata']['PLANNING_UNIT_NAME']
            self.send_response(
                {'status': 'Preprocessing', 'info': "Exporting planning grid.."})
            await pg.exportToShapefile(exportFolder + os.sep + EXPORT_PU_SHP_FOLDER + os.sep, pu_name)
            # export the planning grid metadata - convert the envelope geometry field to text
            self.send_response(
                {'status': 'Preprocessing', 'info': "Exporting planning grid metadata.."})
            cmd = '"' + db_config.OGR2OGR_EXECUTABLE + '" -f "CSV" "' + exportFolder + os.sep + EXPORT_PU_METADATA + '" PG:"host=' + db_config.DATABASE_HOST + ' user=' + db_config.DATABASE_USER + ' dbname=' + db_config.DATABASE_NAME + ' password=' + db_config.DATABASE_PASSWORD + \
                ' ACTIVE_SCHEMA=marxan" -sql "SELECT feature_class_name, alias, description, country_id, aoi_id, domain, _area, ST_AsText(envelope) envelope, creation_date, source, created_by, tilesetid, planning_unit_count FROM marxan.metadata_planning_units WHERE feature_class_name = \'' + \
                pu_name + '\';" -lco SEPARATOR=TAB'
            await run_command(cmd)
            # zip the whole folder
            self.send_response(
                {'status': 'Preprocessing', 'info': "Zipping project.."})
            await IOLoop.current().run_in_executor(None, zip_folder, exportFolder, exportFolder)
            # rename with a mxw extension
            os.rename(exportFolder + ".zip", exportFolder + ".mxw")
            # delete the folder
            shutil.rmtree(exportFolder)
            # return the results
            self.close({'info': "Export project complete", 'filename': self.get_argument(
                'user') + "_" + self.get_argument('project') + ".mxw"})


class ImportProject(WebSocketHandler):
    """
    REST WebSocket Handler. Imports a *.mxw file as a new project.

    Required Arguments:
        user (str): The name of the user.
        project (str): The name of the project to create from the import file.
        description (str): A description for the imported project.
        filename (str): The name of the *.mxw file (minus the .mxw extension).

    Returns:
        WebSocket dict messages with the following keys:
        {
            "info": Detailed progress statements on the import process,
            "elapsedtime": The elapsed time in seconds of the import,
            "status": One of Preprocessing or Finished.
        }
    """

    @staticmethod
    def update_dataframe(df, mapping, df_join_field, mapping_join_field, new_values_field):
        """
        Updates the values in the `df` DataFrame using a mapping DataFrame. The values in `df_join_field`
        are replaced by those in `new_values_field` from the mapping DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame to update.
            mapping (pd.DataFrame): The mapping DataFrame used to update values in `df`.
            df_join_field (str): The field in `df` used for the join and whose values will be updated.
            mapping_join_field (str): The field in `mapping` used for the join with `df`.
            new_values_field (str): The field in `mapping` containing the new values to populate in `df`.

        Returns:
            pd.DataFrame: The updated DataFrame.
        """
        # Create a copy of the mapping DataFrame with renamed join fields
        mapping_copy = mapping.rename(
            columns={mapping_join_field: df_join_field})
        # Perform a left join to incorporate the mapping values
        updated_df = df.merge(
            mapping_copy[[df_join_field, new_values_field]], on=df_join_field, how='left')
        # Replace the original join field values with the new values
        updated_df[df_join_field] = updated_df[new_values_field]
        # Drop the new values field as it's no longer needed
        updated_df = updated_df.drop(columns=[new_values_field])
        # Reorder columns to maintain original column order
        column_order = [df_join_field] + \
            [col for col in df.columns if col != df_join_field]
        return updated_df[column_order]

    async def open(self):
        try:
            validate_args(self.request.arguments, [
                'user', 'project', 'filename', 'description'
            ])

            user = self.get_argument('user')
            project = self.get_argument('project').strip()
            description = self.get_argument('description')
            filename = self.get_argument('filename')
            project_folder = os.path.join(
                project_paths.USERS_FOLDER, user, project)

            if os.path.exists(project_folder):
                shutil.rmtree(project_folder)
            os.makedirs(project_folder)

            # Unzip files to the project folder
            zip_path = os.path.join(project_paths.IMPORT_FOLDER, filename)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(project_folder)

            await super().open({'info': "Importing project.."})

        except ServicesError:
            pass

        except zipfile.BadZipFile:
            shutil.rmtree(project_folder, ignore_errors=True)
            self.startTime = datetime.datetime.now()
            self.close({'error': 'File is not a zip file'})
            return

        try:
            # Import features
            self.send_response(
                {'status': 'Preprocessing', 'info': "Importing features.."})
            cmd = (
                f'"{db_config.OGR2OGR_EXECUTABLE}" -f "PostgreSQL" PG:"host={
                    db_config.DATABASE_HOST} user={db_config.DATABASE_USER} '
                f'dbname={db_config.DATABASE_NAME} password={
                    db_config.DATABASE_PASSWORD}" "{project_folder + EXPORT_F_SHP_FOLDER}" '
                f'-nlt GEOMETRY -lco SCHEMA=marxan -lco GEOMETRY_NAME=geometry -t_srs EPSG:4326 -lco precision=NO -skipfailures'
            )
            await run_command(cmd)

            # Load feature metadata and process new features
            feature_metadata = pd.read_csv(os.path.join(
                project_folder, EXPORT_F_METADATA), sep='\t')

            for idx, row in feature_metadata.iterrows():
                results = await pg.execute(
                    "SELECT * FROM marxan.metadata_interest_features WHERE feature_class_name = %s;",
                    data=[row['feature_class_name']],
                    return_format="Array"
                )
                if not results:
                    self.send_response(
                        {'status': 'Preprocessing', 'info': f"Importing {row['alias']}"})
                    await finish_feature_import(row['feature_class_name'],
                                                row['alias'],
                                                row['description'],
                                                f"Imported with project {
                        user}/{project}",
                        user)
                else:
                    self.send_response({'status': 'Preprocessing', 'info': f"{
                                       row['alias']} already exists - skipping"})

            # Update feature ID mappings in spec.dat and puvspr.dat
            self.send_response(
                {'status': 'Preprocessing', 'info': 'Updating feature ID values..'})

            feature_class_names = feature_metadata['feature_class_name'].tolist(
            )
            updated_features = await pg.execute(
                "SELECT unique_id, feature_class_name FROM marxan.metadata_interest_features WHERE feature_class_name = ANY (ARRAY[%s]);",
                data=[feature_class_names],
                return_format="DataFrame"
            )
            # join the source dataframe and the new data
            feature_metadata = feature_metadata[['feature_class_name', 'oid']].rename(
                columns={'feature_class_name': 'fcn', 'oid': 'id'}).set_index('fcn')
            updated_features = updated_features[['feature_class_name', 'oid']].rename(
                columns={'feature_class_name': 'fcn', 'oid': 'new_id'}).set_index('fcn')
            # create a data frame with the old oid and the new old for the feature class
            mapping = feature_metadata.join(updated_features)

            # Update feature_preprocessing.dat
            feature_preprocessing = file_data_to_df(os.path.join(
                self.folder_input, "feature_preprocessing.dat"))
            feature_preprocessing = self.update_dataframe(
                feature_preprocessing,  mapping, 'id', 'id', 'new_id')
            feature_preprocessing.to_csv(os.path.join(
                self.folder_input, "feature_preprocessing.dat"), index=False)

            # Update spec.dat
            spec_dat = file_data_to_df(os.path.join(
                self.folder_input, self.projectData["files"]["SPECNAME"]))
            spec_dat = self.update_dataframe(
                spec_dat,  mapping, 'id', 'id', 'new_id')
            await write_csv(self, "SPECNAME", spec_dat)

            # Update puvspr.dat
            puvspr_dat = file_data_to_df(os.path.join(
                self.folder_input, self.projectData["files"]["PUVSPRNAME"]))
            puvspr_dat = self.update_dataframe(
                puvspr_dat, mapping, 'species', 'id', 'new_id')
            puvspr_dat = puvspr_dat.sort_values(by=['pu', 'species'])
            await write_csv(self, "PUVSPRNAME", puvspr_dat)

            # Clear output folder
            delete_all_files(self.folder_output)

            # Import planning grid metadata
            self.send_response(
                {'status': 'Preprocessing', 'info': 'Importing planning grid..'})

            planning_grid_metadata = pd.read_csv(os.path.join(
                project_folder, EXPORT_PU_METADATA), sep='\t')
            planning_grid_metadata = planning_grid_metadata.where(
                pd.notnull(planning_grid_metadata), None)
            grid_row = planning_grid_metadata.iloc[0]

            existing_grid = await pg.execute(
                "SELECT * FROM marxan.metadata_planning_units WHERE feature_class_name = %s;",
                data=[grid_row['feature_class_name']],
                return_format="Array"
            )

            if not existing_grid:
                await pg.execute(
                    """
                    INSERT INTO marxan.metadata_planning_units (
                        feature_class_name, alias, description, country_id, aoi_id, domain, _area, envelope,
                        creation_date, source, created_by, tilesetid, planning_unit_count
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_GeomFromText(%s), '4326'), NOW(),
                        'Imported from .mxw file', %s, %s, %s);
                    """,
                    data=[
                        grid_row['feature_class_name'], grid_row['alias'], grid_row['description'],
                        grid_row['country_id'], grid_row['aoi_id'], grid_row['domain'], grid_row['_area'],
                        grid_row['envelope'], user, grid_row['tilesetid'], grid_row['planning_unit_count']
                    ]
                )

                # Import planning grid shapefile
                cmd = (
                    f'"{db_config.OGR2OGR_EXECUTABLE}" -f "PostgreSQL" PG:"host={
                        db_config.DATABASE_HOST} user={db_config.DATABASE_USER} '
                    f'dbname={db_config.DATABASE_NAME} password={
                        db_config.DATABASE_PASSWORD}" "{project_folder + EXPORT_PU_SHP_FOLDER}" '
                    f'-nlt GEOMETRY -lco SCHEMA=marxan -lco GEOMETRY_NAME=geometry -t_srs EPSG:4326 -lco precision=NO -lco FID=puid -skipfailures'
                )
                await run_command(cmd)

            # Update project description
            update_file_parameters(
                os.path.join(project_folder, "input.dat"),
                {'DESCRIPTION': description}
            )

            # Cleanup temporary files and folders
            shutil.rmtree(os.path.join(project_folder, EXPORT_F_SHP_FOLDER))
            shutil.rmtree(os.path.join(project_folder, EXPORT_PU_SHP_FOLDER))
            os.remove(os.path.join(project_folder, EXPORT_F_METADATA))
            os.remove(os.path.join(project_folder, EXPORT_PU_METADATA))
            os.remove(zip_path)

            # Finalize import
            self.close({'info': "Import project complete"})

        except Exception as e:
            shutil.rmtree(project_folder, ignore_errors=True)
            self.close({'error': str(e)})

####################################################################################################################################################################################################################################################################
# baseclass for handling long-running PostGIS queries using WebSockets
####################################################################################################################################################################################################################################################################


class QueryWebSocketHandler(WebSocketHandler):
    """Base class for handling long-running PostGIS queries using WebSockets.

    Attributes:
        pid: A string with the back-end process id of the PostGIS query (prefixed with 'q'). This allows the query to be stopped.
    """
    # runs a PostGIS query asynchronously and writes the pid to the client so the query can be stopped

    async def executeQuery(self, sql, data=None, return_format=None):
        print('data: ', data)
        print('sql: ', sql)
        print('self: ', self)
        try:
            print('return await pg.execute')
            return await pg.execute(sql, data=data, return_format=return_format, socketHandler=self)
        except psycopg2.OperationalError as e:
            self.close({'error': "Preprocessing stopped by operating system"})
        except asyncio.CancelledError:
            self.close({'error': "Preprocessing stopped by " + self.user})

####################################################################################################################################################################################################################################################################
# WebSocket subclasses
####################################################################################################################################################################################################################################################################

# preprocesses the features by intersecting them with the planning units
# wss://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/PreprocessFeature?user=andrew&project=Tonga%20marine%2030km2&planning_grid_name=pu_ton_marine_hexagon_30&feature_class_name=volcano&alias=volcano&id=63408475


class PreprocessFeature(QueryWebSocketHandler):
    """
    REST WebSocket Handler. Preprocesses features by intersecting them with planning units. Summarizes polygon areas or point values for each planning unit.

    Required Arguments:
        user (str): The name of the user.
        project (str): The name of the project.
        id (str): The feature OID.
        feature_class_name (str): The feature class name.
        alias (str): The alias for the feature.
        planning_grid_name (str): The name of the planning grid.

    Returns:
        dict: WebSocket messages with keys:
            - "info": Detailed progress messages.
            - "elapsedtime": Time taken for processing.
            - "status": Either "Preprocessing" or "Finished".
            - "feature_class_name": The name of the preprocessed feature class.
            - "id": The feature OID.
            - "pu_area": Total area of the feature in the planning grid.
            - "pu_count": Total number of planning grids intersecting the feature.
    """

    async def open(self):
        try:
            alias = self.get_argument('alias')
            await super().open({'info': f"Preprocessing '{alias}'.."})
        except ServicesError:
            pass  # Authentication/authorization error
        else:
            validate_args(self.request.arguments, [
                'user', 'project', 'id', 'feature_class_name', 'alias', 'planning_grid_name'
            ])

            try:
                # Determine geometry type
                feature_class_name = self.get_argument('feature_class_name')
                planning_grid_name = self.get_argument('planning_grid_name')

                geometry_type = await pg.getGeometryType(feature_class_name)

                if geometry_type != 'ST_Point':
                    # Query for polygon intersection and area
                    query = sql.SQL(
                        """
                        SELECT metadata.oid::integer species, grid.puid pu,
                               ST_Area(ST_Transform(ST_Union(ST_Intersection(grid.geometry, feature.geometry)), 3410)) amount
                        FROM marxan.{grid} grid, marxan.{feature} feature, marxan.metadata_interest_features metadata
                        WHERE ST_Intersects(grid.geometry, feature.geometry)
                          AND metadata.feature_class_name = %s
                        GROUP BY 1, 2;
                        """
                    ).format(grid=sql.Identifier(planning_grid_name), feature=sql.Identifier(feature_class_name))
                else:
                    # Query for point intersection and sum of values
                    query = sql.SQL(
                        """
                        SELECT metadata.oid::integer species, grid.puid pu, SUM(feature.value) amount
                        FROM marxan.{grid} grid, marxan.{feature} feature, marxan.metadata_interest_features metadata
                        WHERE ST_Intersects(grid.geometry, feature.geometry)
                          AND metadata.feature_class_name = %s
                        GROUP BY 1, 2;
                        """
                    ).format(grid=sql.Identifier(planning_grid_name), feature=sql.Identifier(feature_class_name))

                intersection_data = await self.executeQuery(query, data=[feature_class_name], return_format="DataFrame")

            except ServicesError as e:
                self.close({'error': e.args[0]})
                return

            try:
                # Load existing PUVSPR data
                puvspr_path = os.path.join(
                    self.folder_input, self.projectData["files"]["PUVSPRNAME"])
                try:
                    existing_data = file_data_to_df(puvspr_path)
                except FileNotFoundError:
                    # Initialize empty DataFrame if no existing data
                    existing_data = pd.DataFrame(
                        columns=['species', 'pu', 'amount'])

                # Remove existing records for this species
                species_id = int(self.get_argument('id'))
                existing_data = existing_data[existing_data['species']
                                              != species_id]

                # Append new intersection data
                updated_data = pd.concat([existing_data, intersection_data])
                updated_data = updated_data.sort_values(by=['pu', 'species'])

                # Write updated PUVSPR data
                await write_csv(self, "PUVSPRNAME", updated_data)

                # Calculate statistics for the feature
                filtered_data = updated_data[updated_data['species']
                                             == species_id]
                pu_count = filtered_data['pu'].count()
                pu_area = filtered_data['amount'].sum()

                # Create and write summary record
                summary_record = pd.DataFrame({
                    'id': [species_id],
                    'pu_area': [pu_area],
                    'pu_count': [pu_count]
                }).astype({'id': 'int', 'pu_area': 'float', 'pu_count': 'int'})

                feature_preprocessing_path = os.path.join(
                    self.folder_input, "feature_preprocessing.dat")
                write_df_to_file(feature_preprocessing_path, summary_record)

            except ServicesError as e:
                self.close({'error': e.args[0]})
                return

            # Update input.dat and finalize response
            update_file_parameters(
                os.path.join(self.folder_project, "input.dat"),
                {'PUVSPRNAME': "puvspr.dat"}
            )

            self.close({
                'info': f"Feature '{alias}' preprocessed",
                'feature_class_name': feature_class_name,
                'pu_area': str(pu_area),
                'pu_count': str(pu_count),
                'id': str(species_id)
            })


class ProcessProtectedAreas(QueryWebSocketHandler):
    """
    REST WebSocket Handler for processing protected areas by intersecting them with planning grids.
    Supports preprocessing for a single project or reprocessing for multiple projects.

    Args:
        user (str): The name of the user (required for reprocessing).
        project (str): The name of the project (required for preprocessing).
        planning_grid_name (str): The name of the planning grid (required for preprocessing).

    Returns:
        WebSocket dict messages with keys:
            "info": Informational messages on the operation,
            "elapsedtime": The elapsed time in seconds of the run,
            "status": "Preprocessing" or "Finished",
            "intersections": A list of intersection data normalized by IUCN category (for preprocessing only).
    """

    async def open(self):  # sourcery skip: use-contextlib-suppress
        try:
            # Determine the mode (preprocessing or reprocessing) based on provided arguments
            if 'planning_grid_name' in self.request.arguments:
                await self.preprocess()
            elif 'user' in self.request.arguments:
                await self.reprocess()
            else:
                raise ValueError(
                    "Invalid arguments. Provide either 'planning_grid_name' or 'user'.")
        except ServicesError:  # Handle authentication/authorization errors
            pass

    async def preprocess(self):
        """
        Preprocess protected areas for a single project.
        """
        validate_args(self.request.arguments, [
            'user', 'project', 'planning_grid_name'])

        planning_grid_name = self.get_argument('planning_grid_name')
        await process_protected_areas(self, planning_grid_name=planning_grid_name, folder=self.folder_input)

        # Load intersection data
        protected_areas_df = file_data_to_df(
            os.path.join(self.folder_input, "protected_area_intersections.dat")
        )
        self.protectedAreaIntersectionsData = normalize_dataframe(
            protected_areas_df, "iucn_cat", "puid"
        )

        # Respond with results
        if not self.protectedAreaIntersectionsData:
            self.close(
                {'error': "No intersections between the protected areas and planning grid."})
        else:
            self.close({
                'info': 'Preprocessing finished',
                'intersections': self.protectedAreaIntersectionsData
            })

    async def reprocess(self):
        """
        Reprocess protected areas for all projects in the specified folder.
        """
        validate_args(self.request.arguments, ['user'])

        user = self.get_argument('user')
        folder = (
            project_paths.CASE_STUDIES_FOLDER
            if user == 'case_studies' else os.path.join(project_paths.USERS_FOLDER, user)
        )

        project_folders = await process_protected_areas(self, folder=folder)

        # Respond with results
        self.close({'info': 'Reprocessing finished',
                   'projects': project_folders})


class preprocessPlanningUnits(QueryWebSocketHandler):
    """REST WebSocket Handler. Preprocesses the planning units to get the boundary lengths where they intersect - produces the bounds.dat file. The required arguments in the request.arguments parameter are:

    Args:
        user (string): The name of the user.
        project (string): The name of the project.
    Returns:
        WebSocket dict messages with one or more of the following keys (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message,
            "elapsedtime": The elapsed time in seconds of the run,
            "status": One of Preprocessing or Finished
        }
    """

    async def open(self):
        try:
            await super().open({'info': "Calculating boundary lengths"})
        except ServicesError:  # authentication/authorisation error
            pass
        else:
            validate_args(self.request.arguments, ['user', 'project'])
            # get the project data
            await get_project_data(pg, self)
            if (not self.projectData["metadata"]["OLDVERSION"]):
                # new version of marxan - get the boundary lengths
                feature_class_name = get_unique_feature_name("tmp_")
                await pg.execute(sql.SQL("DROP TABLE IF EXISTS marxan.{};").format(sql.Identifier(feature_class_name)))
                # do the intersection
                results = await self.executeQuery(sql.SQL("CREATE TABLE marxan.{feature_class_name} AS SELECT DISTINCT a.puid id1, b.puid id2, ST_Length(ST_CollectionExtract(ST_Intersection(ST_Transform(a.geometry, 3410), ST_Transform(b.geometry, 3410)), 2))/1000 boundary  FROM marxan.{planning_unit_name} a, marxan.{planning_unit_name} b  WHERE a.puid < b.puid AND ST_Touches(a.geometry, b.geometry);").format(feature_class_name=sql.Identifier(feature_class_name), planning_unit_name=sql.Identifier(self.projectData["metadata"]["PLANNING_UNIT_NAME"])))
                # delete the file if it already exists
                if (os.path.exists(self.folder_input + "bounds.dat")):
                    os.remove(self.folder_input + "bounds.dat")
                # write the boundary lengths to file
                await pg.execute(sql.SQL("SELECT * FROM marxan.{};").format(sql.Identifier(feature_class_name)), return_format="File", filename=self.folder_input + "bounds.dat")
                # delete the tmp table
                await pg.execute(sql.SQL("DROP TABLE IF EXISTS marxan.{};").format(sql.Identifier(feature_class_name)))
                # update the input.dat file
                update_file_parameters(
                    self.folder_project + "input.dat", {'BOUNDNAME': 'bounds.dat'})
            # set the response
            self.close({'info': 'Boundary lengths calculated'})

# wss://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/createPlanningUnitGrid?iso3=AND&domain=Terrestrial&areakm2=50&shape=hexagon


class createPlanningUnitGrid(QueryWebSocketHandler):
    """REST WebSocket Handler. Creates a new planning grid. Sends an error if the planning grid already exists. The required arguments in the request.arguments parameter are:

    Args:
        iso3 (string): The country iso3 3-letter code.
        domain (string): The domain for the planning grid. One of marine or terrestrial.
        areakm2 (string): The area of the planning grid in Km2.
        shape (string): The shape of the planning grid units. One of square or hexagon.
    Returns:
        WebSocket dict messages with one or more of the following keys (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message,
            "elapsedtime": The elapsed time in seconds of the run,
            "status": One of Preprocessing or Finished,
            "feature_class_name": The name of the feature class created,
            "alias": The alias of the planning grid created,
            "uploadId": The Mapbox tileset upload id
        }
    """

    async def open(self):
        try:
            await super().open({'info': "Creating planning grid.."})
        except ServicesError:  # authentication/authorisation error
            pass
        else:
            validate_args(self.request.arguments,
                          ['iso3', 'domain', 'areakm2', 'shape'])
            area_km2 = self.get_argument('areakm2')
            iso3 = self.get_argument('iso3')
            domain = self.get_argument('domain')
            shape = self.get_argument('shape')
            # get the feature class name
            fc = "pu_" + iso3.lower() + "_" + domain.lower() + "_" + \
                shape.lower() + "_" + areakm2
            # see if the planning grid already exists
            records = await pg.execute(
                "SELECT * FROM marxan.metadata_planning_units WHERE feature_class_name =(%s);",
                data=[fc],
                return_format="Array")
            if len(records):
                self.close({'error': "That item already exists"})
            else:
                # estimate how many planning units are in the grid that will be created
                table = "marxan.gaul_2015_simplified_1km" if domain.lower(
                ) == 'terrestrial' else "marxan.eez_simplified_1km"

                # Execute the query to estimate the planning unit count
                query = f"""
                    SELECT ST_Area(ST_Transform(wkb_geometry, 3410)) / (%s * 1000000)
                    FROM {table}
                    WHERE iso3 = %s;
                """
                result = await pg.execute(query, data=[area_km2, iso3], return_format="Array")

                # Return the count from the query result
                unitCount = result[0][0]

                # see if the unit count is above the project_paths.PLANNING_GRID_UNITS_LIMIT
                if (int(unitCount) > project_paths.PLANNING_GRID_UNITS_LIMIT):
                    self.close({'error': "Number of planning units &gt; " + str(
                        project_paths.PLANNING_GRID_UNITS_LIMIT) + " (=" + str(int(unitCount)) + ")."})
                else:
                    results = await self.executeQuery("SELECT * FROM marxan.planning_grid(%s,%s,%s,%s,%s);", [area_km2, iso3, domain, shape, self.get_current_user()], return_format="Array")
                    if results:
                        # get the planning grid alias
                        alias = results[0][0]
                        # create a primary key so the feature class can be used in ArcGIS
                        # await pg.createPrimaryKey(fc, "puid")
                        # start the upload to Mapbox
                        uploadId = await upload_tileset_to_mapbox(fc, fc)
                        # set the response
                        self.close({'info': "Planning grid '" + alias + "' created",
                                    'feature_class_name': fc,
                                    'alias': alias,
                                    'uploadId': uploadId})

# wss://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/runGapAnalysis?user=admin&project=British%20Columbia%20Marine%20Case%20Study


class runGapAnalysis(QueryWebSocketHandler):
    """REST WebSocket Handler. Runs a gap analysis. The required arguments in the request.arguments parameter are:

    Args:
        user (string): The name of the user.
        project (string): The name of the project.
    Returns:
        WebSocket dict messages with one or more of the following keys (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message,
            "elapsedtime": The elapsed time in seconds of the run,
            "status": One of Preprocessing or Finished,
            "data": dict[]: The gap analysis results. Each dict has the keys: country_area,current_protected_area,current_protected_percent,endemic,total_area,_alias,_feature_class_name
        }
    """

    async def open(self):
        try:
            await super().open({'info': "Running gap analysis.."})
        except ServicesError:  # authentication/authorisation error
            pass
        else:
            validate_args(self.request.arguments, ['user', 'project'])
            # get the identifiers of the features for the project
            df = file_data_to_df(os.path.join(
                self.folder_input, self.projectData["files"]["SPECNAME"]))

            featureIds = df['id'].to_numpy().tolist()
            # get the planning grid name
            await get_project_data(pg, self)
            # get a safe project name to use in the name of the table that will be produced
            project_name = get_safe_project_name(self.get_argument("project"))
            # run the gap analysis
            df = await self.executeQuery("SELECT * FROM marxan.gap_analysis(%s,%s,%s,%s)", data=[self.projectData["metadata"]["PLANNING_UNIT_NAME"], featureIds, self.get_argument("user"), project_name], return_format="DataFrame")
            # return the results
            self.close({'info': "Gap analysis complete",
                        'data': df.to_dict(orient="records")})


class resetDatabase(QueryWebSocketHandler):
    """REST WebSocket Handler. Resets the database and files to their original state. This can only be run if the server configuration parameter project_paths.ENABLE_RESET is set to True. The required arguments in the request.arguments parameter are:

    Args:
        None
    Returns:
        WebSocket dict messages with one or more of the following keys (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Contains detailed progress information on the database reset,
            "elapsedtime": The elapsed time in seconds of the run,
            "status": One of Preprocessing or Finished
        }
    """

    async def open(self):
        try:
            await super().open({'info': "Resetting database.."})
        except ServicesError:
            pass
        else:
            # check the request is not coming from the local machine, i.e. being run directly and not from a web client which is safer
            if self.request.remote_ip == "127.0.0.1":
                self.close({'error': "Unable to run from localhost"})
            else:
                # run git reset --hard
                cmd = "git reset --hard"
                self.send_response(
                    {'status': 'Preprocessing', 'info': "Running git reset --hard"})
                result = await run_command(cmd)
                # delete all users other than admin, guest and _clumping
                user_folders = glob.glob(
                    project_paths.USERS_FOLDER + "*/")
                for user_folder in user_folders:
                    if os.path.split(user_folder[:-1])[1] not in ['admin', '_clumping', 'guest']:
                        shutil.rmtree(user_folder)
                # delete the features that are not in use
                specDatFiles = get_files_in_folder(
                    project_paths.CASE_STUDIES_FOLDER, "spec.dat")
                # iterate through the spec.dat files and get a unique list of feature ids
                featureIdsToKeep = []
                for file in specDatFiles:
                    # load the spec.dat file
                    df = file_data_to_df(file)
                    # get the unique feature ids
                    ids = df.id.unique().tolist()
                    # merge these ids into the featureIds array
                    featureIdsToKeep.extend(ids)
                # delete the features that are not in use
                df = await pg.execute("DELETE FROM marxan.metadata_interest_features WHERE NOT oid = ANY (ARRAY[%s]);", data=[featureIdsToKeep])
                self.send_response(
                    {'status': 'Preprocessing', 'info': "Deleted features"})
                # delete the planning grids that are not in use
                planningGridFiles = get_files_in_folder(
                    project_paths.CASE_STUDIES_FOLDER, "input.dat")
                # iterate through the input.dat files and get a unique list of planning grids
                planningGridsToKeep = []
                for file in planningGridFiles:
                    # get the input.dat file data
                    tmpObj = ExtendableObject()
                    tmpObj.project = "unimportant"
                    tmpObj.folder_project = os.path.dirname(file) + os.sep
                    await get_project_data(pg, tmpObj)
                    # get the planning grid
                    planningGridsToKeep.append(
                        tmpObj.projectData["metadata"]['PLANNING_UNIT_NAME'])
                df = await pg.execute("DELETE FROM marxan.metadata_planning_units WHERE NOT feature_class_name = ANY (ARRAY[%s]);", data=[planningGridsToKeep])
                self.send_response(
                    {'status': 'Preprocessing', 'info': "Deleted planning grids"})
                # run a cleanup
                self.send_response(
                    {'status': 'Preprocessing', 'info': "Cleaning up.."})
                await cleanup()
                self.close({'info': "Reset complete"})


class updateWDPA(QueryWebSocketHandler):
    """REST WebSocket Handler. Updates the WDPA table in PostGIS using the publically available downloadUrl. The required arguments in the request.arguments parameter are:

    Args:
        downloadUrl (string): The url endpoint where the new version of the WDPA can be downloaded from. This is normally set in the Marxan Registry.
    Returns:
        WebSocket dict messages with one or more of the following keys (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Contains detailed progress information on the update,
            "elapsedtime": The elapsed time in seconds of the update,
            "status": One of Preprocessing or Finished
        }
    """
    # authenticate and get the user folder and project folders

    async def open(self):
        try:
            await super().open({'info': "Updating WDPA.."})
        except ServicesError:  # authentication/authorisation error
            pass
        else:
            validate_args(self.request.arguments, ['downloadUrl'])
            if "unittest" in list(self.request.arguments.keys()):
                unittest = True
                # if we are running a unit test then download the WDPA from a minimal zipped file geodatabase on google storage
                downloadUrl = 'https://storage.googleapis.com/geeimageserver.appspot.com/WDPA_Jun2020.zip'
            else:
                unittest = False
                downloadUrl = self.get_argument("downloadUrl")
            try:
                # download the new wdpa zip
                self.send_response(
                    {'status': 'Preprocessing', 'info': "Downloading " + downloadUrl})
                await self.asyncDownload(downloadUrl, project_paths.IMPORT_FOLDER + "wdpa.zip")
            except (ServicesError) as e:  # download failed
                self.close({'error': e.args[0], 'info': 'WDPA not updated'})
            else:
                self.send_response(
                    {'status': 'Preprocessing', 'info': "WDPA downloaded"})
                try:
                    # download finished - upzip the file geodatabase
                    self.send_response({
                        'status': 'Preprocessing',
                        'info': "Unzipping file geodatabase '" + "wdpa.zip" + "'"
                    })
                    files = await IOLoop.current().run_in_executor(None, unzip_file, project_paths.IMPORT_FOLDER, "wdpa.zip")
                    # check the contents of the unzipped file - the contents should include a folder ending in .gdb - this is the file geodatabase
                    fileGDBPath = [f for f in files if f[-5:]
                                   == '.gdb' + os.sep][0]
                except IndexError:  # file geodatabase not found
                    self.close(
                        {'error': "The WDPA file geodatabase was not found in the zip file", 'info': 'WDPA not updated'})
                # error unzipping - probably the disk space has run out
                except (ServicesError) as e:
                    self.close(
                        {'error': e.args[0], 'info': 'WDPA not updated'})
                else:
                    self.send_response(
                        {'status': 'Preprocessing', 'info': "Unzipped file geodatabase"})
                    # delete the zip file
                    os.remove(project_paths.IMPORT_FOLDER + "wdpa.zip")
                    # get the name of the source feature class - this will be WDPA_poly_<shortmonth><year>, e.g. WDPA_poly_Jun2020 and can be taken from the file geodatabase path, e.g. WDPA_Jun2020_Public/WDPA_Jun2020_Public.gdb/
                    sourceFeatureClass = 'WDPA_poly_' + fileGDBPath[5:12]
                    try:
                        # import the new wdpa into a temporary PostGIS feature class in EPSG:4326
                        # get a unique feature class name for the tmp imported feature class - this is necessary as ogr2ogr automatically creates a spatial index called <featureclassname>_geometry_geom_idx on import - which will end up being the name of the index on the wdpa table preventing further imports (as the index will already exist)
                        feature_class_name = get_unique_feature_name(
                            "wdpa_")
                        self.send_response(
                            {'status': "Preprocessing", 'info': "Importing '" + sourceFeatureClass + "' into PostGIS.."})
                        # import the wdpa to a tmp feature class
                        await pg.importFileGDBFeatureClass(project_paths.IMPORT_FOLDER, fileGDBPath, sourceFeatureClass, feature_class_name, splitAtDateline=False)
                        self.send_response(
                            {'status': "Preprocessing", 'info': "Imported into '" + feature_class_name + "'"})
                        if not unittest:
                            # rename the existing wdpa feature class
                            await pg.execute("ALTER TABLE marxan.wdpa RENAME TO wdpa_old;")
                            self.send_response(
                                {'status': "Preprocessing", 'info': "Renamed 'wdpa' to 'wdpa_old'"})
                            # rename the tmp feature class
                            await pg.execute(sql.SQL("ALTER TABLE marxan.{} RENAME TO wdpa;").format(sql.Identifier(feature_class_name)))
                            self.send_response(
                                {'status': "Preprocessing", 'info': "Renamed '" + feature_class_name + "' to 'wdpa'"})
                            # drop the columns that are not needed
                            await pg.execute("ALTER TABLE marxan.wdpa DROP COLUMN IF EXISTS ogc_fid,DROP COLUMN IF EXISTS wdpa_pid,DROP COLUMN IF EXISTS pa_def,DROP COLUMN IF EXISTS name,DROP COLUMN IF EXISTS orig_name,DROP COLUMN IF EXISTS desig_eng,DROP COLUMN IF EXISTS desig_type,DROP COLUMN IF EXISTS int_crit,DROP COLUMN IF EXISTS marine,DROP COLUMN IF EXISTS rep_m_area,DROP COLUMN IF EXISTS gis_m_area,DROP COLUMN IF EXISTS rep_area,DROP COLUMN IF EXISTS gis_area,DROP COLUMN IF EXISTS no_take,DROP COLUMN IF EXISTS no_tk_area,DROP COLUMN IF EXISTS status_yr,DROP COLUMN IF EXISTS gov_type,DROP COLUMN IF EXISTS own_type,DROP COLUMN IF EXISTS mang_auth,DROP COLUMN IF EXISTS mang_plan,DROP COLUMN IF EXISTS verif,DROP COLUMN IF EXISTS metadataid,DROP COLUMN IF EXISTS sub_loc,DROP COLUMN IF EXISTS parent_iso;")
                            self.send_response(
                                {'status': "Preprocessing", 'info': "Removed unneccesary columns"})
                            # delete the old wdpa feature class
                            await pg.execute("DROP TABLE IF EXISTS marxan.wdpa_old;")
                            self.send_response(
                                {'status': "Preprocessing", 'info': "Deleted 'wdpa_old' table"})
                            # delete all of the existing dissolved country wdpa feature classes
                            await pg.execute("SELECT * FROM marxan.deleteDissolvedWDPAFeatureClasses()")
                            self.send_response(
                                {'status': "Preprocessing", 'info': "Deleted dissolved country WDPA feature classes"})
                        else:
                            # delete the tmp feature
                            await pg.execute(sql.SQL("DROP TABLE IF EXISTS marxan.{}").format(sql.Identifier(feature_class_name)))
                            self.send_response(
                                {'status': "Preprocessing", 'info': "Unittest has not replaced existing WDPA file"})
                    except (OSError) as e:  # TODO Add other exception classes especially PostGIS ones
                        self.close(
                            {'error': 'No space left on device importing the WDPA into PostGIS', 'info': 'WDPA not updated'})
                    else:
                        if not unittest:
                            # delete all of the existing intersections between planning units and the old version of the WDPA
                            self.send_response(
                                {'status': "Preprocessing", 'info': 'Invalidating existing WDPA intersections'})
                            intersection_files = get_files_in_folder(
                                project_paths.USERS_FOLDER, "protected_area_intersections.dat")

                            # Path to the empty template file
                            empty_file_path = os.path.join(
                                project_paths.EMPTY_PROJECT_TEMPLATE_FOLDER, "input", "protected_area_intersections.dat")

                            # Replace each file with the empty template
                            for file_path in intersection_files:
                                shutil.copyfile(empty_file_path, file_path)
                            # redo the protected area preprocessing on any of the case studies that are included by default with all newly registered users - otherwise the existing data in the input/protected_area_intersections.dat files will be out-of-date
                            await _reprocessProtectedAreas(self, project_paths.CASE_STUDIES_FOLDER)
                            # update the WDPA_VERSION variable in the server.dat file
                            update_file_parameters(
                                project_paths.PROJECT_FOLDER + "server.dat", {"WDPA_VERSION": self.get_argument("wdpaVersion")})
                        else:
                            self.send_response(
                                {'status': "Preprocessing", 'info': "Unittest has not invalidated existing WDPA intersections"})
                        # send the response
                        self.close(
                            {'info': 'WDPA update completed succesfully'})
                finally:
                    # delete the zip file
                    if os.path.exists(project_paths.IMPORT_FOLDER + "wdpa.zip"):
                        os.remove(
                            project_paths.IMPORT_FOLDER + "wdpa.zip")
                    # delete the unzipped files
                    for f in files:
                        if os.path.exists(project_paths.IMPORT_FOLDER + f):
                            try:
                                os.remove(project_paths.IMPORT_FOLDER + f)
                            except IsADirectoryError:
                                shutil.rmtree(
                                    project_paths.IMPORT_FOLDER + f)

    async def asyncDownload(self, url, file):
        """Downloads the WDPA asyncronously.

        Args:
            url (string): The url to download the file geodatabase from.
            file (string): The name of the file to save as.
        Returns:
            None
        """
        # initialise a variable to hold the size downloaded
        file_size_dl = 0

        try:
            http_client = AsyncHTTPClient()
            # Disable request timeout
            request = HTTPRequest(url, request_timeout=None)
            response = await http_client.fetch(request, streaming_callback=None, raise_error=True)

            # Get the content length if available
            file_size = int(response.headers.get("Content-Length", 0))
            file_size_dl = 0

            try:
                with open(file, "wb") as f:
                    async for chunk in response.body_stream:
                        f.write(chunk)
                        file_size_dl += len(chunk)

                        # Update the download progress
                        self.ping_message = f"{
                            int((file_size_dl / file_size) * 100)} Completed "

            except Exception as e:
                raise ServicesError("Error getting a file: %s" % e)
            finally:
                f.close()
                delattr(self, 'ping_message')

        except Exception as e:
            raise ServicesError("Error getting the url: %s" % url)
        except (OSError) as e:  # out of disk space probably
            f.close()
            os.remove(file)
            raise ServicesError("Out of disk space on device")
# ****
#  *********
#  *****************
#  ***********************
#  ***********************************
#  *******************************************
#  ******************************************************
#  ************************************************************************
#  *********************************************************************************
#  ***************************************************************************************************
# * tornado functions
#  ***************************************************************************************************
#  *********************************************************************************
#  ******************************************************
#  ***********************************


def getPressuresActivitiesDatabase(padfile_path):
    try:
        pad = pd.read_sql('select * from marxan.pad', con=engine)
    except exc.ProgrammingError as err:
        print(err)
    finally:
        pad = pd.read_csv(padfile_path)
        pad.columns = pad.columns.str.lower()
        pad["rppscore"] = np.where(
            pad['rpptitle'] == 'low', 0.3, 1)
        pad.to_sql('pad', con=engine, schema='marxan', if_exists='replace')
    return pad


class GetAtlasLayersHandler(BaseHandler):
    """
    Get the atlas layers from the atlas GMS and allow them to be added to the map

    Args:
        RequestHandler (RequestHandler): Tornado handler class for handling requests
    """

    def get(self):
        user = 'cartig'
        password = 'x88F#haYZ8E3h&'
        layers = []
        # try getting the details from the server and if theres an issue fall back to local file version
        # local file version is from Friday 28th Feb 2020
        try:
            r = requests.get('http://www.atlas-horizon2020.eu/gs/wms?request=getCapabilities',
                             auth=(user, password))
            try:
                root = ET.fromstring(r.text)
                for layer in root.iter('{http://www.opengis.net/wms}Layer'):
                    try:
                        layer_link = layer.find(
                            # .encode('utf8')
                            '{http://www.opengis.net/wms}Name').text
                        title_name = layer.find(
                            # .encode('utf8')
                            '{http://www.opengis.net/wms}Title').text
                        layers.append(json.dumps({
                            'title': title_name,
                            'layer': layer_link
                        }))
                    except AttributeError:
                        continue
            except ET.ParseError:
                with open('./data/layers.json') as json_file:
                    layers = json.load(json_file)
                    print('layers: ', layers)
        except ConnectionError as error:
            with open('./data/layers.json') as json_file:
                layers = json.load(json_file)
                print('layers: ', layers)

        print('json.dumps(layers): ', json.dumps(layers))
        self.finish(json.dumps(layers))


class GetActivitiesHandler(BaseHandler):
    async def get(self):
        pad = getPressuresActivitiesDatabase(config['pad'])
        try:
            activities = []
            activitytitles = pad.activitytitle.unique()

            for idx, act in enumerate(activitytitles):
                activities.append({
                    "category": pad[pad.activitytitle == act].categorytitle.unique()[0],
                    "activity": act
                })
            self.send_response({"data": json.dumps(activities)})
        except Exception as e:
            print(self, e.args[0])


async def _getAllImpacts(obj):
    """Gets all feature information from the PostGIS database. These are set on the passed obj in the allImpacts attribute.

    Args:
        obj (BaseHandler): The request handler instance.
    Returns:
        None
    """
    print('getting all impacts.......')
    obj.allImpacts = await pg.execute("SELECT feature_class_name, alias, description, extent, to_char(creation_date, 'DD/MM/YY HH24:MI:SS')::text AS creation_date, tilesetid, source, id, created_by FROM marxan.metadata_impacts ORDER BY lower(alias);", return_format="DataFrame")


class GetAllImpactsHandler(BaseHandler):
    """REST HTTP handler. Gets all species information from the PostGIS database. The required arguments in the request.arguments parameter are:

    Args:
        None
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
        "info": Informational message,
        "data": dict[]: A list of the features. Each dict contains the keys: id,feature_class_name,alias,description,area,extent,creation_date,tilesetid,source,created_by
        }
    """

    async def get(self):
        print('Get all impacts handerler.....')
        try:
            # get all the species data
            await _getAllImpacts(self)
            # set the response
            self.send_response({"info": "All impact data received",
                                "data": self.allImpacts.to_dict(orient="records")})
        except Exception as e:
            print(self, e.args[0])


class GetUploadedActivitiesHandler(BaseHandler):
    """
        REST HTTP handler. Gets all species information from the PostGIS database. The required arguments in the request.arguments parameter are:

        Args:
            None
        Returns:
            A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

            {
            "info": Informational message,
            "data": dict[]: A list of the features. Each dict contains the keys: id,feature_class_name,alias,description,area,extent,creation_date,tilesetid,source,created_by
            }
    """

    async def get(self):
        print('Get all uploaded activities handerler.....')
        try:
            # get all the species data
            self.allUploadedActivities = await pg.execute("SELECT id, filename, activity, description, to_char(creation_date, 'DD/MM/YY HH24:MI:SS')::text AS creation_date, source, created_by FROM marxan.metadata_activities ORDER BY lower(activity);", return_format="DataFrame")

            # set the response
            self.send_response({"info": "All activity data received",
                                "data": self.allUploadedActivities.to_dict(orient="records")})
        except Exception as e:
            print(self, e.args[0])


class UploadRasterHandler(BaseHandler):
    def post(self):
        try:
            print('uploadRaster....')
            validate_args(self.request.arguments, [
                'activity', 'filename'])
            activity = self.get_argument('activity')
            filename = self.get_argument('filename')

            uploaded_rast = self.request.files['value'][0].body

            with engine.begin() as connection:
                pad_data = connection.execute(
                    f"""select activitytitle, pressuretitle, rppscore from marxan.pad WHERE pad.activitytitle = '{activity}';""").fetchall()

            print('reprojecting and normalising uploaded raster....')
            with MemoryFile(uploaded_rast) as memfile:
                ds = memfile.open()
                uploaded = reproject_and_normalise_upload(
                    raster_name=filename,
                    data=ds,
                    reprojection_crs=config["jose_crs_str"],
                    wgs84=config["wgs84_str"],
                )

            print('creating pressures.....')
            with rasterio.open(uploaded, 'r+') as src:
                meta = src.meta
                band = np.ma.masked_values(src.read(1, masked=True), 0)
                for pressure in pad_data:
                    new_band = band.copy()
                    update_band = new_band*pressure[2]
                    title = 'data/pressures/' + \
                        replace_chars(pressure[1]) + '.tif'
                    with rasterio.open(title, 'w', **meta) as dst:
                        dst.write(update_band, 1)

            self.send_response({
                'info': "File '" + filename + "' uploaded and pressures created",
                'file': filename
            })
        except ServicesError as e:
            raise_error(self, e.args[0])
        finally:
            return


async def save_rast_metadata(description, filename, activity, activity_name):
    print('saving raster metadata...')
    data_array = None
    try:
        data_array = await pg.execute(sql.SQL("""
            INSERT INTO marxan.metadata_activities
            (creation_date, description, source, created_by,
             filename, activity, activity_name, extent)
            SELECT
                now(), %s, %s, %s, %s, %s, %s, rast.extent FROM
                (SELECT Box2D(ST_Envelope(rast)) extent
                FROM ( SELECT rid, rast FROM marxan.{}) as rast2 ) as rast
            RETURNING id""").format(sql.Identifier(activity_name)),
            data=[description, "raster", "cartig",
                  filename.lower(), activity, activity_name],
            return_format="Array")
        print('data_array: ', data_array)
        return data_array
    except (ServicesError) as e:
        print('e: ', e)
        self.close({
            'error': e.args[0],
            'info': 'Error uploading to database....'
        })
    finally:
        if data_array is not None:
            return data_array[0]
        return


class SaveRasterHandler(WebSocketHandler):
    async def open(self):
        try:
            await super().open({'info': "Uploading raster to database..."})
        except ServicesError as e:  # authentication/authorisation error
            print('ServicesError as e: ', e)
            pass
        else:
            print('saveRasterHandler...')
            validate_args(self.request.arguments,
                          ['activity', 'filename', 'description'])
            activity = self.get_argument('activity')
            filename = self.get_argument('filename')
            description = self.get_argument('description')
            raster_loc = 'data/tmp/' + filename.lower()
            activity_name = get_unique_feature_name("activity_")
            connection = psql_str()
            try:
                cmds = "raster2pgsql -s 100026 -d -I -C -F " + raster_loc + \
                    " marxan." + activity_name + connection
                subprocess.call(cmds, shell=True)
            except TypeError as e:
                print('e: ', e)
                self.close({
                    'error': e.args[0],
                    'info': 'Error saving raster to database....'
                })
            try:
                data_array = await save_rast_metadata(description, filename, activity, activity_name)
                self.close({
                    'info': "Raster uploaded and saved to database",
                    'data_array': data_array
                })

            except (ServicesError) as e:
                print('e: ', e)
                self.close({
                    'error': e.args[0],
                    'info': 'Error uploading to database....'
                })


def setup_sens_matrix():
    print('Setting up sensitivity matrix....')
    habitat_list = [item['label']
                    for item in
                    get_tif_list('/'+config['input_coral'], 'asc') +
                    get_tif_list('/'+config['input_fish'], 'asc')]
    sens_mat = project_paths['sensmat']
    print('habitat_list: ', habitat_list)
    for habitat_name in habitat_list:
        sens_mat.loc[habitat_name] = sens_mat.loc['VME']
    return sens_mat


async def _finishImportingImpact(feature_class_name, activity, description, user):
    """Finishes creating a feature by adding a spatial index and a record in the metadata_interest_features table.

    Args:
        feature_class_name (string): The feature class to finish creating.
       activity (string): Theactivity of the feature class that will be used as an alias in the metadata_interest_features table.
        source (string): The source for the feature.
        user (string): The user who created the feature.
    Returns:
        int: The id of the feature created.
    Raises:
        ServicesError: If the feature already exists.
    """
    print('finishing importing raster...')
    # get the Mapbox tilesetId
    id = None
    tilesetId = config['mbuser'] + "." + feature_class_name
    try:
        # create a record for this new feature in the metadata_interest_features table
        print("creating record for feature in db")
        id = await pg.execute(
            sql.SQL("""
            INSERT INTO marxan.metadata_impacts (feature_class_name, alias, description, creation_date, tilesetid, extent, source, created_by) SELECT %s, %s, %s, now(), %s, rast.extent, %s, %s FROM (SELECT Box2D(ST_Envelope(rast)) extent FROM ( SELECT rid, rast FROM marxan.{}) as rast2 ) as rast RETURNING tableoid""")
            .format(sql.Identifier(feature_class_name)),
            data=[feature_class_name, activity, description,
                  tilesetId, "raster", "cartig"],
            return_format="Array")
        return id
    except (Exception) as e:
        print('Unable to create record in db e: ', e)
    finally:
        if id is not None:
            return id[0]
        return


class CumulativeImpactHandler(WebSocketHandler):
    async def open(self):
        try:
            await super().open({'info': "Running Cumulative Impact Function..."})
        except ServicesError as e:  # authentication/authorisation error
            print('ServicesError as e: ', e)
            pass
        else:
            # validate the input arguments
            id = None
            nodata_val = 0
            validate_args(self.request.arguments, ['selectedIds'])
            activityIds = self.get_argument('selectedIds')
            print('activityIds: ', activityIds)
            sql = "select activity, description from marxan.metadata_activities where id = %s ;" % activityIds
            records = await pg.execute(sql, return_format="Array")
            records = records[0]
            activity = records[0]
            description = records[1]
            connect_str = psql_str()
            feature_class_name = get_unique_feature_name("impact_")
            self.send_response(
                {'Preprocessing': "Building sensitivity matrix..."})
            stressors_list = get_tif_list('/data/pressures', 'tif')
            ecosys_list = get_tif_list('/data/rasters/ecosystem', 'tif')
            sens_mat = setup_sens_matrix()
            self.send_response(
                {'Preprocessing': "Running Cumulative impact function..."})
            print({'Preprocessing': "Running Cumulative impact function..."})

            impact, meta = cumul_impact(ecosys_list,
                                        sens_mat,
                                        stressors_list,
                                        nodata_val)

            self.send_response({'Preprocessing': "Reprojecting rasters..."})
            print({'Preprocessing': "Reprojecting rasters..."})
            reproject_raster_to_all_habs(tmp_file='./data/tmp/impact2.tif',
                                         data=impact,
                                         meta=meta,
                                         out_file='./data/tmp/impact.tif')
            impact_file = 'data/tmp/impact.tif'
            cropped_impact = 'data/uploaded_rasters/'+feature_class_name+'.tif'
            project_raster(rast1='data/tmp/impact.tif',
                           template_file='data/rasters/all_habitats.tif',
                           output_file=impact_file)

            wgs84_rast = reproject_raster(file_path=impact_file,
                                          output_folder='data/uploaded_rasters/')
            try:
                self.send_response(
                    {'info': 'Saving cumulative impact raster to database...'})
                cmds = "raster2pgsql -s 4326 -c -I -C -F " + wgs84_rast + \
                    " marxan." + feature_class_name + connect_str
                subprocess.call(cmds, shell=True)
            except TypeError as e:
                self.send_response(
                    {'error': 'Unable to save Cumulative Impact raster to database...'})
                print(
                    "Pass in the location of the file as a string, not anything else....")

            try:
                self.send_response(
                    {'info': 'Saving to meta data table and uploading to mapbox...'})
                id = await _finishImportingImpact(feature_class_name,
                                                  activity.replace(
                                                      ' ', '_').lower(),
                                                  description,
                                                  self.get_current_user())
                self.close({
                    'info': "Cumulative Impact run and raster uploaded to mapbox",
                    # 'uploadId': uploadId
                })

            except (ServicesError) as e:
                print('e: ', e)
                self.close({
                    'error': e.args[0],
                    'info': 'Failed to run CI function....'
                })


class CreateCostsFromImpactHandler(WebSocketHandler):
    async def open(self):
        print('CreateCostsFromImpactHandler: ')
        try:
            await super().open({'info': "Creating Costs from Cumulative Impact..."})
        except ServicesError as e:  # authentication/authorisation error
            print('ServicesError as e: ', e)
            pass
        else:
            # validate the input arguments

            print('self.get_argument(user): ', self.get_argument('user'))
            print('self.get_argument(project): ', self.get_argument('project'))

            id = None
            nodata_val = 0
            validate_args(self.request.arguments,
                          ['user', 'project', 'pu_filename',
                           'impact_filename', 'impact_type'])
            sql = "select filename from marxan.%s;" % self.get_argument(
                'impact_filename')
            records = await pg.execute(sql, return_format="Array")
            impact_filename = records[0][0]
            file_loc = "data/uploaded_rasters/" + impact_filename
            create_cost_from_impact(self.get_argument('user'),
                                    self.get_argument('project'),
                                    self.get_argument('pu_filename'),
                                    file_loc,
                                    self.get_argument('impact_type'))
            self.close({
                'info': "New cost file created from Cumulative Impact",
            })


def add_shapefile_to_db(filename, gridname, tablename):
    print('Adding filename: ', filename, " to database...")
    print('"| psql -h " + DATABASE_HOST + " -p " + db_config.PORT + \
            " -U " + DATABASE_USER + " -d " + DATABASE_NAME: ', "| psql -h " + db_config.DATABASE_HOST + " -p 5432 -U " + db_config.DATABASE_USER + " -d " + db_config.DATABASE_NAME)
    try:
        cmds = "shp2pgsql -d -g geom -I " + filename + " " + tablename +  \
            "| psql -h " + db_config.DATABASE_HOST + " -p 5432 -U " + \
            db_config.DATABASE_USER + " -d " + db_config.DATABASE_NAME

        subprocess.call(cmds, shell=True)
        return True
    except TypeError as e:
        print("Pass in the location of the file as a string, not anything else....")
        return False


class createMarinePlanningUnitGridHandler(QueryWebSocketHandler):
    """REST WebSocket Handler. Creates a new planning grid. Sends an error if the planning grid already exists. The required arguments in the request.arguments parameter are:

    Args:
        areakm2 (string): The area of the planning grid in Km2.
        shape (string): The shape of the planning grid units. One of square or hexagon.
    Returns:
        WebSocket dict messages with one or more of the following keys (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message,
            "elapsedtime": The elapsed time in seconds of the run,
            "status": One of Preprocessing or Finished,
            "feature_class_name": The name of the feature class created,
            "alias": The alias of the planning grid created,
            "uploadId": The Mapbox tileset upload id
        }
    """

    async def open(self):
        try:
            await super().open({'info': "Creating marine planning grid from shapefile.."})
        except ServicesError:  # authentication/authorisation error
            pass
        else:
            # validate args, set feature class name, see if grid already exists
            validate_args(self.request.arguments,
                          ['filename', 'planningGridName', 'areakm2', 'shape'])
            areakm2 = self.get_argument('areakm2')
            filename = self.get_argument('filename')
            gridname = self.get_argument('planningGridName')
            shape = self.get_argument('shape')
            fc = "pu_" + gridname + "_marine_" + shape.lower() + "_" + areakm2
            records = await pg.execute(
                "SELECT * FROM marxan.metadata_planning_units WHERE feature_class_name =(%s);",
                data=[fc],
                return_format="Array")
            if len(records):
                self.close({'error': "That item already exists"})
            else:
                # REPROJECT THE SHAPEFILE
                # unzip the shapefile and get the name of the shapefile without an extension, e.g. PlanningUnitsData.zip -> planningunits.shp -> planningunits
                rootfilename = await IOLoop.current().run_in_executor(None,
                                                                      unzip_shapefile,
                                                                      'data/tmp/',
                                                                      filename)
                check_zipped_shapefile('data/tmp/'+filename)
                repro_file = 'data/tmp/reprojected_'+rootfilename+'.shp'
                reproject_shape('data/tmp/'+rootfilename +
                                '.shp', repro_file, 4326)
                tablename = 'impact.'+fc
                # ADD THE SHAPEFILE TO THE DB
                try:
                    add_shapefile_to_db(repro_file, gridname, tablename)
                except ServicesError as e:
                    print('Shapefile not uploaded: ', e)
                    raise
                # MAKE SURE SHAPEFILE IS IN CORRECT CRS
                # UPLOAD SHAPEFILE TO DATABASE AND RETURN WHATS NEEDED
                # THEN PROCEED WITH THE HEXING FUNCTION

                results = await self.executeQuery("SELECT * FROM impact.planning_grid(%s,%s,%s,%s,%s);",
                                                  [areakm2, tablename, fc, shape, self.get_current_user()])
                print('results: ', results)
                if results:
                    # get the planning grid alias
                    alias = results[0][0]
                    # start the upload to Mapbox
                    # uploadId = await upload_tileset_to_mapbox(fc, fc)
                    # set the response
                    self.close({'info': "Planning grid '" + alias + "' created",
                                'feature_class_name': fc,
                                'alias': alias,
                                'uploadId': uploadId})

# wss://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/runGapAnalysis?user=admin&project=British%20Columbia%20Marine%20Case%20Study

# tornado functions
####################################################################################################################################################################################################################################################################


class Application(tornado.web.Application):
    """Tornado Application class which defines all of the request handlers."""

    def __init__(self):
        if not hasattr(db_config, 'COOKIE_SECRET') or not db_config.COOKIE_SECRET:
            raise ValueError("db_config.COOKIE_SECRET is not set.")

        settings = self._define_settings()
        handlers = self._define_handlers()
        super(Application, self).__init__(handlers, **settings)

    def _define_settings(self):
        """Define settings for the Tornado application."""
        return {
            'cookie_secret': db_config.COOKIE_SECRET,
            'static_path': project_paths.EXPORT_FOLDER,
            'static_url_prefix': '/resources/'
        }

    def _define_handlers(self):
        """Define all request handlers for the application."""

        return [
            ("/server/auth", AuthHandler),
            ("/server/projects", ProjectHandler, dict(pg=pg,
             get_species_data=get_species_data, update_species=update_species_file)),
            ("/server/exportProject", exportProject),
            ("/server/importProject", ImportProject),
            ("/server/users", UserHandler, dict(pg=pg)),
            ("/server/features", FeatureHandler, dict(pg=pg, finish_feature_import=finish_feature_import,
             upload_tileset_to_mapbox=upload_tileset_to_mapbox)),
            ("/server/planning-units", PlanningUnitHandler,
             dict(pg=pg, upload_tileset=upload_tileset)),

            ("/server/updateCosts", updateCosts),
            ("/server/deleteCost", deleteCost),
            ("/server/createCostsFromImpact", CreateCostsFromImpactHandler),



            ("/server/createFeaturePreprocessingFileFromImport",
             createFeaturePreprocessingFileFromImport),


            ("/server/importFeatures", importFeatures),
            ("/server/createFeaturesFromWFS", createFeaturesFromWFS),


            ("/server/deleteShapefile", deleteShapefile),

            ("/server/createPlanningUnitGrid", createPlanningUnitGrid),  # websocket
            ("/server/createMarinePlanningUnitGrid",
             createMarinePlanningUnitGridHandler),  # websocket


            ("/server/getPlanningUnitGrids", getPlanningUnitGrids),
            ("/server/importPlanningUnitGrid", ImportPlanningUnitGrid),
            ("/server/listProjectsForPlanningGrid", listProjectsForPlanningGrid),
            ("/server/getPlanningUnitsCostData", getPlanningUnitsCostData),
            ("/server/updatePUFile", UpdatePUFile),
            ("/server/getPUData", getPUData),


            ("/server/getServerData", getServerData),
            ("/server/getAtlasLayers", GetAtlasLayersHandler),
            ("/server/getActivities", GetActivitiesHandler),
            ("/server/getAllImpacts", GetAllImpactsHandler),
            ("/server/getCountries", getCountries),


            ("/server/getAllSpeciesData", getAllSpeciesData),
            ("/server/updateSpecFile", updateSpecFile),


            ("/server/uploadTilesetToMapBox", uploadTilesetToMapBox),
            ("/server/uploadFileToFolder", uploadFileToFolder),
            ("/server/unzipShapefile", unzipShapefile),
            ("/server/getShapefileFieldnames", getShapefileFieldnames),


            # currently not used - bugs in the Marxan output log
            ("/server/getMarxanLog", getMarxanLog),
            ("/server/getResults", getResults),
            ("/server/getSolution", GetSolution),
            ("/server/preprocessFeature", PreprocessFeature),
            ("/server/preprocessPlanningUnits", preprocessPlanningUnits),
            ("/server/processProtectedAreas", ProcessProtectedAreas),

            ("/server/runMarxan", runMarxan),
            ("/server/stopProcess", stopProcess),
            ("/server/getRunLogs", getRunLogs),
            ("/server/clearRunLogs", clearRunLogs),
            ("/server/updateWDPA", updateWDPA),
            ("/server/runGapAnalysis", runGapAnalysis),
            ("/server/deleteGapAnalysis", deleteGapAnalysis),

            ("/server/importGBIFData", importGBIFData),
            ("/server/dismissNotification", dismissNotification),
            ("/server/resetNotifications", resetNotifications),
            ("/server/testRoleAuthorisation", testRoleAuthorisation),
            ("/server/addParameter", addParameter),
            ("/server/resetDatabase", resetDatabase),
            ("/server/runSQLFile", runSQLFile),
            ("/server/cleanup", cleanup),
            ("/server/shutdown", shutdown),
            ("/server/testTornado", testTornado),

            ("/server/uploadRaster", UploadRasterHandler),
            ("/server/saveRaster", SaveRasterHandler),
            ("/server/getUploadedActivities", GetUploadedActivitiesHandler),
            ("/server/runCumumlativeImpact", CumulativeImpactHandler),


            ("/server/uploadFile", uploadFile),


            ("/server/exports/(.*)", StaticFileHandler,
             {"path": project_paths.EXPORT_FOLDER}),
            # default handler if the REST services is cannot be found on this server - maybe a newer client is requesting a method on an old server
            ("/server/(.*)", methodNotFound),
            # assuming the marxan-client is installed in the same folder as the marxan-server all files will go to the client build folder
            (r"/(.*)", StaticFileHandler, {"path": FRONTEND_BUILD_FOLDER})
        ]


async def initialiseApp():
    """Initialises the application with all of the global variables"""

    global project_paths, db_config

    project_paths = get_folder_path_config()
    db_config = DBConfig()
    await set_global_vars()

    # LOGGING SECTION
    # turn on logging. Get parent logger. Set the logging level. Set format for streaming logger
    tornado.options.parse_command_line()
    root_logger = logging.getLogger()
    root_logger.setLevel(LOGGING_LEVEL)

    root_streamhandler = root_logger.handlers[0]
    f1 = '%(color)s[%(levelname)1.1s %(asctime)s.%(msecs)03d]%(end_color)s '
    f2 = '%(message)s'
    root_streamhandler.setFormatter(LogFormatter(fmt=f1 + f2,
                                                 datefmt='%d-%m-%y %H:%M:%S',
                                                 color=True))
    # add a file logger
    if not project_paths.DISABLE_FILE_LOGGING:
        file_log_handler = logging.FileHandler(
            os.path.join(project_paths.PROJECT_FOLDER, 'marxan-server.log'))
        file_log_handler.setFormatter(LogFormatter(fmt=f1 + f2,
                                                   datefmt='%d-%m-%y %H:%M:%S',
                                                   color=False))
        root_logger.addHandler(file_log_handler)

    app = Application()
    # if there is an https certificate then use the certificate information from the server.dat file to return data securely
    if project_paths.CERTFILE is None:
        app.listen(db_config.SERVER_PORT)
    else:
        app.listen(db_config.SERVER_PORT,
                   ssl_options={"certfile": project_paths.CERTFILE,
                                "keyfile": project_paths.KEYFILE})

    protocol = "https://" if project_paths.CERTFILE != None else "http://"
    navigateTo = f"{protocol}<host>:{
        db_config.SERVER_PORT}/index.html" if db_config.SERVER_PORT != 80 else f"{protocol}<host>/index.html"

    # open the web browser if the call includes a url, e.g. python marxan-server.py http://localhost/index.html
    if len(sys.argv) > 1:
        if MARXAN_CLIENT_VERSION == "Not installed":
            log("Ignoring <url> parameter - the marxan-client is not installed", Fore.GREEN)
        else:
            url = sys.argv[1]  # normally "http://localhost/index.html"
            log("Opening Marxan Web at '" + url + "' ..\n", Fore.GREEN)
            webbrowser.open(url, new=1, autoraise=True)
    elif MARXAN_CLIENT_VERSION != "Not installed":
        log("Goto to " + navigateTo + " to open Marxan Web", Fore.GREEN)
        log("Or run 'python marxan-server.py " + navigateTo +
            "' to automatically open Marxan Web in a browser\n", Fore.GREEN)
    logging.warning("marxan-server started")
    # otherwise subprocesses fail on windows
    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(AnyThreadEventLoopPolicy())
    await SHUTDOWN_EVENT.wait()
    log("Closing Postgres connections..")
    # close the database connection
    pg.pool.close()
    await pg.pool.wait_closed()

if __name__ == "__main__":
    try:
        # Initialise the app
        tornado.ioloop.IOLoop.current().run_sync(initialiseApp)
    except KeyboardInterrupt:
        if (os.path.exists(project_paths.PROJECT_FOLDER + "shutdown.dat")):
            logging.warning("Deleting the shutdown file")
            os.remove(project_paths.PROJECT_FOLDER + "shutdown.dat")
        logging.warning("marxan-server stopping due to KeyboardInterrupt")
    except Exception as e:
        if e.args[0] == 98:
            log(f"The port {db_config.SERVER_PORT} is already in use")
        else:
            log(e.args)
    finally:
        logging.warning("marxan-server stopped")
        SHUTDOWN_EVENT.set()
