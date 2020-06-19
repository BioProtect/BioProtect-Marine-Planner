#!/home/ubuntu/miniconda2/envs/python36/bin/python3.6
import psutil
import urllib
import tornado.options
import webbrowser
import logging
import fnmatch
import json
import psycopg2
import pandas
import os
import re
import time
import traceback
import glob
import time
import datetime
import select
import subprocess
import sys
import zipfile
import shutil
import uuid
import signal
import colorama
import io
import requests
import platform
import ctypes
import aiopg
import asyncio
import aiohttp
import monkeypatch
import numpy
import shlex
from psycopg2.extensions import register_adapter, AsIs
from tornado.websocket import WebSocketClosedError
from tornado.iostream import StreamClosedError
from tornado.process import Subprocess
from tornado.log import LogFormatter
from tornado.web import HTTPError, StaticFileHandler
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.platform.asyncio import AnyThreadEventLoopPolicy
from tornado import concurrent
from tornado import gen, queues, httpclient, concurrent
from google.cloud import logging as googlelogger
from datetime import timedelta, timezone
from colorama import Fore, Back, Style
from sqlalchemy import create_engine
from collections import OrderedDict
from subprocess import Popen, PIPE, CalledProcessError
from threading import Thread
import requests

from urllib.parse import urlparse
from urllib import request
from psycopg2 import sql
from mapbox import Uploader, errors
from osgeo import ogr

####################################################################################################################################################################################################################################################################
# constant declarations
####################################################################################################################################################################################################################################################################

# SECURITY SETTINGS
# REST services that have do not need authentication/authorisation
PERMITTED_METHODS = ["getServerData", "createUser", "validateUser",
                     "resendPassword", "testTornado", "getProjectsWithGrids"]
# Add REST services that you want to lock down to specific roles - a class added to an array will make that method unavailable for that role
ROLE_UNAUTHORISED_METHODS = {
    "ReadOnly": ["createProject", "createImportProject", "upgradeProject", "deleteProject", "cloneProject", "createProjectGroup", "deleteProjects", "renameProject", "updateProjectParameters", "getCountries", "deletePlanningUnitGrid", "createPlanningUnitGrid", "uploadTilesetToMapBox", "uploadFileToFolder", "uploadFile", "importPlanningUnitGrid", "createFeaturePreprocessingFileFromImport", "createUser", "getUsers", "updateUserParameters", "getFeature", "importFeatures", "getPlanningUnitsData", "updatePUFile", "getSpeciesData", "getSpeciesPreProcessingData", "updateSpecFile", "getProtectedAreaIntersectionsData", "getMarxanLog", "getBestSolution", "getOutputSummary", "getSummedSolution", "getMissingValues", "preprocessFeature", "preprocessPlanningUnits", "preprocessProtectedAreas", "runMarxan", "stopProcess", "testRoleAuthorisation", "deleteFeature", "deleteUser", "getRunLogs", "clearRunLogs", "updateWDPA", "unzipShapefile", "getShapefileFieldnames", "createFeatureFromLinestring", "runGapAnalysis", "toggleEnableGuestUser", "importGBIFData", "deleteGapAnalysis", "shutdown", "addParameter", "block", "resetDatabase", "cleanup", "exportProject", "importProject", 'getCosts', 'updateCosts', 'deleteCost', 'runSQLFile', 'exportPlanningUnitGrid', 'exportFeature'],
    "User": ["testRoleAuthorisation", "deleteFeature", "getUsers", "deleteUser", "deletePlanningUnitGrid", "clearRunLogs", "updateWDPA", "toggleEnableGuestUser", "shutdown", "addParameter", "block", "resetDatabase", "cleanup", 'runSQLFile'],
    "Admin": []
}
MARXAN_SERVER_VERSION = "v0.9.6"
MARXAN_LOG_FILE = 'marxan-server.log'
MARXAN_REGISTRY = "https://marxanweb.github.io/general/registry/marxan.json"
MARXAN_REGISTRY_JSON = "marxan.json"
GUEST_USERNAME = "guest"
NOT_AUTHENTICATED_ERROR = "Request could not be authenticated. No secure cookie found."
NO_REFERER_ERROR = "The request header does not specify a referer and this is required for CORS access."
MAPBOX_USER = "blishten"
SERVER_CONFIG_FILENAME = "server.dat"
RUN_LOG_FILENAME = "runlog.dat"
USER_DATA_FILENAME = "user.dat"
PROJECT_DATA_FILENAME = "input.dat"
OUTPUT_LOG_FILENAME = "output_log.dat"
PLANNING_UNITS_FILENAME = "pu.dat"
PUVSPR_FILENAME = "puvspr.dat"
SPEC_FILENAME = "spec.dat"
BOUNDARY_LENGTH_FILENAME = "bounds.dat"
BEST_SOLUTION_FILENAME = "output_mvbest"
OUTPUT_SUMMARY_FILENAME = "output_sum"
SUMMED_SOLUTION_FILENAME = "output_ssoln"
FEATURE_PREPROCESSING_FILENAME = "feature_preprocessing.dat"
PROTECTED_AREA_INTERSECTIONS_FILENAME = "protected_area_intersections.dat"
NOTIFICATIONS_FILENAME = "notifications.dat"
SHUTDOWN_FILENAME = "shutdown.dat"
SOLUTION_FILE_PREFIX = "output_r"
MISSING_VALUES_FILE_PREFIX = "output_mv"
WDPA_DOWNLOAD_FILE = "wdpa.zip"
EXPORT_F_SHP_FOLDER = "f_shps"
EXPORT_PU_SHP_FOLDER = "pu_shps"
EXPORT_F_METADATA = 'features.csv'
EXPORT_PU_METADATA = 'planning_grid.csv'
GBIF_API_ROOT = "https://api.gbif.org/v1/"
GBIF_CONCURRENCY = 10
GBIF_PAGE_SIZE = 300
GBIF_POINT_BUFFER_RADIUS = 1000
# from the GBIF docs here: https://www.gbif.org/developer/occurrence#search
GBIF_OCCURRENCE_LIMIT = 200000
UNIFORM_COST_NAME = "Equal area"
DOCS_ROOT = "https://docs.marxanweb.org/"
ERRORS_PAGE = DOCS_ROOT + "errors.html"
SHUTDOWN_EVENT = tornado.locks.Event()  # to allow Tornado to exit gracefully
PING_INTERVAL = 30000  # interval between regular pings when using websockets
SHOW_START_LOG = True  # to disable the start logging from unit tests
DICT_PAD = 25  # text is right padded this much in dictionary outputs
LOGGING_LEVEL = logging.DEBUG  # Tornado logging level that controls what is logged to the console - options are logging.INFO, logging.DEBUG, logging.WARNING, logging.ERROR, logging.CRITICAL. All SQL statements can be logged by setting this to logging.DEBUG

####################################################################################################################################################################################################################################################################
# generic functions that dont belong to a class so can be called by subclasses of tornado.web.RequestHandler and tornado.websocket.WebSocketHandler equally - underscores are used so they dont mask the equivalent url endpoints
####################################################################################################################################################################################################################################################################

# run when the server starts to set all of the global path variables


async def _setGlobalVariables():
    global MBAT
    global MARXAN_FOLDER
    global MARXAN_USERS_FOLDER
    global MARXAN_CLIENT_BUILD_FOLDER
    global CLUMP_FOLDER
    global EXPORT_FOLDER
    global IMPORT_FOLDER
    global MARXAN_EXECUTABLE
    global MARXAN_WEB_RESOURCES_FOLDER
    global CASE_STUDIES_FOLDER
    global EMPTY_PROJECT_TEMPLATE_FOLDER
    global OGR2OGR_EXECUTABLE
    global GDAL_DATA_ENVIRONMENT_VARIABLE
    global CONDA_DEFAULT_ENV_ENVIRONMENT_VARIABLE
    global MARXAN_CLIENT_VERSION
    global CONNECTION_STRING
    global COOKIE_RANDOM_VALUE
    global PERMITTED_DOMAINS
    global SERVER_NAME
    global SERVER_DESCRIPTION
    global DATABASE_NAME
    global DATABASE_HOST
    global DATABASE_USER
    global DATABASE_PASSWORD
    global DATABASE_VERSION_POSTGRESQL
    global DATABASE_VERSION_POSTGIS
    global PORT
    global CERTFILE
    global KEYFILE
    global PLANNING_GRID_UNITS_LIMIT
    global DISABLE_SECURITY
    global DISABLE_FILE_LOGGING
    global ENABLE_RESET
    global pg
    # get data from the marxan registry
    MBAT = _getMBAT()
    # initialise colorama to be able to show log messages on windows in color
    colorama.init()
    # register numpy int64 with psycopg2
    psycopg2.extensions.register_adapter(numpy.int64, psycopg2._psycopg.AsIs)
    # get the folder from this files path
    MARXAN_FOLDER = os.path.dirname(os.path.realpath(__file__)) + os.sep
    # get the data in the server configuration file
    serverData = buildDataDict(MARXAN_FOLDER + 'config.json')
    # get the database connection string
    SERVER_NAME = _getDictValue(serverData, 'SERVER_NAME')
    SERVER_DESCRIPTION = _getDictValue(serverData, 'SERVER_DESCRIPTION')
    DATABASE_NAME = _getDictValue(serverData, 'DATABASE_NAME')
    DATABASE_HOST = _getDictValue(serverData, 'DATABASE_HOST')
    DATABASE_USER = _getDictValue(serverData, 'DATABASE_USER')
    DATABASE_PASSWORD = _getDictValue(serverData, 'DATABASE_PASSWORD')
    DATABASE_NAME = _getDictValue(serverData, 'DATABASE_NAME')
    PORT = str(_getDictValue(serverData, 'PORT'))
    CERTFILE = _getDictValue(serverData, 'CERTFILE')
    KEYFILE = _getDictValue(serverData, 'KEYFILE')
    DISABLE_SECURITY = _getDictValue(serverData, 'DISABLE_SECURITY')
    DISABLE_FILE_LOGGING = _getDictValue(serverData, 'DISABLE_FILE_LOGGING')
    ENABLE_RESET = _getDictValue(serverData, 'ENABLE_RESET')
    # CONNECTION_STRING = "host='" + DATABASE_HOST + "' dbname='" + DATABASE_NAME + "' user='" + DATABASE_USER + "' password='" + DATABASE_PASSWORD + "'"
    CONNECTION_STRING = "postgres://postgres:oxen4chit@database/marxanserver"
    # initialise the connection pool
    pg = PostGIS()
    await pg.initialise()
    # get the database version
    results = await pg.execute("SELECT version(), PostGIS_Version();", returnFormat="Array")
    DATABASE_VERSION_POSTGRESQL, DATABASE_VERSION_POSTGIS = results[0]
    COOKIE_RANDOM_VALUE = _getDictValue(serverData, 'COOKIE_RANDOM_VALUE')
    PERMITTED_DOMAINS = _getDictValue(
        serverData, 'PERMITTED_DOMAINS').split(",")
    PLANNING_GRID_UNITS_LIMIT = int(_getDictValue(
        serverData, 'PLANNING_GRID_UNITS_LIMIT'))
    # get the GDAL_DATA environment variable
    if ('GDAL_DATA' in os.environ.keys()):
        GDAL_DATA_ENVIRONMENT_VARIABLE = os.environ['GDAL_DATA']
    else:
        GDAL_DATA_ENVIRONMENT_VARIABLE = "Not set"
    # get the name of the current conda environment
    if ('CONDA_DEFAULT_ENV' in os.environ.keys()):
        CONDA_DEFAULT_ENV_ENVIRONMENT_VARIABLE = os.environ['CONDA_DEFAULT_ENV']
    else:
        CONDA_DEFAULT_ENV_ENVIRONMENT_VARIABLE = "Not set"
    # OUTPUT THE INFORMATION ABOUT THE MARXAN-SERVER SOFTWARE
    log("Starting marxan-server " + MARXAN_SERVER_VERSION +
        " listening on port " + PORT + " ..", Fore.GREEN)
    # print out which operating system is being used
    log(_padDict("Operating system:", platform.system(), DICT_PAD))
    log(_padDict("Tornado version:", tornado.version, DICT_PAD))
    log(_padDict("Permitted domains:", _getDictValue(
        serverData, 'PERMITTED_DOMAINS'), DICT_PAD))
    # output the ssl information if it is being used
    if CERTFILE != "None":
        log(_padDict("SSL certificate file:", CERTFILE, DICT_PAD))
        testUrl = "https://"
    else:
        log(_padDict("SSL certificate file:", "None", DICT_PAD))
        testUrl = "http://"
    testUrl = testUrl + "<host>:" + PORT + "/marxan-server/testTornado" if (
        PORT != '80') else testUrl + "<host>/marxan-server/testTornado"
    if KEYFILE != "None":
        log(_padDict("Private key file:", KEYFILE, DICT_PAD))
    else:
        log(_padDict("Private key file:", "None", DICT_PAD))
    log(_padDict("Database:", CONNECTION_STRING, DICT_PAD))
    log(_padDict("PostgreSQL:", DATABASE_VERSION_POSTGRESQL, DICT_PAD))
    log(_padDict("PostGIS:", DATABASE_VERSION_POSTGIS, DICT_PAD))
    log(_padDict("WDPA Version:", _getDictValue(
        serverData, 'WDPA_VERSION'), DICT_PAD))
    log(_padDict("Planning grid limit:", str(PLANNING_GRID_UNITS_LIMIT), DICT_PAD))
    log(_padDict("Disable security:", str(DISABLE_SECURITY), DICT_PAD))
    log(_padDict("Disable file logging:", str(DISABLE_FILE_LOGGING), DICT_PAD))
    log(_padDict("Enable reset:", str(ENABLE_RESET), DICT_PAD))
    log(_padDict("Conda environment:", CONDA_DEFAULT_ENV_ENVIRONMENT_VARIABLE, DICT_PAD))
    log(_padDict("Python executable:", sys.executable, DICT_PAD))
    # get the path to the ogr2ogr file - it should be in the miniconda bin folder
    if platform.system() == "Windows":
        ogr2ogr_executable = "ogr2ogr.exe"
        # sys.executable is the Python.exe file and will likely be in C:\Users\a_cottam\Miniconda2 folder - ogr2ogr is then in /library/bin on windows
        OGR2OGR_PATH = os.path.dirname(
            sys.executable) + os.sep + "library" + os.sep + "bin" + os.sep
        marxan_executable = "Marxan.exe"  # TODO Use Marxan_x64.exe for 64 bit processors
        stopCmd = "Press CTRL+C or CTRL+Fn+Pause to stop the server\n"
    else:
        ogr2ogr_executable = "ogr2ogr"
        # sys.executable is the Python.exe file and will likely be in /home/ubuntu//miniconda2/bin/ - the same place as ogr2ogr
        OGR2OGR_PATH = os.path.dirname(sys.executable) + os.sep
        marxan_executable = "MarOpt_v243_Linux64"
        stopCmd = "Press CTRL+C to stop the server\n"
    # if the ogr2ogr executable path is not in the miniconda bin directory, then hard-code it here and uncomment the line
    # OGR2OGR_PATH = ""
    OGR2OGR_EXECUTABLE = OGR2OGR_PATH + ogr2ogr_executable
    if not os.path.exists(OGR2OGR_EXECUTABLE):
        raise MarxanServicesError(" ogr2ogr executable:\t'" + OGR2OGR_EXECUTABLE +
                                  "' could not be found. Set it manually in the marxan-server.py file.")
    else:
        log(_padDict("ogr2ogr executable:", OGR2OGR_EXECUTABLE, DICT_PAD))
    # set the various folder paths
    MARXAN_USERS_FOLDER = MARXAN_FOLDER + "users" + os.sep
    CLUMP_FOLDER = MARXAN_USERS_FOLDER + "_clumping" + os.sep
    EXPORT_FOLDER = MARXAN_FOLDER + "exports" + os.sep
    IMPORT_FOLDER = MARXAN_FOLDER + "imports" + os.sep
    MARXAN_EXECUTABLE = MARXAN_FOLDER + marxan_executable
    MARXAN_WEB_RESOURCES_FOLDER = MARXAN_FOLDER + "_marxan_web_resources" + os.sep
    CASE_STUDIES_FOLDER = MARXAN_WEB_RESOURCES_FOLDER + "case_studies" + os.sep
    EMPTY_PROJECT_TEMPLATE_FOLDER = MARXAN_WEB_RESOURCES_FOLDER + "empty_project" + os.sep
    log(_padDict("GDAL_DATA path:", GDAL_DATA_ENVIRONMENT_VARIABLE, DICT_PAD))
    log(_padDict("Marxan executable:", MARXAN_EXECUTABLE, DICT_PAD))
    log("\nTo test marxan-server goto " + testUrl, Fore.GREEN)
    log(stopCmd, Fore.RED)
    # get the parent folder
    PARENT_FOLDER = MARXAN_FOLDER[:MARXAN_FOLDER[:-1].rindex(os.sep)] + os.sep
    # OUTPUT THE INFORMATION ABOUT THE MARXAN-CLIENT SOFTWARE IF PRESENT
    packageJson = PARENT_FOLDER + "marxan-client" + os.sep + "package.json"
    if os.path.exists(packageJson):
        MARXAN_CLIENT_BUILD_FOLDER = PARENT_FOLDER + "marxan-client" + os.sep + "build"
        # open the node.js package.json file for the marxan-client app to read the version of the software
        f = open(packageJson)
        MARXAN_CLIENT_VERSION = json.load(f)['version']
        f.close()
        log("marxan-client " + MARXAN_CLIENT_VERSION + " installed", Fore.GREEN)
    else:
        MARXAN_CLIENT_BUILD_FOLDER = ""
        MARXAN_CLIENT_VERSION = "Not installed"
        log("marxan-client is not installed\n", Fore.GREEN)


def buildDataDict(filename):
    if not os.path.exists(filename):
        raise MarxanServicesError("The file '" + filename + "' does not exist")
    # get the file contents
    with open(filename) as json_file:
        return json.load(json_file)
    return {}


# outputs a key: value from a dictionary into 2 columns with width w
def _padDict(key, val, width):
    return key + (width - len(key))*" " + val


# logs the string to the logging handlers using the passed colorama color
def log(_str, _color=Fore.RESET):
    if SHOW_START_LOG:
        # print to the console
        if not DISABLE_FILE_LOGGING:
            # print to file
            if type(_str) is tuple:
                _str = "".join(_str)
            _writeFileUnicode(
                "".join([MARXAN_FOLDER, MARXAN_LOG_FILE]), _str, "a")


def _raiseError(obj, msg):
    # send a response with the error message
    if hasattr(obj, "send_response"):
        obj.send_response({"error": msg})
        obj.finish()
    # log the error
    logging.warning(msg)

# gets that method part of the REST service path, e.g. /marxan-server/validateUser will return validateUser


def _getRESTMethod(path):
    pos = path.rfind("/")
    if pos > -1:
        return path[pos+1:]
    return ""


# creates a new user
# dont know about this. Saving users in files, no password hashing.
# **************************************************************** SORT THIS ****************************************************************
def _createUser(obj, user, fullname, email, password):
    # get the list of users
    users = _getUsers()
    if user in users:
        raise MarxanServicesError("User '" + user + "' already exists")
    # create the user folder
    obj.folder_user = MARXAN_USERS_FOLDER + user + os.sep
    os.mkdir(obj.folder_user)
    # copy the user.dat file
    shutil.copyfile(MARXAN_WEB_RESOURCES_FOLDER +
                    USER_DATA_FILENAME, obj.folder_user + USER_DATA_FILENAME)
    # copy the notifications.dat file
    shutil.copyfile(MARXAN_WEB_RESOURCES_FOLDER + NOTIFICATIONS_FILENAME,
                    obj.folder_user + NOTIFICATIONS_FILENAME)
    # update the user.dat file parameters
    _updateParameters(obj.folder_user + USER_DATA_FILENAME, {'NAME': fullname, 'EMAIL': email,
                                                             'PASSWORD': password, 'CREATEDATE': datetime.datetime.now().strftime("%d/%m/%y %H:%M:%S")})


# gets a simple list of users
def _getUsers():
    not_user_folders = ["input", "output", "MarxanData", "MarxanData_unix"]
    # get a list of folders underneath the marxan users folder
    user_folders = glob.glob(MARXAN_USERS_FOLDER + "*/")
    print('user_folders: ', user_folders)
    # convert these into a list of users
    users = [user[:-1][user[:-1].rfind(os.sep)+1:] for user in user_folders]
    for folder in not_user_folders:
        if folder in users:
            users.remove(folder)
    return [u for u in users if u[:1] != "_"]


def _getUsersData(users):
    # gets all the users data for the passed users
    users.sort()
    usersData = []
    # create a extendable object to hold the user data
    tmpObj = ExtendableObject()
    # iterate through the users
    for user in users:
        tmpObj.folder_user = MARXAN_USERS_FOLDER + user + os.sep
        # get the data for the user
        _getUserData(tmpObj)
        # create a dict to save the data
        combinedDict = tmpObj.userData.copy()  # pylint:disable=no-member
        combinedDict.update({'user': user})
        usersData.append(combinedDict)
    return usersData


def _getNotificationsData(obj):
    # get the data from the notifications file
    s = _readFile(obj.folder_user + NOTIFICATIONS_FILENAME)
    if (s == ""):
        return []
    return s.split(",")


def _dismissNotification(obj, notificationid):
    # get the data from the notifications file
    ids = _getNotificationsData(obj)
    ids.append(notificationid)
    _writeFileUnicode(obj.folder_user + NOTIFICATIONS_FILENAME, ",".join(ids))


def _resetNotifications(obj):
    _writeFileUnicode(obj.folder_user + NOTIFICATIONS_FILENAME, "")

# returns the project name without internal spaces or other invalid characters


def _getSafeProjectName(project_name):
    return project_name.strip().replace(" ", "_")

# gets the projects for the specified user


async def _getProjectsForUser(user):
    # get a list of folders underneath the users home folder
    project_folders = glob.glob(MARXAN_USERS_FOLDER + user + os.sep + "*/")
    # sort the folders
    project_folders.sort()
    projects = []
    # iterate through the project folders and get the parameters for each project to return
    tmpObj = ExtendableObject()
    for dir in project_folders:
        # get the name of the folder
        project = dir[:-1][dir[:-1].rfind(os.sep)+1:]
        if (project[:2] != "__"):  # folders beginning with __ are system folders
            # get the data from the input file for this project
            tmpObj.project = project
            tmpObj.folder_project = MARXAN_USERS_FOLDER + user + os.sep + project + os.sep
            await _getProjectData(tmpObj)
            # create a dict to save the data
            projects.append({'user': user, 'name': project, 'description': tmpObj.projectData["metadata"]["DESCRIPTION"], 'createdate': tmpObj.projectData["metadata"][
                            "CREATEDATE"], 'oldVersion': tmpObj.projectData["metadata"]["OLDVERSION"], 'private': tmpObj.projectData["metadata"]["PRIVATE"]})  # pylint:disable=no-member
    return projects


# gets all projects for all users
async def _getAllProjects():
    allProjects = []
    # get a list of users
    users = _getUsers()
    # iterate through the users and get the project data
    for user in users:
        projects = await _getProjectsForUser(user)
        allProjects.extend(projects)
    return allProjects


# gets the projects for the current user
async def _getProjects(obj):
    if ((obj.user == GUEST_USERNAME) or (obj.get_secure_cookie("role").decode("utf-8") == "Admin")):
        obj.projects = await _getAllProjects()
    else:
        obj.projects = await _getProjectsForUser(obj.user)


# creates a new empty project with the passed parameters
def _createProject(obj, name):
    # make sure the project does not already exist
    if os.path.exists(obj.folder_user + name):
        raise MarxanServicesError("The project '" + name + "' already exists")
    # copy the _project_template folder to the new location
    _copyDirectory(EMPTY_PROJECT_TEMPLATE_FOLDER, obj.folder_user + name)
    # set the paths to this project in the passed object - the arguments are normally passed as lists in tornado.get_argument - and the _setFolderPaths expects bytes not strings as they normally come from self.request.arguments
    _setFolderPaths(obj, {'user': [obj.user.encode(
        "utf-8")], 'project': [name.encode("utf-8")]})


# deletes a project
def _deleteProject(obj):
    # delete the folder and all of its contents
    try:
        shutil.rmtree(obj.folder_project)
    except (WindowsError) as e:  # pylint:disable=undefined-variable
        raise MarxanServicesError(e.strerror)


# clones a project from the source_folder which is a full folder path to the destination_folder which is a full folder path
def _cloneProject(source_folder, destination_folder):
    # get the project name
    original_project_name = source_folder[:-1].split(os.sep)[-1]
    # get the new project folder
    new_project_folder = destination_folder + original_project_name + os.sep
    # recursively check that the folder does not exist until we get a new folder that doesnt exist
    while (os.path.exists(new_project_folder)):
        new_project_folder = new_project_folder[:-1] + "_copy" + os.sep
    # copy the project
    shutil.copytree(source_folder, new_project_folder)
    # update the description and create date
    _updateParameters(new_project_folder + PROJECT_DATA_FILENAME, {'DESCRIPTION': "Clone of project '" +
                                                                   original_project_name + "'",  'CREATEDATE': datetime.datetime.now().strftime("%d/%m/%y %H:%M:%S")})
    # return the name of the new project
    return new_project_folder[:-1].split(os.sep)[-1]


# sets the various paths to the users folder and project folders using the request arguments in the passed object
def _setFolderPaths(obj, arguments):
    if "user" in list(arguments.keys()):
        # argument values are bytes but not always for projects for some reason...
        user = arguments["user"][0].decode("utf-8")
        obj.folder_user = MARXAN_USERS_FOLDER + user + os.sep
        obj.user = user
        # get the project folder and the input and output folders
        if "project" in list(arguments.keys()):
            proj = arguments["project"][0]
            if type(proj) is not str:
                proj = proj.decode("utf-8")
            obj.folder_project = obj.folder_user + proj + os.sep
            obj.folder_input = obj.folder_project + "input" + os.sep
            obj.folder_output = obj.folder_project + "output" + os.sep
            obj.project = obj.get_argument("project")

# get the project data from the input.dat file as a categorised list of settings - using the obj.folder_project path and creating an attribute called projectData in the obj for the return data


async def _getProjectData(obj):
    paramsArray = []
    filesDict = {}
    metadataDict = {}
    rendererDict = {}
    # get the file contents
    s = _readFileUnicode(obj.folder_project + PROJECT_DATA_FILENAME)
    # get the keys from the file
    keys = _getKeys(s)
    # iterate through the keys and get their values
    for k in keys:
        # some parameters we do not need to return
        # Input Files section of input.dat file
        if k in ["PUNAME", "SPECNAME", "PUVSPRNAME", "BOUNDNAME", "BLOCKDEF"]:
            key, value = _getKeyValue(s, k)
            filesDict.update({key:  value})
        elif k in ['BLM', 'PROP', 'RANDSEED', 'NUMREPS', 'NUMITNS', 'STARTTEMP', 'NUMTEMP', 'COSTTHRESH', 'THRESHPEN1', 'THRESHPEN2', 'SAVERUN', 'SAVEBEST', 'SAVESUMMARY', 'SAVESCEN', 'SAVETARGMET', 'SAVESUMSOLN', 'SAVEPENALTY', 'SAVELOG', 'RUNMODE', 'MISSLEVEL', 'ITIMPTYPE', 'HEURTYPE', 'CLUMPTYPE', 'VERBOSITY', 'SAVESOLUTIONSMATRIX']:
            key, value = _getKeyValue(s, k)  # run parameters
            paramsArray.append({'key': key, 'value': value})
        # metadata section of the input.dat file
        elif k in ['DESCRIPTION', 'CREATEDATE', 'PLANNING_UNIT_NAME', 'OLDVERSION', 'IUCN_CATEGORY', 'PRIVATE', 'COSTS']:
            key, value = _getKeyValue(s, k)
            metadataDict.update({key: value})
            if k == 'PLANNING_UNIT_NAME':
                df2 = await pg.execute("select * from marxan.get_planning_units_metadata(%s)", data=[value], returnFormat="DataFrame")
                if (df2.shape[0] == 0):
                    metadataDict.update({'pu_alias': value, 'pu_description': 'No description', 'pu_domain': 'Unknown domain',
                                         'pu_area': 'Unknown area', 'pu_creation_date': 'Unknown date', 'pu_created_by': 'Unknown', 'pu_country': 'Unknown'})
                else:
                    # get the data from the metadata_planning_units table
                    metadataDict.update({'pu_alias': df2.iloc[0]['alias'], 'pu_country': df2.iloc[0]['country'], 'pu_description': df2.iloc[0]['description'], 'pu_domain': df2.iloc[0]
                                         ['domain'], 'pu_area': df2.iloc[0]['area'], 'pu_creation_date': df2.iloc[0]['creation_date'], 'pu_created_by': df2.iloc[0]['created_by']})

        # renderer section of the input.dat file
        elif k in ['CLASSIFICATION', 'NUMCLASSES', 'COLORCODE', 'TOPCLASSES', 'OPACITY']:
            key, value = _getKeyValue(s, k)
            rendererDict.update({key: value})
    # set the project data
    obj.projectData = {}
    obj.projectData.update({'project': obj.project, 'metadata': metadataDict,
                            'files': filesDict, 'runParameters': paramsArray, 'renderer': rendererDict})


# gets the name of the input file from the projects input.dat file using the obj.folder_project path
async def _getProjectInputFilename(obj, fileToGet):
    if not hasattr(obj, "projectData"):
        await _getProjectData(obj)
    return obj.projectData["files"][fileToGet]


# gets the projects input data using the fileToGet, e.g. SPECNAME will return the data from the file corresponding to the input.dat file SPECNAME setting
async def _getProjectInputData(obj, fileToGet, errorIfNotExists=False):
    filename = obj.folder_input + os.sep + await _getProjectInputFilename(obj, fileToGet)
    return _loadCSV(filename, errorIfNotExists)


# gets the key/value pairs from a text file as a dictionary
def _getKeyValuesFromFile(filename):
    if not os.path.exists(filename):
        raise MarxanServicesError("The file '" + filename + "' does not exist")
    # get the file contents
    s = _readFileUnicode(filename)
    # get the keys from the file
    keys = _getKeys(s)
    # iterate through the keys, get their values and add them to a dictionary
    data = {}
    for k in keys:
        key, value = _getKeyValue(s, k)
        # update the  dict
        if value == "true":
            value = True
        if value == "false":
            value = False
        data[key] = value
    return data


def _get_free_space_mb():
    """Return folder/drive free space (in megabytes)."""
    if platform.system() == 'Windows':
        free_bytes = ctypes.c_ulonglong(0)
        ctypes.windll.kernel32.GetDiskFreeSpaceExW(ctypes.c_wchar_p(
            MARXAN_FOLDER), None, None, ctypes.pointer(free_bytes))
        space = free_bytes.value / 1024 / 1024  # pylint:disable=old-division
    else:
        st = os.statvfs(MARXAN_FOLDER)
        space = st.f_bavail * st.f_frsize / 1024 / 1024  # pylint:disable=old-division
    return (str("{:.1f}".format(space/1000)) + " Gb")


# gets the server data
def _getServerData(obj):
    # get the data from the server configuration file - these key/values are changed by the marxan-client
    obj.serverData = _getKeyValuesFromFile(
        MARXAN_FOLDER + SERVER_CONFIG_FILENAME)
    # get the free space in Mb
    space = _get_free_space_mb()
    # get the number of processors
    processors = psutil.cpu_count()
    # get the virtual memory
    memory = (
        str("{:.1f}".format(psutil.virtual_memory().total/1000000000)) + " Gb")
    # set the return values: permitted CORS domains - these are set in this Python module; the server os and hardware; the version of the marxan-server software
    obj.serverData.update({
        "RAM": memory,
        "PROCESSOR_COUNT": processors,
        "DATABASE_VERSION_POSTGIS": DATABASE_VERSION_POSTGIS,
        "DATABASE_VERSION_POSTGRESQL": DATABASE_VERSION_POSTGRESQL,
        "SYSTEM": platform.system(),
        "NODE": platform.node(),
        "RELEASE": platform.release(),
        "VERSION": platform.version(),
        "MACHINE": platform.machine(),
        "PROCESSOR": platform.processor(),
        "MARXAN_SERVER_VERSION": MARXAN_SERVER_VERSION,
        "MARXAN_CLIENT_VERSION": MARXAN_CLIENT_VERSION,
        "SERVER_NAME": SERVER_NAME,
        "SERVER_DESCRIPTION": SERVER_DESCRIPTION,
        "DISK_FREE_SPACE": space})


# get the data on the user from the user.dat file
def _getUserData(obj):
    print('obj: ', obj)
    data = _getKeyValuesFromFile(obj.folder_user + USER_DATA_FILENAME)
    print('USER_DATA_FILENAME: ', USER_DATA_FILENAME)
    print('data: ', data)
    # set the userData attribute on this object
    obj.userData = data


# get the species data from the spec.dat file as a DataFrame (and joins it to the data from the PostGIS database if it is the Marxan web version)
async def _getSpeciesData(obj):
    # get the values from the spec.dat file - speciesDataFilename will be empty if it doesn't exist yet
    df = await _getProjectInputData(obj, "SPECNAME")
    # create the output data frame using the id field as an index
    output_df = df.set_index("id")
    # add the index as a column
    output_df['oid'] = output_df.index
    # see if the version of marxan is the old version
    if obj.projectData["metadata"]["OLDVERSION"]:
        # return the data from the spec.dat file with additional fields manually added
        output_df['tmp'] = 'Unique identifer: '
        # if the spec.dat file has a field called 'name' then this will be used as the alias
        if ('name' in output_df.columns):
            output_df['alias'] = output_df['name']
        else:
            output_df['alias'] = output_df['tmp'].str.cat(
                (output_df['oid']).apply(str))  # returns: 'Unique identifer: 4702435'
        output_df['feature_class_name'] = output_df['oid']
        output_df['description'] = "No description"
        output_df['creation_date'] = "Unknown"
        output_df['area'] = -1
        output_df['tilesetid'] = ''
        output_df['created_by'] = 'Unknown'
        try:
            output_df = output_df[["alias", "feature_class_name", "description",
                                   "creation_date", "area", "tilesetid", "prop", "spf", "oid", "created_by"]]
        except (KeyError) as e:
            raise MarxanServicesError("Unable to load spec.dat data. " + e.args[1] + ". Column names: " + ",".join(df.columns.to_list(
            )).encode('unicode_escape'))  # .encode('unicode_escape') in case there are tab characters which will be escaped to \\t
    else:
        # get the postgis feature data - this doesnt use _getAllSpeciesData because we have to join on the oid column
        df2 = await pg.execute("select * from marxan.get_features()", returnFormat="DataFrame")
        # join the species data to the PostGIS data
        output_df = output_df.join(df2.set_index("oid"))
    # rename the columns that are sent back to the client as the names of various properties are different in Marxan compared to the web client
    output_df = output_df.rename(
        index=str, columns={'prop': 'target_value', 'oid': 'id'})
    # get the target as an integer - Marxan has it as a percentage, i.e. convert 0.17 -> 17
    output_df['target_value'] = (output_df['target_value'] * 100).astype(int)
    obj.speciesData = output_df


# gets data for a single feature
async def _getFeature(obj, oid):
    obj.data = await pg.execute("SELECT oid::integer id,feature_class_name,alias,description,_area area,extent, to_char(creation_date, 'DD/MM/YY HH24:MI:SS')::text AS creation_date, tilesetid, source, created_by FROM marxan.metadata_interest_features WHERE oid=%s;", data=[oid], returnFormat="DataFrame")

# get all species information from the PostGIS database


async def _getAllSpeciesData(obj):
    obj.allSpeciesData = await pg.execute("SELECT oid::integer id,feature_class_name , alias , description , _area area, extent, to_char(creation_date, 'DD/MM/YY HH24:MI:SS')::text AS creation_date, tilesetid, source, created_by FROM marxan.metadata_interest_features ORDER BY lower(alias);", returnFormat="DataFrame")


# get the information about which species have already been preprocessed
def _getSpeciesPreProcessingData(obj):
    obj.speciesPreProcessingData = _loadCSV(
        obj.folder_input + FEATURE_PREPROCESSING_FILENAME)


# get the planning units status information
async def _getPlanningUnitsData(obj):
    df = await _getProjectInputData(obj, "PUNAME")
    # normalise the planning unit data to make the payload smaller
    obj.planningUnitsData = _normaliseDataFrame(df, "status", "id")


# get the planning units cost information
async def _getPlanningUnitsCostData(obj):
    df = await _getProjectInputData(obj, "PUNAME")
    # normalise the planning unit cost data to make the payload smaller
    obj.planningUnitsData = _normaliseDataFrame(df, "cost", "id", 9)

# gets a list of the custom cost profiles for a project - these are defined in the input/*.cost files


def _getCosts(obj):
    # get all files that end in .cost
    costFiles = glob.glob(obj.folder_input + "*.cost")
    # get the names of the files
    costNames = [os.path.basename(f)[:-5] for f in costFiles]
    # add the default cost profile
    costNames.append(UNIFORM_COST_NAME)
    costNames.sort()
    # return the costNames
    obj.costNames = costNames

# updates the costs in the pu.dat file using the costname file


async def _updateCosts(obj, costname):
    filename = obj.folder_input + costname + ".cost"
    # load the pu.dat file
    df = await _getProjectInputData(obj, "PUNAME")
    # default costs are uniform
    if costname == UNIFORM_COST_NAME:
        df['cost'] = 1
    else:
        # check the cost file exists
        if not os.path.exists(filename):
            raise MarxanServicesError(
                "The cost file '" + costname + "' does not exist")
        # load the costs file
        df2 = _loadCSV(filename)
        # join the costs file (which has id,cost) to the pu.dat file (which has status)
        df = df2.join(df[['status']])
    # update the input.dat file
    _updateParameters(obj.folder_project +
                      PROJECT_DATA_FILENAME, {'COSTS': costname})
    await _writeCSV(obj, "PUNAME", df)

# deletes a cost profile


def _deleteCost(obj, costname):
    filename = obj.folder_input + costname + ".cost"
    # check the cost file exists
    if not os.path.exists(filename):
        raise MarxanServicesError(
            "The cost file '" + costname + "' does not exist")
    else:
        os.remove(filename)

# gets the data for the planning grids


async def _getPlanningUnitGrids():
    return await pg.execute("SELECT feature_class_name ,alias ,description ,to_char(creation_date, 'DD/MM/YY HH24:MI:SS')::text AS creation_date ,country_id ,aoi_id,domain,_area,ST_AsText(envelope) envelope, pu.source, original_n country, created_by,tilesetid, planning_unit_count FROM marxan.metadata_planning_units pu LEFT OUTER JOIN marxan.gaul_2015_simplified_1km ON id_country = country_id order by lower(alias);", returnFormat="Dict")

# estimates the number of planning grid units in the passed country, area and domain


async def _estimatePlanningUnitCount(areakm2, iso3, domain):
    # see if we are using terrestrial or marine
    if (domain == 'Terrestrial'):
        unitCount = await pg.execute("SELECT ST_Area(ST_Transform(wkb_geometry, 3410))/(%s*1000000) FROM marxan.gaul_2015_simplified_1km WHERE iso3 = %s;", data=[areakm2, iso3], returnFormat="Array")
    else:
        unitCount = await pg.execute("SELECT ST_Area(ST_Transform(wkb_geometry, 3410))/(%s*1000000) FROM marxan.eez_simplified_1km WHERE iso3 = %s;", data=[areakm2, iso3], returnFormat="Array")
    return unitCount[0][0]


# get the protected area intersections information
def _getProtectedAreaIntersectionsData(obj):
    df = _loadCSV(obj.folder_input + PROTECTED_AREA_INTERSECTIONS_FILENAME)
    # normalise the protected area intersections to make the payload smaller
    obj.protectedAreaIntersectionsData = _normaliseDataFrame(
        df, "iucn_cat", "puid")


# resets all of the protected area intersections information - for example when a new version of the wdpa is installed
def _invalidateProtectedAreaIntersections():
    # get all of the existing protected area intersection files
    files = _getFilesInFolderRecursive(
        MARXAN_USERS_FOLDER, PROTECTED_AREA_INTERSECTIONS_FILENAME)
    # iterate through all of these files and replace them with an empty file
    for file in files:
        shutil.copyfile(EMPTY_PROJECT_TEMPLATE_FOLDER + "input" +
                        os.sep + PROTECTED_AREA_INTERSECTIONS_FILENAME, file)


# gets the marxan log after a run
def _getMarxanLog(obj):
    if (os.path.exists(obj.folder_output + OUTPUT_LOG_FILENAME)):
        log = _readFileUnicode(obj.folder_output + OUTPUT_LOG_FILENAME)
    else:
        log = ""
    obj.marxanLog = log


# the extension of the output files depends on the settings SAVE* in the input.dat file and probably on the version of marxan. This function gets the correct filename+extension (normally either csv or txt)
def _getOutputFilename(filename):
    # filename is the full filename without an extension
    files = glob.glob(filename + "*")
    if (len(files)) > 0:
        extension = files[0][-4:]
        return filename + extension
    else:
        raise MarxanServicesError(
            "The output file '" + filename + "' does not exist")


# loads the data from the marxan best solution file
def _getBestSolution(obj):
    filename = _getOutputFilename(obj.folder_output + BEST_SOLUTION_FILENAME)
    obj.bestSolution = _loadCSV(filename)


# loads the data from the marxan output summary file
def _getOutputSummary(obj):
    filename = _getOutputFilename(obj.folder_output + OUTPUT_SUMMARY_FILENAME)
    obj.outputSummary = _loadCSV(filename)


# loads the data from the marxan summed solution file
def _getSummedSolution(obj):
    filename = _getOutputFilename(obj.folder_output + SUMMED_SOLUTION_FILENAME)
    df = _loadCSV(filename)
    obj.summedSolution = _normaliseDataFrame(df, "number", "planning_unit")


# loads the data from a marxan single solution file
def _getSolution(obj, solutionId):
    try:
        filename = _getOutputFilename(
            obj.folder_output + SOLUTION_FILE_PREFIX + "%05d" % int(solutionId))
    except MarxanServicesError as e:  # the solution no longer exists - probably a clumping project
        obj.solution = []
        if (obj.get_argument('user') != "_clumping"):
            raise MarxanServicesError("Solution '" + str(solutionId) +
                                      "' in project '" + obj.get_argument('project') + "' no longer exists")
        else:
            pass
    else:
        if os.path.exists(filename):
            df = _loadCSV(filename)
            # normalise the data by the planning unit field and solution field - these may be called planning_unit,solution or PUID,SOLUTION - so get their names by position
            obj.solution = _normaliseDataFrame(
                df, df.columns[1], df.columns[0])


def _getMissingValues(obj, solutionId):
    filename = _getOutputFilename(
        obj.folder_output + MISSING_VALUES_FILE_PREFIX + "%05d" % int(solutionId))
    df = _loadCSV(filename)
    obj.missingValues = df.to_dict(orient="split")["data"]


# updates/creates the spec.dat file with the passed interest features
async def _updateSpeciesFile(obj, interest_features, target_values, spf_values, create=False):
    # get the features to create/update as a list of integer ids
    ids = _txtIntsToList(interest_features)
    props = _txtIntsToList(target_values)
    spfs = spf_values.split(",")
    if create:
        # there are no existing ids as we are creating a new pu.dat file
        removedIds = []
    else:
        # get the current list of features
        df = await _getProjectInputData(obj, "SPECNAME")
        # get the current list of columns
        cols = list(df.columns.values)
        if df.empty:
            currentIds = []
        else:
            currentIds = df.id.unique().tolist()
        # get the list of features to remove from the current list (i.e. they are not in the passed list of interest features)
        removedIds = list(set(currentIds) - set(ids))
        # update the puvspr.dat file and the feature preprocessing files to remove any species that are no longer in the project
        if len(removedIds) > 0:
            # get the name of the puvspr file from the project data
            puvsprFilename = await _getProjectInputFilename(obj, "PUVSPRNAME")
            # update the puvspr.dat file
            if (os.path.exists(obj.folder_input + puvsprFilename)):
                _deleteRecordsInTextFile(
                    obj.folder_input + puvsprFilename, "species", removedIds, False)
            # update the preprocessing.dat file to remove any species that are no longer in the project - these will need to be preprocessed again
            if (os.path.exists(obj.folder_input + FEATURE_PREPROCESSING_FILENAME)):
                _deleteRecordsInTextFile(
                    obj.folder_input + FEATURE_PREPROCESSING_FILENAME, "id", removedIds, False)
    # create the dataframe to write to file
    records = []
    for i in range(len(ids)):
        if i not in removedIds:
            records.append(
                {'id': ids[i], 'prop': str(props[i]/100), 'spf': spfs[i]})
    # create a data frame with the records
    if len(records) == 0:
        new_df = pandas.DataFrame(columns=['id', 'prop', 'spf'])
    else:
        new_df = pandas.DataFrame(records)
    # if there are optional columns like target, targetocc or name etc., merge the new data with any existing data in the spec.dat file
    if create == False:
        if (len(cols) != 3):
            # first drop the columns that have new data
            df = df.drop(columns=['prop', 'spf'])
            # now merge the two data frames
            new_df = pandas.merge(df, new_df, on='id')
            # output the fields in the correct order id,prop,spf
            new_df = new_df[cols]
    # sort the records by the id field
    if not new_df.empty:
        new_df = new_df.sort_values(by=['id'])
    # write the data to file
    await _writeCSV(obj, "SPECNAME", new_df)


# create the array of the puids
def _puidsArrayToPuDatFormat(puid_array, pu_status):
    return pandas.DataFrame([[int(i), pu_status] for i in puid_array], columns=['id', 'status_new']).astype({'id': 'int64', 'status_new': 'int64'})


# creates the pu.dat file using the ids from the PostGIS feature class as the planning unit ids in the pu.dat file
async def _createPuFile(obj, planning_grid_name):
    # get the path to the pu.dat file
    filename = obj.folder_input + PLANNING_UNITS_FILENAME
    # create the pu.dat file using a postgis query
    await pg.execute(sql.SQL("SELECT puid as id,1::double precision as cost,0::integer as status FROM marxan.{};").format(sql.Identifier(planning_grid_name)), returnFormat="File", filename=filename)
    # update the input.dat file
    _updateParameters(obj.folder_project + PROJECT_DATA_FILENAME,
                      {'PUNAME': PLANNING_UNITS_FILENAME})


# updates the pu.dat file with the passed arrays of ids for the various statuses
async def _updatePuFile(obj, status1_ids, status2_ids, status3_ids):
    status1 = _puidsArrayToPuDatFormat(status1_ids, 1)
    status2 = _puidsArrayToPuDatFormat(status2_ids, 2)
    status3 = _puidsArrayToPuDatFormat(status3_ids, 3)
    # read the data from the pu.dat file
    df = await _getProjectInputData(obj, "PUNAME")
    # reset the status for all planning units
    df['status'] = 0
    # concatenate the status arrays
    df2 = pandas.concat([status1, status2, status3])
    # join the new statuses to the ones from the pu.dat file
    df = df.merge(df2, on='id', how='left')
    # update the status value
    df['status_final'] = df['status_new'].fillna(df['status']).astype('int')
    df = df.drop(['status_new', 'status'], axis=1)
    df.rename(columns={'status_final': 'status'}, inplace=True)
    # set the datatypes
    df = df.astype({'id': 'int64', 'cost': 'int64', 'status': 'int64'})
    # sort the records by the id fied
    df = df.sort_values(by=['id'])
    # write to file
    await _writeCSV(obj, "PUNAME", df)


# loads a csv file and returns the data as a dataframe or an empty dataframe if the file does not exist. If errorIfNotExists is True then it raises an error.
def _loadCSV(filename, errorIfNotExists=False):
    if (os.path.exists(filename)):
        # sep = None forces the Python parsing engine to detect the separator as it can be tab or comman in marxan
        df = pandas.read_csv(filename, sep=None, engine='python')
    else:
        if errorIfNotExists:
            raise MarxanServicesError(
                "The file '" + filename + "' does not exist")
        else:
            df = pandas.DataFrame()
    return df


# saves the dataframe to a csv file specified by the fileToWrite, e.g. _writeCSV(self, "PUVSPRNAME", df) - this only applies to the files managed by Marxan in the input.dat file, e.g. SPECNAME, PUNAME, PUVSPRNAME, BOUNDNAME
async def _writeCSV(obj, fileToWrite, df, writeIndex=False):
    _filename = await _getProjectInputFilename(obj, fileToWrite)
    if _filename == "":  # the file has not previously been created
        raise MarxanServicesError(
            "The filename for the " + fileToWrite + ".dat file has not been set in the input.dat file")
    df.to_csv(obj.folder_input + _filename, index=writeIndex)


# writes the dataframe to the file - for files not managed in the input.dat file or if the filename has not yet been set in the input.dat file
def _writeToDatFile(file, dataframe):
    # see if the file exists
    if (os.path.exists(file)):
        # read the current data
        # sep = None forces the Python parsing engine to detect the separator as it can be tab or comman in marxan
        df = pandas.read_csv(file, sep=None, engine='python')
    else:
        # create the new dataframe
        df = pandas.DataFrame()
    # append the new records
    df = df.append(dataframe)
    # write the file
    df.to_csv(file, index=False)


# binary write useful for shapefiles etc.
def _writeFile(filename, data):
    f = open(filename, 'wb')
    f.write(data)
    f.close()


# gets a files contents as a string
def _readFile(filename):
    f = open(filename)
    s = f.read()
    f.close()
    return s


# gets a files contents as a unicode string
def _readFileUnicode(filename):
    f = io.open(filename, mode="r", encoding="utf-8")
    try:
        s = f.read()
    except (UnicodeDecodeError) as e:
        f = io.open(filename, mode="r", encoding="ISO-8859-1")
        s = f.read()
    f.close()
    return s


# writes a files contents as a unicode string
def _writeFileUnicode(filename, s, mode='w'):
    f = io.open(filename, mode, encoding="utf-8")
    f.write(s)
    f.close()


# deletes all of the files in the passed folder
def _deleteAllFiles(folder):
    files = glob.glob(folder + "*")
    for f in files:
        if f[:-3] != 'dat':  # dont try to remove the log file as it is accessed by the runMarxan function to return the data as it is written
            os.remove(f)


# copies a directory from src to dest recursively
def _copyDirectory(src, dest):
    try:
        shutil.copytree(src, dest)
    # Directories are the same
    except shutil.Error as e:
        raise MarxanServicesError('Directory not copied. Error: %s' % e)
    # Any error saying that the directory doesn't exist
    except OSError as e:
        raise MarxanServicesError('Directory not copied. Error: %s' % e)

# creates a new parameter in the *.dat file, either user (user.dat), project (input.dat) or server (server.dat), by iterating through all the files and adding the key/value if it doesnt already exist


def _addParameter(_type, key, value):
    results = []
    if (_type == 'user'):
        # get all of the user.dat files on the server
        _files = glob.glob(MARXAN_USERS_FOLDER + "*" + os.sep + "user.dat")
    elif (_type == 'project'):
        # get all of the input.dat files on the server
        _files = glob.glob(MARXAN_USERS_FOLDER + "*" +
                           os.sep + "*" + os.sep + "input.dat")
    elif (_type == 'server'):
        # get all of the input.dat files on the server
        _files = glob.glob(MARXAN_FOLDER + SERVER_CONFIG_FILENAME)
    # iterate through the files and add the keys if necessary
    for file in _files:
        # get the file contents
        s = _readFile(file)
        # get the existing keys
        keys = _getKeys(s)
        # if not present
        if not (key in keys):
            # add the key
            s = s + key + " " + value + "\n"
            _writeFileUnicode(file, s)
            logging.warning("Key '" + key + "' added to " + file)
            results.append("Key " + key + " added to " + file)
        else:
            # update the existing value
            _updateParameters(file, {key: value})
            logging.warning("Key '" + key + "' updated to '" +
                            value + "' in file " + file)
            results.append("Key '" + key + "' updated to '" +
                           value + "' in file " + file)
    return results


# updates the parameters in the *.dat file with the new parameters passed as a dict
def _updateParameters(data_file, newParams):
    if newParams:
        # get the existing parameters
        s = _readFileUnicode(data_file)
        # update any that are passed in as query params
        for k, v in newParams.items():
            try:
                p1 = s.index(k)  # get the first position of the parameter
                if p1 > -1:
                    # get the position of the end of line
                    p2 = _getEndOfLine(s[p1:])
                    s = s[:p1] + k + " " + v + s[(p1 + p2):]
                # write these parameters back to the *.dat file
                _writeFileUnicode(data_file, s)
            except ValueError:
                continue
    return


# gets the position of the end of the line which may be different in windows/unix generated files
def _getEndOfLine(text):
    try:
        p = text.index("\r\n")  # windows uses carriage return + line feed
    except (ValueError):
        p = text.index("\n")  # unix uses just line feed
    return p


# returns the key value from a dict or raises an error if the key doesnt exist
def _getDictValue(dict, key):
    if key not in list(dict.keys()):
        raise MarxanServicesError(
            "The key '" + key + "' does not exist in the dictionary")
    else:
        return dict[key]


# returns all the keys from a set of KEY/VALUE pairs in a string expression
def _getKeys(s):
    # get all the parameter keys
    # this will match against both windows and unix line endings, e.g. \r\n and \n
    matches = re.findall('\\n[A-Z1-9_]{2,}', s, re.DOTALL)
    return [m[1:] for m in matches]


# gets the key value combination from the text, e.g. PUNAME pu.dat
def _getKeyValue(text, parameterName):
    p1 = text.index(parameterName)
    # the end of line marker could either be a \r\n or a \n - get the position of both and see which is the first
    try:
        pCrLf = text[p1:].index("\r\n")
    except (ValueError):
        pCrLf = -1
    pLf = text[p1:].index("\n")
    if pCrLf > -1:
        if pLf > -1:
            if pLf < pCrLf:
                p2 = pLf
            else:
                p2 = pCrLf
    else:
        p2 = pLf
    value = text[p1 + len(parameterName) + 1:(p2 + p1)]
    # convert any boolean text strings to boolean values - these will then get returned as javascript bool types
    if value == "True":
        value = True
    if value == "False":
        value = False
    return parameterName, value


# converts a data frame with duplicate values into a normalised array
def _normaliseDataFrame(df, columnToNormaliseBy, puidColumnName, classes=None):
    if df.empty:
        return []
    response = []
    # if we are constraining the response to a fixed number of classes, then classify the data into that number of classes
    if (classes):
        # get the min value
        minValue = df.min()[columnToNormaliseBy]
        # get the max value
        maxValue = df.max()[columnToNormaliseBy]
        # if all the columnToNormaliseBy data is the same value then there is only one class
        classes = 1 if (minValue == maxValue) else classes
        # get the bin size - add 1 so that the maxValue will be in the n-1 bin
        binSize = ((maxValue + 1) - minValue) / classes
        # initialise the array of puids
        bins = [[(binSize * (i + 1)) + minValue, []] for i in range(classes)]
        # iterate through the dataframe
        for index, row in df.iterrows():
            # get the bin
            bin = int((row[columnToNormaliseBy] - minValue) / binSize)
            # append the puid
            bins[bin][1].append(int(row[puidColumnName]))
        response = bins
    else:
        # get the groups from the data (i.e. the unique values for columnToNormaliseBy in ascending order)
        groups = df.groupby(by=columnToNormaliseBy).groups
        # build the response, e.g. a normal data frame with repeated values in the columnToNormaliseBy -> [["VI", [7, 8, 9]], ["IV", [0, 1, 2, 3, 4]], ["V", [5, 6]]]
        response = [[g, df[puidColumnName][groups[g]].values.tolist()]
                    for g in groups if g not in [0]]
    return response

# updates the values in the dataframe df using a mapping dataframe - the values in the df_join_field are replaced by those in new_values_field
# e.g. df has id,prop,spf; mapping has id,new_id; calling _updateDataFrame(df, mapping, 'id', 'id', 'new_id') will update the df.id field with mapping.new_id


def _updateDataFrame(df, mapping, df_join_field, mapping_join_field, new_values_field):
    # set the index on the df
    df.set_index(df_join_field, inplace=True)
    # get a copy of the mapping df
    _mapping = mapping.copy()
    # rename the join field to match the join field in the df
    _mapping = _mapping.rename(columns={mapping_join_field: df_join_field})
    # set the index on the mapping DF
    _mapping.set_index(df_join_field, inplace=True)
    # get the column names from the df
    column_names = df.columns.to_list()
    # join the two data frames
    df = df.join(_mapping)
    # remove the index from the df
    df = df.reset_index()
    df = df.drop(df_join_field, axis=1)
    # rename the field which contains the new values
    df = df.rename(columns={new_values_field: df_join_field})
    column_names.insert(0, df_join_field)
    return df[column_names]

# gets the statistics for a species from the puvspr.dat file, i.e. count and area, as a dataframe record


def _getPuvsprStats(df, speciesId):
    # get the count of intersecting planning units
    pu_count = df[df.species.isin([speciesId])].agg(
        {'pu': ['count']})['pu'].iloc[0]
    # get the total area of the feature across all planning units
    pu_area = df[df.species.isin([speciesId])].agg(
        {'amount': ['sum']})['amount'].iloc[0]
    # return the pu_area and pu_count to the preprocessing.dat file
    return pandas.DataFrame({'id': speciesId, 'pu_area': [pu_area], 'pu_count': [pu_count]}).astype({'id': 'int', 'pu_area': 'float', 'pu_count': 'int'})


# deletes the records in the text file that have id values that match the passed ids
def _deleteRecordsInTextFile(filename, id_columnname, ids, write_index):
    if (filename) and (os.path.exists(filename)):
        # if the file exists then get the existing data
        df = _loadCSV(filename)
        # remove the records with the matching ids
        df = df[~df[id_columnname].isin(ids)]
        # write the results back to the file
        df.to_csv(filename, index=write_index)
    else:
        raise MarxanServicesError("The file '" + filename + "' does not exist")


# converts a comma-separated set of integer values to a list of integers
def _txtIntsToList(txtInts):
    if txtInts:
        return [int(s) for s in txtInts.split(",")]
    else:
        return []


# checks that all of the arguments in argumentList are in the arguments dictionary
def _validateArguments(arguments, argumentList):
    for argument in argumentList:
        if argument not in list(arguments.keys()):
            raise MarxanServicesError("Missing input argument:" + argument)


# converts the raw arguments from the request.arguments parameter into a simple dict excluding those in omitArgumentList
# e.g. _getSimpleArguments(self, ['user','project','callback']) would convert {'project': ['Tonga marine 30km2'], 'callback': ['__jp13'], 'COLORCODE': ['PiYG'], 'user': ['andrew']} to {'COLORCODE': 'PiYG'}
def _getSimpleArguments(obj, omitArgumentList):
    returnDict = {}
    for argument in obj.request.arguments:
        if argument not in omitArgumentList:
            # get_argument gets the argument as unicode - HTTPRequest.arguments gets it as a byte string
            returnDict.update({argument: obj.get_argument(argument)})
    return returnDict


# gets the passed argument name as an array of integers, e.g. ['12,15,4,6'] -> [12,15,4,6]
def _getIntArrayFromArg(arguments, argName):
    if argName in list(arguments.keys()):
        return [int(s) for s in arguments[argName][0].decode("utf-8").split(",")]
    else:
        return []

# creates a zip file (zipfilename) with all the files that have the root name feature_class_name in the folder


def _createZipfile(folder, feature_class_name):
    # get the matching files
    lstFileNames = glob.glob(folder + feature_class_name + '.*')
    # get the zip filename
    zipfilename = folder + feature_class_name + ".zip"
    with zipfile.ZipFile(zipfilename, 'w') as myzip:
        for f in lstFileNames:
            arcname = os.path.split(f)[1]
            myzip.write(f, arcname)
    # delete all of the archive files
    _deleteArchiveFiles(folder, feature_class_name)
    return zipfilename

# deletes all files matching the archivename in the folder


def _deleteArchiveFiles(folder, archivename):
    # get the matching files
    files = glob.glob(folder + archivename + '.*')
    # if there are any matching files then delete them
    if len(files) > 0:
        [os.remove(f) for f in files if f[-3:] in ['shx', 'shp', 'xml', 'sbx', 'prj', 'sbn',
                                                   'dbf', 'cpg', 'qpj', 'SHX', 'SHP', 'XML', 'SBX', 'PRJ', 'SBN', 'DBF', 'CPG', 'QPJ']]

# deletes a zip file and the archive files, e.g. deleteZippedShapefile(MARXAN_FOLDER, "pngprovshapes.zip","pngprov")


def _deleteZippedShapefile(folder, zipfile, archivename):
    # delete any archive files
    _deleteArchiveFiles(folder, archivename)
    # delete the zip file
    if (zipfile != "" and os.path.exists(folder + zipfile)):
        os.remove(folder + zipfile)

# zips a folder and all of its subfolders and puts the file into the target_dir folder - archive names are relative to foldername


def _zipfolder(foldername, zipFile):
    zipobj = zipfile.ZipFile(zipFile + '.zip', 'w', zipfile.ZIP_DEFLATED)
    # get the length of the folder
    folder_length = len(foldername) + 1
    for base, dirs, files in os.walk(foldername):
        for file in files:
            # get the archive name
            if base == foldername:
                arcname = file
            else:
                arcname = base[folder_length:] + os.sep + file
            filename = os.path.join(base, file)
            # add the file to the zip
            zipobj.write(filename, arcname)

# unzips a zip file and returns the rootname - if rejectMultipleShapefiles is True then an exception will be thrown if the zip file contains multiple shapefiles -  if searchTerm is specified then only the files that match the searchTerm will be extracted


def _unzipFile(folder, filename, rejectMultipleShapefiles=True, searchTerm=None):
    # unzip the shapefile
    if not os.path.exists(folder + filename):
        raise MarxanServicesError(
            "The zip file '" + filename + "' does not exist")
    zip_ref = zipfile.ZipFile(folder + filename, 'r')
    filenames = zip_ref.namelist()
    # check there is only one set of files
    extensions = [f[-3:] for f in filenames]
    if (len(extensions) != len(set(extensions))) and rejectMultipleShapefiles:
        raise MarxanServicesError("The zip file contains multiple shapefiles. See <a href='" +
                                  ERRORS_PAGE + "#the-zip-file-contains-multiple-shapefiles' target='blank'>here</a>")
    # if a search term is specified then only get those files with the matching search term
    if searchTerm:
        filenames = [f for f in filenames if searchTerm in f]
        if (len(filenames) == 0):
            raise MarxanServicesError(
                "There were no files in the zip file matching the text '" + searchTerm + "'")
    # do not accept zip files that contain nested files/folders
    if (filenames[0].find(os.sep) == -1):
        # get the root filename
        rootfilename = filenames[0][:-4]
        # if a search term is specified then only extract files that include the search term
        try:
            if searchTerm:
                for f in filenames:
                    zip_ref.extract(f, folder)
            else:
                zip_ref.extractall(folder)
        except (OSError) as e:
            # delete the already extracted files
            for f in filenames:
                if os.path.exists(folder + f):
                    os.remove(folder + f)
            raise MarxanServicesError(
                "No space left on device extracting the file '" + rootfilename + "'")
        else:
            zip_ref.close()
            return rootfilename
    else:  # nested files/folders - raise an error
        raise MarxanServicesError("The zipped file should not contain directories. See <a href='" +
                                  DOCS_ROOT + "/user.html#importing-existing-marxan-projects' target='blank'>here</a>")


# checks to see if the tileset already exists on mapbox
def _tilesetExists(tilesetid):
    url = "https://api.mapbox.com/tilesets/v1/" + \
        MAPBOX_USER + "." + tilesetid + "?access_token=" + MBAT
    try:
        requests.get(url)
    except (Exception) as e:
        if (e.code == 404):
            return False
    else:
        return True


# starts an upload job to mapbox from the passed feature class and returns the uploadid
async def _uploadTilesetToMapbox(feature_class_name, mapbox_layer_name):
    if (_tilesetExists(feature_class_name)):
        return "0"
    # create the file to upload to MapBox - now using shapefiles as kml files only import the name and description properties into a mapbox tileset
    zipfilename = await _exportAndZipShapefile(EXPORT_FOLDER, feature_class_name, "EPSG:3857")
    try:
        # upload to mapbox
        uploadId = _uploadTileset(zipfilename, feature_class_name)
        return uploadId
    finally:
        # delete the temporary shapefile file and zip file
        _deleteZippedShapefile(
            EXPORT_FOLDER, feature_class_name + ".zip", feature_class_name)

# uploads a tileset to mapbox using the filename of the file (filename) to upload and the name of the resulting tileset (_name)


def _uploadTileset(filename, _name):
    # create an instance of the upload service
    service = Uploader(access_token=MBAT)
    with open(filename, 'rb') as src:
        upload_resp = service.upload(src, _name)
        if 'id' in upload_resp.json().keys():
            return upload_resp.json()['id']
        else:
            raise MarxanServicesError("Failed to get an upload ID from Mapbox")

# deletes a tileset


def _deleteTileset(tilesetid):
    url = "https://api.mapbox.com/tilesets/v1/" + \
        MAPBOX_USER + "." + tilesetid + "?access_token=" + MBAT
    response = requests.delete(url)


# deletes a feature
async def _deleteFeature(feature_class_name):
    # get the data for the feature
    data = await pg.execute("SELECT oid, created_by FROM marxan.metadata_interest_features WHERE feature_class_name = %s;", data=[feature_class_name], returnFormat="Dict")
    # return if it is not found
    if len(data) == 0:
        return
    # if it is a system supplied planning grid then raise an error
    if "created_by" in data[0].keys():
        if data[0]['created_by'] == 'global admin':
            raise MarxanServicesError("The feature cannot be deleted as it is a system supplied item. See <a href='" + DOCS_ROOT +
                                      "user.html#the-planning-grid-cannot-be-deleted-as-it-is-a-system-supplied-item' target='blank'>here</a>")
    # get a list of projects that the feature is used in
    projects = _getProjectsForFeature(int(data[0]['oid']))
    # if it is in use then return an error
    if len(projects) > 0:
        raise MarxanServicesError(
            "The feature cannot be deleted as it is currently being used")
    # delete the feature
    await _deleteFeatureClass(feature_class_name)
    # delete the metadata record
    await pg.execute("DELETE FROM marxan.metadata_interest_features WHERE feature_class_name =%s;", [feature_class_name])
    # delete the Mapbox tileset
    _deleteTileset(feature_class_name)


async def _deleteFeatureClass(feature_class_name):
    # delete the feature class
    await pg.execute(sql.SQL("DROP TABLE IF EXISTS marxan.{};").format(sql.Identifier(feature_class_name)))


def _getUniqueFeatureclassName(prefix):
    # mapbox tileset ids are limited to 32 characters
    return prefix + uuid.uuid4().hex[:(32 - len(prefix))]

# finishes the db creation and uploads the feature to mapbox


async def _finishCreatingFeature(feature_class_name, name, description, source, user):
    # add an index and a record in the metadata_interest_features table
    id = await _finishImportingFeature(feature_class_name, name, description, source, user)
    # start the upload to mapbox
    uploadId = await _uploadTilesetToMapbox(feature_class_name, feature_class_name)
    return id, uploadId

# finishes a feature import by adding a spatial index and a record in the metadata_interest_features table


async def _finishImportingFeature(feature_class_name, name, description, source, user):
    # get the Mapbox tilesetId
    tilesetId = MAPBOX_USER + "." + feature_class_name
    # create an index on the geometry column
    await pg.execute(sql.SQL("CREATE INDEX idx_" + uuid.uuid4().hex + " ON marxan.{} USING GIST (geometry);").format(sql.Identifier(feature_class_name)))
    # create a primary key
    try:
        # drop any existing id columns (including the automatically created ogc_fid column)
        await pg.execute(sql.SQL("ALTER TABLE marxan.{} DROP COLUMN IF EXISTS id;").format(sql.Identifier(feature_class_name)))
        await pg.execute(sql.SQL("ALTER TABLE marxan.{} DROP COLUMN IF EXISTS ogc_fid;").format(sql.Identifier(feature_class_name)))
        await pg.execute(sql.SQL("ALTER TABLE marxan.{} ADD COLUMN id SERIAL PRIMARY KEY;").format(sql.Identifier(feature_class_name)))
    # primary key already exists
    except psycopg2.errors.InvalidTableDefinition:
        logging.warning("primary key already exists")
        pass
    try:
        # create a record for this new feature in the metadata_interest_features table
        id = await pg.execute(sql.SQL("INSERT INTO marxan.metadata_interest_features (feature_class_name, alias, description, creation_date, _area, tilesetid, extent, source, created_by) SELECT %s, %s, %s, now(), sub._area, %s, sub.extent, %s, %s FROM (SELECT ST_Area(ST_Transform(geom, 3410)) _area, box2d(geom) extent FROM (SELECT ST_Union(geometry) geom FROM marxan.{}) AS sub2) AS sub RETURNING oid;").format(sql.Identifier(feature_class_name)), data=[feature_class_name, name, description, tilesetId, source, user], returnFormat="Array")
    except (MarxanServicesError) as e:
        await _deleteFeatureClass(feature_class_name)
        if "Database integrity error" in e.args[0]:
            raise MarxanServicesError(
                "The feature '" + name + "' already exists")
        else:
            raise MarxanServicesError(e.args[0])
    return id[0]


# imports the planning unit grid from a zipped shapefile (given by filename) and starts the upload to Mapbox
async def _importPlanningUnitGrid(filename, name, description, user):
    # unzip the shapefile and get the name of the shapefile without an extension, e.g. PlanningUnitsData.zip -> planningunits.shp -> planningunits
    rootfilename = await IOLoop.current().run_in_executor(None, _unzipFile, IMPORT_FOLDER, filename)
    # get a unique feature class name for the import
    feature_class_name = _getUniqueFeatureclassName("pu_")
    try:
        # make sure the puid column is lowercase
        if _shapefileHasField(IMPORT_FOLDER + rootfilename + ".shp", "PUID"):
            raise MarxanServicesError(
                "The field 'puid' in the zipped shapefile is uppercase and it must be lowercase")
        # import the shapefile into PostGIS
        # create a record for this new feature in the metadata_planning_units table
        await pg.execute("INSERT INTO marxan.metadata_planning_units(feature_class_name,alias,description,creation_date, source,created_by,tilesetid) VALUES (%s,%s,%s,now(),'Imported from shapefile',%s,%s);", [feature_class_name, name, description, user, MAPBOX_USER + "." + feature_class_name])
        # import the shapefile
        await pg.importShapefile(IMPORT_FOLDER, rootfilename + ".shp", feature_class_name)
        # check the geometry
        await pg.isValid(feature_class_name)
        # make sure the puid column is an integer
        await pg.execute(sql.SQL("ALTER TABLE marxan.{} ALTER COLUMN puid TYPE integer;").format(sql.Identifier(feature_class_name)))
        # create the envelope for the new planning grid
        await pg.execute(sql.SQL("UPDATE marxan.metadata_planning_units SET envelope = (SELECT ST_Transform(ST_Envelope(ST_Collect(f.geometry)), 4326) FROM (SELECT ST_Envelope(geometry) AS geometry FROM marxan.{}) AS f) WHERE feature_class_name = %s;").format(sql.Identifier(feature_class_name)), [feature_class_name])
        # set the number of planning units for the new planning grid
        await pg.execute(sql.SQL("UPDATE marxan.metadata_planning_units SET planning_unit_count = (SELECT count(puid) FROM marxan.{}) WHERE feature_class_name = %s;").format(sql.Identifier(feature_class_name)), [feature_class_name])
        # start the upload to mapbox
        uploadId = _uploadTileset(IMPORT_FOLDER + filename, feature_class_name)
    except (MarxanServicesError) as e:
        if 'column' and 'puid' and 'does not exist' in e.args[0]:
            raise MarxanServicesError("The field 'puid' does not exist in the shapefile. See <a href='" +
                                      ERRORS_PAGE + "#the-field-puid-does-not-exist-in-the-shapefile' target='blank'>here</a>")
        elif 'violates unique constraint' in e.args[0]:
            raise MarxanServicesError(
                "The planning grid '" + name + "' already exists")
        else:
            raise
    finally:
        # delete the shapefile and the zip file
        _deleteZippedShapefile(IMPORT_FOLDER, filename, rootfilename)
        pass
    return {'feature_class_name': feature_class_name, 'uploadId': uploadId, 'alias': name}


# deletes a planning grid
async def _deletePlanningUnitGrid(planning_grid):
    # get the data for the planning grid
    data = await pg.execute("SELECT created_by, source FROM marxan.metadata_planning_units WHERE feature_class_name = %s;", data=[planning_grid], returnFormat="Dict")
    # return if it is not found
    if len(data) == 0:
        return
    # if it is a system supplied planning grid then raise an error
    if "created_by" in data[0].keys():
        if data[0]['created_by'] == 'global admin':
            raise MarxanServicesError("The planning grid cannot be deleted as it is a system supplied item. See <a href='" +
                                      DOCS_ROOT + "user.html#the-planning-grid-cannot-be-deleted-as-it-is-a-system-supplied-item' target='blank'>here</a>")
    # get a list of projects that the planning grid is used in
    projects = _getProjectsForPlanningGrid(planning_grid)
    # if it is in use then return an error
    if len(projects) > 0:
        raise MarxanServicesError(
            "The planning grid cannot be deleted as it is currently being used")
    # Delete the tileset on Mapbox only if the planning grid is an imported one - we dont want to delete standard country tilesets from Mapbox as they may be in use elsewhere
    if (data[0]['source'] != 'planning_grid function'):
        _deleteTileset(planning_grid)
    # delete the new planning unit record from the metadata_planning_units table
    await pg.execute("DELETE FROM marxan.metadata_planning_units WHERE feature_class_name = %s;", data=[planning_grid])
    # delete the feature class
    await pg.execute(sql.SQL("DROP TABLE IF EXISTS marxan.{};").format(sql.Identifier(planning_grid)))


# searches the folder recursively for the filename and returns an array of full filenames, e.g. ['/home/ubuntu/environment/marxan-server/users/admin/British Columbia Marine Case Study/input/spec.dat', etc]
def _getFilesInFolderRecursive(folder, filename):
    foundFiles = []
    for root, dirs, files in os.walk(folder):
        _files = [root + os.sep + f for f in files if (f == filename)]
        if len(_files) > 0:
            foundFiles.append(_files[0])
    return foundFiles


# searches the dataframe to see whether the value occurs in the column
def _dataFrameContainsValue(df, column_name, value):
    if (value in df[column_name].values):
        return True
    else:
        return False


# returns a list of projects that contain the feature with the passed featureId
def _getProjectsForFeature(featureId):
    specDatFiles = _getFilesInFolderRecursive(
        MARXAN_USERS_FOLDER, SPEC_FILENAME)
    projects = []
    for file in specDatFiles:
        # get the values from the spec.dat file
        df = _loadCSV(file)
        # search the dataframe for the species id of id
        if _dataFrameContainsValue(df, 'id', featureId):
            # get the project paths
            prjPaths = file[len(MARXAN_USERS_FOLDER):].split(os.sep)
            # if the featureid is in the projects spec.dat file, check it is not an imported project
            prjFolder = file[:len(MARXAN_USERS_FOLDER)] + \
                prjPaths[0] + os.sep + prjPaths[1] + os.sep
            # open the input file and get the key values
            values = _getKeyValuesFromFile(prjFolder + "input.dat")
            # get the OLDVERSION
            if 'OLDVERSION' in values.keys():
                if not values['OLDVERSION']:
                    projects.append({'user': prjPaths[0], 'name': prjPaths[1]})
    return projects


# returns a list of projects that use the planning grid
def _getProjectsForPlanningGrid(feature_class_name):
    inputDatFiles = _getFilesInFolderRecursive(
        MARXAN_USERS_FOLDER, "input.dat")
    projects = []
    for file in inputDatFiles:
        # open the input file and get the key values
        values = _getKeyValuesFromFile(file)
        # get the PLANNING_UNIT_NAME
        if 'PLANNING_UNIT_NAME' in values.keys():
            if values['PLANNING_UNIT_NAME'] == feature_class_name:
                prjPaths = file[len(MARXAN_USERS_FOLDER):].split(os.sep)
                projects.append({'user': prjPaths[0], 'name': prjPaths[1]})
    return projects


# populates the data in the feature_preprocessing.dat file from an existing puvspr.dat file, e.g. after an import from an old version of Marxan
async def _createFeaturePreprocessingFileFromImport(obj):
    # load the puvspr data
    df = await _getProjectInputData(obj, "PUVSPRNAME")
    if df.empty:
        raise MarxanServicesError(
            "There are no records in the puvspr.dat file")
    # calculate the statistics for all features - each record will have the species id, the sum of the planning unit areas for that species and the count of the planning units
    pivotted = df.pivot_table(index=['species'], aggfunc=[
                              'sum', 'count'], values='amount')
    # flatten the pivot table
    pivotted.columns = pivotted.columns.to_series().str.join('_')
    # create a field called id which has the same values as the species column
    pivotted['id'] = pivotted.index
    # reorder the columns
    pivotted = pivotted[['id', 'sum_amount', 'count_amount']]
    # rename the columns
    pivotted.columns = ['id', 'pu_area', 'pu_count']
    # output the file
    pivotted.to_csv(obj.folder_input +
                    FEATURE_PREPROCESSING_FILENAME, index=False)

# retrieves the gml data using the WFS endpoint and feature type


def _getGML(endpoint, featuretype):
    response = requests.get(
        endpoint + "&request=getfeature&typeNames=" + featuretype)
    return response.text

# detects whether the request is for a websocket from a tornado.httputil.HTTPServerRequest


def _requestIsWebSocket(request):
    if "upgrade" in request.headers:
        if request.headers["upgrade"] == "websocket":
            return True
        else:
            return False
    else:
        return True


# to prevent CORS errors in the client
def _checkCORS(obj):
    # no CORS policy if security is disabled or if the server is running on localhost or if the request is for a permitted method
    # or if the user is 'guest' (if this is enabled) - dont set any headers - this will only work for GET requests - cross-domwin POST requests must have the headers
    print("In check CORS...........")
    print('GUEST_USERNAME: ', GUEST_USERNAME)
    print('obj.current_user: ', obj.current_user)
    print('obj.request.host[:9]: ', obj.request.host[:9])
    print('DISABLE_SECURITY: ', DISABLE_SECURITY)
    if (DISABLE_SECURITY or obj.request.host[:9] == "localhost" or (obj.current_user == GUEST_USERNAME)):
        return
    # set the CORS headers
    _setCORS(obj)


# sets the CORS headers
def _setCORS(obj):
    print('obj: ', obj)
    print('obj.request: ', obj.request)
    # get the referer
    print('list(obj.request.headers.keys(): ',
          list(obj.request.headers.keys()))
    print('PERMITTED_DOMAINS: ', PERMITTED_DOMAINS)
    print('PERMITTED_METHODS: ', PERMITTED_METHODS)
    print('_getRESTMethod(obj.request.path): ',
          _getRESTMethod(obj.request.path))
    if "Referer" in list(obj.request.headers.keys()):
        # get the referer url, e.g. https://marxan-client-blishten.c9users.io/ or https://beta.biopama.org/marxan-client/build/
        referer = obj.request.headers.get("Referer")
        # get the origin
        parsed = urlparse(referer)
        origin = parsed.scheme + "://" + parsed.netloc
        # get the method
        method = _getRESTMethod(obj.request.path)

        # check the origin is permitted either by being in the list of permitted domains or if the referer and host are on the same machine, i.e. not cross domain - OR if a permitted method is being called
        if (origin in PERMITTED_DOMAINS) or (referer.find(obj.request.host_name) != -1) or (method in PERMITTED_METHODS):
            obj.set_header("Access-Control-Allow-Origin", origin)
            obj.set_header("Access-Control-Allow-Credentials", "true")
        else:
            # , reason = "The origin '" + referer + "' does not have permission to access the service (CORS error)"
            raise HTTPError(403, "The origin '" + origin +
                            "' does not have permission to access the service (CORS error)")
    else:
        raise HTTPError(403, NO_REFERER_ERROR)


# test all requests to make sure the user is authenticated - if not returns a 403
def _authenticate(obj):
    if DISABLE_SECURITY:
        return
    # check for an authenticated user
    if not obj.current_user:
        # if not return a 401
        raise HTTPError(401, NOT_AUTHENTICATED_ERROR)


# tests the role has access to the method
def _authoriseRole(obj, method):
    if DISABLE_SECURITY:
        return
    # get the requested role
    role = obj.get_secure_cookie("role").decode("utf-8")
    # get the list of methods that this role cannot access
    unauthorised = ROLE_UNAUTHORISED_METHODS[role]
    if method in unauthorised:
        raise HTTPError(403, "The '" + role +
                        "' role does not have permission to access the '" + method + "' service")


# tests if the user can access the service - Admin users can access projects belonging to other users
def _authoriseUser(obj):
    if DISABLE_SECURITY:
        return
    # if the call includes a user argument
    if "user" in list(obj.request.arguments.keys()):
        # see if the user argument matches the obj.current_user and is not the _clumping project (this is the only exception as it is needed for the clumping)
        if ((obj.get_argument("user") != obj.current_user) and (obj.get_argument("user") != "_clumping") and (obj.current_user != GUEST_USERNAME)):
            # get the requested role
            role = obj.get_secure_cookie("role").decode("utf-8")
            if role != "Admin":
                raise HTTPError(403, "The user '" + obj.current_user +
                                "' has no permission to access a project of another user")


# returns a boolean indicating whether the guest user is enabled on this server
def _guestUserEnabled(obj):
    _getServerData(obj)
    # get the current state
    return obj.serverData['ENABLE_GUEST_USER']

# exports a feature class to a shapfile and then zips it


async def _exportAndZipShapefile(folder, feature_class_name, tEpsgCode="EPSG:4326"):
    # export the shapefile
    await pg.exportToShapefile(folder, feature_class_name, tEpsgCode)
    # zip it up
    zipfilename = _createZipfile(folder, feature_class_name)
    return zipfilename

# returns true if the passed shapefile has the fieldname - this is case sensitive


def _shapefileHasField(shapefile, fieldname):
    # check that all the required files are present for the shapefile
    _checkZippedShapefile(shapefile)
    # get the field name list
    fieldnames = _getShapefileFieldNames(shapefile)
    return fieldname in fieldnames


# gets the names of the fields in a shapefile
def _getShapefileFieldNames(shapefile):
    fieldnames = []
    ogr.UseExceptions()
    try:
        dataSource = ogr.Open(shapefile)
        if not dataSource:
            raise MarxanServicesError(
                "Shapefile '" + shapefile + "' not found")
        daLayer = dataSource.GetLayer(0)
    except (RuntimeError) as e:
        raise MarxanServicesError(e.args[1])
    else:
        layerDefinition = daLayer.GetLayerDefn()
        count = layerDefinition.GetFieldCount()
        for i in range(count):
            fieldnames.append(layerDefinition.GetFieldDefn(i).GetName())
        return fieldnames


# returns True if the users project is currently running
def _isProjectRunning(user, project):
    # get the data from the run log file
    df = _loadCSV(MARXAN_FOLDER + RUN_LOG_FILENAME)
    # filter for running projects from the passed user and project
    return not df.loc[(df['status'] == 'Running') & (df["user"] == user) & (df["project"] == project)].empty


# gets the data from the run log as a dataframe
def _getRunLogs():
    # get the data from the run log file
    df = _loadCSV(MARXAN_FOLDER + RUN_LOG_FILENAME)
    # for those processes that are running, update the number of runs completed
    runningProjects = df.loc[df['status'] == 'Running']
    for index, row in runningProjects.iterrows():
        # get the output folder
        tmpObj = ExtendableObject()
        tmpObj.folder_output = MARXAN_USERS_FOLDER + \
            row['user'] + os.sep + row['project'] + os.sep + "output" + os.sep
        # get the number of runs completed
        numRunsCompleted = _getNumberOfRunsCompleted(tmpObj)
        # get the index for the record that needs to be updated
        i = df.loc[df['pid'] == row['pid']].index.tolist()[0]
        # update the number of runs
        df.loc[i, 'runs'] = str(numRunsCompleted) + \
            df.loc[i, 'runs'][df.loc[i, 'runs'].find("/"):]
    return df


# gets the number of runs required from the input.dat file
async def _getNumberOfRunsRequired(obj):
    if not hasattr(obj, "projectData"):
        await _getProjectData(obj)
    return [int(s['value']) for s in obj.projectData['runParameters'] if s['key'] == 'NUMREPS'][0]


# gets the number of runs completed from the output files
def _getNumberOfRunsCompleted(obj):
    files = glob.glob(obj.folder_output + "output_r*")
    return len(files)


# updates the run log with the details of the marxan job when it has stopped for whatever reason - endtime, runtime, runs and status are updated
def _updateRunLog(pid, startTime, numRunsCompleted, numRunsRequired, status):
    try:
        # load the run log
        df = _getRunLogs()
        # get the index for the record that needs to be updated
        i = df.loc[df['pid'] == pid].index.tolist()[0]
    except:
        # probably the user cleared the run log so the pid was not found
        raise MarxanServicesError(
            "Unable to update run log for pid " + str(pid) + " with status " + status)
    else:
        # update the dataframe in place
        if startTime:
            df.loc[i, 'endtime'] = datetime.datetime.now().strftime(
                "%d/%m/%y %H:%M:%S")
            df.loc[i, 'runtime'] = str(
                (datetime.datetime.now() - startTime).seconds) + "s"
        if numRunsCompleted:
            df.loc[i, 'runs'] = str(numRunsCompleted) + \
                "/" + str(numRunsRequired)
        if (df.loc[i, 'status'] == 'Running'):  # only update the status if it isnt already set
            df.loc[i, 'status'] = status
        # write the dataframe back to the run log
        df.to_csv(MARXAN_FOLDER + RUN_LOG_FILENAME, index=False, sep='\t')
        return df.loc[i, 'status']


def _debugSQLStatement(sql, connection):
    if type(sql) is str:
        logging.debug(sql)
    elif type(sql) is bytes:
        logging.debug(sql.decode("utf-8"))
    else:
        logging.debug(sql.as_string(connection))


# checks that all the necessary files in the shapefile are present - if not raises an error
def _checkZippedShapefile(shapefile):
    # check there are any files present
    files = glob.glob(shapefile[:-4] + "*")
    if (len(files) == 0):
        raise MarxanServicesError(
            "The shapefile '" + shapefile + "' was not found")
    # check all the required files are present .shp, .shx and .dbf
    if not os.path.exists(shapefile):
        raise MarxanServicesError("The *.shp file is missing in the zipfile. See <a href='" +
                                  ERRORS_PAGE + "#the-extension-file-is-missing-in-the-zipfile' target='blank'>here</a>")
    if (not os.path.exists(shapefile[:-3] + "shx")) and (not os.path.exists(shapefile[:-3] + "SHX")):
        raise MarxanServicesError("The *.shx file is missing in the zipfile. See <a href='" +
                                  ERRORS_PAGE + "#the-extension-file-is-missing-in-the-zipfile' target='blank'>here</a>")
    if (not os.path.exists(shapefile[:-3] + "dbf")) and (not os.path.exists(shapefile[:-3] + "DBF")):
        raise MarxanServicesError("The *.dbf file is missing in the zipfile. See <a href='" +
                                  ERRORS_PAGE + "#the-extension-file-is-missing-in-the-zipfile' target='blank'>here</a>")


def _getMBAT():
    with urllib.request.urlopen(MARXAN_REGISTRY) as response:
        data = response.read().decode("utf-8")
        # load as json
        _json = json.loads(data)
        # check the MBAT is there
        if 'MBAT' in _json.keys():
            return _json['MBAT']
        else:
            raise MarxanServicesError("MBAT not found in Marxan Registry")

# imports a dataframe into a table - this is not part of the PostGIS class as it uses a different connection string - and it is not asynchronous


def _importDataFrame(df, table_name):
    engine_text = 'postgresql://' + DATABASE_USER + ':' + \
        DATABASE_PASSWORD + '@' + DATABASE_HOST + '/' + DATABASE_NAME
    engine = create_engine(engine_text)
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    contents = output.getvalue()
    cur.copy_from(output, 'marxan.' + table_name,
                  null="")  # null values become ''
    conn.commit()
    conn.close()


def _getExceptionLastLine(exc_info):
    lastLine = traceback.format_exception(*exc_info)[-1]
    lastLine = lastLine[lastLine.find(":")+2:]
    return lastLine


def _deleteShutdownFile():
    if (os.path.exists(MARXAN_FOLDER + SHUTDOWN_FILENAME)):
        print("Deleting the shutdown file")
        os.remove(MARXAN_FOLDER + SHUTDOWN_FILENAME)

# runs a command in a separate process
@gen.coroutine
def _runCmd(cmd):
    if platform.system() != "Windows":
        try:
            # run the import as an asyncronous subprocess
            process = Subprocess([*shlex.split(cmd)])
            # sometimes this creates zombie processes and hangs
            result = yield process.wait_for_exit()
        except CalledProcessError as e:
            raise MarxanServicesError(
                "Error running command: " + cmd + "\n" + e.args[0])
    else:
        # run the import using the python subprocess module
        resultBytes = subprocess.check_output(
            cmd, shell=True, stderr=subprocess.STDOUT)
        result = 0 if (resultBytes.decode("utf-8") == '') else -1
    return result

# runs a set of maintenance tasks to remove orphaned tables, tmp tables and clumping projects that may remain on the server


async def _cleanup():
    # database cleanup
    await pg.execute("SELECT marxan.deletedissolvedwdpafeatureclasses()")
    await pg.execute("SELECT marxan.deleteorphanedfeatures()")
    await pg.execute("SELECT marxan.deletescratchfeatureclasses()")
    # file cleanup
    files = glob.glob(CLUMP_FOLDER + "*")
    for file in files:
        # get the file date
        fileDate = datetime.datetime.fromtimestamp(os.path.getmtime(file))
        # get the timedelta from now
        td = datetime.datetime.now() - fileDate
        if td.days > 1:
            # if the file is older than 1 day, then delete it
            os.remove(file)

####################################################################################################################################################################################################################################################################
# generic classes
####################################################################################################################################################################################################################################################################


class MarxanServicesError(Exception):
    def __init__(self, *args, **kwargs):
        super(MarxanServicesError, self)


class ExtendableObject(object):
    pass


####################################################################################################################################################################################################################################################################
# class to return data from postgis asynchronously
####################################################################################################################################################################################################################################################################
class PostGIS():
    async def initialise(self):
        # the minsize/maxsize parameters are critical otherwise you get aiopg errors (unclosed connections, GeneratorExit exceptions) - these values may not be ideal in all cases
        self.pool = await aiopg.create_pool(CONNECTION_STRING, timeout=None, minsize=50, maxsize=250)

    # executes a query and optionally returns the records or writes them to file
    async def execute(self, sql, data=None, returnFormat=None, filename=None, socketHandler=None):
        try:
            conn = await self.pool.acquire()
        except psycopg2.IntegrityError as e:
            raise MarxanServicesError("Database integrity error: " + e.args[0])
        except psycopg2.OperationalError as e:
            if ("terminating connection due to administrator command" in e.args[0]):
                raise MarxanServicesError("The database server was shutdown")
        except Exception as e:
            log("Error in pg.execute: " + e.args[0])
            raise MarxanServicesError(e.args[0])
        else:
            async with conn.cursor() as cur:
                # do any argument binding
                sql = cur.mogrify(sql, data) if data is not None else sql
                # debug the SQL if in DEBUG mode
                _debugSQLStatement(sql, cur.connection.raw)
                # if a socketHandler is passed the query is being run from a MarxanWebSocketHandler class
                if socketHandler:
                    # send a websocket message with the pid if the socketHandler is specified - this is so the query can be stopped - and prefix it with a 'q'
                    socketHandler.pid = 'q' + str(await cur.connection.get_backend_pid())
                    socketHandler.send_response(
                        {'status': 'pid', 'pid': socketHandler.pid})
                # run the query
                try:
                    await cur.execute(sql)
                except psycopg2.errors.UniqueViolation as e:
                    raise MarxanServicesError("That item already exists")
                # if the query doesnt return any records then return
                if returnFormat == None:
                    return
                # get the results
                records = []
                records = await cur.fetchall()
                if returnFormat == "Array":
                    return records
                else:
                    # get the column names for the query
                    columns = [desc[0] for desc in cur.description]
                    # create a data frame
                    df = pandas.DataFrame.from_records(
                        records, columns=columns)
                    if returnFormat == "DataFrame":
                        return df
                    # convert to a dictionary
                    elif returnFormat == "Dict":
                        return df.to_dict(orient="records")
                    # output the results to a file
                    elif returnFormat == "File":
                        df.to_csv(filename, index=False)
        finally:
            await self.pool.release(conn)

    # uses ogr2ogr to import a file into PostGIS (shapefile or gml file)
    async def importFile(self, folder, filename, feature_class_name, sEpsgCode, tEpsgCode, splitAtDateline=True):
        try:
            # drop the feature class if it already exists
            await self.execute(sql.SQL("DROP TABLE IF EXISTS marxan.{};").format(sql.Identifier(feature_class_name)))
            # using ogr2ogr - rename the geometry field from the default (wkb_geometry) to geometry
            cmd = '"' + OGR2OGR_EXECUTABLE + '" -f "PostgreSQL" PG:"host=' + DATABASE_HOST + ' user=' + DATABASE_USER + ' dbname=' + DATABASE_NAME + ' password=' + DATABASE_PASSWORD + '" "' + \
                folder + filename + '" -nlt GEOMETRY -lco SCHEMA=marxan -lco GEOMETRY_NAME=geometry -nln ' + \
                feature_class_name + ' -s_srs ' + sEpsgCode + \
                ' -t_srs ' + tEpsgCode + ' -lco precision=NO'
            logging.debug(cmd)
            # run the command
            result = await _runCmd(cmd)
            if result == 0:
                # split the feature class at the dateline
                if (splitAtDateline):
                    await self.execute(sql.SQL("UPDATE marxan.{} SET geometry = marxan.ST_SplitAtDateLine(geometry);").format(sql.Identifier(feature_class_name)))
            else:
                raise MarxanServicesError(
                    "Import failed with returncode " + str(result))
        except (MarxanServicesError) as e:
            raise
        except CalledProcessError as e:  # ogr2ogr error
            raise MarxanServicesError(e.args[0])

    # imports a shapefile into PostGIS (sEpsgCode is the source SRS and tEpsgCode is the target SRS)
    async def importShapefile(self, folder, shapefile, feature_class_name, sEpsgCode="EPSG:4326", tEpsgCode="EPSG:4326", splitAtDateline=True):
        # check that all the required files are present for the shapefile
        _checkZippedShapefile(folder + shapefile)
        # import the file
        await self.importFile(folder, shapefile, feature_class_name, sEpsgCode, tEpsgCode, splitAtDateline)

    # imports a gml file (sEpsgCode is the source SRS and tEpsgCode is the target SRS)
    async def importGml(self, folder, gmlfilename, feature_class_name, sEpsgCode="EPSG:4326", tEpsgCode="EPSG:4326", splitAtDateline=True):
        # import the file
        await self.importFile(folder, gmlfilename, feature_class_name, sEpsgCode, tEpsgCode, splitAtDateline)

    # exports a feature class from postgis to a shapefile in the exportFolder
    async def exportToShapefile(self, exportFolder, feature_class_sname, tEpsgCode="EPSG:4326"):
        # get the command to execute
        cmd = '"' + OGR2OGR_EXECUTABLE + '" -f "ESRI Shapefile" "' + exportFolder + '" PG:"host=' + DATABASE_HOST + ' user=' + DATABASE_USER + ' dbname=' + DATABASE_NAME + \
            ' password=' + DATABASE_PASSWORD + ' ACTIVE_SCHEMA=marxan" -sql "SELECT * FROM ' + \
            feature_class_sname + ';" -nln ' + feature_class_sname + ' -t_srs ' + tEpsgCode
        # run the command
        try:
            results = await _runCmd(cmd)
        # catch any unforeseen circumstances
        except CalledProcessError as e:
            raise MarxanServicesError(
                "Error exporting shapefile. " + e.output.decode("utf-8"))
        return results

    # tests to see if a feature class is valid - raises an error if not
    async def isValid(self, feature_class_name):
        # will return [false],[false,true] or [true] - so the first row will be [false] or [false,true]
        _isValid = await self.execute(sql.SQL("SELECT DISTINCT ST_IsValid(geometry) FROM marxan.{} LIMIT 1;").format(sql.Identifier(feature_class_name)), returnFormat="Array")
        if not _isValid[0][0]:
            # delete the feature class
            await self.execute(sql.SQL("DROP TABLE IF EXISTS marxan.{};").format(sql.Identifier(feature_class_name)))
            raise MarxanServicesError("The input shapefile has invalid geometries. See <a href='" +
                                      ERRORS_PAGE + "#the-input-shapefile-has-invalid geometries' target='blank'>here</a>")

    # creates a primary key on the column in the passed feature_class
    async def createPrimaryKey(self, feature_class_name, column):
        await self.execute(sql.SQL("ALTER TABLE marxan.{tbl} ADD CONSTRAINT {key} PRIMARY KEY ({col});").format(tbl=sql.Identifier(feature_class_name), key=sql.Identifier("idx_" + uuid.uuid4().hex), col=sql.Identifier(column)))


####################################################################################################################################################################################################################################################################
# subclass of Popen to allow registering callbacks when processes complete on Windows (tornado.process.Subprocess.set_exit_callback is not supported on Windows)
####################################################################################################################################################################################################################################################################
class MarxanSubprocess(Popen):
    # registers a callback function on Windows by creating another thread and polling the process to see when it is finished
    def set_exit_callback_windows(self, callback, *args, **kwargs):
        # set a reference to the thread so we can free it when the process ends
        self._thread = Thread(target=self._poll_completion,
                              args=(callback, args, kwargs)).start()

    def _poll_completion(self, callback, args, kwargs):
        # poll the process to see when it ends
        while self.poll() is None:
            time.sleep(1)
        # call the callback function with the process return code
        callback(self.returncode)
        # free the thread memory
        self._thread = None


####################################################################################################################################################################################################################################################################
# baseclass for handling REST requests
####################################################################################################################################################################################################################################################################
class MarxanRESTHandler(tornado.web.RequestHandler):
    # to prevent CORS errors in the client
    def set_default_headers(self):
        print('set_default_headers: ')
        print('DISABLE_SECURITY: ', DISABLE_SECURITY)
        if DISABLE_SECURITY:
            # The value of the 'Access-Control-Allow-Origin' header in the response must not be the wildcard '*' when the request's credentials mode is 'include'
            # self.set_header("Access-Control-Allow-Origin", "*")
            self.set_header("Access-Control-Allow-Origin", "http://localhost")
            # self.set_header("Content-Type", "application/json")
            # self.set_header("Access-Control-Allow-Headers", "content-type")
            self.set_header('Access-Control-Allow-Methods',
                            'POST, GET, OPTIONS, PATCH, PUT')
            self.set_header('Access-Control-Allow-Credentials', 'true')

        # else:

    # get the current user

    def get_current_user(self):
        if self.get_secure_cookie("user"):
            return self.get_secure_cookie("user").decode("utf-8")

    # called before the request is processed - does the neccessary authentication/authorisation

    def prepare(self):
        # get the requested method
        method = _getRESTMethod(self.request.path)
        # allow access to some methods without authentication/authorisation, e.g. to create new users or validate a user
        if method not in PERMITTED_METHODS:
            # check the referer can call the REST end point from their domain
            _checkCORS(self)
            # check the request is authenticated
            _authenticate(self)
            # check the users role has access to the requested service
            _authoriseRole(self, method)
            # check the user has access to the specific resource, i.e. the 'User' role cannot access projects from other users
            _authoriseUser(self)
            # instantiate the response dictionary
            self.response = {}
        else:
            if (method != "testTornado"):
                # a permitted method so set the CORS headers
                _setCORS(self)
        # set the folder paths for the user and optionally project
        _setFolderPaths(self, self.request.arguments)
        # self.send_response({"error": repr(e)})

    # used by all descendent classes to write the return payload and send it
    def send_response(self, response):
        try:
            # set the return header as json
            self.set_header('Content-Type', 'application/json')
            # convert the response dictionary to json
            content = json.dumps(response)
        # sometimes the Marxan log causes json encoding issues
        except (UnicodeDecodeError) as e:
            if 'log' in list(response.keys()):
                response.update({"log": "Server warning: Unable to encode the Marxan log. <br/>" +
                                 repr(e), "warning": "Unable to encode the Marxan log"})
                content = json.dumps(response)
        finally:
            if "callback" in list(self.request.arguments.keys()):
                self.write(self.get_argument("callback") + "(" + content + ")")
            else:
                self.write(content)

    # uncaught exception handling that captures any exceptions in the descendent classes and writes them back to the client
    def write_error(self, status_code, **kwargs):
        if "exc_info" in kwargs:
            trace = ""
            for line in traceback.format_exception(*kwargs["exc_info"]):
                trace = trace + line
            lastLine = traceback.format_exception(
                *kwargs["exc_info"])[len(traceback.format_exception(*kwargs["exc_info"]))-1]
            # remove the exception class from the lastline
            lastLine = lastLine[lastLine.find(":")+2:]
            # when an error is encountered, the headers are reset causing CORS errors - so set them again here
            if not DISABLE_SECURITY:
                try:
                    _checkCORS(self)  # may throw a 403
                except:  # so handle the exception
                    pass
            # self.set_status(status_code) #this will return an HTTP server error rather than a 200 status code
            # returning an http status code of 200 can be caught by jsonp
            self.set_status(200)
            self.send_response({"error": lastLine, "trace": trace})
            self.finish()

####################################################################################################################################################################################################################################################################
# RequestHandler subclasses
####################################################################################################################################################################################################################################################################


class methodNotFound(MarxanRESTHandler):
    def prepare(self):
        if 'Upgrade' in self.request.headers:
            # websocket unsupported method - but we cant send back a WebSocket response so raise a 501
            raise tornado.web.HTTPError(
                500, "The method is not supported or the parameters are wrong on this Marxan Server " + MARXAN_SERVER_VERSION)
        else:
            # GET/POST unsupported method
            _raiseError(
                self, "The method is not supported or the parameters are wrong on this Marxan Server " + MARXAN_SERVER_VERSION)

# toggles whether the guest user is enabled or not on this server


class toggleEnableGuestUser(MarxanRESTHandler):
    def get(self):
        try:
            _getServerData(self)
            # get the current state
            enabled = self.serverData['ENABLE_GUEST_USER']
            if enabled:
                enabledString = "False"
            else:
                enabledString = "True"
            _updateParameters(MARXAN_FOLDER + SERVER_CONFIG_FILENAME,
                              {"ENABLE_GUEST_USER": enabledString})
            # set the response
            self.send_response({'enabled': not enabled})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# creates a new user
# POST ONLY


class createUser(MarxanRESTHandler):
    def post(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, [
                               "user", "password", "fullname", "email"])
            # create the user
            _createUser(self, self.get_argument('user'), self.get_argument(
                'fullname'), self.get_argument('email'), self.get_argument('password'))
            # copy each case study into the users folder
            folders = glob.glob(CASE_STUDIES_FOLDER + "*/")
            # iterate through the case studies
            for folder in folders:
                _cloneProject(folder, MARXAN_USERS_FOLDER +
                              self.get_argument('user') + os.sep)
            # set the response
            self.send_response(
                {'info': "User '" + self.get_argument('user') + "' created"})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# creates a project
# POST ONLY


class createProject(MarxanRESTHandler):
    async def post(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, [
                               'user', 'project', 'description', 'planning_grid_name', 'interest_features', 'target_values', 'spf_values'])
            # create the empty project folder
            _createProject(self, self.get_argument('project'))
            # update the projects parameters
            _updateParameters(self.folder_project + PROJECT_DATA_FILENAME, {'DESCRIPTION': self.get_argument(
                'description'), 'CREATEDATE': datetime.datetime.now().strftime("%d/%m/%y %H:%M:%S"), 'PLANNING_UNIT_NAME': self.get_argument('planning_grid_name')})
            # create the spec.dat file
            await _updateSpeciesFile(self, self.get_argument("interest_features"), self.get_argument("target_values"), self.get_argument("spf_values"), True)
            # create the pu.dat file
            await _createPuFile(self, self.get_argument('planning_grid_name'))
            # set the response
            self.send_response({'info': "Project '" + self.get_argument('project') + "' created",
                                'name': self.get_argument('project'), 'user': self.get_argument('user')})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])


# creates a simple project for the import wizard
# POST ONLY
class createImportProject(MarxanRESTHandler):
    def post(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['user', 'project'])
            # create the empty project folder
            _createProject(self, self.get_argument('project'))
            # set the response
            self.send_response({'info': "Project '" + self.get_argument(
                'project') + "' created", 'name': self.get_argument('project')})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# updates a project from the Marxan old version to the new version
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/upgradeProject?user=andrew&project=test2&callback=__jp7


class upgradeProject(MarxanRESTHandler):
    async def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['user', 'project'])
            # get the projects existing data from the input.dat file
            old = _readFileUnicode(self.folder_project + PROJECT_DATA_FILENAME)
            # get an empty projects data
            new = _readFileUnicode(
                EMPTY_PROJECT_TEMPLATE_FOLDER + PROJECT_DATA_FILENAME)
            # everything from the 'DESCRIPTION No description' needs to be added
            pos = new.find("DESCRIPTION No description")
            if pos > -1:
                newText = new[pos:]
                old = old + "\n" + newText
                _writeFileUnicode(self.folder_project +
                                  PROJECT_DATA_FILENAME, old)
            else:
                raise MarxanServicesError(
                    "Unable to update the old version of Marxan to the new one")
            # populate the feature_preprocessing.dat file using data in the puvspr.dat file
            await _createFeaturePreprocessingFileFromImport(self)
            # delete the contents of the output folder
            _deleteAllFiles(self.folder_output)
            # set the response
            self.send_response({'info': "Project '" + self.get_argument("project") +
                                "' updated", 'project': self.get_argument("project")})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# deletes a project
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/deleteProject?user=andrew&project=test2&callback=__jp7


class deleteProject(MarxanRESTHandler):
    async def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['user', 'project'])
            # get the existing projects
            await _getProjects(self)
            if len(self.projects) == 1:
                raise MarxanServicesError("You cannot delete all projects")
            _deleteProject(self)
            # set the response
            self.send_response({'info': "Project '" + self.get_argument("project") +
                                "' deleted", 'project': self.get_argument("project")})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])


# clones the project
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/cloneProject?user=admin&project=Start%20project&callback=__jp15
class cloneProject(MarxanRESTHandler):
    def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['user', 'project'])
            # clone the folder recursively
            clonedName = _cloneProject(self.folder_project, self.folder_user)
            # set the response
            self.send_response(
                {'info': "Project '" + clonedName + "' created", 'name': clonedName})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# creates n clones of the project with a range of BLM values in the _clumping folder
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/createProjectGroup?user=admin&project=Start%20project&copies=5&blmValues=0.1,0.2,0.3,0.4,0.5&callback=__jp15


class createProjectGroup(MarxanRESTHandler):
    def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, [
                               'user', 'project', 'copies', 'blmValues'])
            # get the BLM values as a list
            blmValuesList = self.get_argument("blmValues").split(",")
            # initialise the project name array
            projects = []
            # clone the users project folder n times
            for i in range(int(self.get_argument("copies"))):
                # get the project name
                projectName = uuid.uuid4().hex
                # add the project name to the array
                projects.append({'projectName': projectName, 'clump': i})
                shutil.copytree(self.folder_project,
                                CLUMP_FOLDER + projectName)
                # delete the contents of the output folder in that cloned project
                _deleteAllFiles(CLUMP_FOLDER + projectName +
                                os.sep + "output" + os.sep)
                # update the BLM and NUMREP values in the project
                _updateParameters(CLUMP_FOLDER + projectName + os.sep +
                                  PROJECT_DATA_FILENAME, {'BLM': blmValuesList[i], 'NUMREPS': '1'})
            # set the response
            self.send_response(
                {'info': "Project group created", 'data': projects})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])


# deletes a project cluster
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/deleteProjects?projectNames=2dabf1b862da4c2e87b2cd9d8b38bb73,81eda0a43a3248a8b4881caae160667a,313b0d3f733142e3949cf6129855be19,739f40f4d1c94907b2aa814470bcd7f7,15210235bec341238a816ce43eb2b341&callback=__jp15
class deleteProjects(MarxanRESTHandler):
    def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['projectNames'])
            # get the project names
            projectNames = self.get_argument("projectNames").split(",")
            # delete the folders
            for projectName in projectNames:
                if os.path.exists(CLUMP_FOLDER + projectName):
                    shutil.rmtree(CLUMP_FOLDER + projectName)
            # set the response
            self.send_response({'info': "Projects deleted"})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])


# renames a project
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/renameProject?user=andrew&project=Tonga%20marine%2030km2&newName=Tonga%20marine%2030km&callback=__jp5
class renameProject(MarxanRESTHandler):
    def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, [
                               'user', 'project', 'newName'])
            # rename the folder
            os.rename(self.folder_project, self.folder_user +
                      self.get_argument("newName"))
            # set the new name as the users last project so it will load on login
            _updateParameters(self.folder_user + USER_DATA_FILENAME,
                              {'LASTPROJECT': self.get_argument("newName")})
            # set the response
            self.send_response({"info": "Project renamed to '" + self.get_argument(
                "newName") + "'", 'project': self.get_argument("project")})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/getCountries?callback=__jp0


class getCountries(MarxanRESTHandler):
    async def get(self):
        try:
            content = await pg.execute("SELECT t.iso3, t.name_iso31, CASE WHEN m.iso3 IS NULL THEN False ELSE True END has_marine FROM marxan.gaul_2015_simplified_1km t LEFT JOIN marxan.eez_simplified_1km m on t.iso3 = m.iso3 WHERE t.iso3 NOT LIKE '%|%' ORDER BY lower(t.name_iso31);", returnFormat="Dict")
            self.send_response({'records': content})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/getPlanningUnitGrids?callback=__jp0


class getPlanningUnitGrids(MarxanRESTHandler):
    async def get(self):
        try:
            planningUnitGrids = await _getPlanningUnitGrids()
            self.send_response(
                {'info': 'Planning unit grids retrieved', 'planning_unit_grids': planningUnitGrids})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# imports a zipped planning unit shapefile which has been uploaded to the marxan root folder into PostGIS as a planning unit grid feature class
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/importPlanningUnitGrid?filename=pu_sample.zip&name=pu_test&description=wibble&callback=__jp5


class importPlanningUnitGrid(MarxanRESTHandler):
    async def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, [
                               'filename', 'name', 'description'])
            # import the shapefile
            data = await _importPlanningUnitGrid(self.get_argument('filename'), self.get_argument('name'), self.get_argument('description'), self.get_current_user())
            # set the response
            self.send_response({'info': "Planning grid '" + self.get_argument('name') + "' imported",
                                'feature_class_name': data['feature_class_name'], 'uploadId': data['uploadId'], 'alias': data['alias']})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# exports a planning unit grid to a shapefile
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/exportPlanningUnitGrid?name=pu_ton_marine_hexagon_50


class exportPlanningUnitGrid(MarxanRESTHandler):
    async def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['name'])
            # export the shapefile
            zipfilename = await _exportAndZipShapefile(EXPORT_FOLDER, self.get_argument('name'))
            # set the response
            self.send_response({'info': "Planning grid '" + self.get_argument(
                'name') + "' exported", 'filename': self.get_argument('name') + ".zip"})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])


# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/deletePlanningUnitGrid?planning_grid_name=pu_f9609f7a4cb0406e8bea4bfa00772&callback=__jp10
class deletePlanningUnitGrid(MarxanRESTHandler):
    async def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['planning_grid_name'])
            # call the internal function
            await _deletePlanningUnitGrid(self.get_argument('planning_grid_name'))
            # set the response
            self.send_response({'info': 'Planning grid deleted'})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# validates a user with the passed credentials
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/validateUser?user=andrew&password=thargal88&callback=__jp2


class validateUser(MarxanRESTHandler):
    def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['user', 'password'])
            # see if the guest user is enabled
            if ((not _guestUserEnabled(self)) and (self.get_argument("user") == GUEST_USERNAME)):
                raise MarxanServicesError(
                    "The guest user is not enabled on this server")
            # get the user data from the user.dat file
            _getUserData(self)
            # compare the passed password to the one in the user.dat file
            if self.get_argument("password") == self.userData["PASSWORD"]:
                # if the request is secure, then set the secure response header for the cookie
                secure = True if self.request.protocol == 'https' else False
                # set a response cookie for the authenticated user
                self.set_secure_cookie("user", self.get_argument(
                    "user"), httponly=True, samesite=None, secure=secure)
                # set a response cookie for the authenticated users role
                self.set_secure_cookie(
                    "role", self.userData["ROLE"], httponly=True, samesite=None, secure=secure)
                # set the response
                self.send_response(
                    {'info': "User " + self.user + " validated"})
            else:
                # invalid login
                raise MarxanServicesError("Invalid user/password")
        except MarxanServicesError as e:
            _raiseError(self, "Login failed: " + e.args[0])


# logs the user out and resets the cookies
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/logout?callback=__jp2
class logout(MarxanRESTHandler):
    def get(self):
        try:
            self.clear_cookie("user")
            self.clear_cookie("role")
            # set the response
            self.send_response({'info': "User logged out"})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# resends the password to the passed email address (NOT CURRENTLY IMPLEMENTED)


class resendPassword(MarxanRESTHandler):
    def get(self):
        # set the response
        self.send_response({'info': 'Not currently implemented'})


# gets a users information from the user folder
# curl 'https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/getUser?user=andrew&callback=__jp1' -H 'If-None-Match: "0798406453417c47c0b5ab5bd11d56a60fb4df7d"' -H 'Accept-Encoding: gzip, deflate, br' -H 'Accept-Language: en-US,en;q=0.9,fr;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110Safari/537.36' -H 'Accept: */*' -H 'Referer: https://marxan-client-blishten.c9users.io/' -H 'Cookie: c9.live.user.jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpZCI6IjE2MzQxNDgiLCJuYW1lIjoiYmxpc2h0ZW4iLCJjb2RlIjoiOWNBUzdEQldsdWYwU2oyU01ZaEYiLCJpYXQiOjE1NDgxNDg0MTQsImV4cCI6MTU0ODIzNDgxNH0.yJ9mPz4bM7L3htL8vXVFMCcQpTO0pkRvhNHJP9WnJo8; c9.live.user.sso=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpZCI6IjE2MzQxNDgiLCJuYW1lIjoiYmxpc2h0ZW4iLCJpYXQiOjE1NDgxNDg0MTQsImV4cCI6MTU0ODIzNDgxNH0.ifW5qlkpC19iyMNBgZLtGZzxuMRyHKWldGg3He-__gI; role="2|1:0|10:1548151226|4:role|8:QWRtaW4=|d703b0f18c81cf22c85f41c536f99589ce11492925d85833e78d3d66f4d7fd62"; user="2|1:0|10:1548151226|4:user|8:YW5kcmV3|e5ed3b87979273b1b8d1b8983310280507941fe05fb665847e7dd5dacf36348d"' -H 'Connection: keep-alive' --compressed
class getUser(MarxanRESTHandler):
    def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['user'])
            # get the user data from the user.dat file
            _getUserData(self)
            # get the notifications data from the notifications.dat file
            ids = _getNotificationsData(self)
            # get the permissions for the users role
            role = self.userData["ROLE"]
            unauthorised = ROLE_UNAUTHORISED_METHODS[role]
            # set the response
            self.send_response({'info': "User data received", "userData": {k: v for k, v in self.userData.items(
            ) if k != 'PASSWORD'}, "unauthorisedMethods": unauthorised, 'dismissedNotifications': ids})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])


# gets a list of all users
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/getUsers
class getUsers(MarxanRESTHandler):
    def get(self):
        try:
            # get the users
            users = _getUsers()
            # get all the data for those users
            usersData = _getUsersData(users)
            # set the response
            self.send_response(
                {'info': 'Users data received', 'users': usersData})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])


# deletes a user
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/deleteUser?user=asd2
class deleteUser(MarxanRESTHandler):
    def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['user'])
            shutil.rmtree(self.folder_user)
            # set the response
            self.send_response({'info': 'User deleted'})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# gets project information from the input.dat file
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/getProject?user=admin&project=Start%20project&callback=__jp2


class getProject(MarxanRESTHandler):
    async def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['user', 'project'])
            if (self.get_argument("user") == GUEST_USERNAME):
                self.send_response(
                    {'error': "Logged on as read-only guest user"})
            else:
                # see if the project exists
                if not os.path.exists(self.folder_project):
                    raise MarxanServicesError(
                        "The project '" + self.get_argument("project") + "' does not exist")
                # if the project name is an empty string, then get the first project for the user
                if (self.get_argument("project") == ""):
                    self.projects = await _getProjectsForUser(self.get_argument("user"))
                    project = self.projects[0]['name']
                    # set the project argument
                    self.request.arguments['project'] = [
                        project.encode("utf-8")]
                    # and set the paths to this project
                    _setFolderPaths(self, self.request.arguments)
                # get the project data from the input.dat file
                await _getProjectData(self)
                # get the species data from the spec.dat file and the PostGIS database
                await _getSpeciesData(self)
                # get the species preprocessing from the feature_preprocessing.dat file
                _getSpeciesPreProcessingData(self)
                # get the planning units information
                await _getPlanningUnitsData(self)
                # get the protected area intersections
                _getProtectedAreaIntersectionsData(self)
                # get the costs data
                _getCosts(self)
                # set the project as the users last project so it will load on login - but only if the current user is loading one of their own projects
                if (self.current_user == self.get_argument("user")):
                    _updateParameters(self.folder_user + USER_DATA_FILENAME,
                                      {'LASTPROJECT': self.get_argument("project")})
                # set the response
                self.send_response({'user': self.get_argument("user"), 'project': self.projectData["project"], 'metadata': self.projectData["metadata"], 'files': self.projectData["files"], 'runParameters': self.projectData["runParameters"], 'renderer': self.projectData["renderer"], 'features': self.speciesData.to_dict(
                    orient="records"), 'feature_preprocessing': self.speciesPreProcessingData.to_dict(orient="split")["data"], 'planning_units': self.planningUnitsData, 'protected_area_intersections': self.protectedAreaIntersectionsData, 'costnames': self.costNames})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# gets feature information from postgis
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/getFeature?oid=63407942&callback=__jp2


class getFeature(MarxanRESTHandler):
    async def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['oid'])
            # get the data
            await _getFeature(self, self.get_argument("oid"))
            # set the response
            self.send_response({"data": self.data.to_dict(orient="records")})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/exportFeature?name=intersesting_habitat
# exports a feature to a shapefile


class exportFeature(MarxanRESTHandler):
    async def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['name'])
            # export the shapefile
            zipfilename = await _exportAndZipShapefile(EXPORT_FOLDER, self.get_argument('name'))
            # set the response
            self.send_response(
                {'info': "Feature '" + self.get_argument('name') + "' exported", 'filename': zipfilename})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])


# gets the features planning unit ids from the puvspr.dat file
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/getFeaturePlanningUnits?user=andrew&project=Tonga%20marine%2030Km2&oid=63407942&callback=__jp2
class getFeaturePlanningUnits(MarxanRESTHandler):
    async def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, [
                               'user', 'project', 'oid'])
            # get the data from the puvspr.dat file as a dataframe
            df = await _getProjectInputData(self, "PUVSPRNAME")
            # get the planning unit ids as a list
            puids = df.loc[df['species'] == int(
                self.get_argument("oid"))]['pu'].unique().tolist()
            # set the response
            self.send_response({"data": puids})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])


# gets species information for a specific project from the spec.dat file
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/getSpeciesData?user=admin&project=Start%20project&callback=__jp3
class getSpeciesData(MarxanRESTHandler):
    async def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['user', 'project'])
            # get the species data from the spec.dat file and PostGIS
            await _getSpeciesData(self)
            # set the response
            self.send_response(
                {"data": self.speciesData.to_dict(orient="records")})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])


# gets all species information from the PostGIS database
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/getAllSpeciesData?callback=__jp2
class getAllSpeciesData(MarxanRESTHandler):
    async def get(self):
        try:
            # get all the species data
            await _getAllSpeciesData(self)
            # set the response
            self.send_response({"info": "All species data received",
                                "data": self.allSpeciesData.to_dict(orient="records")})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# gets the species preprocessing information from the feature_preprocessing.dat file
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/getSpeciesPreProcessingData?user=admin&project=Start%20project&callback=__jp2


class getSpeciesPreProcessingData(MarxanRESTHandler):
    def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['user', 'project'])
            # get the species preprocessing data
            _getSpeciesPreProcessingData(self)
            # set the response
            self.send_response(
                {"data": self.speciesPreProcessingData.to_dict(orient="split")["data"]})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# gets the planning units status information from the pu.dat file
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/getPlanningUnitsData?user=admin&project=Start%20project&callback=__jp2


class getPlanningUnitsData(MarxanRESTHandler):
    async def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['user', 'project'])
            # get the planning units information
            await _getPlanningUnitsData(self)
            # set the response
            self.send_response({"data": self.planningUnitsData})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])


# gets the planning units cost information from the pu.dat file
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/getPlanningUnitsCostData?user=admin&project=Start%20project&callback=__jp2
class getPlanningUnitsCostData(MarxanRESTHandler):
    async def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['user', 'project'])
            # get the planning units cost information
            await _getPlanningUnitsCostData(self)
            # set the response
            self.send_response({"data": self.planningUnitsData})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# gets a list of the custom cost profiles for a project
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/getCosts?user=admin&project=Start%20project&callback=__jp2


class getCosts(MarxanRESTHandler):
    def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['user', 'project'])
            # get the list of cost files for the project
            _getCosts(self)
            # set the response
            self.send_response({"data": self.costNames})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# updates a projects costs in the pu.dat file using the named cost profile
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/updateCosts?user=admin&project=Start%20project&costname=wibble&callback=__jp2


class updateCosts(MarxanRESTHandler):
    async def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, [
                               'user', 'project', 'costname'])
            # update the costs
            await _updateCosts(self, self.get_argument("costname"))
            # set the response
            self.send_response({"info": 'Costs updated'})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# deletes a cost profile
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/deleteCosts?user=admin&project=Start%20project&costname=wibble&callback=__jp2


class deleteCost(MarxanRESTHandler):
    def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, [
                               'user', 'project', 'costname'])
            # delete the cost
            _deleteCost(self, self.get_argument("costname"))
            # set the response
            self.send_response({"info": 'Cost deleted'})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])


# gets the intersections of the planning units with the protected areas from the protected_area_intersections.dat file
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/getProtectedAreaIntersectionsData?user=admin&project=Start%20project&callback=__jp2
class getProtectedAreaIntersectionsData(MarxanRESTHandler):
    def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['user', 'project'])
            # get the protected area intersections
            _getProtectedAreaIntersectionsData(self)
            # set the response
            self.send_response({"data": self.protectedAreaIntersectionsData})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])


# gets the Marxan log for the project
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/getMarxanLog?user=admin&project=Start%20project&callback=__jp2
class getMarxanLog(MarxanRESTHandler):
    def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['user', 'project'])
            # get the log
            _getMarxanLog(self)
            # set the response
            self.send_response({"log": self.marxanLog})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])


# gets the best solution for the project
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/getBestSolution?user=admin&project=Start%20project&callback=__jp2
class getBestSolution(MarxanRESTHandler):
    def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['user', 'project'])
            # get the best solution
            _getBestSolution(self)
            # set the response
            self.send_response(
                {"data": self.bestSolution.to_dict(orient="split")["data"]})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# gets the output summary for the project
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/getOutputSummary?user=admin&project=Start%20project&callback=__jp2


class getOutputSummary(MarxanRESTHandler):
    def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['user', 'project'])
            # get the output sum
            _getOutputSummary(self)
            # set the response
            self.send_response(
                {"data": self.outputSummary.to_dict(orient="split")["data"]})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])


# gets the summed solution for the project
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/getSummedSolution?user=admin&project=Start%20project&callback=__jp2
class getSummedSolution(MarxanRESTHandler):
    def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['user', 'project'])
            # get the summed solution
            _getSummedSolution(self)
            # set the response
            self.send_response({"data": self.summedSolution})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])


# gets an individual solution
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/getSolution?user=admin&project=Start%20project&solution=1&callback=__jp7
class getSolution(MarxanRESTHandler):
    def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, [
                               'user', 'project', 'solution'])
            # get the solution
            _getSolution(self, self.get_argument("solution"))
            # get the corresponding missing values file, e.g. output_mv00031.txt - not for clumping projects
            if (self.get_argument("user") != '_clumping'):
                _getMissingValues(self, self.get_argument("solution"))
                # set the response
                self.send_response({'solution': self.solution, 'mv': self.missingValues, 'user': self.get_argument(
                    "user"), 'project': self.get_argument("project")})
            else:
                self.send_response({'solution': self.solution, 'user': self.get_argument(
                    "user"), 'project': self.get_argument("project")})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# gets the missing values for a single solution
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/getMissingValues?user=admin&project=Start%20project&solution=1&callback=__jp7


class getMissingValues(MarxanRESTHandler):
    def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, [
                               'user', 'project', 'solution'])
            # get the missing values file, e.g. output_mv00031.txt
            _getMissingValues(self, self.get_argument("solution"))
            # set the response
            self.send_response({'missingValues': self.missingValues})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# gets the combined results for the project
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/getResults?user=admin&project=Start%20project&callback=__jp2


class getResults(MarxanRESTHandler):
    def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['user', 'project'])
            # get the log
            _getMarxanLog(self)
            # get the best solution
            _getBestSolution(self)
            # get the output sum
            _getOutputSummary(self)
            # get the summed solution
            _getSummedSolution(self)
            # set the response
            self.send_response({'info': 'Results loaded', 'log': self.marxanLog, 'mvbest': self.bestSolution.to_dict(orient="split")[
                               "data"], 'summary': self.outputSummary.to_dict(orient="split")["data"], 'ssoln': self.summedSolution})
        except (MarxanServicesError):
            self.send_response({'info': 'No results available'})


# gets the data from the server.dat file as an abject
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/getServerData
class getServerData(MarxanRESTHandler):
    def get(self):
        try:
            # get the data from the server.dat file
            _getServerData(self)
            # get any shutdown timeouts if they have been set
            shutdownTime = _readFile(MARXAN_FOLDER + SHUTDOWN_FILENAME) if (
                os.path.exists(MARXAN_FOLDER + SHUTDOWN_FILENAME)) else None
            if shutdownTime:
                self.serverData.update({'SHUTDOWNTIME': shutdownTime})
            # delete sensitive information from the server config data
            del self.serverData['COOKIE_RANDOM_VALUE']
            del self.serverData['DATABASE_HOST']
            del self.serverData['DATABASE_NAME']
            del self.serverData['DATABASE_PASSWORD']
            del self.serverData['DATABASE_USER']
            # set the response
            self.send_response(
                {'info': 'Server data loaded', 'serverData': self.serverData})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# gets a list of projects for the user
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/getProjects?user=andrew&callback=__jp2


class getProjects(MarxanRESTHandler):
    async def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['user'])
            # get the projects
            await _getProjects(self)
            # set the response
            self.send_response({"projects": self.projects})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])


# gets all projects and their planning unit grids
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/getProjectsWithGrids?&callback=__jp2
class getProjectsWithGrids(MarxanRESTHandler):
    async def get(self):
        try:
            matches = []
            for root, dirnames, filenames in os.walk(MARXAN_USERS_FOLDER):
                for filename in fnmatch.filter(filenames, 'input.dat'):
                    matches.append(os.path.join(root, filename))
            projects = []
            for match in matches:
                # get the user from the matching filename
                user = match[len(MARXAN_USERS_FOLDER):match[len(MARXAN_USERS_FOLDER):].find(
                    os.sep) + len(MARXAN_USERS_FOLDER)]
                # get the project from the matching filename
                project = match[match.find(
                    user) + len(user) + 1:match.rfind(os.sep)]
                # open the input file and get the key values
                values = _getKeyValuesFromFile(match)
                projects.append({'user': user, 'project': project, 'feature_class_name':
                                 values['PLANNING_UNIT_NAME'], 'description': values['DESCRIPTION']})
            # make the projects dict into a dataframe so we can join it to the data from the planning grids table
            df = pandas.DataFrame(projects)
            # set an index on the dataframe
            df = df.set_index("feature_class_name")
            # get the planning unit grids
            grids = await _getPlanningUnitGrids()
            # make a dataframe of the planning grids records
            df2 = pandas.DataFrame(grids)
            # remove the duplicate description column
            df2 = df2.drop(
                columns=['description', 'aoi_id', 'country_id', 'source'])
            # set an index on the dataframe
            df2 = df2.set_index("feature_class_name")
            # join the projects to the planning grids and replace NaNs (in number fields) with None - otherwise the output json is '_area': NaN which causes some json parses to raise an error - it will be replaced with '_area': null
            df = df.join(df2).replace({pandas.np.nan: None})
            self.send_response(
                {'info': "Projects data returned", 'data': df.to_dict(orient="records")})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])


# updates the spec.dat file with the posted data
class updateSpecFile(MarxanRESTHandler):
    async def post(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, [
                               'user', 'project', 'interest_features', 'spf_values', 'target_values'])
            # update the spec.dat file and other related files
            await _updateSpeciesFile(self, self.get_argument("interest_features"), self.get_argument("target_values"), self.get_argument("spf_values"))
            # set the response
            self.send_response({'info': "spec.dat file updated"})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])


# updates the pu.dat file with the posted data
class updatePUFile(MarxanRESTHandler):
    async def post(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['user', 'project'])
            # get the ids for the different statuses
            status1_ids = _getIntArrayFromArg(
                self.request.arguments, "status1")
            status2_ids = _getIntArrayFromArg(
                self.request.arguments, "status2")
            status3_ids = _getIntArrayFromArg(
                self.request.arguments, "status3")
            # update the file
            await _updatePuFile(self, status1_ids, status2_ids, status3_ids)
            # set the response
            self.send_response({'info': "pu.dat file updated"})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])


# returns data for a planning unit including a set of features if there are some
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/getPUData?user=admin&project=Start%20project&puid=10561&callback=__jp2
class getPUData(MarxanRESTHandler):
    async def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, [
                               'user', 'project', 'puid'])
            # get the planning unit data
            pu_df = await _getProjectInputData(self, "PUNAME")
            pu_data = pu_df.loc[pu_df['id'] == int(
                self.get_argument('puid'))].iloc[0]
            # get a set of feature IDs from the puvspr file
            df = await _getProjectInputData(self, "PUVSPRNAME")
            if not df.empty:
                features = df.loc[df['pu'] == int(self.get_argument('puid'))]
            else:
                features = pandas.DataFrame()
            # set the response
            self.send_response({"info": 'Planning unit data returned', 'data': {
                               'features': features.to_dict(orient="records"), 'pu_data': pu_data.to_dict()}})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# used to populate the feature_preprocessing.dat file from an imported puvspr.dat file
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/createFeaturePreprocessingFileFromImport?user=andrew&project=test&callback=__jp2
# not currently used


class createFeaturePreprocessingFileFromImport(MarxanRESTHandler):
    async def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['user', 'project'])
            # run the internal routine
            await _createFeaturePreprocessingFileFromImport(self)
            # set the response
            self.send_response(
                {'info': "feature_preprocessing.dat file populated"})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# creates a new parameter in the *.dat file, either user (user.dat) or project (project.dat), by iterating through all the files and adding the key/value if it doesnt already exist
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/addParameter?type=user&key=REPORTUNITS&value=Ha&callback=__jp2


class addParameter(MarxanRESTHandler):
    def get(self):
        try:
            # validate the input arguments - the type parameter is one of {'user','project'}
            _validateArguments(self.request.arguments, [
                               'type', 'key', 'value'])
            # add the parameter
            results = _addParameter(self.get_argument(
                'type'), self.get_argument('key'), self.get_argument('value'))
            # set the response
            self.send_response({'info': results})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])


# updates parameters in the users user.dat file
# POST ONLY
class updateUserParameters(MarxanRESTHandler):
    def post(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['user'])
            # get the parameters to update as a simple dict
            params = _getSimpleArguments(self, ['user', 'callback'])
            # update the parameters
            _updateParameters(self.folder_user + USER_DATA_FILENAME, params)
            # set the response
            self.send_response(
                {'info': ",".join(list(params.keys())) + " parameters updated"})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# updates parameters in the projects input.dat file
# POST ONLY


class updateProjectParameters(MarxanRESTHandler):
    def post(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['user', 'project'])
            # get the parameters to update as a simple dict
            params = _getSimpleArguments(self, ['user', 'project', 'callback'])
            # update the parameters
            _updateParameters(self.folder_project +
                              PROJECT_DATA_FILENAME, params)
            # set the response
            self.send_response(
                {'info': ",".join(list(params.keys())) + " parameters updated"})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])


# lists all of the projects that a feature is in
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/listProjectsForFeature?feature_class_id=63407942&callback=__jp9
class listProjectsForFeature(MarxanRESTHandler):
    def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['feature_class_id'])
            # get the projects which contain the feature
            projects = _getProjectsForFeature(
                int(self.get_argument('feature_class_id')))
            # set the response for uploading to mapbox
            self.send_response(
                {'info': "Projects info returned", "projects": projects})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# lists all of the projects that a planning grid is used in
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/listProjectsForPlanningGrid?feature_class_name=pu_89979654c5d044baa27b6008f9d06&callback=__jp9


class listProjectsForPlanningGrid(MarxanRESTHandler):
    def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['feature_class_name'])
            # get the projects which contain the planning grid
            projects = _getProjectsForPlanningGrid(
                self.get_argument('feature_class_name'))
            # set the response for uploading to mapbox
            self.send_response(
                {'info': "Projects info returned", "projects": projects})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# uploads a feature class with the passed feature class name to MapBox as a tileset using the MapBox Uploads API
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/uploadTilesetToMapBox?feature_class_name=pu_ton_marine_hexagon_20&mapbox_layer_name=hexagon&callback=__jp9


class uploadTilesetToMapBox(MarxanRESTHandler):
    async def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, [
                               'feature_class_name', 'mapbox_layer_name'])
            uploadId = await _uploadTilesetToMapbox(self.get_argument('feature_class_name'), self.get_argument('mapbox_layer_name'))
            # set the response for uploading to mapbox
            self.send_response({'info': "Tileset '" + self.get_argument(
                'feature_class_name') + "' uploading", 'uploadid': uploadId})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# uploads a file to a specific folder
# POST ONLY


class uploadFileToFolder(MarxanRESTHandler):
    def post(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, [
                               'filename', 'destFolder'])
            # write the file to the server
            _writeFile(MARXAN_FOLDER + self.get_argument('destFolder') + os.sep +
                       self.get_argument('filename'), self.request.files['value'][0].body)
            # set the response
            self.send_response({'info': "File '" + self.get_argument('filename') +
                                "' uploaded", 'file': self.get_argument('filename')})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# uploads a file to the project folder - 3 input parameters: user, project, filename (relative) and the file itself as a request file
# POST ONLY


class uploadFile(MarxanRESTHandler):
    def post(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, [
                               'user', 'project', 'filename'])
            # write the file to the server
            _writeFile(self.folder_project + self.get_argument('filename'),
                       self.request.files['value'][0].body)
            # set the response
            self.send_response({'info': "File '" + self.get_argument('filename') +
                                "' uploaded", 'file': self.get_argument('filename')})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# unzips an already uploaded shapefile and returns the rootname
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/unzipShapefile?filename=test&callback=__jp5


class unzipShapefile(MarxanRESTHandler):
    async def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['filename'])
            # write the file to the server
            rootfilename = await IOLoop.current().run_in_executor(None, _unzipFile, IMPORT_FOLDER, self.get_argument('filename'))
            # set the response
            self.send_response({'info': "File '" + self.get_argument(
                'filename') + "' unzipped", 'rootfilename': rootfilename})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# gets a field list from a shapefile
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/getShapefileFieldnames=test&callback=__jp5


class getShapefileFieldnames(MarxanRESTHandler):
    def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['filename'])
            # get the field list
            fields = _getShapefileFieldNames(
                IMPORT_FOLDER + self.get_argument('filename'))
            # set the response
            self.send_response(
                {'info': "Field list returned", 'fieldnames': fields})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# deletes a feature from the PostGIS database
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/deleteFeature?feature_name=test_feature1&callback=__jp5


class deleteFeature(MarxanRESTHandler):
    async def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['feature_name'])
            await _deleteFeature(self.get_argument('feature_name'))
            # set the response
            self.send_response({'info': "Feature deleted"})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])


# deletes a shapefile
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/deleteShapefile?zipfile=test.zip&shapefile=wibble.shp&callback=__jp5
class deleteShapefile(MarxanRESTHandler):
    def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, [
                               'zipfile', 'shapefile'])
            _deleteZippedShapefile(IMPORT_FOLDER, self.get_argument(
                'zipfile'), self.get_argument('shapefile')[:-4])
            # set the response
            self.send_response({'info': "Shapefile deleted"})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])


# creates a new feature from a passed linestring
class createFeatureFromLinestring(MarxanRESTHandler):
    async def post(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, [
                               'name', 'description', 'linestring'])
            # create the undissolved feature class
            # get a unique feature class name for the import
            feature_class_name = _getUniqueFeatureclassName("f_")
            # create the table and split the feature at the dateline
            await pg.execute(sql.SQL("CREATE TABLE marxan.{} AS SELECT marxan.ST_SplitAtDateLine(ST_SetSRID(ST_MakePolygon(%s)::geometry, 4326)) AS geometry;").format(sql.Identifier(feature_class_name)), [self.get_argument('linestring')])
            # add an index and a record in the metadata_interest_features table and start the upload to mapbox
            id, uploadId = await _finishCreatingFeature(feature_class_name, self.get_argument('name'), self.get_argument('description'), "Drawn on screen", self.get_current_user())
            # set the response
            self.send_response({'info': "Feature '" + self.get_argument('name') + "' created",
                                'id': id, 'feature_class_name': feature_class_name, 'uploadId': uploadId})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# kills a running process
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/stopProcess?pid=m12345&callback=__jp5


class stopProcess(MarxanRESTHandler):
    async def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['pid'])
            # get the pid from the pid request parameter - this will be an identifier (m=marxan run, q=query) followed by the pid, e.g. m1234 is a marxan run process with a pid of 1234
            pid = self.get_argument('pid')[1:]
            try:
                # if the process is a marxan run, then update the run log
                if (self.get_argument('pid')[:1] == 'm'):
                    # to distinguish between a process killed by the user and by the OS, we need to update the runlog.dat file to set this process as stopped and not killed
                    _updateRunLog(int(pid), None, None, None, 'Stopped')
                    # now kill the process
                    os.kill(int(pid), signal.SIGKILL)
                else:
                    # cancel the query
                    await pg.execute("SELECT pg_cancel_backend(%s);", [pid])
            except OSError:
                raise MarxanServicesError("The pid does not exist")
            except PermissionError:
                raise MarxanServicesError(
                    "Unable to stop process: PermissionDenied")
            else:
                self.send_response({'info': "pid '" + pid + "' terminated"})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# gets the run log
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/getRunLogs?


class getRunLogs(MarxanRESTHandler):
    def get(self):
        try:
            runlog = _getRunLogs()
            self.send_response({'info': "Run log returned",
                                'data': runlog.to_dict(orient="records")})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# clears the run log
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/clearRunLogs?


class clearRunLogs(MarxanRESTHandler):
    def get(self):
        try:
            runlog = _getRunLogs()
            runlog.loc[runlog['pid'] == -
                       1].to_csv(MARXAN_FOLDER + RUN_LOG_FILENAME, index=False, sep='\t')
            self.send_response({'info': "Run log cleared"})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/dismissNotification?user=admin&notificationid=1


class dismissNotification(MarxanRESTHandler):
    def get(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['notificationid'])
            # dismiss the notification
            _dismissNotification(self, self.get_argument('notificationid'))
            self.send_response({'info': "Notification dismissed"})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])


# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/resetNotifications?user=admin
class resetNotifications(MarxanRESTHandler):
    def get(self):
        try:
            # reset the notification
            _resetNotifications(self)
            self.send_response({'info': "Notifications reset"})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])


# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/deleteGapAnalysis?user=admin&project=Start%20project
class deleteGapAnalysis(MarxanRESTHandler):
    async def get(self):
        try:
            _validateArguments(self.request.arguments, ['user', 'project'])
            # delete the gap analysis
            project_name = _getSafeProjectName(self.get_argument("project"))
            table_name = "gap_" + \
                self.get_argument("user") + "_" + project_name
            await pg.execute(sql.SQL("DROP TABLE IF EXISTS marxan.{};").format(sql.Identifier(table_name.lower())))
            self.send_response({'info': "Gap analysis deleted"})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])


# for testing role access to servivces
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/testRoleAuthorisation&callback=__jp5
class testRoleAuthorisation(MarxanRESTHandler):
    def get(self):
        self.send_response({'info': "Service successful"})

# runs an already uploaded sql script - only called from client applications


class runSQLFile(MarxanRESTHandler):
    async def get(self):
        try:
            _validateArguments(self.request.arguments, ['filename'])
            # check the SQL file exists
            if not os.path.exists(MARXAN_FOLDER + self.get_argument("filename")):
                raise MarxanServicesError(
                    "File '" + self.get_argument("filename") + "' does not exist")
            # see if suppressOutput is set
            suppressOutput = '>/dev/null' if 'suppressOutput' in self.request.arguments else ''
            # set the command
            cmd = 'sudo -u postgres psql -f ' + MARXAN_FOLDER + self.get_argument(
                "filename") + ' postgresql://' + DATABASE_USER + ':' + DATABASE_PASSWORD + '@localhost:5432/marxanserver ' + suppressOutput
            # run the command
            result = await _runCmd(cmd)
            self.send_response({'info': result})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# cleans up the database and clumping files
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/cleanup


class cleanup(MarxanRESTHandler):
    async def get(self):
        try:
            _cleanup()
            self.send_response({'info': "Cleanup succesful"})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])


# shuts down the marxan-server and computer after a period of time - currently only on Unix
# https://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/shutdown&delay=10&callback=_wibble
class shutdown(MarxanRESTHandler):
    async def get(self):
        try:
            if platform.system() != "Windows":
                _validateArguments(self.request.arguments, ['delay'])
                minutes = int(self.get_argument("delay"))
                # this wont be sent until the await returns
                self.send_response({'info': "Shutting down"})
                # if we shutdown is postponed, write the shutdown file
                if (minutes != 0):
                    # write the shutdown file with the time in UTC isoformat
                    _writeFileUnicode(MARXAN_FOLDER + SHUTDOWN_FILENAME, (datetime.datetime.now(
                        timezone.utc) + timedelta(minutes/1440)).isoformat())
                # wait for so many minutes
                await asyncio.sleep(minutes * 60)
                # delete the shutdown file
                _deleteShutdownFile()
                # shutdown the os
                os.system('sudo shutdown now')
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])

# blocks tornado for the passed number of seconds - for testing


class block(MarxanRESTHandler):
    def get(self):
        try:
            _validateArguments(self.request.arguments, ['seconds'])
            time.sleep(int(self.get_argument("seconds")))
            self.send_response({'info': "Blocking finished"})
        except MarxanServicesError as e:
            _raiseError(self, e.args[0])


# tests tornado is working properly
class testTornado(MarxanRESTHandler):
    def get(self):
        self.send_response({'info': "Tornado running"})


####################################################################################################################################################################################################################################################################
# baseclass for handling WebSockets
####################################################################################################################################################################################################################################################################
class MarxanWebSocketHandler(tornado.websocket.WebSocketHandler):
    # get the current user
    def get_current_user(self):
        if self.get_secure_cookie("user"):
            returnVal = self.get_secure_cookie("user").decode("utf-8")
        else:
            returnVal = None
        return returnVal

    # check CORS access for the websocket
    def check_origin(self, origin):
        if DISABLE_SECURITY:
            return True
        # the request is valid for CORS if the origin is in the list of permitted domains, or the origin is the same as the host, i.e. same machine
        # we need to get the host_name from the origin so parse it with urlparse
        parsed = urlparse(origin)
        if (origin in PERMITTED_DOMAINS) or (parsed.netloc.find(self.request.host_name) != -1):
            return True
        else:
            raise HTTPError(403, "The origin '" + origin +
                            "' does not have permission to access the service (CORS error)")

    # called when the websocket is opened - does authentication/authorisation then gets the folder paths for the user and optionally the project
    async def open(self, startMessage):
        try:
            # set the start time of the websocket
            self.startTime = datetime.datetime.now()
            # set the start message
            startMessage.update({'status': 'Started'})
            self.send_response(startMessage)
            # set the folder paths for the user and optionally project
            if "user" in self.request.arguments.keys():
                _setFolderPaths(self, self.request.arguments)
                # get the project data
                await _getProjectData(self)
            # check the request is authenticated
            _authenticate(self)
            # get the requested method
            method = _getRESTMethod(self.request.path)
            # check the users role has access to the requested service
            _authoriseRole(self, method)
            # check the user has access to the specific resource, i.e. the 'User' role cannot access projects from other users
            _authoriseUser(self)
            # send a preprocessing message
            self.send_response(
                {"status": "Preprocessing", "info": "Preprocessing.."})

            def sendPing():
                if (hasattr(self, 'ping_message')):
                    self.send_response(
                        {"status": "Preprocessing", "info": self.ping_message})
                else:
                    self.send_response({"status": "WebSocketOpen"})
            # start the web socket ping messages to keep the connection alive
            self.pc = PeriodicCallback(sendPing, (PING_INTERVAL))
            self.pc.start()
            # flag to indicate if a client has sent a final message - sometimes the PeriodicCallback does not stop straight away and additional messages are sent when the websocket is closed
            self.clientSentFinalMsg = False
        except (HTTPError) as e:
            error = _getExceptionLastLine(sys.exc_info())
            # close the websocket
            self.close({'error': error})
            # raise an error
            raise MarxanServicesError("Request denied")

    # sends the message with a timestamp
    def send_response(self, message):
        # add in the start time
        elapsedtime = str(
            (datetime.datetime.now() - self.startTime).seconds) + "s"
        message.update({'elapsedtime': elapsedtime})
        # add in messages from descendent classes
        if hasattr(self, 'pid'):
            message.update({'pid': self.pid})
        if hasattr(self, 'marxanProcess'):
            message.update({'pid': 'm' + str(self.marxanProcess.pid)})
        try:
            self.write_message(message)
        except WebSocketClosedError:
            self.close(clean=False)

    def close(self, closeMessage={}, clean=True):
        # stop the ping messages
        if hasattr(self, 'pc'):
            if self.pc.is_running:
                self.pc.stop()
        # send a message if the socket is still open
        if clean:
            closeMessage.update({'status': 'Finished'})
            self.send_response(closeMessage)
            # log any errors
            if 'error' in closeMessage.keys():
                logging.warning(closeMessage['error'])
            # prevent any additional messages
            self.clientSentFinalMsg = True
        else:
            if not self.clientSentFinalMsg:
                logging.warning(
                    "The client closed the connection: " + self.request.uri)
                self.clientSentFinalMsg = True
        # close the websocket cleanly
        super().close(1000)

####################################################################################################################################################################################################################################################################
# MarxanWebSocketHandler subclasses
####################################################################################################################################################################################################################################################################

# wss://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/runMarxan?user=admin&project=Start%20project
# starts a Marxan run on the server and streams back the output as websockets


class runMarxan(MarxanWebSocketHandler):
    # authenticate and get the user folder and project folders
    async def open(self):
        try:
            await super().open({'info': "Running Marxan.."})
        except MarxanServicesError:
            pass
        else:
            # see if the project is already running - if it is then return an error
            if _isProjectRunning(self.get_argument("user"), self.get_argument("project")):
                self.close({'error': "The project is already running. See <a href='" + ERRORS_PAGE +
                            "#the-project-is-already-running' target='blank'>here</a>", 'info': ''})
            else:
                # set the current folder to the project folder so files can be found in the input.dat file
                if (os.path.exists(self.folder_project)):
                    os.chdir(self.folder_project)
                    # delete all of the current output files
                    _deleteAllFiles(self.folder_output)
                    # run marxan
                    # the "exec " in front allows you to get the pid of the child process, i.e. marxan, and therefore to be able to kill the process using os.kill(pid, signal.SIGTERM) instead of the tornado process - see here: https://stackoverflow.com/questions/4789837/how-to-terminate-a-python-subprocess-launched-with-shell-true/4791612#4791612
                    try:
                        if platform.system() != "Windows":
                            # in Unix operating systems, the log is streamed from stdout to a Tornado STREAM
                            self.marxanProcess = Subprocess(
                                [MARXAN_EXECUTABLE], stdout=Subprocess.STREAM, stdin=PIPE)
                            # add a callback when the process finishes
                            self.marxanProcess.set_exit_callback(
                                self.finishOutput)
                        else:
                            # custom class as the Subprocess.STREAM option does not work on Windows - see here: https://www.tornadoweb.org/en/stable/process.html?highlight=Subprocess#tornado.process.Subprocess
                            self.marxanProcess = MarxanSubprocess(
                                [MARXAN_EXECUTABLE], stdout=PIPE, stdin=PIPE)
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
                            self.close({'error': "The executable '" + MARXAN_EXECUTABLE +
                                        "' is blocked by group policy. For more information, contact your system administrator."})
                    else:  # no errors
                        # get the number of runs that were in the input.dat file
                        self.numRunsRequired = await _getNumberOfRunsRequired(self)
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

    # called on the first IOLoop callback and then streams the marxan output back to the client

    async def stream_marxan_output(self):
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

    # writes the details of the started marxan job to the RUN_LOG_FILENAME file as a single line
    def logRun(self):
        # get the user name
        self.user = self.get_argument('user')
        self.project = self.get_argument('project')
        # create the data record - pid, user, project, starttime, endtime, runtime, runs (e.g. 3/10), status = running, completed, stopped (by user), killed (by OS)
        record = [str(self.marxanProcess.pid), self.user, self.project, datetime.datetime.now(
        ).strftime("%d/%m/%y %H:%M:%S"), '', '', '0/' + str(self.numRunsRequired), 'Running']
        # add the tab separators
        recordLine = "\t".join(record)
        # append the record to the run log file
        _writeFileUnicode(MARXAN_FOLDER + RUN_LOG_FILENAME,
                          recordLine + "\n", "a")

    # finishes writing the output of a stream and writes the run log
    def finishOutput(self, returnCode):
        try:
            # close the output stream
            self.marxanProcess.stdout.close()
            if (self.user != '_clumping'):  # dont log clumping runs
                # get the number of runs completed
                numRunsCompleted = _getNumberOfRunsCompleted(self)
                # write the response depending on if the run completed or not
                if (numRunsCompleted == self.numRunsRequired):
                    _updateRunLog(self.marxanProcess.pid, self.startTime,
                                  numRunsCompleted, self.numRunsRequired, 'Completed')
                    self.close({'info': 'Run completed',
                                'project': self.project, 'user': self.user})
                else:  # if the user stopped it then the run log should already have a status of Stopped
                    actualStatus = _updateRunLog(
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
        except MarxanServicesError as e:
            self.close({'error': e.args[0]})

# updates the WDPA table in PostGIS using the publically available downloadUrl


class updateWDPA(MarxanWebSocketHandler):
    # authenticate and get the user folder and project folders
    async def open(self):
        try:
            await super().open({'info': "Updating WDPA.."})
        except MarxanServicesError:  # authentication/authorisation error
            pass
        else:
            try:
                # download the new wdpa zip
                self.send_response(
                    {'status': 'Preprocessing', 'info': "Downloading " + self.get_argument("downloadUrl")})
                await self.asyncDownload(self.get_argument("downloadUrl"), IMPORT_FOLDER + WDPA_DOWNLOAD_FILE)
            except (MarxanServicesError) as e:  # download failed
                self.close({'error': e.args[0], 'info': 'WDPA not updated'})
            else:
                self.send_response(
                    {'status': 'Preprocessing', 'info': "Downloaded"})
                try:
                    # download finished - upzip the polygons shapefile
                    self.send_response(
                        {'status': 'Preprocessing', 'info': "Unzipping shapefile '" + WDPA_DOWNLOAD_FILE + "'"})
                    rootfilename = await IOLoop.current().run_in_executor(None, _unzipFile, IMPORT_FOLDER, WDPA_DOWNLOAD_FILE, False, "polygons")
                # error unzipping - either the polygons shapefile does not exist or the disk space has run out
                except (MarxanServicesError) as e:
                    # delete the zip file
                    os.remove(IMPORT_FOLDER + WDPA_DOWNLOAD_FILE)
                    self.close(
                        {'error': e.args[0], 'info': 'WDPA not updated'})
                else:
                    self.send_response(
                        {'status': 'Preprocessing', 'info': "Unzipped shapefile"})
                    # delete the zip file
                    os.remove(IMPORT_FOLDER + WDPA_DOWNLOAD_FILE)
                    try:
                        # import the new wdpa into a temporary PostGIS feature class in EPSG:4326
                        # get a unique feature class name for the tmp imported feature class - this is necessary as ogr2ogr automatically creates a spatial index called <featureclassname>_geometry_geom_idx on import - which will end up being the name of the index on the wdpa table preventing further imports (as the index will already exist)
                        feature_class_name = _getUniqueFeatureclassName(
                            "wdpa_")
                        self.send_response(
                            {'status': "Preprocessing", 'info': "Importing '" + rootfilename + "' into PostGIS.."})
                        # import the wdpa to a tmp feature class
                        await pg.importShapefile(IMPORT_FOLDER, rootfilename + ".shp", feature_class_name, splitAtDateline=False)
                        self.send_response(
                            {'status': "Preprocessing", 'info': "Imported into '" + feature_class_name + "'"})
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
                            {'status': "Preprocessing", 'info': "Deleted dissolved country WDPAP feature classes"})
                    except (OSError) as e:  # TODO Add other exception classes especially PostGIS ones
                        self.close(
                            {'error': 'No space left on device importing the WDPA into PostGIS', 'info': 'WDPA not updated'})
                    else:
                        # update the WDPA_VERSION variable in the server.dat file
                        _updateParameters(MARXAN_FOLDER + SERVER_CONFIG_FILENAME,
                                          {"WDPA_VERSION": self.get_argument("wdpaVersion")})
                        # delete all of the existing intersections between planning units and the old version of the WDPA
                        self.send_response(
                            {'status': "Preprocessing", 'info': 'Invalidating existing WDPA intersections'})
                        _invalidateProtectedAreaIntersections()
                        # send the response
                        self.close(
                            {'info': 'WDPA update completed succesfully'})
                    finally:
                        # delete the shapefile
                        _deleteZippedShapefile(
                            IMPORT_FOLDER, WDPA_DOWNLOAD_FILE, rootfilename)

    async def asyncDownload(self, url, file):
        # initialise a variable to hold the size downloaded
        file_size_dl = 0
        try:
            async with aiohttp.ClientSession() as session:
                try:
                    timeout = aiohttp.ClientTimeout(total=None)
                    async with aiohttp.ClientSession(timeout=timeout) as session:
                        async with session.get(url) as resp:
                            # get the file size
                            file_size = resp.headers["Content-Length"]
                            try:
                                with open(file, 'wb') as f:
                                    while True:
                                        chunk = await resp.content.read(100000000)
                                        if not chunk:
                                            break
                                        f.write(chunk)
                                        file_size_dl += len(chunk)
                                        self.ping_message = str(
                                            int((file_size_dl/int(file_size))*100)) + "% downloaded"
                            except Exception as e:
                                raise MarxanServicesError(
                                    "Error getting a file: %s" % e)
                            finally:
                                f.close()
                except Exception as e:
                    raise MarxanServicesError(
                        "Error getting the url: %s" % url)
        except (OSError) as e:  # out of disk space probably
            f.close()
            os.remove(file)
            raise MarxanServicesError("Out of disk space on device")
        finally:
            await session.close()


# imports a set of features from an unzipped shapefile
class importFeatures(MarxanWebSocketHandler):
    async def open(self):
        try:
            await super().open({'info': "Importing features.."})
        except MarxanServicesError:  # authentication/authorisation error
            pass
        else:
            # validate the input arguments
            _validateArguments(self.request.arguments, ['shapefile'])
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
                scratch_name = _getUniqueFeatureclassName("scratch_")
                # first, import the shapefile into a PostGIS feature class in EPSG:4326
                await pg.importShapefile(IMPORT_FOLDER, shapefile, scratch_name)
                # check the geometry
                self.send_response(
                    {'status': 'Preprocessing', 'info': "Checking the geometry.."})
                await pg.isValid(scratch_name)
                # get the feature names
                if name:  # single feature name
                    feature_names = [name]
                else:  # get the feature names from a field in the shapefile
                    splitfield = self.get_argument('splitfield')
                    features = await pg.execute(sql.SQL("SELECT {splitfield} FROM marxan.{scratchTable}").format(splitfield=sql.Identifier(splitfield), scratchTable=sql.Identifier(scratch_name)), returnFormat="DataFrame")
                    feature_names = list(set(features[splitfield].tolist()))
                    print('feature_names: ', feature_names)
                    # if they are not unique then return an error
                    # if (len(feature_names) != len(set(feature_names))):
                    #     raise MarxanServicesError("Feature names are not unique for the field '" + splitfield + "'")
                # split the imported feature class into separate feature classes
                for feature_name in feature_names:
                    # create the new feature class
                    if name:  # single feature name
                        feature_class_name = _getUniqueFeatureclassName("f_")
                        await pg.execute(
                            sql.SQL("CREATE TABLE marxan.{feature_class_name} AS SELECT * FROM marxan.{scratchTable};").format(
                                feature_class_name=sql.Identifier(
                                    feature_class_name),
                                scratchTable=sql.Identifier(scratch_name)
                            ), [feature_name])
                        description = self.get_argument('description')
                    else:  # multiple feature names
                        feature_class_name = _getUniqueFeatureclassName("fs_")
                        await pg.execute(sql.SQL("CREATE TABLE marxan.{feature_class_name} AS SELECT * FROM marxan.{scratchTable} WHERE {splitField} = %s;").format(feature_class_name=sql.Identifier(feature_class_name), scratchTable=sql.Identifier(scratch_name), splitField=sql.Identifier(splitfield)), [feature_name])
                        description = "Imported from '" + shapefile + \
                            "' and split by '" + splitfield + "' field"
                    # add an index and a record in the metadata_interest_features table and start the upload to mapbox
                    id, uploadId = await _finishCreatingFeature(feature_class_name, feature_name, description, "Imported shapefile", self.get_current_user())
                    # append the uploadId to the uploadIds array
                    uploadIds.append(uploadId)
                    self.send_response({'id': id, 'feature_class_name': feature_class_name, 'uploadId': uploadId,
                                        'info': "Feature '" + feature_name + "' imported", 'status': 'FeatureCreated'})
                # complete
                self.close({'info': "Features imported",
                            'uploadIds': uploadIds})
            except (MarxanServicesError) as e:
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
class importGBIFData(MarxanWebSocketHandler):
    async def open(self):
        try:
            await super().open({'info': "Importing features from GBIF.."})
        except MarxanServicesError:  # authentication/authorisation error
            pass
        else:
            # validate the input arguments
            _validateArguments(self.request.arguments, [
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
                    # insert the records - this calls _importDataFrame which is blocking
                    await IOLoop.current().run_in_executor(None, _importDataFrame, df, feature_class_name)
                    # update the geometry field
                    await pg.execute(sql.SQL("UPDATE marxan.{} SET geometry=marxan.ST_SplitAtDateline(ST_Transform(ST_Buffer(ST_Transform(ST_SetSRID(ST_Point(lng, lat),4326),3410),%s),4326))").format(sql.Identifier(feature_class_name)), [GBIF_POINT_BUFFER_RADIUS])
                    # get the gbif vernacular name
                    feature_name = self.get_argument('scientificName')
                    vernacularNames = self.getVernacularNames(taxonKey)
                    description = self.getCommonName(vernacularNames)
                    # add an index and a record in the metadata_interest_features table and start the upload to mapbox
                    id, uploadId = await _finishCreatingFeature(feature_class_name, feature_name, description, "Imported from GBIF", self.get_current_user())
                    self.send_response({'id': id, 'feature_class_name': feature_class_name, 'uploadId': uploadId,
                                        'info': "Feature '" + feature_name + "' imported", 'status': 'FeatureCreated'})
                    # complete
                    self.close(
                        {'info': "Features imported", 'uploadId': uploadId})
                else:
                    raise MarxanServicesError(
                        "No records for " + self.get_argument('scientificName'))

            except (MarxanServicesError) as e:
                if "already exists" in e.args[0]:
                    self.close({'error': "The feature '" + feature_name +
                                "' already exists", 'info': 'Failed to import features'})
                else:
                    self.close(
                        {'error': e.args[0], 'info': 'Failed to import features'})

    # parallel asynchronous loading og gbif data
    async def getGBIFOccurrences(self, taxonKey):
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
            data = [OrderedDict({
                'eventDate': item['eventDate'] if 'eventDate' in item.keys() else None,
                'gbifID': item['gbifID'],
                'lng':item['decimalLongitude'],
                'lat': item['decimalLatitude'],
                'geometry': ''
            }) for item in _json['results']]
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
            raise MarxanServicesError(
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
        return pandas.DataFrame(latLongs)

    def getVernacularNames(self, taxonKey):
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
        commonNames = [i['vernacularName']
                       for i in vernacularNames if i['language'] == language]
        if (len(commonNames) > 0):
            return commonNames[0]
        else:
            return 'No common name'

# creates a new feature (or set of features) from a WFS endpoint


class createFeaturesFromWFS(MarxanWebSocketHandler):
    async def open(self):
        try:
            await super().open({'info': "Importing features.."})
        except MarxanServicesError:  # authentication/authorisation error
            pass
        else:
            # validate the input arguments
            _validateArguments(self.request.arguments, [
                               'srs', 'endpoint', 'name', 'description', 'featuretype'])
            try:
                # get a unique feature class name for the import
                feature_class_name = _getUniqueFeatureclassName("f_")
                # get the WFS data as GML
                gml = await IOLoop.current().run_in_executor(None, _getGML, self.get_argument('endpoint'), self.get_argument('featuretype'))
                # write it to file
                _writeFileUnicode(
                    IMPORT_FOLDER + feature_class_name + ".gml", gml)
                # import the GML into a PostGIS feature class in EPSG:4326
                await pg.importGml(IMPORT_FOLDER, feature_class_name + ".gml", feature_class_name, sEpsgCode=self.get_argument('srs'))
                # check the geometry
                self.send_response(
                    {'status': 'Preprocessing', 'info': "Checking the geometry.."})
                await pg.isValid(feature_class_name)
                # add an index and a record in the metadata_interest_features table and start the upload to mapbox
                id, uploadId = await _finishCreatingFeature(feature_class_name, self.get_argument('name'), self.get_argument('description'), "Imported from web service", self.get_current_user())
                self.send_response({'id': id, 'feature_class_name': feature_class_name, 'uploadId': uploadId,
                                    'info': "Feature '" + self.get_argument('name') + "' imported", 'status': 'FeatureCreated'})
                # complete
                self.close({'info': "Features imported", 'uploadId': uploadId})
            except (MarxanServicesError) as e:
                if "already exists" in e.args[0]:
                    self.close({'error': "The feature '" + self.get_argument('name') +
                                "' already exists", 'info': 'Failed to import features'})
                else:
                    self.close(
                        {'error': e.args[0], 'info': 'Failed to import features'})
            finally:
                # delete the gml file
                if os.path.exists(IMPORT_FOLDER + feature_class_name + ".gml"):
                    os.remove(IMPORT_FOLDER + feature_class_name + ".gml")
                # delete the gfs file
                if os.path.exists(IMPORT_FOLDER + feature_class_name + ".gfs"):
                    os.remove(IMPORT_FOLDER + feature_class_name + ".gfs")

# exports a project


class exportProject(MarxanWebSocketHandler):
    async def open(self):
        try:
            await super().open({'info': "Exporting project.."})
        except MarxanServicesError:  # authentication/authorisation error
            pass
        else:
            _validateArguments(self.request.arguments, ['user', 'project'])
            self.send_response(
                {'status': 'Preprocessing', 'info': "Copying project folder.."})
            # create a folder in the export folder to hold all the files
            exportFolder = EXPORT_FOLDER + \
                self.get_argument('user') + "_" + self.get_argument('project')
            # remote the folder if it already exists
            if os.path.exists(exportFolder):
                shutil.rmtree(exportFolder)
            # copy the project folder
            shutil.copytree(self.folder_project, exportFolder)
            # FEATURES
            # get the species data from the spec.dat file and the PostGIS database
            await _getSpeciesData(self)
            # get the feature class names that must be exported from postgis
            feature_class_names = self.speciesData['feature_class_name'].tolist(
            )
            # export all of the feature classes as shapefiles
            self.send_response(
                {'status': 'Preprocessing', 'info': "Exporting features.."})
            cmd = '"' + OGR2OGR_EXECUTABLE + '" -f "ESRI Shapefile" "' + exportFolder + os.sep + EXPORT_F_SHP_FOLDER + os.sep + '" PG:"host=' + DATABASE_HOST + \
                ' user=' + DATABASE_USER + ' dbname=' + DATABASE_NAME + ' password=' + \
                DATABASE_PASSWORD + ' ACTIVE_SCHEMA=marxan" ' + \
                " ".join(feature_class_names)
            await _runCmd(cmd)
            # export the features metadata
            self.send_response(
                {'status': 'Preprocessing', 'info': "Exporting feature metadata.."})
            escapedFeatureNames = "\'" + \
                "\',\'".join(feature_class_names) + "\'"
            cmd = '"' + OGR2OGR_EXECUTABLE + '" -f "CSV" "' + exportFolder + os.sep + EXPORT_F_METADATA + '" PG:"host=' + DATABASE_HOST + ' user=' + DATABASE_USER + ' dbname=' + DATABASE_NAME + ' password=' + DATABASE_PASSWORD + \
                ' ACTIVE_SCHEMA=marxan" -sql "SELECT oid, feature_class_name, alias, description FROM metadata_interest_features WHERE feature_class_name = ANY (ARRAY[' + \
                escapedFeatureNames + ']);" -lco SEPARATOR=TAB'
            await _runCmd(cmd)
            # PLANNING GRIDS
            # export the planning unit grid
            pu_name = self.projectData['metadata']['PLANNING_UNIT_NAME']
            self.send_response(
                {'status': 'Preprocessing', 'info': "Exporting planning grid.."})
            await pg.exportToShapefile(exportFolder + os.sep + EXPORT_PU_SHP_FOLDER + os.sep, pu_name)
            # export the planning grid metadata - convert the envelope geometry field to text
            self.send_response(
                {'status': 'Preprocessing', 'info': "Exporting planning grid metadata.."})
            cmd = '"' + OGR2OGR_EXECUTABLE + '" -f "CSV" "' + exportFolder + os.sep + EXPORT_PU_METADATA + '" PG:"host=' + DATABASE_HOST + ' user=' + DATABASE_USER + ' dbname=' + DATABASE_NAME + ' password=' + DATABASE_PASSWORD + \
                ' ACTIVE_SCHEMA=marxan" -sql "SELECT feature_class_name, alias, description, country_id, aoi_id, domain, _area, ST_AsText(envelope) envelope, creation_date, source, created_by, tilesetid, planning_unit_count FROM marxan.metadata_planning_units WHERE feature_class_name = \'' + \
                pu_name + '\';" -lco SEPARATOR=TAB'
            await _runCmd(cmd)
            # zip the whole folder
            self.send_response(
                {'status': 'Preprocessing', 'info': "Zipping project.."})
            await IOLoop.current().run_in_executor(None, _zipfolder, exportFolder, exportFolder)
            # rename with a mxw extension
            os.rename(exportFolder + ".zip", exportFolder + ".mxw")
            # delete the folder
            shutil.rmtree(exportFolder)
            # return the results
            self.close({'info': "Export project complete", 'filename': self.get_argument(
                'user') + "_" + self.get_argument('project') + ".mxw"})

# imports a project that has already been uploaded to the imports folder


class importProject(MarxanWebSocketHandler):
    async def open(self):
        try:
            _validateArguments(self.request.arguments, [
                               'user', 'project', 'filename', 'description'])
            user = self.get_argument('user')
            project = self.get_argument('project').strip()
            projectFolder = MARXAN_USERS_FOLDER + user + os.sep + project + os.sep
            if os.path.exists(projectFolder):
                shutil.rmtree(projectFolder)
            # create the project folder
            os.mkdir(projectFolder)
            # unzip the files to the project folder
            zip_ref = zipfile.ZipFile(
                IMPORT_FOLDER + self.get_argument('filename'), 'r')
            zip_ref.extractall(projectFolder)
            await super().open({'info': "Importing project.."})
        except MarxanServicesError:  # authentication/authorisation error
            pass
        else:
            # FEATURES
            # import all of the shapefiles - those that already exist are skipped
            self.send_response(
                {'status': 'Preprocessing', 'info': "Importing features.."})
            cmd = '"' + OGR2OGR_EXECUTABLE + '" -f "PostgreSQL" PG:"host=' + DATABASE_HOST + ' user=' + DATABASE_USER + ' dbname=' + DATABASE_NAME + ' password=' + DATABASE_PASSWORD + \
                '" "' + projectFolder + EXPORT_F_SHP_FOLDER + os.sep + \
                '" -nlt GEOMETRY -lco SCHEMA=marxan -lco GEOMETRY_NAME=geometry -t_srs EPSG:4326 -lco precision=NO -skipfailures'
            result = await _runCmd(cmd)
            # load the metadata
            df = pandas.read_csv(projectFolder + EXPORT_F_METADATA, sep='\t')
            # iterate throught the features and if there is no record in the metadata_interest_features table then add it
            for index, row in df.iterrows():
                results = await pg.execute("SELECT * FROM marxan.metadata_interest_features WHERE feature_class_name = %s;", data=[row['feature_class_name']], returnFormat="Array")
                if len(results) == 0:
                    # add the new feature
                    self.send_response(
                        {'status': 'Preprocessing', 'info': "Importing " + row['alias']})
                    await _finishImportingFeature(row['feature_class_name'], row['alias'], row['description'], 'Imported with project ' + user + '/' + project, user)
                else:
                    self.send_response(
                        {'status': 'Preprocessing', 'info': row['alias'] + ' already exists - skipping'})
            # get the ids of the features so we can remap them in the spec.dat and the puvspr.dat files
            self.send_response(
                {'status': 'Preprocessing', 'info': 'Updating feature id values..'})
            feature_class_names = df['feature_class_name'].tolist()
            df2 = await pg.execute("SELECT oid, feature_class_name FROM marxan.metadata_interest_features WHERE feature_class_name = ANY (ARRAY[%s]);", data=[feature_class_names], returnFormat="DataFrame")
            # join the source dataframe and the new data
            df = df[['feature_class_name', 'oid']].rename(
                columns={'feature_class_name': 'fcn', 'oid': 'id'}).set_index('fcn')
            df2 = df2[['feature_class_name', 'oid']].rename(
                columns={'feature_class_name': 'fcn', 'oid': 'new_id'}).set_index('fcn')
            # create a data frame with the old oid and the new old for the feature class
            mapping = df.join(df2)
            # update the values in the feature_preprocessing.dat file
            _getSpeciesPreProcessingData(self)
            df = _updateDataFrame(
                self.speciesPreProcessingData, mapping, 'id', 'id', 'new_id')
            df.to_csv(self.folder_input +
                      FEATURE_PREPROCESSING_FILENAME, index=False)
            # update the values in the spec.dat file
            df = await _getProjectInputData(self, "SPECNAME")
            df = _updateDataFrame(df, mapping, 'id', 'id', 'new_id')
            await _writeCSV(self, "SPECNAME", df)
            # update the values in the puvsrp.dat file
            df = await _getProjectInputData(self, "PUVSPRNAME")
            df = _updateDataFrame(df, mapping, 'species', 'id', 'new_id')
            df = df.sort_values(by=['pu', 'species'])
            await _writeCSV(self, "PUVSPRNAME", df)
            # clear the output so we dont have to update the feature ids in any other file
            _deleteAllFiles(self.folder_output)
            # PLANNING GRID
            # load the metadata
            df = pandas.read_csv(projectFolder + EXPORT_PU_METADATA, sep='\t')
            # replace NaN with None
            df = df.where(pandas.notnull(df), None)
            # get the row from the dataframe
            row = df.iloc[0]
            # see if the planning grid already exists
            self.send_response(
                {'status': 'Preprocessing', 'info': 'Importing planning grid..'})
            results = await pg.execute("SELECT * FROM marxan.metadata_planning_units WHERE feature_class_name = %s;", data=[row['feature_class_name']], returnFormat="Array")
            if len(results) == 0:
                # add the new planning grid metadata
                await pg.execute("INSERT INTO marxan.metadata_planning_units(feature_class_name, alias, description, country_id, aoi_id, domain, _area, envelope, creation_date, source, created_by, tilesetid, planning_unit_count) VALUES (%s,%s,%s,%s,%s,%s,%s,ST_SetSRID(ST_GeomFromText(%s),'4326'),now(),'Imported from .mxw file',%s,%s,%s);", data=[row['feature_class_name'], row['alias'], row['description'], row['country_id'], row['aoi_id'], row['domain'], row['_area'], row['envelope'], user, row['tilesetid'], row['planning_unit_count']])
                # import the planning grid shapefile
                self.send_response(
                    {'status': 'Preprocessing', 'info': 'Importing planning grid ' + row['alias']})
                cmd = '"' + OGR2OGR_EXECUTABLE + '" -f "PostgreSQL" PG:"host=' + DATABASE_HOST + ' user=' + DATABASE_USER + ' dbname=' + DATABASE_NAME + ' password=' + DATABASE_PASSWORD + '" "' + \
                    projectFolder + EXPORT_PU_SHP_FOLDER + os.sep + \
                    '" -nlt GEOMETRY -lco SCHEMA=marxan -lco GEOMETRY_NAME=geometry -t_srs EPSG:4326 -lco precision=NO -lco FID=puid -skipfailures'
                await _runCmd(cmd)
            else:
                self.send_response(
                    {'status': 'Preprocessing', 'info': row['alias'] + ' metadata already exists - skipping'})
            # update the project description
            _updateParameters(projectFolder + PROJECT_DATA_FILENAME,
                              {'DESCRIPTION': self.get_argument('description')})
            # cleanup
            shutil.rmtree(projectFolder + EXPORT_F_SHP_FOLDER)
            shutil.rmtree(projectFolder + EXPORT_PU_SHP_FOLDER)
            os.remove(projectFolder + EXPORT_F_METADATA)
            os.remove(projectFolder + EXPORT_PU_METADATA)
            os.remove(IMPORT_FOLDER + self.get_argument('filename'))
            # return the results
            self.close({'info': "Import project complete"})

####################################################################################################################################################################################################################################################################
# baseclass for handling long-running PostGIS queries using WebSockets
####################################################################################################################################################################################################################################################################


class QueryWebSocketHandler(MarxanWebSocketHandler):

    # runs a PostGIS query asynchronously and writes the pid to the client so the query can be stopped
    async def executeQuery(self, sql, data=None, returnFormat=None):
        try:
            return await pg.execute(sql, data=data, returnFormat=returnFormat, socketHandler=self)
        except psycopg2.OperationalError as e:
            self.close({'error': "Preprocessing stopped by operating system"})
        except asyncio.CancelledError:
            self.close({'error': "Preprocessing stopped by " + self.user})

####################################################################################################################################################################################################################################################################
# WebSocket subclasses
####################################################################################################################################################################################################################################################################
# preprocesses the features by intersecting them with the planning units
# wss://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/preprocessFeature?user=andrew&project=Tonga%20marine%2030km2&planning_grid_name=pu_ton_marine_hexagon_30&feature_class_name=volcano&alias=volcano&id=63408475


class preprocessFeature(QueryWebSocketHandler):
    async def open(self):
        try:
            await super().open({'info': "Preprocessing '" + self.get_argument('alias') + "'.."})
        except MarxanServicesError:  # authentication/authorisation error
            pass
        else:
            _validateArguments(self.request.arguments, [
                               'user', 'project', 'id', 'feature_class_name', 'alias', 'planning_grid_name'])
            # run the query asynchronously and wait for the results
            intersectionData = await self.executeQuery(sql.SQL("SELECT metadata.oid::integer species, puid pu, ST_Area(ST_Transform(ST_Union(ST_Intersection(grid.geometry,feature.geometry)),3410)) amount from marxan.{grid} grid, marxan.{feature} feature, marxan.metadata_interest_features metadata where st_intersects(grid.geometry,feature.geometry) and metadata.feature_class_name = %s group by 1,2;").format(grid=sql.Identifier(self.get_argument('planning_grid_name')), feature=sql.Identifier(self.get_argument('feature_class_name'))), data=[self.get_argument('feature_class_name')], returnFormat="DataFrame")
            # get the existing data
            try:
                # load the existing preprocessing data
                df = await _getProjectInputData(self, "PUVSPRNAME", True)
            except:
                # no existing preprocessing data so use the empty data frame
                d = {'amount': pandas.Series([], dtype='float64'), 'species': pandas.Series(
                    [], dtype='int64'), 'pu': pandas.Series([], dtype='int64')}
                # reorder the columns
                df = pandas.DataFrame(data=d)[['species', 'pu', 'amount']]
            # get the species id from the arguments
            speciesId = int(self.get_argument('id'))
            # make sure there are not existing records for this feature - otherwise we will get duplicates
            df = df[~df.species.isin([speciesId])]
            # append the intersection data to the existing data
            df = df.append(intersectionData)
            # sort the values by the pu column then the species column
            df = df.sort_values(by=['pu', 'species'])
            try:
                # write the data to the PUVSPR.dat file
                await _writeCSV(self, "PUVSPRNAME", df)
                # get the summary information and write it to the feature preprocessing file
                record = _getPuvsprStats(df, speciesId)
                _writeToDatFile(self.folder_input +
                                FEATURE_PREPROCESSING_FILENAME, record)
            except (MarxanServicesError) as e:
                self.close({'error': e.args[1]})
            else:
                # update the input.dat file
                _updateParameters(
                    self.folder_project + PROJECT_DATA_FILENAME, {'PUVSPRNAME': PUVSPR_FILENAME})
                # set the response
                self.close({'info': "Feature '" + self.get_argument('alias') + "' preprocessed", "feature_class_name": self.get_argument(
                    'feature_class_name'), "pu_area": str(record.iloc[0]['pu_area']), "pu_count": str(record.iloc[0]['pu_count']), "id": str(speciesId)})


# preprocesses the protected areas by intersecting them with the planning units
# wss://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/preprocessProtectedAreas?user=andrew&project=Tonga%20marine%2030km2&planning_grid_name=pu_ton_marine_hexagon_30
class preprocessProtectedAreas(QueryWebSocketHandler):
    async def open(self):
        try:
            await super().open({'info': "Preprocessing protected areas"})
        except MarxanServicesError:  # authentication/authorisation error
            pass
        else:
            _validateArguments(self.request.arguments, [
                               'user', 'project', 'planning_grid_name'])
            # do the intersection with the protected areas
            intersectionData = await self.executeQuery(sql.SQL("SELECT DISTINCT iucn_cat, grid.puid FROM marxan.wdpa, marxan.{} grid WHERE ST_Intersects(wdpa.geometry, grid.geometry) AND wdpaid IN (SELECT wdpaid FROM (SELECT envelope FROM marxan.metadata_planning_units WHERE feature_class_name =  %s) AS sub, marxan.wdpa WHERE ST_Intersects(wdpa.geometry, envelope)) ORDER BY 1,2").format(sql.Identifier(self.get_argument('planning_grid_name'))), data=[self.get_argument('planning_grid_name')], returnFormat="DataFrame")
            # write the intersections to file
            intersectionData.to_csv(
                self.folder_input + PROTECTED_AREA_INTERSECTIONS_FILENAME, index=False)
            # get the data
            _getProtectedAreaIntersectionsData(self)
            # set the response
            self.close({'info': 'Preprocessing finished',
                        'intersections': self.protectedAreaIntersectionsData})


# preprocesses the planning units to get the boundary lengths where they intersect - produces the bounds.dat file
# wss://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/preprocessPlanningUnits?user=admin&project=Start%20project
class preprocessPlanningUnits(QueryWebSocketHandler):
    async def open(self):
        try:
            await super().open({'info': "Calculating boundary lengths"})
        except MarxanServicesError:  # authentication/authorisation error
            pass
        else:
            _validateArguments(self.request.arguments, ['user', 'project'])
            # get the project data
            await _getProjectData(self)
            if (not self.projectData["metadata"]["OLDVERSION"]):
                # new version of marxan - get the boundary lengths
                feature_class_name = _getUniqueFeatureclassName("tmp_")
                await pg.execute(sql.SQL("DROP TABLE IF EXISTS marxan.{};").format(sql.Identifier(feature_class_name)))
                # do the intersection
                results = await self.executeQuery(sql.SQL("CREATE TABLE marxan.{feature_class_name} AS SELECT DISTINCT a.puid id1, b.puid id2, ST_Length(ST_CollectionExtract(ST_Intersection(ST_Transform(a.geometry, 3410), ST_Transform(b.geometry, 3410)), 2))/1000 boundary  FROM marxan.{planning_unit_name} a, marxan.{planning_unit_name} b  WHERE a.puid < b.puid AND ST_Touches(a.geometry, b.geometry);").format(feature_class_name=sql.Identifier(feature_class_name), planning_unit_name=sql.Identifier(self.projectData["metadata"]["PLANNING_UNIT_NAME"])))
                # delete the file if it already exists
                if (os.path.exists(self.folder_input + BOUNDARY_LENGTH_FILENAME)):
                    os.remove(self.folder_input + BOUNDARY_LENGTH_FILENAME)
                # write the boundary lengths to file
                await pg.execute(sql.SQL("SELECT * FROM marxan.{};").format(sql.Identifier(feature_class_name)), returnFormat="File", filename=self.folder_input + BOUNDARY_LENGTH_FILENAME)
                # delete the tmp table
                await pg.execute(sql.SQL("DROP TABLE IF EXISTS marxan.{};").format(sql.Identifier(feature_class_name)))
                # update the input.dat file
                _updateParameters(
                    self.folder_project + PROJECT_DATA_FILENAME, {'BOUNDNAME': 'bounds.dat'})
            # set the response
            self.close({'info': 'Boundary lengths calculated'})

# creates a new planning grid
# wss://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/createPlanningUnitGrid?iso3=AND&domain=Terrestrial&areakm2=50&shape=hexagon


class createPlanningUnitGrid(QueryWebSocketHandler):
    async def open(self):
        try:
            await super().open({'info': "Creating planning grid.."})
        except MarxanServicesError:  # authentication/authorisation error
            pass
        else:
            _validateArguments(self.request.arguments, [
                               'iso3', 'domain', 'areakm2', 'shape'])
            # get the feature class name
            fc = "pu_" + self.get_argument('iso3').lower() + "_" + self.get_argument('domain').lower(
            ) + "_" + self.get_argument('shape').lower() + "_" + self.get_argument('areakm2')
            # see if the planning grid already exists
            records = await pg.execute("SELECT * FROM marxan.metadata_planning_units WHERE feature_class_name =(%s);", data=[fc], returnFormat="Array")
            if len(records):
                self.close({'error': "That item already exists"})
            else:
                # estimate how many planning units are in the grid that will be created
                unitCount = await _estimatePlanningUnitCount(self.get_argument('areakm2'), self.get_argument('iso3'), self.get_argument('domain'))
                # see if the unit count is above the PLANNING_GRID_UNITS_LIMIT
                if (int(unitCount) > PLANNING_GRID_UNITS_LIMIT):
                    self.close({'error': "Number of planning units &gt; " + str(PLANNING_GRID_UNITS_LIMIT) + " (=" + str(int(unitCount)) +
                                "). See <a href='" + ERRORS_PAGE + "#number-of-planning-units-exceeds-the-threshold' target='blank'>here</a>"})
                else:
                    results = await self.executeQuery("SELECT * FROM marxan.planning_grid(%s,%s,%s,%s,%s);", [self.get_argument('areakm2'), self.get_argument('iso3'), self.get_argument('domain'), self.get_argument('shape'), self.get_current_user()], returnFormat="Array")
                    if results:
                        # get the planning grid alias
                        alias = results[0][0]
                        # create a primary key so the feature class can be used in ArcGIS
                        await pg.createPrimaryKey(fc, "puid")
                        # start the upload to Mapbox
                        uploadId = await _uploadTilesetToMapbox(fc, fc)
                        # set the response
                        self.close({'info': "Planning grid '" + alias + "' created",
                                    'feature_class_name': fc, 'alias': alias, 'uploadId': uploadId})

# runs a gap analysis
# wss://61c92e42cb1042699911c485c38d52ae.vfs.cloud9.eu-west-1.amazonaws.com:8081/marxan-server/runGapAnalysis?user=admin&project=British%20Columbia%20Marine%20Case%20Study


class runGapAnalysis(QueryWebSocketHandler):
    async def open(self):
        try:
            await super().open({'info': "Running gap analysis.."})
        except MarxanServicesError:  # authentication/authorisation error
            pass
        else:
            _validateArguments(self.request.arguments, ['user', 'project'])
            # get the identifiers of the features for the project
            df = await _getProjectInputData(self, "SPECNAME")
            featureIds = df['id'].to_numpy().tolist()
            # get the planning grid name
            await _getProjectData(self)
            # get a safe project name to use in the name of the table that will be produced
            project_name = _getSafeProjectName(self.get_argument("project"))
            # run the gap analysis
            df = await self.executeQuery("SELECT * FROM marxan.gap_analysis(%s,%s,%s,%s)", data=[self.projectData["metadata"]["PLANNING_UNIT_NAME"], featureIds, self.get_argument("user"), project_name], returnFormat="DataFrame")
            # return the results
            self.close({'info': "Gap analysis complete",
                        'data': df.to_dict(orient="records")})

# resets the database and files to their original state


class resetDatabase(QueryWebSocketHandler):
    async def open(self):
        try:
            await super().open({'info': "Resetting database.."})
        except MarxanServicesError:
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
                result = await _runCmd(cmd)
                # delete all users other than admin, guest and _clumping
                user_folders = glob.glob(MARXAN_USERS_FOLDER + "*/")
                for user_folder in user_folders:
                    if os.path.split(user_folder[:-1])[1] not in ['admin', '_clumping', 'guest']:
                        shutil.rmtree(user_folder)
                # delete the features that are not in use
                specDatFiles = _getFilesInFolderRecursive(
                    CASE_STUDIES_FOLDER, SPEC_FILENAME)
                # iterate through the spec.dat files and get a unique list of feature ids
                featureIdsToKeep = []
                for file in specDatFiles:
                    # load the spec.dat file
                    df = _loadCSV(file)
                    # get the unique feature ids
                    ids = df.id.unique().tolist()
                    # merge these ids into the featureIds array
                    featureIdsToKeep.extend(ids)
                # delete the features that are not in use
                df = await pg.execute("DELETE FROM marxan.metadata_interest_features WHERE NOT oid = ANY (ARRAY[%s]);", data=[featureIdsToKeep])
                self.send_response(
                    {'status': 'Preprocessing', 'info': "Deleted features"})
                # delete the planning grids that are not in use
                planningGridFiles = _getFilesInFolderRecursive(
                    CASE_STUDIES_FOLDER, PROJECT_DATA_FILENAME)
                # iterate through the input.dat files and get a unique list of planning grids
                planningGridsToKeep = []
                for file in planningGridFiles:
                    # get the input.dat file data
                    tmpObj = ExtendableObject()
                    tmpObj.project = "unimportant"
                    tmpObj.folder_project = os.path.dirname(file) + os.sep
                    await _getProjectData(tmpObj)
                    # get the planning grid
                    planningGridsToKeep.append(
                        tmpObj.projectData["metadata"]['PLANNING_UNIT_NAME'])
                df = await pg.execute("DELETE FROM marxan.metadata_planning_units WHERE NOT feature_class_name = ANY (ARRAY[%s]);", data=[planningGridsToKeep])
                self.send_response(
                    {'status': 'Preprocessing', 'info': "Deleted planning grids"})
                # run a cleanup
                self.send_response(
                    {'status': 'Preprocessing', 'info': "Cleaning up.."})
                await _cleanup()
                self.close({'info': "Reset complete"})

####################################################################################################################################################################################################################################################################
# tornado functions
####################################################################################################################################################################################################################################################################


class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            ("/marxan-server/getServerData", getServerData),
            ("/marxan-server/getProjects", getProjects),
            ("/marxan-server/getProjectsWithGrids", getProjectsWithGrids),
            ("/marxan-server/getProject", getProject),
            ("/marxan-server/createProject", createProject),
            ("/marxan-server/createImportProject", createImportProject),
            ("/marxan-server/upgradeProject", upgradeProject),
            ("/marxan-server/deleteProject", deleteProject),
            ("/marxan-server/cloneProject", cloneProject),
            ("/marxan-server/exportProject", exportProject),
            ("/marxan-server/importProject", importProject),
            ("/marxan-server/createProjectGroup", createProjectGroup),
            ("/marxan-server/deleteProjects", deleteProjects),
            ("/marxan-server/renameProject", renameProject),
            ("/marxan-server/updateProjectParameters", updateProjectParameters),
            ("/marxan-server/listProjectsForFeature", listProjectsForFeature),
            ("/marxan-server/listProjectsForPlanningGrid",
             listProjectsForPlanningGrid),
            ("/marxan-server/getCountries", getCountries),
            ("/marxan-server/getPlanningUnitGrids", getPlanningUnitGrids),
            ("/marxan-server/createPlanningUnitGrid", createPlanningUnitGrid),
            ("/marxan-server/exportPlanningUnitGrid", exportPlanningUnitGrid),
            ("/marxan-server/deletePlanningUnitGrid", deletePlanningUnitGrid),
            ("/marxan-server/uploadTilesetToMapBox", uploadTilesetToMapBox),
            ("/marxan-server/uploadFileToFolder", uploadFileToFolder),
            ("/marxan-server/unzipShapefile", unzipShapefile),
            ("/marxan-server/deleteShapefile", deleteShapefile),
            ("/marxan-server/getShapefileFieldnames", getShapefileFieldnames),
            ("/marxan-server/uploadFile", uploadFile),
            ("/marxan-server/importPlanningUnitGrid", importPlanningUnitGrid),
            ("/marxan-server/createFeaturePreprocessingFileFromImport",
             createFeaturePreprocessingFileFromImport),
            ("/marxan-server/toggleEnableGuestUser", toggleEnableGuestUser),
            ("/marxan-server/createUser", createUser),
            ("/marxan-server/validateUser", validateUser),
            ("/marxan-server/logout", logout),
            ("/marxan-server/resendPassword", resendPassword),
            ("/marxan-server/getUser", getUser),
            ("/marxan-server/getUsers", getUsers),
            ("/marxan-server/deleteUser", deleteUser),
            ("/marxan-server/updateUserParameters", updateUserParameters),
            ("/marxan-server/getFeature", getFeature),
            ("/marxan-server/importFeatures", importFeatures),
            ("/marxan-server/exportFeature", exportFeature),
            ("/marxan-server/deleteFeature", deleteFeature),
            ("/marxan-server/createFeatureFromLinestring",
             createFeatureFromLinestring),
            ("/marxan-server/createFeaturesFromWFS", createFeaturesFromWFS),
            ("/marxan-server/getFeaturePlanningUnits", getFeaturePlanningUnits),
            ("/marxan-server/getPlanningUnitsData",
             getPlanningUnitsData),  # currently not used
            ("/marxan-server/getPlanningUnitsCostData", getPlanningUnitsCostData),
            ("/marxan-server/updatePUFile", updatePUFile),
            ("/marxan-server/getPUData", getPUData),
            ("/marxan-server/getSpeciesData", getSpeciesData),  # currently not used
            ("/marxan-server/getAllSpeciesData", getAllSpeciesData),
            ("/marxan-server/getSpeciesPreProcessingData",
             getSpeciesPreProcessingData),  # currently not used
            ("/marxan-server/updateSpecFile", updateSpecFile),
            ("/marxan-server/getProtectedAreaIntersectionsData",
             getProtectedAreaIntersectionsData),  # currently not used
            # currently not used - bugs in the Marxan output log
            ("/marxan-server/getMarxanLog", getMarxanLog),
            ("/marxan-server/getBestSolution",
             getBestSolution),  # currently not used
            ("/marxan-server/getOutputSummary",
             getOutputSummary),  # currently not used
            ("/marxan-server/getSummedSolution",
             getSummedSolution),  # currently not used
            ("/marxan-server/getResults", getResults),
            ("/marxan-server/getSolution", getSolution),
            ("/marxan-server/getMissingValues",
             getMissingValues),  # currently not used
            ("/marxan-server/preprocessFeature", preprocessFeature),
            ("/marxan-server/preprocessPlanningUnits", preprocessPlanningUnits),
            ("/marxan-server/preprocessProtectedAreas", preprocessProtectedAreas),
            ("/marxan-server/runMarxan", runMarxan),
            ("/marxan-server/stopProcess", stopProcess),
            ("/marxan-server/getRunLogs", getRunLogs),
            ("/marxan-server/clearRunLogs", clearRunLogs),
            ("/marxan-server/updateWDPA", updateWDPA),
            ("/marxan-server/runGapAnalysis", runGapAnalysis),
            ("/marxan-server/deleteGapAnalysis", deleteGapAnalysis),
            ("/marxan-server/getCosts", getCosts),
            ("/marxan-server/updateCosts", updateCosts),
            ("/marxan-server/deleteCost", deleteCost),
            ("/marxan-server/importGBIFData", importGBIFData),
            ("/marxan-server/dismissNotification", dismissNotification),
            ("/marxan-server/resetNotifications", resetNotifications),
            ("/marxan-server/testRoleAuthorisation", testRoleAuthorisation),
            ("/marxan-server/addParameter", addParameter),
            ("/marxan-server/resetDatabase", resetDatabase),
            ("/marxan-server/runSQLFile", runSQLFile),
            ("/marxan-server/cleanup", cleanup),
            ("/marxan-server/shutdown", shutdown),
            ("/marxan-server/block", block),
            ("/marxan-server/testTornado", testTornado),
            ("/marxan-server/exports/(.*)",
             tornado.web.StaticFileHandler, dict(path=EXPORT_FOLDER)),
            # default handler if the REST services is cannot be found on this server - maybe a newer client is requesting a method on an old server
            ("/marxan-server/(.*)", methodNotFound),
            # assuming the marxan-client is installed in the same folder as the marxan-server all files will go to the client build folder
            (r"/(.*)", StaticFileHandler, {"path": MARXAN_CLIENT_BUILD_FOLDER})
        ]
        settings = dict(
            cookie_secret=COOKIE_RANDOM_VALUE,
            static_path=EXPORT_FOLDER
        )
        super(Application, self).__init__(handlers, **settings)


async def initialiseApp():
    # set the global variables
    await _setGlobalVariables()
    # LOGGING SECTION
    # turn on tornado logging
    tornado.options.parse_command_line()
    # get the parent logger of all tornado loggers
    root_logger = logging.getLogger()
    # set the logging level
    root_logger.setLevel(LOGGING_LEVEL)
    # set your format for the streaming logger
    root_streamhandler = root_logger.handlers[0]
    f1 = '%(color)s[%(levelname)1.1s %(asctime)s.%(msecs)03d]%(end_color)s '
    f2 = '%(message)s'
    root_streamhandler.setFormatter(LogFormatter(
        fmt=f1 + f2, datefmt='%d-%m-%y %H:%M:%S', color=True))
    # google cloud logger if enabled
    if 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ.keys():
        client = googlelogger.Client()
        client.setup_logging()
        # override the handlers as Google Cloud Logging adds another stream handler which we dont need
        root_logger.handlers = root_logger.handlers[1:]
        root_logger.handlers[0].setFormatter(LogFormatter(
            fmt=f2 + " (" + SERVER_NAME + ")", datefmt='%d-%m-%y %H:%M:%S', color=False))
        root_logger.handlers[1].setFormatter(LogFormatter(
            fmt=f1 + f2, datefmt='%d-%m-%y %H:%M:%S', color=True))
        log("Logging to Google Cloud Logging", Fore.GREEN)
    # add a file logger
    if not DISABLE_FILE_LOGGING:
        file_log_handler = logging.FileHandler(MARXAN_FOLDER + MARXAN_LOG_FILE)
        file_log_handler.setFormatter(LogFormatter(
            fmt=f1 + f2, datefmt='%d-%m-%y %H:%M:%S', color=False))
        root_logger.addHandler(file_log_handler)
    # logging.disable(logging.ERROR)
    # initialise the app
    app = Application()
    # start listening on whichever port, and if there is an https certificate then use the certificate information from the server.dat file to return data securely
    if CERTFILE != "None":
        app.listen(PORT, ssl_options={
                   "certfile": CERTFILE, "keyfile": KEYFILE})
        navigateTo = "https://"
    else:
        app.listen(PORT)
        navigateTo = "http://"
    navigateTo = navigateTo + "<host>:" + PORT + \
        "/index.html" if (PORT != '80') else navigateTo + "<host>/index.html"
    # open the web browser if the call includes a url, e.g. python marxan-server.py http://localhost/index.html
    if len(sys.argv) > 1:
        if MARXAN_CLIENT_VERSION == "Not installed":
            log("Ignoring <url> parameter - the marxan-client is not installed", Fore.GREEN)
        else:
            url = sys.argv[1]  # normally "http://localhost/index.html"
            log("Opening Marxan Web at '" + url + "' ..\n", Fore.GREEN)
            webbrowser.open(url, new=1, autoraise=True)
    else:
        if MARXAN_CLIENT_VERSION != "Not installed":
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
    logging.warning("marxan-server stopped automatically")

if __name__ == "__main__":
    try:
        # initialise the app
        try:
            tornado.ioloop.IOLoop.current().run_sync(initialiseApp)
        except KeyboardInterrupt:
            _deleteShutdownFile()
            pass
        finally:
            logging.warning("marxan-server stopped by KeyboardInterrupt")
            SHUTDOWN_EVENT.set()

    except Exception as e:
        print('e: ', e)
        if (e.args[0] == 98):
            log("The port " + str(PORT) + " is already in use")
        else:
            log(e.args)
