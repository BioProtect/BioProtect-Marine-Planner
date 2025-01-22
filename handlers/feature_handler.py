import os
import pandas as pd
import requests
from psycopg2 import sql
from services.file_service import (
    file_data_to_df, get_key_values_from_file, write_to_file)
from services.project_service import get_projects_for_feature
from services.service_error import ServicesError, raise_error
from services.file_service import create_zipfile
from handlers.base_handler import BaseHandler
import uuid


class FeatureHandler(BaseHandler):
    """
    REST HTTP handler for feature-related operations, including creation, deletion, import, export,
    fetching, and listing projects for a feature.
    """

    def initialize(self, pg, proj_paths, finish_feature_import, upload_tileset_to_mapbox):
        self.pg = pg
        self.proj_paths = proj_paths
        self.finish_feature_import = finish_feature_import
        self.upload_tileset_to_mapbox = upload_tileset_to_mapbox

    def validate_args(self, args, required_keys):
        # sourcery skip: use-named-expression
        """Checks that all of the arguments in argumentList are in the arguments dictionary."""
        missing = [key for key in required_keys if key not in args]
        if missing:
            raise ServicesError(f"Missing required arguments: {
                                ', '.join(missing)}")

    async def get(self):
        """
        Handles GET requests for various feature-related actions based on query parameters.
        """
        try:
            action = self.get_argument('action', None)

            if action == 'get':
                await self.get_feature()
            elif action == 'delete':
                await self.delete_feature()
            elif action == 'export':
                await self.export_feature()
            elif action == 'list_projects':
                self.list_projects_for_feature()
            elif action == 'planning_units':
                await self.get_feature_planning_units()
            else:
                raise ServicesError("Invalid action specified.")

        except ServicesError as e:
            raise_error(self, e.args[0])

    async def post(self):
        """
        Handles POST requests for feature-related actions based on query parameters.
        """
        try:
            action = self.get_argument('action', None)

            if action == 'create_from_linestring':
                await self.create_feature_from_linestring()
            else:
                raise ServicesError("Invalid action specified.")

        except ServicesError as e:
            raise_error(self, e.args[0])

    async def get_feature(self):
        """Fetches feature information from PostGIS."""
        self.validate_args(self.request.arguments, ['unique_id'])
        unique_id = self.get_argument("unique_id")

        query = (
            """
            SELECT unique_id::integer AS id, feature_class_name, alias, description,
            _area AS area, extent, to_char(creation_date, 'DD/MM/YY HH24:MI:SS') AS creation_date,
            tilesetid, source, created_by
            FROM marxan.metadata_interest_features
            WHERE unique_id = %s;
            """
        )

        data = await self.pg.execute(query, data=[unique_id], return_format="DataFrame")
        self.send_response({"data": data.to_dict(orient="records")})

    async def delete_feature(self):
        """Deletes a feature class and its associated metadata record."""
        self.validate_args(self.request.arguments, ['feature_name'])
        feature_class_name = self.get_argument('feature_name')

        feature_data = await self.pg.execute(
            """
            SELECT unique_id, created_by FROM marxan.metadata_interest_features WHERE feature_class_name = %s;
            """,
            data=[feature_class_name],
            return_format="Dict"
        )

        if not feature_data:
            return

        if feature_data[0].get("created_by") == "global admin":
            raise ServicesError(
                "This is a system feature and cannot be deleted.")

        projects = get_projects_for_feature(
            feature_data[0]["unique_id"], self.proj_paths.USERS_FOLDER)
        if projects:
            raise ServicesError(
                "The feature cannot be deleted as it is used in one or more projects.")

        await self.pg.execute(sql.SQL("DROP TABLE IF EXISTS marxan.{};").format(sql.Identifier(feature_class_name)))
        await self.pg.execute("DELETE FROM marxan.metadata_interest_features WHERE feature_class_name = %s;", [feature_class_name])

        try:
            response = requests.delete(
                f"https://api.mapbox.com/tilesets/v1/{self.proj_paths.MAPBOX_USER}.{
                    feature_class_name}?access_token={self.proj_paths.MBAT}"
            )
            if response.status_code != 204:
                raise ServicesError(f"Failed to delete tileset: {
                                    response.status_code} - {response.text}")

        except Exception as e:
            print(f"Warning: Unable to delete tileset for feature '{
                  feature_class_name}': {e}")

        self.send_response({'info': "Feature deleted"})

    async def export_feature(self):
        """Exports a feature to a shapefile and zips it."""
        self.validate_args(self.request.arguments, ['name'])
        feature_class_name = self.get_argument('name')
        folder = self.proj_paths.EXPORT_FOLDER

        await self.pg.exportToShapefile(folder, feature_class_name, tEpsgCode="EPSG:4326")
        zipfilename = create_zipfile(folder, feature_class_name)

        self.send_response({
            'info': f"Feature '{feature_class_name}' exported",
            'filename': zipfilename
        })

    async def create_feature_from_linestring(self):
        """Creates a new feature from a provided linestring."""
        self.validate_args(self.request.arguments, [
                           'name', 'description', 'linestring'])

        user = self.get_current_user()
        name = self.get_argument('name')
        description = self.get_argument('description')
        linestring = self.get_argument('linestring')
        feature_class_name = "f_" + uuid.uuid4().hex[:30]

        create_table_query = sql.SQL(
            """
            CREATE TABLE marxan.{} AS
            SELECT marxan.ST_SplitAtDateLine(ST_SetSRID(ST_MakePolygon(%s)::geometry, 4326)) AS geometry;
            """
        ).format(sql.Identifier(feature_class_name))

        await self.pg.execute(create_table_query, [linestring])
        feature_id = await self.finish_feature_import(feature_class_name, name, description, "Drawn on screen", user)
        upload_id = await self.upload_tileset_to_mapbox(feature_class_name, feature_class_name)

        self.send_response({
            'info': f"Feature '{name}' created",
            'id': feature_id,
            'feature_class_name': feature_class_name,
            'uploadId': upload_id
        })

    async def get_feature_planning_units(self):
        """Gets the planning unit IDs for a feature."""

        self.validate_args(self.request.arguments, ['user', 'project', 'oid'])
        # unique_ids = self.get_argument("oid")
        ids = self.get_argument("unique_id")

        file_name = os.path.join(
            self.folder_input, self.projectData["files"]["PUVSPRNAME"])
        df = pd.read_csv(file_name, sep=None, engine='python') if os.path.exists(
            file_name) else pd.DataFrame()
        puids = df.loc[df['species'] == int(ids)]['pu'].unique().tolist()

        self.send_response({"data": puids})

    def list_projects_for_feature(self):
        """Lists all projects containing a specific feature."""

        self.validate_args(self.request.arguments, ['feature_class_id'])

        projects = get_projects_for_feature(
            int(self.get_argument('feature_class_id')), self.proj_paths.USERS_FOLDER)

        self.send_response({
            'info': "Projects info returned",
            "projects": projects
        })


class importFeatures(MarxanWebSocketHandler):
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


class createFeaturesFromWFS(MarxanWebSocketHandler):
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
