import os
import pandas as pd
from psycopg2 import sql
from services.file_service import (
    file_data_to_df, normalize_dataframe, write_csv, set_folder_paths)
from services.project_service import get_projects_for_planning_grid
from services.service_error import ServicesError, raise_error


class PlanningUnitHandler(BaseHandler):
    """
    Handles planning unit-related operations, such as importing, exporting, deleting,
    retrieving, and updating planning units.
    """

    def initialize(self, pg, proj_paths):
        self.pg = pg
        self.proj_paths = proj_paths

    def validate_args(self, arguments, required_keys):
        """Validates that all required arguments are present."""
        missing = [key for key in required_keys if key not in arguments]
        if missing:
            raise ServicesError(f"Missing required arguments: {
                                ', '.join(missing)}")

    async def get(self):
        """
        Handles GET requests for planning unit-related actions.
        """
        action = self.get_argument('action', None)

        try:
            if action == 'delete':
                await self.delete_planning_unit_grid()
            elif action == 'export':
                await self.export_planning_unit_grid()
            elif action == 'list':
                await self.get_planning_unit_grids()
            elif action == 'projects':
                self.list_projects_for_planning_grid()
            elif action == 'cost_data':
                await self.get_planning_units_cost_data()
            elif action == 'data':
                await self.get_planning_unit_data()
            else:
                raise ServicesError("Invalid action specified.")

        except ServicesError as e:
            raise_error(self, e.args[0])

    async def post(self):
        """
        Handles POST requests for planning unit updates.
        """
        try:
            action = self.get_argument('action', None)
            if action == 'update':
                await self.update_pu_file()
            elif action == 'import':
                await self.import_planning_unit_grid()
            else:
                raise ServicesError("Invalid action specified.")

        except ServicesError as e:
            raise_error(self, e.args[0])

    async def delete_planning_unit_grid(self):
        self.validate_args(self.request.arguments, ['planning_grid_name'])
        planning_grid = self.get_argument('planning_grid_name')

        grid_data = await self.pg.execute(
            """
            SELECT created_by, source 
            FROM marxan.metadata_planning_units 
            WHERE feature_class_name = %s;
            """,
            data=[planning_grid],
            return_format="Dict"
        )

        if not grid_data:
            return

        created_by = grid_data[0].get("created_by")
        if created_by == "global admin":
            raise ServicesError(
                "The planning grid cannot be deleted as it is a system-supplied item.")

        projects = get_projects_for_planning_grid(planning_grid)
        if projects:
            raise ServicesError(
                "Grid cannot be deleted as it is used in one or more projects.")

        source = grid_data[0].get("source")
        if source != "planning_grid function":
            del_tileset(planning_grid)

        await self.pg.execute(
            "DELETE FROM marxan.metadata_planning_units WHERE feature_class_name = %s;",
            data=[planning_grid]
        )

        await self.pg.execute(
            sql.SQL("DROP TABLE IF EXISTS marxan.{};").format(
                sql.Identifier(planning_grid))
        )

        self.send_response({'info': 'Planning grid deleted'})

    async def export_planning_unit_grid(self):
        self.validate_args(self.request.arguments, ['name'])
        feature_class_name = self.get_argument('name')
        folder = self.proj_paths.EXPORT_FOLDER

        await self.pg.exportToShapefile(folder, feature_class_name, tEpsgCode="EPSG:4326")
        zipfilename = create_zipfile(folder, feature_class_name)

        self.send_response({
            'info': f"Planning grid '{feature_class_name}' exported",
            'filename': f"{zipfilename}.zip"
        })

    async def get_planning_unit_grids(self):
        planning_unit_grids = await get_pu_grids()
        self.send_response({
            'info': 'Planning unit grids retrieved',
            'planning_unit_grids': planning_unit_grids
        })

    async def import_planning_unit_grid(self):
        self.validate_args(self.request.arguments, [
                           'filename', 'name', 'description'])
        filename = self.get_argument('filename')
        name = self.get_argument('name')
        description = self.get_argument('description')
        user = self.get_current_user()

        root_filename = await asyncio.get_running_loop().run_in_executor(
            None, unzip_shapefile, self.proj_paths.IMPORT_FOLDER, filename
        )

        feature_class_name = get_unique_feature_name("pu_")
        tileset_id = f"{MAPBOX_USER}.{feature_class_name}"
        shapefile_path = os.path.join(
            self.proj_paths.IMPORT_FOLDER, f"{root_filename}.shp")

        try:
            check_zipped_shapefile(shapefile_path)
            fieldnames = get_shapefile_fieldnames(shapefile_path)
            if "PUID" in fieldnames:
                raise ServicesError(
                    "The field 'puid' in the shapefile must be lowercase.")

            await self.pg.execute(
                """
                INSERT INTO marxan.metadata_planning_units(
                    feature_class_name, alias, description, creation_date, source, created_by, tilesetid
                ) VALUES (%s, %s, %s, now(), 'Imported from shapefile', %s, %s);
                """,
                [feature_class_name, name, description, user, tileset_id]
            )

            await self.pg.importShapefile(self.proj_paths.IMPORT_FOLDER, f"{root_filename}.shp", feature_class_name)
            await self.pg.isValid(feature_class_name)

            await self.pg.execute(
                sql.SQL("ALTER TABLE marxan.{} ALTER COLUMN puid TYPE integer;").format(
                    sql.Identifier(feature_class_name))
            )

            await self.pg.execute(
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

            await self.pg.execute(
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

            upload_id = upload_tileset(shapefile_path, feature_class_name)

        except ServicesError as e:
            raise
        finally:
            await asyncio.get_running_loop().run_in_executor(
                None, delete_zipped_shapefile, self.proj_paths.IMPORT_FOLDER, filename, root_filename
            )

        self.send_response({
            'info': f"Planning grid '{name}' imported",
            'feature_class_name': feature_class_name,
            'uploadId': upload_id,
            'alias': name
        })

    def list_projects_for_planning_grid(self):
        self.validate_args(self.request.arguments, ['feature_class_name'])
        projects = get_projects_for_planning_grid(
            self.get_argument('feature_class_name'))
        self.send_response({
            'info': "Projects info returned",
            'projects': projects
        })

    async def get_planning_units_cost_data(self):
        self.validate_args(self.request.arguments, ['user', 'project'])
        set_folder_paths(self, self.request.arguments,
                         self.proj_paths.USERS_FOLDER)

        df = file_data_to_df(os.path.join(
            self.folder_input, self.projectData["files"]["PUNAME"]))
        data = normalize_dataframe(df, "cost", "id", 9)

        self.send_response({
            "data": data[0],
            'min': str(data[1]),
            'max': str(data[2])
        })

    async def update_pu_file(self):
        self.validate_args(self.request.arguments, ['user', 'project'])

        status1_ids = self.get_int_array_from_arg(
            self.request.arguments, "status1")
        status2_ids = self.get_int_array_from_arg(
            self.request.arguments, "status2")
        status3_ids = self.get_int_array_from_arg
