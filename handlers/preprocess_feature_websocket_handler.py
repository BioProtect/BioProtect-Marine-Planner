# preprocesses the features by intersecting them with the planning units
# wss://<server>:8081/server/PreprocessFeature?user=andrew&project=Tonga%20marine%2030km2&planning_grid_name=pu_ton_marine_hexagon_30&feature_class_name=volcano&alias=volcano&id=63408475
import os

import pandas as pd
from handlers.websocket_handler import SocketHandler
from services.service_error import ServicesError


class PreprocessFeature(SocketHandler):
    """
    REST WebSocket Handler. Preprocesses features by intersecting them with planning units. Summarizes polygon areas or point values for each planning unit.

    Required Arguments:
        user (str): The name of the user.
        project (str): The name of the project.
        id (str): The feature id.
        feature_class_name (str): The feature class name.
        alias (str): The alias for the feature.
        planning_grid_name (str): The name of the planning grid.

    Returns:
        dict: WebSocket messages with keys:
            - "info": Detailed progress messages.
            - "feature_class_name": The name of the preprocessed feature class.
            - "id": The feature id.
            - "pu_area": Total area of the feature in the planning grid.
            - "pu_count": Total number of planning grids intersecting the feature.
    """

    def initialize(self, pg):
        super().initialize()
        self.pg = pg

    @staticmethod
    def file_to_df(file_name):
        """Reads a file and returns the data as a DataFrame

        Args:
            file_name (string): The name of the file to read.
        Returns:
            DataFrame: The data from the file.
        """
        return pd.read_csv(file_name, sep=None, engine='python') if os.path.exists(file_name) else pd.DataFrame()

    async def open(self):
        try:
            alias = self.get_argument('alias')
            await super().open({'info': f"Preprocessing '{alias}'.."})
        except ServicesError:
            pass  # Authentication/authorization error
        else:
            self.validate_args(self.request.arguments, [
                'user', 'project_id', 'feature_id', 'feature_class_name', 'alias', 'planning_grid_name'
            ])

            project_id = self.get_argument("project_id")
            feature_id = self.get_argument("feature_id")

            try:
                # Determine geometry type
                feature_class_name = self.get_argument('feature_class_name')
                planning_grid_name = self.get_argument('planning_grid_name')
                geometry_type = await self.pg.get_geometry_type(feature_class_name)
                if geometry_type not in ("ST_Point",):
                    geometry_type = "ST_Polygon"

                # 1. Clear existing data
                await self.pg.execute(
                    "SELECT bioprotect.clear_feature_data(%s, %s)", [project_id, feature_id])

                # 2. Insert intersections
                await self.pg.execute(
                    "SELECT bioprotect.insert_feature_pu_amounts(%s, %s, %s, %s, %s)",
                    [project_id, feature_id, feature_class_name, planning_grid_name, geometry_type])

                # 3. Aggregate Stats
                await self.pg.execute(
                    "SELECT bioprotect.aggregate_feature_stats(%s, %s)", [project_id, feature_id])

                # 4. Fetch summary
                stats = await self.pg.execute(
                    """
                    SELECT pu_area, pu_count
                    FROM bioprotect.feature_preprocessing
                    WHERE project_id = %s AND feature_unique_id = %s
                    """,
                    [project_id, feature_id],
                    return_format="Dict"
                )
                pu_area = stats[0]["pu_area"] if stats else 0
                pu_count = stats[0]["pu_count"] if stats else 0

                # Final response
                self.close({
                    'info': f"Feature '{alias}' preprocessed",
                    'feature_class_name': feature_class_name,
                    'id': feature_id,
                    'pu_area': pu_area,
                    'pu_count': pu_count
                })

            except ServicesError as e:
                self.close({'error': e.args[0]})
