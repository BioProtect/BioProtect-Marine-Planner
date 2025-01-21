from services.feature_service import get_feature_data
from services.service_error import ServicesError, raise_error, validate_arguments
from server.handlers.base_handler import BaseHandler

# handlers/get_feature_handler.py


class GetFeatureHandler(BaseHandler):
    """REST HTTP handler. Gets feature information from PostGIS.

    Args:
        oid (string): The feature oid.

    Returns:
        A dict with the structure (if there's an error, the error message is included in an 'error' key/value pair):
        {
            "data": dict containing the keys: id,feature_class_name,alias,description,area,extent,creation_date,tilesetid,source,created_by
        }
    """

    def initialize(self, pg):
        """Initialize the handler with a PostGIS database instance."""
        self.pg = pg

    async def get(self):
        try:
            # Validate input arguments
            validate_arguments(self.request.arguments, ['oid'])
            oid = self.get_argument("oid")

            # Call the external function to get feature data
            feature_data = await get_feature_data(self.pg, oid)

            # Set the response with the retrieved data
            self.send_response({
                "data": feature_data.to_dict(orient="records")
            })
        except ServicesError as e:
            raise ()(self, e.args[0])
