import glob
import json
import shutil
from datetime import datetime
from os.path import basename, join, normpath
from types import SimpleNamespace

# JWT utility for generating tokens
from asyncpg.exceptions import UniqueViolationError
from handlers.base_handler import BaseHandler
# Assumes you have these utility functions
from passlib.hash import bcrypt
from psycopg2 import sql
from services.file_service import (get_key_values_from_file,
                                   get_notifications_data,
                                   update_file_parameters)
from services.project_service import clone_a_project
from services.service_error import ServicesError, raise_error


class UserHandler(BaseHandler):
    def initialize(self, pg):
        self.pg = pg

    def validate_args(self, arguments, required_arguments):
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

    async def get(self, user_id=None):
        """
        Retrieve a single user by ID or all users if no ID is provided.
        """
        query = """
        SELECT id, username, last_project, show_popup, basemap, role, use_feature_colours, report_units, refresh_tokens
        """
        try:
            user_id = int(user_id)
        except ValueError:
            self.set_status(400)
            self.write(json.dumps({"error": "Invalid user ID"}))
            return

        if user_id:
            query = query + "FROM users WHERE id = $1"
            result = await self.pg.execute(query, data=[user_id], return_format="Dict")
            if not result:
                self.set_status(404)
                self.write({"message": "User not found"})
                return

            response = json.dumps(result[0])
            callback = self.get_argument("callback", None)
            if callback:
                self.write(f"{callback}({response})")
            else:
                self.write(response)
        else:
            users = await self.pg.execute(query, return_format="Array")
            self.write(json.dumps({"users": users}))

    async def put(self, user_id):
        """
        Update an existing user.
        """
        try:
            body = json.loads(self.request.body)
            updates = []
            params = []
            index = 1

            for field in ["username", "email", "password", "role"]:
                value = body.get(field)
                if value:
                    if field == "password":
                        value = bcrypt.hash(value)
                    updates.append(f"{field} = ${index}")
                    params.append(value)
                    index += 1

            if not updates:
                self.set_status(400)
                self.write({"message": "No fields to update"})
                return

            params.append(user_id)
            query = f"UPDATE users SET {
                ', '.join(updates)} WHERE id = ${index}"
            result = await self.pg.execute(query, *params)
            if result == "UPDATE 0":
                self.set_status(404)
                self.write({"message": "User not found"})
            else:
                self.write({"message": "User updated"})

        except Exception as e:
            self.set_status(500)
            self.write({"message": "Error updating user", "error": str(e)})

    async def delete(self, user_id):
        """
        Delete a user.
        """
        try:
            query = "DELETE FROM users WHERE id = $1"
            result = await self.pg.execute(query, int(user_id))
            if result == "DELETE 0":
                self.set_status(404)
                self.write({"message": "User not found"})
            else:
                self.write({"message": "User deleted"})

        except Exception as e:
            self.set_status(500)
            self.write({"message": "Error deleting user", "error": str(e)})
