import glob
import json
import shutil
from os.path import basename, join, normpath
from types import SimpleNamespace

from asyncpg.exceptions import UniqueViolationError
from handlers.base_handler import BaseHandler
from passlib.hash import bcrypt
from psycopg2 import sql
from services.user_service import get_notifications_data
from services.file_service import (
    get_key_values_from_file, update_file_parameters)
from services.project_service import clone_a_project
from services.service_error import ServicesError, raise_error

# JWT utility for generating tokens


class UserHandler(BaseHandler):
    """
    REST HTTP handler for user-related operations, including creation, validation, deletion,
    updating parameters, and retrieving user data.
    """

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
            raise ServicesError(f"Missing arguments: {
                                ', '.join(missing_arguments)}")

    async def get(self):
        """
        Handles GET requests for various user-related actions based on query parameters.
        """
        try:
            action = self.get_argument('action', None)

            if action == 'get':
                await self.get_user()
            elif action == 'list':
                await self.get_users()
            elif action == 'validate':
                await self.validate_user()
            elif action == 'logout':
                await self.logout_user()
            elif action == 'delete':
                await self.delete_user()
            elif action == 'resend_password':
                await self.resend_password()
            else:
                raise ServicesError("Invalid action specified.")

        except ServicesError as e:
            raise_error(self, e.args[0])

    async def post(self):
        """
        Handles POST requests for user-related actions based on query parameters.
        """
        try:
            action = self.get_argument('action', None)

            if action == 'create':
                await self.create_user()
            elif action == 'update':
                await self.update_user_parameters()
            else:
                raise ServicesError("Invalid action specified.")

        except ServicesError as e:
            raise_error(self, e.args[0])

    def get_user_data(self, return_password=False):
        user_data_path = join(self.folder_user, "user.dat")
        user_data = get_key_values_from_file(user_data_path)

        # Filter out the password unless requested
        self.userData = (
            user_data if return_password else {
                key: value for key, value in user_data.items() if key != 'PASSWORD'}
        )

    async def create_user(self):
        self.validate_args(self.request.arguments, [
                           "user", "password", "fullname", "email"])
        try:
            body = json.loads(self.request.body)
            username = body.get("username")
            email = body.get("email")
            password = body.get("password")
            role = body.get("role", "user")
            fullname = self.get_argument('fullname')

            if not username or not email or not password:
                self.set_status(400)
                self.write(
                    {"message": "Username, email, and password are required"})
                return

            password_hash = bcrypt.hash(password)
            new_user = await self.pg.execute(
                """
                INSERT INTO users (username, email, password_hash, role, report_units, basemap, date_created, show_popup, use_feature_colours)
                VALUES ($1, $2, $3, 'Admin', 'Km2', 'Light', CURRENT_TIMESTAMP, FALSE, FALSE)
                """,
                data=[username, email, password_hash],
                return_format="Dict"
            )

            case_studies = glob.glob(
                join(self.proj_paths.CASE_STUDIES_FOLDER, "*/"))
            for case_study in case_studies:
                clone_a_project(case_study, join(
                    self.proj_paths.USERS_FOLDER, username))

            self.set_status(201)
            self.write({"message": "User created", "user": new_user})

        except UniqueViolationError:
            self.set_status(409)
            self.write({"message": "Username or email already exists"})
        except Exception as e:
            self.set_status(500)
            self.write({"message": "Error creating user", "error": str(e)})

    async def validate_user(self):
        self.validate_args(self.request.arguments, ["user", "password"])

        self.get_user_data(self, True)

        if self.get_argument("password") == self.userData["PASSWORD"]:
            secure = self.request.protocol == 'https'
            self.set_secure_cookie("user", self.get_argument(
                "user"), httponly=True, secure=secure)
            self.set_secure_cookie(
                "role", self.userData["ROLE"], httponly=True, secure=secure)

            self.send_response({
                'validated': True,
                'info': f"User {self.user} validated"
            })
        else:
            raise ServicesError("Invalid user/password")

    async def logout_user(self):
        self.clear_cookie("user")
        self.clear_cookie("role")
        self.send_response({'info': "User logged out"})

    async def resend_password(self):
        self.send_response({'info': "Not currently implemented"})

    async def get_user(self):
        self.validate_args(self.request.arguments, ["user"])

        query = """
                SELECT id, username, password_hash, role, last_project, show_popup, basemap, use_feature_colours, report_units, refresh_tokens 
                FROM users WHERE username = $1
            """
        userData = await self.pg.execute(query, [self.get_current_user()], return_format="Dict")
        notifications = get_notifications_data(self)
        self.send_response({
            'info': "User data received",
            "userData": userData,
            "unauthorisedMethods": [],
            'dismissedNotifications': notifications
        })

    async def get_users(self):
        user_folders = glob.glob(join(self.proj_paths.USERS_FOLDER, "*/"))

        # Extract usernames from the folder paths
        users = [basename(normpath(folder)) for folder in user_folders]

        # Remove unwanted special folders
        excluded_folders = {"input", "output", "MarxanData", "MarxanData_unix"}
        users = [
            u for u in users if u not in excluded_folders and not u.startswith("_")]

        users.sort()
        users_data = []

        for user in users:
            user_folder = join(self.proj_paths.USERS_FOLDER, user)
            tmp_obj = SimpleNamespace()
            tmp_obj.folder_user = user_folder
            self.get_user_data(tmp_obj)
            # Add the user's data to the list
            user_data = tmp_obj.userData.copy()  # pylint:disable=no-member
            user_data.update({'user': user})
            users_data.append(user_data)

        self.send_response(
            {'info': "Users data received", 'users': users_data})

    async def delete_user(self):
        self.validate_args(self.request.arguments, ["user"])

        try:
            shutil.rmtree(self.folder_user)
            self.send_response({'info': "User deleted"})
        except Exception as e:
            raise ServicesError(f"Failed to delete user: {e}")

    async def update_user_parameters(self):
        self.validate_args(self.request.arguments, ["user"])

        params = {
            key: self.get_argument(key)
            for key in self.request.arguments
            if key not in ["user", "callback"]
        }

        update_file_parameters(join(self.folder_user, "user.dat"), params)

        self.send_response({
            'info': ", ".join(params.keys()) + " parameters updated"
        })

    # NOT YET IMPLEMENTED *************************************************

    async def get_user_by_id(self, user_id=None):
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

    # NOT YET IMPLEMENTED *************************************************
    async def update_user(self, user_id):
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

    # NOT YET IMPLEMENTED *************************************************

    async def delete_user_by_id(self, user_id):
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
