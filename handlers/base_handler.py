import contextlib
import json
import traceback

from classes.folder_path_config import get_folder_path_config
from tornado.web import RequestHandler

folder_path_config = get_folder_path_config()


class BaseHandler(RequestHandler):
    """Base class to handle all HTTP requests. Handles authentication, authorisation, exception handling, writing headers and sending responses. All REST request handlers derive from this class.

    Attributes:
        user: A string with the name of the user making the request (if the request.arguments contains a user key).
        folder_user: A string with the path to the users folder (if the request.arguments contains a user key).
        project: A string with the name of the project (if the request.arguments contains a project key).
        folder_project: A string with the path to the project folder (if the request.arguments contains a project key).
        folder_input: A string with the path to the projects input folder (if the request.arguments contains a project key).
        folder_output: A string with the path to the projects output folder (if the request.arguments contains a project key).
    """

    def set_default_headers(self):
        """Writes CORS headers in the response to prevent CORS errors in the client"""
        if folder_path_config.DISABLE_SECURITY:
            self.set_header("Access-Control-Allow-Origin",
                            "http://localhost:3000")
            self.set_header("Access-Control-Allow-Methods",
                            "GET, POST, OPTIONS")
            self.set_header("Access-Control-Allow-Headers",
                            "Content-Type, Authorization")
            self.set_header("Access-Control-Allow-Credentials", "true")

    def options(self, *args, **kwargs):
        # Respond to preflight OPTIONS request
        self.set_status(204)  # No Content
        self.finish()

    def get_current_user(self):
        """Gets the current user.
        Args:
            None
        Returns:
            string: The name of the currently authenticated user.
        """
        if self.get_secure_cookie("user"):
            return self.get_secure_cookie("user").decode("utf-8")

    def send_response(self, response):
        """Used by all descendent classes to write the response data and send it.

        Args:
            response (dict): The response data to write as a dict.
        Returns:
            None
        """
        try:
            self.set_header('Content-Type', 'application/json')
            content = json.dumps(response)
        except (UnicodeDecodeError) as e:
            if 'log' in response:
                response.update({
                    "log": f"Server warning: Unable to encode the Marxan log. <br/>{repr(e)}",
                    "warning": "Unable to encode the Marxan log"
                })
                content = json.dumps(response)
        finally:
            if "callback" in self.request.arguments:
                callback = self.get_argument("callback")
                self.write(f"{callback}({content})")
            else:
                self.write(content)

    def write_error(self, status_code, **kwargs):
        """
        Handles uncaught exceptions in descendant classes by sending the stack trace to the client.

        Args:
            status_code (int): HTTP status code of the error.
            **kwargs: Additional arguments passed by Tornado, including exception info.
        Returns:
            None
        """
        # If no exception info is provided, return immediately
        if "exc_info" not in kwargs:
            self.set_status(500)
            self.write({"error": "An unknown error occurred."})
            self.finish()
            return

        # Extract exception details
        exc_info = kwargs["exc_info"]
        trace = "".join(traceback.format_exception(*exc_info))
        last_line = traceback.format_exception(
            *exc_info)[-1].split(":", 1)[-1].strip()

        # Set CORS headers if needed
        if not folder_path_config.DISABLE_SECURITY:
            with contextlib.suppress(Exception):
                _checkCORS(self)

        # Respond with an HTTP 200 status code and the error details
        self.set_status(200)
        self.send_response({
            "error": last_line,
            "trace": trace
        })
        self.finish()
