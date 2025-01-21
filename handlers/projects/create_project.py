class createProject(BaseHandler):
    """REST HTTP handler. Creates a project. The required arguments in the request.arguments parameter are:

    Args:
        user (string): The name of the user.
        project (string): The name of the project to create.
        description (string): The description for the project.
        planning_grid_name (string): The name of the planning grid used in the project.
        interest_features (string): A comma-separated string with the interest features.
        target_values (string): A comma-separated string with the corresponding interest feature targets.
        spf_values (string): A comma-separated string with the corresponding interest feature spf values.
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message,
            "name": The name of the project created,
            "user": The name of the user
        }
    """

    async def post(self):
        try:
            # validate the input arguments
            _validateArguments(self.request.arguments, [
                               'user', 'project', 'description', 'planning_grid_name', 'interest_features', 'target_values', 'spf_values'])
            # create the empty project folder
            _createProject(self, self.get_argument('project'))
            # update the projects parameters
            _updateParameters(self.folder_project + "input.dat", {'DESCRIPTION': self.get_argument(
                'description'), 'CREATEDATE': datetime.datetime.now().strftime("%d/%m/%y %H:%M:%S"), 'PLANNING_UNIT_NAME': self.get_argument('planning_grid_name')})
            # create the spec.dat file
            await _updateSpeciesFile(self, self.get_argument("interest_features"), self.get_argument("target_values"), self.get_argument("spf_values"), True)
            # create the pu.dat file
            await _createPuFile(self, self.get_argument('planning_grid_name'))
            # set the response
            self.send_response({'info': "Project '" + self.get_argument('project') + "' created",
                                'name': self.get_argument('project'), 'user': self.get_argument('user')})
        except MarxanServicesError as e:
            raise_error(self, e.args[0])
