

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

    @ staticmethod
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

    @ staticmethod
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
