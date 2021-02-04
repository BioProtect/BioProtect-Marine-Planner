import json
from os import pardir, path, sep
from string import punctuation

import geopandas as gpd
import pandas as pd
from dotenv import dotenv_values

dir_path = path.dirname(path.realpath(__file__))
print('dir_path: ', dir_path)
data_path = "/".join(dir_path.split('/')[:-1])
print('data_path: ', data_path)


def sensitivity_text_to_int(text):
    if text == "Low":
        return 1
    elif text == "Medium":
        return 2
    elif text == "High":
        return 3
    else:
        return 0


def replace_chars(text):
    out = text.translate(str.maketrans("", "", punctuation))
    out = out.replace("  ", "_").replace(" ", "_").lower()
    return out


def format_eunis(text):
    if text:
        return re.split('[ :]', text)[0]
    return text


def add_unique_eunis_codes(shape_file):
    """
    open the all habitats shape file.
    Get a list of unique eunis codes from the shapefile and create a dict from it.
    Add a new field to the shapefile referencing the eunis codes as an int
    return updated shapefile
    Args:
        shape_file (path to file): path to the shapefile to read in

    Returns:
        tuple: returns updated shapefile with new int unique eunis codes, and dict of eunis codes
    """
    all_habitats = gpd.read_file(shape_file)
    unique_eunis_codes = all_habitats.EUNIScombD.unique()
    # unique_eunis_codes = unique_eunis_codes[(unique_eunis_codes != 'Na')]
    unique_eunis_codes = unique_eunis_codes[np.where(
        (unique_eunis_codes != None) & (unique_eunis_codes != 'Na'))]
    # unique_eunis_codes = unique_eunis_codes[unique_eunis_codes is not None]
    eunis_dict = {format_eunis(val): idx for idx,
                  val in enumerate(unique_eunis_codes)}
    return eunis_dict


def setup_sensitivity_matrix(maresa_file, jncc_file):
    """ Load sensitivity information
        The second component of the cumulative effects assessment (pressures being the first) is the sensitivity of each ecosystem component to each pressure

        MarESA sensitivity assessments are completed through the MarLIN Memorandum of Agreement, which is funded by the Joint Nature Conservation Committee, Natural England,
        Defra, Marine Scotland, Scottish Natural Heritage and NRW. MarLIN is run by the Marine Biological Association, who undertake the sensitivity assessments.


        MarESA Sensitivity Info
    """
    eco_sys = pd.read_excel((data_path + sep + maresa_file),
                            sheet_name=2, engine='openpyxl')
    mar_esa = eco_sys.dropna(subset=['EUNIS_Code'])
    jncc = pd.read_csv((data_path + sep + jncc_file),
                       encoding='unicode_escape')

    # remove any items where JNCC_Pressure contains 'local' str shrink dataframe down to 3 needed columns
    # remove any rows where MarESA_Pressure is NAN remove items that have duplicated eunis_codes and maresa_pressures reset the indices
    jncc = jncc[~jncc.JNCC_Pressure.str.contains('local', regex=False)]
    jncc = jncc[["EUNIS_Code_Assessment", "MarESA_Pressure", "Sensitivity"]]
    jncc = jncc[~jncc.MarESA_Pressure.isna()]
    jncc = jncc.drop_duplicates(
        subset=["EUNIS_Code_Assessment", "MarESA_Pressure"])
    jncc.rename(columns={'EUNIS_Code_Assessment': 'EUNIS_Code',
                         'MarESA_Pressure': 'Pressure'}, inplace=True)
    jncc = jncc.reset_index(drop=True)

    # Make composite Resistance/Resilience/Sensitivity Matrices:
    sens_mat = mar_esa[["EUNIS_Code", "Pressure", "Sensitivity"]]
    sens_mat = pd.concat([sens_mat, jncc])
    sens_mat['Pressure'] = sens_mat['Pressure'].map(replace_chars)
    sens_mat = sens_mat.pivot(index="EUNIS_Code",
                              columns="Pressure", values="Sensitivity")
    sens_mat = sens_mat.applymap(sensitivity_text_to_int)
    return sens_mat


def setup_environment():
    config = dotenv_values('.env.local')
    try:
        with open('./data/eunis_codes.json') as json_file:
            eunis_dict = json.load(json_file)
    except FileNotFoundError as err:
        eunis_dict = add_unique_eunis_codes(data_path +
                                            sep +
                                            config["input_all_habitats"])
        with open('./data/eunis_codes.json', 'w') as fp:
            json.dump(eunis_dict, fp)
    sens_mat = setup_sensitivity_matrix(maresa_file=config["maresa"],
                                        jncc_file=config["jncc"])
    config["sensmat"] = sens_mat
    config["eunis"] = eunis_dict
    config["jose_crs_str"] = "+proj=aea +lat_1=43 +lat_2=62 +lat_0=30 +lon_0=-30 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs +ellps=WGS84 +towgs84=0,0,0"
    config["wgs84_str"] = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
    config["ogr2ogr_executable"] = "/usr/bin/ogr2ogr"

    return config
