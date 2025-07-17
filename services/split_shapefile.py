import geopandas as gpd
import os


def split_shapefile_by_name(shpefile_path, output_dir):
    """
    Splits a shapefile into multiple shapefiles based on the 'name' field.
    Each output shapefile is named after the corresponding 'name' value.
    """
    gdf = gpd.read_file(shpefile_path)

    os.makedirs(output_dir, exist_ok=True)

    # Loop and save each feature using its `name` field
    for idx, row in gdf.iterrows():
        print('row: ', row)
        area_name = str(row["Name"]).replace(" ", "_").lower()  # sanitize name
        output_path = os.path.join(output_dir, f"{area_name}.shp")

        single_gdf = gpd.GeoDataFrame([row], crs=gdf.crs)
        single_gdf.to_file(output_path)
        print(f"✅ Saved: {output_path}")


if __name__ == "__main__":
    split_shapefile_by_name(
        "./data/LocalDS_Boundaries/LocalDS_Boundaries.shp", "../data/")
