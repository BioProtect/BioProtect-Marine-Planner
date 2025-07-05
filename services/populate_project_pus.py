import psycopg2
from psycopg2.extras import execute_values
from classes.db_config import DBConfig
from sqlalchemy import create_engine
from geoalchemy2 import Geometry
from classes.db_config import DBConfig
from datetime import datetime
import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine, text
import pandas as pd
from classes.db_config import DBConfig

# Load DB config and connect
config = DBConfig()
db_url = (
    f"postgresql://{config.DATABASE_USER}:"
    f"{config.DATABASE_PASSWORD}@"
    f"{config.DATABASE_HOST}/{config.DATABASE_NAME}"
)
engine = create_engine(db_url)


def get_scale_level(resolution):
    if resolution <= 6:
        return "basin"
    elif resolution == 7:
        return "regional"
    else:
        return "local"


def run():

    # ✅ Connect once for project list
    with engine.connect() as conn:
        projects_df = pd.read_sql("""
            SELECT p.id AS project_id, mpu.alias AS project_area
            FROM bioprotect.projects p
            JOIN bioprotect.metadata_planning_units mpu
              ON p.planning_unit_id = mpu.unique_id
        """, conn)

    for resolution in range(8,  9):
        scale_level = get_scale_level(resolution)
        print(f"\n⏳ Processing resolution {resolution} ({scale_level})")

        for _, row in projects_df.iterrows():
            project_id = row["project_id"]
            project_area = row["project_area"]

            # ✅ Open fresh connection per project loop
            with engine.begin() as conn:
                h3_df = pd.read_sql(text("""
                    SELECT h3_index, cost, status
                    FROM bioprotect.h3_cells
                    WHERE resolution = :resolution
                      AND scale_level = :scale_level
                      AND project_area = :project_area
                """), conn, params={
                    "resolution": resolution,
                    "scale_level": scale_level,
                    "project_area": project_area
                })

                if h3_df.empty:
                    print(
                        f"⚠️ No H3 cells for project {project_id} @ {project_area} - res {resolution}")
                    continue

                existing = pd.read_sql(text("""
                    SELECT h3_index
                    FROM bioprotect.project_pus
                    WHERE project_id = :project_id
                """), conn, params={"project_id": project_id})
                existing_set = set(existing["h3_index"].values)

                insert_df = h3_df[~h3_df["h3_index"].isin(existing_set)]
                if insert_df.empty:
                    print(
                        f"✅ Already populated: project {project_id} ({project_area}) @ res {resolution}")
                    continue

                insert_df["project_id"] = project_id
                insert_df = insert_df[["project_id",
                                       "h3_index", "cost", "status"]]

                insert_df.to_sql(
                    "project_pus",
                    con=conn,
                    schema="bioprotect",
                    if_exists="append",
                    index=False,
                    method="multi"
                )

                print(
                    f"✅ Inserted {len(insert_df)} rows for project {project_id} ({project_area}) @ res {resolution}")

    print("\n🎉 All projects processed.")


if __name__ == "__main__":
    run()
