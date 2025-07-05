import pandas as pd
from sqlalchemy import create_engine, text
from classes.db_config import DBConfig

# Load DB config
config = DBConfig()
db_url = (
    f"postgresql://{config.DATABASE_USER}:"
    f"{config.DATABASE_PASSWORD}@"
    f"{config.DATABASE_HOST}/{config.DATABASE_NAME}"
)
engine = create_engine(db_url)

with engine.begin() as conn:  # ensures transaction
    result = conn.execute(text("""
        SELECT DISTINCT project_area, resolution
        FROM bioprotect.h3_cells
        ORDER BY project_area, resolution;
    """))
    rows = result.fetchall()

    for project_area, resolution in rows:
        scale_safe = project_area.replace(" ", "_").replace("-", "_").lower()
        view_name = f"v_h3_{scale_safe}_res{resolution}"

        print(f"🧱 Creating view: {view_name}")

        # 1. Create the SQL view
        conn.execute(text(f"""
            CREATE OR REPLACE VIEW bioprotect.{view_name} AS
            SELECT * FROM bioprotect.h3_cells
            WHERE project_area = :area AND resolution = :res;
        """), {"area": project_area, "res": resolution})

        print(f"📥 Inserting metadata for: {project_area} (res {resolution})")

        # 2. Insert metadata entry
        conn.execute(text(f"""
            INSERT INTO bioprotect.metadata_planning_units (
                feature_class_name,
                alias,
                description,
                domain,
                _area,
                envelope,
                creation_date,
                source,
                created_by,
                tilesetid,
                planning_unit_count
            )
            SELECT
                :view_name,
                :alias,
                :description,
                'marine',
                0,
                ST_Multi(ST_Envelope(ST_Collect(geometry))),
                NOW(),
                'h3_cells',
                'system',
                :view_name,
                COUNT(*)
            FROM bioprotect.{view_name}
            ON CONFLICT (feature_class_name) DO NOTHING;
        """), {
            "view_name": view_name,
            "alias": f"{project_area} (Res {resolution})",
            "description": f"Auto-generated H3 grid at res {resolution} for {project_area}"
        })

    print("✅ Done creating views and metadata entries.")
