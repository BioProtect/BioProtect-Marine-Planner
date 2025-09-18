# handlers/update_spec_file.py
import tornado.escape
from handlers.base_handler import BaseHandler
from services.service_error import ServicesError, raise_error
from classes.postgis_class import get_pg


class updateSpecFile(BaseHandler):
    """
    Updates project feature links in DB (no spec.dat).
    Expects form-data fields identical to the old endpoint:
      user, project, interest_features, target_values, spf_values
    """

    def initialize(self, pg, get_species_data, update_species):
        super().initialize()
        self.pg = pg
        self.get_species_data = get_species_data
        self.update_species = update_species

    async def post(self):
        try:
            user = self.get_argument("user")
            project_name = self.get_argument("project")
            interest_features = self.get_argument("interest_features")
            target_values = self.get_argument("target_values", None)
            spf_values = self.get_argument("spf_values", None)

            # parse lists
            feature_ids = [int(x)
                           for x in interest_features.split(",") if x.strip()]
            target_list = [x.strip()
                           for x in target_values.split(",")] if target_values else []
            spf_list = [x.strip()
                        for x in spf_values.split(",")] if spf_values else []

            # basic length check (non-fatal; we still link features)
            if (target_list and len(target_list) != len(feature_ids)) or (spf_list and len(spf_list) != len(feature_ids)):
                raise ServicesError(
                    "Lengths of interest_features, target_values, and spf_values must match.")

            # 1) resolve project_id (adjust the users table column if needed)
            row = await self.pg.execute(
                """
                select p.id as project_id
                from bioprotect.projects p
                join bioprotect.users u on u.id = p.user_id
                where u.user = %s and p.name = %s
                """,
                [user, project_name],
                return_format="Array"
            )
            if not row:
                raise ServicesError(
                    f"Project '{project_name}' for user '{user}' not found.")
            project_id = row[0]["project_id"]

            # 2) delete links not in submitted list (if list provided)
            if feature_ids:
                await self.pg.execute(
                    """
                    delete from bioprotect.project_features
                    where project_id = %s
                      and not (feature_unique_id = any(%s))
                    """,
                    [project_id, feature_ids]
                )
            else:
                # empty list means clear all features
                await self.pg.execute(
                    "delete from bioprotect.project_features where project_id = %s",
                    [project_id]
                )

            # 3) upsert new links
            for fid in feature_ids:
                await self.pg.execute(
                    """
                    insert into bioprotect.project_features(project_id, feature_unique_id)
                    values (%s, %s)
                    on conflict (project_id, feature_unique_id) do nothing
                    """,
                    [project_id, fid]
                )

            # 4) optional: update target/spf if columns exist
            #     (silently ignore if the columns aren’t there)
            if target_list or spf_list:
                for idx, fid in enumerate(feature_ids):
                    tv = float(target_list[idx]) if target_list else None
                    spf = float(spf_list[idx]) if spf_list else None
                    # build dynamic pieces only for provided values
                    sets = []
                    params = []
                    if tv is not None:
                        sets.append("target_value = %s")
                        params.append(tv)
                    if spf is not None:
                        sets.append("spf = %s")
                        params.append(spf)
                    if sets:
                        try:
                            await self.pg.execute(
                                f"""
                                update bioprotect.project_features
                                   set {", ".join(sets)}
                                 where project_id = %s and feature_unique_id = %s
                                """,
                                params + [project_id, fid]
                            )
                        except ServicesError:
                            # table doesn't have those columns; ignore gracefully
                            pass

            self.send_response(
                {"info": "Project features updated in database"})
        except ServicesError as e:
            raise_error(self, e.args[0])
        except Exception as e:
            raise_error(self, f"Unexpected error: {e}")
