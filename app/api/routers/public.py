from fastapi import APIRouter

from ...auth.service import search_claimable_teachers
from ...core.config import DB_ENGINE, DB_LOCK
from ...core.db import get_connection, query_all

router = APIRouter()

GROUP_SUBGROUPS_AGGREGATE_SQL = (
    """
    COALESCE(
        (
            SELECT string_agg(subgroup_value, ',' ORDER BY subgroup_value)
            FROM (
                SELECT DISTINCT upper(trim(s.subgroup)) AS subgroup_value
                FROM schedules s
                WHERE s.group_id = g.id
                  AND trim(coalesce(s.subgroup, '')) <> ''
            ) subgroup_values
        ),
        ''
    ) AS generated_subgroups
    """
    if DB_ENGINE == "postgres"
    else
    """
    COALESCE(
        (
            SELECT group_concat(subgroup_value, ',')
            FROM (
                SELECT DISTINCT upper(trim(s.subgroup)) AS subgroup_value
                FROM schedules s
                WHERE s.group_id = g.id
                  AND trim(coalesce(s.subgroup, '')) <> ''
                ORDER BY subgroup_value
            )
        ),
        ''
    ) AS generated_subgroups
    """
)


@router.get("/public/groups")
def public_groups():
    with DB_LOCK:
        with get_connection() as connection:
            return query_all(
                connection,
                """
                SELECT
                    g.id,
                    g.name,
                    g.student_count,
                    g.has_subgroups,
                    g.language,
                    g.programme,
                    g.specialty_code,
                    g.entry_year,
                    g.study_course,
                    CASE
                        WHEN EXISTS (
                            SELECT 1
                            FROM schedules s
                            WHERE s.group_id = g.id
                              AND trim(coalesce(s.subgroup, '')) <> ''
                        )
                        THEN 1
                        ELSE 0
                    END AS auto_has_subgroups,
                    {GROUP_SUBGROUPS_AGGREGATE_SQL}
                FROM groups g
                ORDER BY g.name, g.id
                """.format(GROUP_SUBGROUPS_AGGREGATE_SQL=GROUP_SUBGROUPS_AGGREGATE_SQL),
            )


@router.get("/public/teachers/claim-search")
def public_teachers_claim_search(q: str = ""):
    return search_claimable_teachers(q)
