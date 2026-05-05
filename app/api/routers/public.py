from fastapi import APIRouter
from sqlalchemy import select

from ...auth.service import search_claimable_teachers
from ...core.config import DB_LOCK
from ...core.orm import SessionLocal
from ...models import Group, Schedule

router = APIRouter()


@router.get("/public/groups")
def public_groups():
    with DB_LOCK:
        with SessionLocal() as session:
            groups = session.scalars(select(Group).order_by(Group.name, Group.id)).all()
            subgroup_rows = session.execute(
                select(Schedule.group_id, Schedule.subgroup)
                .where(Schedule.subgroup.is_not(None), Schedule.subgroup != "")
            ).all()

    subgroups_by_group = {}
    for group_id, subgroup in subgroup_rows:
        value = str(subgroup or "").strip().upper()
        if value:
            subgroups_by_group.setdefault(group_id, set()).add(value)

    return [
        {
            "id": group.id,
            "name": group.name,
            "student_count": group.student_count,
            "has_subgroups": group.has_subgroups,
            "language": group.language,
            "programme": group.programme,
            "specialty_code": group.specialty_code,
            "entry_year": group.entry_year,
            "study_course": group.study_course,
            "auto_has_subgroups": 1 if subgroups_by_group.get(group.id) else 0,
            "generated_subgroups": ",".join(sorted(subgroups_by_group.get(group.id, set()))),
        }
        for group in groups
    ]


@router.get("/public/teachers/claim-search")
def public_teachers_claim_search(q: str = ""):
    return search_claimable_teachers(q)
