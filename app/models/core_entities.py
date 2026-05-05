from __future__ import annotations

from sqlalchemy import Integer, Text
from sqlalchemy.orm import Mapped, mapped_column

from ..core.orm import Base


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    email: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    password: Mapped[str] = mapped_column(Text, nullable=False)
    full_name: Mapped[str] = mapped_column(Text, nullable=False)
    role: Mapped[str] = mapped_column(Text, nullable=False)
    token: Mapped[str] = mapped_column(Text, nullable=False)
    avatar_data: Mapped[str | None] = mapped_column(Text)
    department: Mapped[str | None] = mapped_column(Text)
    programme: Mapped[str | None] = mapped_column(Text)
    group_id: Mapped[int | None] = mapped_column(Integer)
    group_name: Mapped[str | None] = mapped_column(Text)
    subgroup: Mapped[str | None] = mapped_column(Text)


class Student(Base):
    __tablename__ = "students"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    email: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    password: Mapped[str] = mapped_column(Text, nullable=False)
    token: Mapped[str] = mapped_column(Text, nullable=False)
    avatar_data: Mapped[str | None] = mapped_column(Text)
    department: Mapped[str | None] = mapped_column(Text)
    programme: Mapped[str | None] = mapped_column(Text)
    group_id: Mapped[int | None] = mapped_column(Integer)
    group_name: Mapped[str | None] = mapped_column(Text)
    subgroup: Mapped[str | None] = mapped_column(Text)
    language: Mapped[str | None] = mapped_column(Text, default="ru")


class Course(Base):
    __tablename__ = "courses"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    code: Mapped[str] = mapped_column(Text, nullable=False)
    credits: Mapped[int | None] = mapped_column(Integer)
    hours: Mapped[int | None] = mapped_column(Integer)
    description: Mapped[str | None] = mapped_column(Text)
    year: Mapped[int | None] = mapped_column(Integer)
    semester: Mapped[int | None] = mapped_column(Integer)
    department: Mapped[str | None] = mapped_column(Text)
    instructor_id: Mapped[int | None] = mapped_column(Integer)
    instructor_name: Mapped[str | None] = mapped_column(Text)
    programme: Mapped[str | None] = mapped_column(Text)
    module_type: Mapped[str | None] = mapped_column(Text)
    module_name: Mapped[str | None] = mapped_column(Text)
    cycle: Mapped[str | None] = mapped_column(Text)
    component: Mapped[str | None] = mapped_column(Text)
    language: Mapped[str | None] = mapped_column(Text)
    academic_year: Mapped[str | None] = mapped_column(Text)
    entry_year: Mapped[str | None] = mapped_column(Text)
    requires_computers: Mapped[int | None] = mapped_column(Integer, default=0)


class CourseComponent(Base):
    __tablename__ = "course_components"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    course_id: Mapped[int] = mapped_column(Integer, nullable=False)
    course_code: Mapped[str] = mapped_column(Text, nullable=False)
    course_name: Mapped[str] = mapped_column(Text, nullable=False)
    programme: Mapped[str | None] = mapped_column(Text)
    study_year: Mapped[int | None] = mapped_column(Integer)
    academic_period: Mapped[int | None] = mapped_column(Integer)
    semester: Mapped[int | None] = mapped_column(Integer)
    lesson_type: Mapped[str] = mapped_column(Text, nullable=False)
    hours: Mapped[int] = mapped_column(Integer, nullable=False)
    weekly_classes: Mapped[int] = mapped_column(Integer, nullable=False)
    requires_computers: Mapped[int | None] = mapped_column(Integer, default=0)
    teacher_id: Mapped[int | None] = mapped_column(Integer)
    teacher_name: Mapped[str | None] = mapped_column(Text)


class IupEntry(Base):
    __tablename__ = "iup_entries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    file_name: Mapped[str] = mapped_column(Text, nullable=False)
    group_name: Mapped[str | None] = mapped_column(Text)
    programme: Mapped[str | None] = mapped_column(Text)
    study_course: Mapped[int | None] = mapped_column(Integer)
    language: Mapped[str | None] = mapped_column(Text)
    academic_year: Mapped[str | None] = mapped_column(Text)
    academic_period: Mapped[int | None] = mapped_column(Integer)
    semester: Mapped[int | None] = mapped_column(Integer)
    component: Mapped[str | None] = mapped_column(Text)
    course_code: Mapped[str] = mapped_column(Text, nullable=False)
    course_name: Mapped[str] = mapped_column(Text, nullable=False)
    credits: Mapped[int | None] = mapped_column(Integer)
    lesson_type: Mapped[str] = mapped_column(Text, nullable=False)
    teacher_id: Mapped[int | None] = mapped_column(Integer)
    teacher_name: Mapped[str | None] = mapped_column(Text)
    hours: Mapped[int | None] = mapped_column(Integer)


class Teacher(Base):
    __tablename__ = "teachers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    email: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    password: Mapped[str | None] = mapped_column(Text)
    token: Mapped[str | None] = mapped_column(Text)
    claim_code: Mapped[str | None] = mapped_column(Text)
    claim_code_expires_at: Mapped[str | None] = mapped_column(Text)
    claim_requested_at: Mapped[str | None] = mapped_column(Text)
    avatar_data: Mapped[str | None] = mapped_column(Text)
    phone: Mapped[str | None] = mapped_column(Text)
    subject_taught: Mapped[str | None] = mapped_column(Text)
    weekly_hours_limit: Mapped[int | None] = mapped_column(Integer)
    name_normalized: Mapped[str | None] = mapped_column(Text)
    name_signature: Mapped[str | None] = mapped_column(Text)
    teaching_languages: Mapped[str | None] = mapped_column(Text, default="ru,kk")


class Room(Base):
    __tablename__ = "rooms"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    number: Mapped[str] = mapped_column(Text, nullable=False)
    capacity: Mapped[int | None] = mapped_column(Integer)
    type: Mapped[str | None] = mapped_column(Text)
    equipment: Mapped[str | None] = mapped_column(Text)
    programme: Mapped[str | None] = mapped_column(Text)
    available: Mapped[int | None] = mapped_column(Integer, default=1)
    computer_count: Mapped[int | None] = mapped_column(Integer, default=0)


class Group(Base):
    __tablename__ = "groups"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    student_count: Mapped[int] = mapped_column(Integer, nullable=False)
    has_subgroups: Mapped[int | None] = mapped_column(Integer, default=0)
    language: Mapped[str | None] = mapped_column(Text, default="ru")
    programme: Mapped[str | None] = mapped_column(Text)
    specialty_code: Mapped[str | None] = mapped_column(Text)
    entry_year: Mapped[int | None] = mapped_column(Integer)
    study_course: Mapped[int | None] = mapped_column(Integer)


class Section(Base):
    __tablename__ = "sections"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    course_id: Mapped[int] = mapped_column(Integer, nullable=False)
    course_name: Mapped[str] = mapped_column(Text, nullable=False)
    group_id: Mapped[int | None] = mapped_column(Integer)
    group_name: Mapped[str | None] = mapped_column(Text)
    classes_count: Mapped[int] = mapped_column(Integer, nullable=False)
    lesson_type: Mapped[str | None] = mapped_column(Text, default="lecture")
    subgroup_mode: Mapped[str | None] = mapped_column(Text, default="auto")
    subgroup_count: Mapped[int | None] = mapped_column(Integer, default=1)
    requires_computers: Mapped[int | None] = mapped_column(Integer, default=0)
    teacher_id: Mapped[int | None] = mapped_column(Integer)
    teacher_name: Mapped[str | None] = mapped_column(Text)
    iup_entry_id: Mapped[int | None] = mapped_column(Integer)
    source: Mapped[str | None] = mapped_column(Text, default="manual")
    match_method: Mapped[str | None] = mapped_column(Text)


class Schedule(Base):
    __tablename__ = "schedules"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    section_id: Mapped[int | None] = mapped_column(Integer)
    course_id: Mapped[int | None] = mapped_column(Integer)
    course_name: Mapped[str] = mapped_column(Text, nullable=False)
    teacher_id: Mapped[int | None] = mapped_column(Integer)
    teacher_name: Mapped[str] = mapped_column(Text, nullable=False)
    room_id: Mapped[int | None] = mapped_column(Integer)
    room_number: Mapped[str] = mapped_column(Text, nullable=False)
    group_id: Mapped[int | None] = mapped_column(Integer)
    group_name: Mapped[str | None] = mapped_column(Text)
    subgroup: Mapped[str | None] = mapped_column(Text)
    day: Mapped[str] = mapped_column(Text, nullable=False)
    start_hour: Mapped[int] = mapped_column(Integer, nullable=False)
    semester: Mapped[int | None] = mapped_column(Integer)
    year: Mapped[int | None] = mapped_column(Integer)
    algorithm: Mapped[str | None] = mapped_column(Text)
    room_programme: Mapped[str | None] = mapped_column(Text)
    room_programme_mismatch: Mapped[int | None] = mapped_column(Integer, default=0)
    relocated_from_room_number: Mapped[str | None] = mapped_column(Text)
    relocation_reason: Mapped[str | None] = mapped_column(Text)


class RoomBlock(Base):
    __tablename__ = "room_blocks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    room_id: Mapped[int] = mapped_column(Integer, nullable=False)
    day: Mapped[str] = mapped_column(Text, nullable=False)
    start_hour: Mapped[int] = mapped_column(Integer, nullable=False)
    end_hour: Mapped[int | None] = mapped_column(Integer)
    semester: Mapped[int | None] = mapped_column(Integer)
    year: Mapped[int | None] = mapped_column(Integer)
    reason: Mapped[str | None] = mapped_column(Text)


class TeacherPreferenceRequest(Base):
    __tablename__ = "teacher_preference_requests"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    teacher_id: Mapped[int] = mapped_column(Integer, nullable=False)
    teacher_name: Mapped[str] = mapped_column(Text, nullable=False)
    preferred_day: Mapped[str] = mapped_column(Text, nullable=False)
    preferred_hour: Mapped[int] = mapped_column(Integer, nullable=False)
    note: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="pending")
    admin_comment: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[str] = mapped_column(Text, nullable=False)
    updated_at: Mapped[str] = mapped_column(Text, nullable=False)


class Notification(Base):
    __tablename__ = "notifications"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    recipient_role: Mapped[str] = mapped_column(Text, nullable=False)
    recipient_id: Mapped[int] = mapped_column(Integer, nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    metadata_json: Mapped[str | None] = mapped_column("metadata", Text)
    notification_type: Mapped[str] = mapped_column(Text, nullable=False)
    is_read: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[str] = mapped_column(Text, nullable=False)
    read_at: Mapped[str | None] = mapped_column(Text)
