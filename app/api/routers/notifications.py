from fastapi import APIRouter, Request

from ...notification_service import (
    delete_all_notifications,
    delete_notification,
    list_notifications,
    mark_all_notifications_as_read,
    mark_notification_as_read,
)

router = APIRouter()


@router.get("/notifications")
def notifications_get(request: Request):
    return list_notifications(request.headers)


@router.post("/notifications/read-all")
def notifications_read_all(request: Request):
    return mark_all_notifications_as_read(request.headers)


@router.delete("/notifications")
def notifications_delete_all(request: Request):
    return delete_all_notifications(request.headers)


@router.put("/notifications/{notification_id}/read")
def notifications_read_one(notification_id: int, request: Request):
    return mark_notification_as_read(request.headers, notification_id)


@router.delete("/notifications/{notification_id}")
def notifications_delete_one(notification_id: int, request: Request):
    return delete_notification(request.headers, notification_id)

