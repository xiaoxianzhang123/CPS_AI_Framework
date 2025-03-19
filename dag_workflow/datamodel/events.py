from enum import Enum
from abc import ABC, abstractmethod
from typing import Callable, List

from .core_models import SingleRunContext, NodeStatus, WorkflowStatus


class EventLevel(Enum):
    DEBUG = 0
    INFO = 1
    WARNING = 2
    ERROR = 3
    CRITICAL = 4


class Event(ABC):
    def __init__(self):
        self.level = EventLevel.INFO

    @abstractmethod
    def to_dict():
        pass


class ChangeEvent(Event):
    def __init__(self):
        super().__init__()


class NodeStatusChangeEvent(ChangeEvent):
    def __init__(
        self,
        context: SingleRunContext,
        node_id: str,
        formal_status: NodeStatus,
        after_status: NodeStatus,
        level: EventLevel = EventLevel.INFO,
    ):
        super().__init__()
        self.context = context
        self.node_id = node_id
        self.formal_status = formal_status
        self.after_status = after_status
        self.level = level

    def to_dict(self):
        return {
            "level": self.level.name,
            "run_id": str(self.context.run_id),
            "node_id": self.node_id,
            "formal_status": self.formal_status.name,
            "after_status": self.after_status.name,
        }


class WorkflowStatusChangeEvent(ChangeEvent):
    def __init__(
        self,
        context: SingleRunContext,
        formal_status: WorkflowStatus,
        after_status: WorkflowStatus,
        level: EventLevel = EventLevel.INFO,
    ):
        super().__init__()
        self.context = context
        self.formal_status = formal_status
        self.after_status = after_status

    def to_dict(self):
        return {
            "level": self.level.name,
            "run_id": str(self.context.run_id),
            "formal_status": self.formal_status.name,
            "after_status": self.after_status.name,
        }


class ErrorEvent(Event):
    def __init__(self):
        self.level = EventLevel.ERROR


class NodeErrorEvent(ErrorEvent):
    def __init__(self, location: str, message: str):
        super().__init__()
        self.level = EventLevel.ERROR
        self.location = location
        self.message = message

    def to_dict(
        self,
    ):
        return {
            "level": self.level.name,
            "location": self.location,
            "message": self.message,
        }


class WorkflowErrorEvent(ErrorEvent):
    def __init__(self, context: SingleRunContext, location: str, message: str):
        super().__init__()
        self.level = EventLevel.ERROR
        self.location = location
        self.message = message
        self.context = context

    def to_dict(
        self,
    ):
        return {
            "level": self.level.name,
            "run_id": str(self.context.run_id),
            "location": self.location,
            "message": self.message,
        }


class UnexpectedErrorEvent(ErrorEvent):
    def __init__(self, location: str, fail_message: str):
        super().__init__()
        self.level = EventLevel.CRITICAL
        self.location = location
        self.fail_message = fail_message

    def to_dict(
        self,
    ):
        return {
            "level": self.level.name,
            "location": self.location,
            "message": self.fail_message,
        }
