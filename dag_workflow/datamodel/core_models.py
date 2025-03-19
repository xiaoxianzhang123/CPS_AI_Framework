import threading
import uuid

from enum import Enum
from typing import Dict, List, Callable


class NodeStatus(Enum):
    PENDING = 0
    RUNNING = 1
    SUCCESS = 2
    FAILED = 3
    SKIPPED = 4
    CANCELED = 5


class WorkflowStatus(Enum):
    PENDING = 0
    RUNNING = 1
    SUCCESS = 2
    FAILED = 3


class SingleRunContext:
    def __init__(self, input_data):
        self.input_data = input_data
        self.run_id = uuid.uuid4()
        self.node_status_dict: Dict[str:NodeStatus] = {}
        self.results: Dict[str:Dict] = {}
        self.workflow_status = WorkflowStatus.PENDING
        self.lock = threading.Lock()
        self.exception_message_list: List[ContextException] = []


class DAGNode:
    def __init__(
        self,
        node_id: str,
        node_task: Callable,
        node_dependencies: List[str] = [],
        node_condition: Callable = None,
    ):
        self.node_id = node_id
        self.task = node_task
        self.dependencies = node_dependencies
        self.condition = node_condition


class ContextException:
    def __init__(self, location: str, message: str):
        self.location = location
        self.message = message
