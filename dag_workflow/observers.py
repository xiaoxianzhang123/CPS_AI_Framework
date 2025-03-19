import json

from dag_workflow.datamodel import (
    Event,
    ErrorEvent,
    ChangeEvent,
)
from dag_workflow.datamodel import WorkflowErrorEvent, NodeErrorEvent


class Observer:
    def on_status_change(self, event: Event):
        pass


class PrintObserver(Observer):
    def on_status_change(self, event: Event):
        ## 带有颜色的输出
        if isinstance(event, ChangeEvent):
            ## 红色输出:
            print(
                f"\033[1;32m[Observer]\033[0m{json.dumps(event.to_dict(), ensure_ascii=False)}"
            )
        elif isinstance(event, ErrorEvent):
            print(
                f"\033[1;31m[Observer]\033[0m{json.dumps(event.to_dict(), ensure_ascii=False)}"
            )
