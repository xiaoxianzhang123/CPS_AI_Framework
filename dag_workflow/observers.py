import json

from .datamodel import (
    Event,
    ErrorEvent,
    ChangeEvent,
)
from .datamodel import WorkflowErrorEvent, NodeErrorEvent
from .console import Panel, console


class Observer:
    def on_status_change(self, event: Event):
        pass


class PrintObserver(Observer):
    def on_status_change(self, event: Event):
        if isinstance(event, ChangeEvent):
            print(
                f"\033[1;32m[Observer]\033[0m{json.dumps(event.to_dict(), ensure_ascii=False)}"
            )
        elif isinstance(event, ErrorEvent):
            print(
                f"\033[1;31m[Observer]\033[0m{json.dumps(event.to_dict(), ensure_ascii=False)}"
            )
            if isinstance(event, WorkflowErrorEvent):
                console.print(
                    Panel(
                        event.message,
                        title=f"WorkflowError-[bold]{event.location} [/bold]",
                        style="red",
                    )
                )
            elif isinstance(event, NodeErrorEvent):
                console.print(
                    Panel(
                        event.message,
                        title=f"NodeError-[bold]{event.location} [/bold]",
                        style="red",
                    )
                )
