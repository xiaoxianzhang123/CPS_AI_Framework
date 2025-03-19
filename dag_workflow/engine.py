import concurrent.futures
import threading
import logging
import time
import sys
import traceback
import os
import json

from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from typing import List, Dict
from contextlib import contextmanager


from .datamodel import NodeStatus, WorkflowStatus
from .datamodel import SingleRunContext
from .datamodel import DAGNode
from .datamodel import Event, NodeStatusChangeEvent, WorkflowStatusChangeEvent
from .datamodel import NodeErrorEvent, WorkflowErrorEvent, UnexpectedErrorEvent
from .datamodel import ContextException
from .observers import Observer, PrintObserver

log_handler = logging.StreamHandler()
log_handler.setLevel(logging.WARNING)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
log_handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(log_handler)


class DAGEngine:
    def __init__(self, print: bool = True):
        self.node_list: List[DAGNode] = []
        self.workflow_queue = Queue()
        self.workflow_executor = ThreadPoolExecutor(max_workers=2)
        self.task_executor = ThreadPoolExecutor(max_workers=4)

        self.dispatcher = threading.Thread(target=self._dispatch_works, daemon=True)
        self.dispatcher.start()
        self.running_workflow: Dict[concurrent.futures.Future, SingleRunContext] = {}
        self.monitor = threading.Thread(target=self._monitor_works, daemon=True)
        self.monitor.start()

        self.observers: List[Observer] = []
        if print:
            self.add_observer(PrintObserver())
        logger.info("DAGEngine is ready")

    def submit_work(self, input_data):
        context = SingleRunContext(input_data=input_data)
        context.node_status_dict = {
            node.node_id: NodeStatus.PENDING for node in self.node_list
        }
        self.workflow_queue.put(context)
        logger.info(f"got workflow {context.run_id}")
        return context.run_id

    def _dispatch_works(self):
        while True:
            workflow_context = self.workflow_queue.get()
            logger.info(f"dispatch workflow {workflow_context.run_id}")
            future = self.workflow_executor.submit(
                self._run_single_workflow, workflow_context
            )
            self.running_workflow[future] = workflow_context

    def _monitor_works(self):
        logger.info("monitoring running workflow")
        while True:
            if self.running_workflow == {}:
                time.sleep(0.5)
                continue
            done, _ = concurrent.futures.wait(
                self.running_workflow.keys(),
                return_when=concurrent.futures.FIRST_COMPLETED,
            )

            for done_future in done:
                if done_future.exception():
                    logger.error(
                        f"workflow {self.running_workflow[done_future]} failed"
                    )
                    # 出现了未预料到的错误
                    failed_context = self.running_workflow[done_future]
                    unexpected_error_event = UnexpectedErrorEvent(
                        location=str(self.running_workflow[done_future].run_id),
                        fail_message=done_future.exception(),
                    )
                    self._notify_observers(unexpected_error_event)
                    self._change_workflow_status(failed_context, WorkflowStatus.FAILED)
                    logger.error(
                        f"final context: {failed_context},{failed_context.exception_message_list}"
                    )
                    self.running_workflow.pop(done_future)
                else:
                    logger.info(
                        f"workflow {self.running_workflow[done_future].run_id} done"
                    )
                    filename = (
                        f"result-{self.running_workflow[done_future].run_id}.json"
                    )
                    filename_temp = filename + ".temp"
                    with open(
                        filename_temp,
                        "w",
                    ) as f:
                        f.write(
                            json.dumps(
                                {
                                    "results": (2 * "\n" + 100 * "=" + 2 * "\n").join(
                                        [
                                            str({k: v})
                                            for k, v in self.running_workflow[
                                                done_future
                                            ].results.items()
                                        ]
                                    ),
                                    "exception_list": str(
                                        self.running_workflow[
                                            done_future
                                        ].exception_message_list
                                    ),
                                },
                                ensure_ascii=False,
                                indent=2,
                            )
                        )
                    os.rename(filename_temp, filename)
                    self.running_workflow.pop(done_future)
                    pass

    def get_result(self, run_id: str):
        # 等待runid的结果:检查本地文件是否存在,若不存在,则等待
        filename = f"result-{run_id}.json"
        while True:
            if os.path.exists(filename):
                with open(filename, "r") as f:
                    try:
                        return json.load(f)
                    except:
                        print("ERROR!!!")
                        return f.read()
                break
            else:
                time.sleep(0.5)

    @contextmanager
    def _change_context_lock(self, context):
        context.lock.acquire()
        try:
            yield
        finally:
            context.lock.release()

    def _change_node_status(
        self, context: SingleRunContext, node_id, status: NodeStatus
    ):
        formal_status = context.node_status_dict[node_id]
        with self._change_context_lock(context):
            context.node_status_dict[node_id] = status

        change_event = NodeStatusChangeEvent(
            context, node_id, formal_status, after_status=status
        )
        self._notify_observers(change_event)

    def _change_workflow_status(
        self, context: SingleRunContext, status: WorkflowStatus
    ):
        formal_status = context.workflow_status
        with self._change_context_lock(context):
            context.workflow_status = status
        change_event = WorkflowStatusChangeEvent(
            context, formal_status, after_status=status
        )
        self._notify_observers(change_event)

    def _notify_observers(self, event: Event):
        for observer in self.observers:
            observer.on_status_change(event)

    def _should_execute(self, context: SingleRunContext, node: DAGNode) -> bool:
        if node.condition and node.condition(context.results):
            return True
        elif node.condition is None:
            return True
        else:
            return False

    def _run_single_workflow(self, context: SingleRunContext):
        logger.info(f"start workflow {context.run_id}")
        try:
            self._change_workflow_status(context, WorkflowStatus.RUNNING)
            futures: Dict[concurrent.futures.Future, DAGNode] = {}
            node_exception = False

            while not node_exception:
                pending_nodes = [
                    node
                    for node in self.node_list
                    if context.node_status_dict[node.node_id] == NodeStatus.PENDING
                    and all(
                        context.node_status_dict[dep] == NodeStatus.SUCCESS
                        for dep in node.dependencies
                    )
                ]

                if pending_nodes == [] and futures == {}:
                    self._change_workflow_status(context, WorkflowStatus.SUCCESS)
                    break

                for node in pending_nodes:
                    if not self._should_execute(context, node):
                        self._change_node_status(
                            context, node.node_id, NodeStatus.SKIPPED
                        )
                        continue

                    future = self.task_executor.submit(node.task, context)
                    futures[future] = node
                    self._change_node_status(context, node.node_id, NodeStatus.RUNNING)

                done, _ = concurrent.futures.wait(
                    futures, return_when=concurrent.futures.FIRST_COMPLETED
                )

                for done_future in done:
                    if done_future.exception():
                        exception = done_future.exception()
                        exc_type, exc_value, exc_traceback = (
                            type(exception),
                            exception,
                            exception.__traceback__,
                        )
                        traceback_str = "".join(
                            traceback.format_exception(
                                exc_type, exc_value, exc_traceback
                            )
                        )

                        self._handle_node_exception(
                            failed_future=done_future,
                            fail_message=traceback_str,
                            context=context,
                            futures=futures,
                        )
                        node_exception = True

                    else:
                        with self._change_context_lock(context):
                            context.results[futures[done_future].node_id] = (
                                done_future.result()
                            )

                        self._change_node_status(
                            context, futures[done_future].node_id, NodeStatus.SUCCESS
                        )
                        futures.pop(done_future)

        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback_str = "".join(
                traceback.format_exception(exc_type, exc_value, exc_traceback)
            )
            logger.error(
                f"workflow {context.run_id} exception {traceback_str},locals:{locals()}"
            )
            self._handle_workflow_exception(traceback_str, context)

    def _handle_node_exception(
        self,
        failed_future: concurrent.futures.Future,
        fail_message,
        context: SingleRunContext,
        futures: Dict[concurrent.futures.Future, DAGNode],
    ):
        """node任务出错时的错误处理逻辑

        0. 通知观察者，nodeerror
        1. 尝试关闭正在running的task线程，pop掉(关不掉？寄，等自己结束吧)
        2. 把所有pending设置为canceled
        3. 当前失败的设置为failed，附加失败信息,添加context.exception记录,pop掉
        4. workflow设置为failed
        """
        failed_node_id = futures[failed_future].node_id
        logger.debug(f"node {failed_node_id} failed with message {fail_message}")

        node_error_event = NodeErrorEvent(location=failed_node_id, message=fail_message)
        self._notify_observers(event=node_error_event)

        self._cancel_pending_tasks(context=context, futures=futures)

        for node in self.node_list:
            if context.node_status_dict[node.node_id] == NodeStatus.PENDING:
                self._change_node_status(context, node.node_id, NodeStatus.CANCELED)

        self._change_node_status(
            context, futures[failed_future].node_id, NodeStatus.FAILED
        )
        context.exception_message_list.append(
            ContextException(
                location=futures[failed_future].node_id, message=fail_message
            )
        )
        futures.pop(failed_future)

        self._change_workflow_status(context, WorkflowStatus.FAILED)

    def _cancel_pending_tasks(
        self,
        context: SingleRunContext,
        futures: Dict[concurrent.futures.Future, DAGNode],
    ):
        for future, node in futures.items():
            if future.cancel():
                self._change_node_status(context, node.node_id, NodeStatus.CANCELED)
                futures.pop(future)

    def _handle_workflow_exception(
        self, failed_message: str, context: SingleRunContext
    ):
        """
        workflow出错处理逻辑

        0. 通知观察者，workflow运行时错误
        1. context中添加错误信息

        这段是有问题的。问题在于，如果不协作式关闭线程，那么线程的运行情况就处于未知状态，
        并且观察者也不会知道线程的运行情况，因为workflow线程已经寄了.但是真的能协作式关闭吗?
        """
        error_event = WorkflowErrorEvent(
            context, location="_run_single_workflow", message=failed_message
        )
        self._notify_observers(error_event)
        self._change_workflow_status(context, WorkflowStatus.FAILED)

        with self._change_context_lock(context):
            context.exception_message_list.append(
                ContextException(
                    location="_run_single_workflow", message=failed_message
                )
            )

    def _handle_unexpected_exception(self, location: str, fail_message: str):
        error_event = UnexpectedErrorEvent(location=location, fail_message=fail_message)

    def add_observer(self, observer: Observer):
        self.observers.append(observer)

    def add_node(self, node: DAGNode):
        self.node_list.append(node)
