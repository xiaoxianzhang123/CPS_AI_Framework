"""
建立两个节点,名为test_node1和test_node2,其中test_node2依赖于test_node1.
"""

from dag_workflow import DAGEngine, DAGNode
from dag_workflow import PrintObserver


test_engine = DAGEngine()


class TestNode(DAGNode):
    def __init__(self, node_id, node_dependencies=..., node_condition=None):
        node_task = self._print
        super().__init__(node_id, node_task, node_dependencies, node_condition)

    def _print(self, context):
        print(f"{self.node_id} is executing...")
        return f"{self.node_id} finished"


test_engine.add_node(
    TestNode(
        node_id="test_node1",
        node_dependencies=[],
    )
)

test_engine.add_node(
    TestNode(
        node_id="test_node2",
        node_dependencies=["test_node1"],
    )
)


task_id = test_engine.submit_work("shit")
print(test_engine.get_result(task_id))
