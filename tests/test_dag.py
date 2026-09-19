import pytest

from sql_debugger.dag import (
    CTE,
    DAG,
    ResolutionError,
    Select,
    SourceTable,
    Subquery,
)


def test_resolve_passes_linear_three_node_dag():
    dag = DAG()
    _ = dag.add_node(SourceTable("src", sql="", children=["mid"]))
    _ = dag.add_node(CTE("mid", sql="", parents=["src"], children=["final"]))
    _ = dag.add_node(Select("final", sql="", parents=["mid"]))

    resolved = dag.resolve()

    assert len(resolved.nodes) == 3


def test_resolve_fails_dag_with_nonexistent_parent_of_source_node():
    dag = DAG()
    _ = dag.add_node(SourceTable("src", sql="", parents=["wrong"], children=["final"]))
    _ = dag.add_node(Select("final", sql="", parents=["src"]))

    with pytest.raises(ResolutionError) as exc_info:
        _ = dag.resolve()

    assert len(exc_info.value.errors) == 2


def test_resolve_fails_dag_with_cyclical_nodes():
    dag = DAG()
    _ = dag.add_node(SourceTable("src", sql="", children=["c1"]))
    _ = dag.add_node(CTE("c1", sql="", parents=["src", "c3"], children=["c2"]))
    _ = dag.add_node(CTE("c2", sql="", parents=["c1"], children=["c3", "final"]))
    _ = dag.add_node(CTE("c3", sql="", parents=["c2"], children=["c1"]))
    _ = dag.add_node(Select("final", sql="", parents=["c2"]))

    with pytest.raises(ResolutionError) as exc_info:
        _ = dag.resolve()
    
    assert len(exc_info.value.errors) == 1


def test_resolve_fails_dag_with_multi_child_subquery():
    dag = DAG()
    _ = dag.add_node(SourceTable("src", sql="", children=["sub"]))
    _ = dag.add_node(Subquery("sub", sql="", parents=["src"], children=["c1", "c2"]))
    _ = dag.add_node(CTE("c1", sql="", parents=["sub"], children=["final"]))
    _ = dag.add_node(CTE("c2", sql="", parents=["sub"], children=["final"]))
    _ = dag.add_node(Select("final", sql="", parents=["c1", "c2"]))

    with pytest.raises(ResolutionError) as exc_info:
        _ = dag.resolve()
    
    assert len(exc_info.value.errors) == 1
