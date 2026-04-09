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
    _ = dag.add_node(SourceTable("src", children=["mid"]))
    _ = dag.add_node(CTE("mid", parents=["src"], children=["final"]))
    _ = dag.add_node(Select("final", parents=["mid"]))

    resolved = dag.resolve()

    assert len(resolved.nodes) == 3


def test_resolve_fails_dag_with_nonexistent_parent_of_source_node():
    dag = DAG()
    _ = dag.add_node(SourceTable("src", parents=["wrong"], children=["final"]))
    _ = dag.add_node(Select("final", parents=["src"]))

    with pytest.raises(ResolutionError) as exc_info:
        _ = dag.resolve()

    assert len(exc_info.value.errors) == 2


def test_resolve_fails_dag_with_cyclical_nodes():
    dag = DAG()
    _ = dag.add_node(SourceTable("src", children=["c1"]))
    _ = dag.add_node(CTE("c1", parents=["src", "c3"], children=["c2"]))
    _ = dag.add_node(CTE("c2", parents=["c1"], children=["c3", "final"]))
    _ = dag.add_node(CTE("c3", parents=["c2"], children=["c1"]))
    _ = dag.add_node(Select("final", parents=["c2"]))

    with pytest.raises(ResolutionError) as exc_info:
        _ = dag.resolve()
    
    assert len(exc_info.value.errors) == 1


def test_resolve_fails_dag_with_multi_child_subquery():
    dag = DAG()
    _ = dag.add_node(SourceTable("src", children=["sub"]))
    _ = dag.add_node(Subquery("sub", parents=["src"], children=["c1", "c2"]))
    _ = dag.add_node(CTE("c1", parents=["sub"], children=["final"]))
    _ = dag.add_node(CTE("c2", parents=["sub"], children=["final"]))
    _ = dag.add_node(Select("final", parents=["c1", "c2"]))

    with pytest.raises(ResolutionError) as exc_info:
        _ = dag.resolve()
    
    assert len(exc_info.value.errors) == 1
