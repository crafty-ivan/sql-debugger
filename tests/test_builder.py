import pytest

from sql_debugger.builder import DAGBuilder, UnsupportedQueryError, build_dag
from sql_debugger.dag import (
    CTE,
    ResolvedDAG,
    Select,
    SetOperation,
    SourceTable,
    Subquery,
)


# TODO: make a pass over this file to simplify and bring in line with style
def resolved(sql: str) -> ResolvedDAG:
    return build_dag(sql).resolve()


def test_linear_cte_chain_builds_source_to_cte_to_terminal():
    dag = resolved(
        "WITH base AS (SELECT id FROM raw.orders) SELECT * FROM base"
    )

    assert set(dag.nodes) == {"raw.orders", "base", "result"}
    assert dag.nodes["base"].node_type is CTE
    assert list(dag.nodes["base"].parents) == ["raw.orders"]
    assert dag.terminal_node.key == "result"
    assert dag.terminal_node.node_type is Select
    assert [node.key for node in dag.origin_nodes] == ["raw.orders"]


def test_node_sql_is_own_fragment_without_with_clause():
    dag = resolved(
        "WITH base AS (SELECT id FROM raw.orders) SELECT * FROM base"
    )

    assert dag.nodes["base"].sql == "SELECT id FROM raw.orders"
    assert dag.nodes["result"].sql == "SELECT * FROM base"
    assert dag.nodes["raw.orders"].sql == ""


def test_self_join_collapses_to_one_source_table_node():
    dag = resolved("SELECT l.a, r.a FROM db.t l JOIN db.t r ON l.id = r.id")

    assert set(dag.nodes) == {"db.t", "result"}
    assert list(dag.nodes["db.t"].children) == ["result"]


def test_cte_referenced_under_two_aliases_yields_one_edge():
    dag = resolved(
        "WITH c AS (SELECT a FROM t1) SELECT * FROM c x JOIN c y ON x.a = y.a"
    )

    assert list(dag.nodes["result"].parents) == ["c"]
    assert list(dag.nodes["c"].children) == ["result"]


def test_nested_derived_tables_use_their_aliases_as_keys():
    dag = resolved(
        "SELECT * FROM (SELECT * FROM (SELECT a FROM t1) inner_q) outer_q"
    )

    assert set(dag.nodes) == {"t1", "inner_q", "outer_q", "result"}
    assert dag.nodes["inner_q"].node_type is Subquery
    assert list(dag.nodes["outer_q"].parents) == ["inner_q"]


def test_unaliased_derived_table_gets_path_derived_key():
    dag = resolved("SELECT * FROM (SELECT a FROM t1)")

    assert "result__subquery_0" in dag.nodes
    assert dag.nodes["result__subquery_0"].node_type is Subquery


def test_where_subquery_becomes_subquery_node_with_one_child():
    dag = resolved("SELECT * FROM t1 WHERE id IN (SELECT id FROM t2)")

    subquery = dag.nodes["result__subquery_0"]
    assert subquery.node_type is Subquery
    assert list(subquery.parents) == ["t2"]
    assert list(subquery.children) == ["result"]
    assert set(dag.nodes["result"].parents) == {"t1", "result__subquery_0"}


def test_root_union_is_set_operation_terminal_over_select_branches():
    dag = resolved("SELECT a FROM t1 UNION ALL SELECT a FROM t2")

    assert dag.terminal_node.key == "result"
    assert dag.terminal_node.node_type is SetOperation
    assert list(dag.terminal_node.parents) == [
        "result__branch_0",
        "result__branch_1",
    ]
    assert dag.nodes["result__branch_0"].node_type is Select


@pytest.mark.parametrize(
    "operator", ["UNION ALL", "UNION", "INTERSECT", "EXCEPT", "MINUS"]
)
def test_every_set_operator_yields_a_set_operation_terminal(operator: str):
    dag = resolved(f"SELECT a FROM t1 {operator} SELECT a FROM t2")

    assert dag.terminal_node.node_type is SetOperation


def test_three_way_union_nests_set_operation_nodes():
    dag = resolved(
        "SELECT a FROM t1 UNION ALL SELECT a FROM t2 UNION ALL SELECT a FROM t3"
    )

    nested = dag.nodes["result__branch_0"]
    assert nested.node_type is SetOperation
    assert list(nested.parents) == [
        "result__branch_0__branch_0",
        "result__branch_0__branch_1",
    ]
    assert list(dag.terminal_node.parents) == ["result__branch_0", "result__branch_1"]


def test_union_bodied_cte_splits_into_set_operation_and_cte():
    dag = resolved(
        "WITH u AS (SELECT a FROM t1 UNION SELECT a FROM t2) SELECT * FROM u"
    )

    assert dag.nodes["u__setop"].node_type is SetOperation
    assert list(dag.nodes["u__setop"].parents) == ["u__branch_0", "u__branch_1"]
    assert dag.nodes["u"].node_type is CTE
    assert list(dag.nodes["u"].parents) == ["u__setop"]
    assert list(dag.nodes["u"].children) == ["result"]


def test_union_bodied_derived_table_splits_into_set_operation_and_subquery():
    dag = resolved(
        "SELECT * FROM (SELECT a FROM t1 UNION ALL SELECT a FROM t2) x"
    )

    assert dag.nodes["x__setop"].node_type is SetOperation
    assert dag.nodes["x"].node_type is Subquery
    assert list(dag.nodes["x"].parents) == ["x__setop"]


def test_unreferenced_cte_is_reported_dead_transitively():
    dag = resolved(
        "WITH unused AS (SELECT a FROM dead_tbl), "
        + "used AS (SELECT a FROM raw.orders) SELECT * FROM used"
    )

    assert {node.key for node in dag.dead_nodes} == {"unused", "dead_tbl"}
    assert dag.nodes["unused"].is_dead_node
    assert dag.nodes["dead_tbl"].is_dead_node
    assert not dag.nodes["used"].is_dead_node
    assert [node.key for node in dag.origin_nodes] == ["raw.orders"]
    assert dag.terminal_node.key == "result"


def test_source_table_feeding_both_live_and_dead_ctes_stays_live():
    dag = resolved(
        "WITH unused AS (SELECT a FROM shared), used AS (SELECT a FROM shared) "
        + "SELECT * FROM used"
    )

    assert not dag.nodes["shared"].is_dead_node
    assert {node.key for node in dag.dead_nodes} == {"unused"}


def test_colliding_aliases_are_disambiguated_with_a_suffix():
    dag = resolved(
        "SELECT * FROM (SELECT a FROM t1) c JOIN (SELECT a FROM t2) c2 ON 1 = 1 "
        + "WHERE a IN (SELECT a FROM (SELECT a FROM t3) c)"
    )

    assert list(dag.nodes["c"].parents) == ["t1"]
    assert list(dag.nodes["c__2"].parents) == ["t3"]


def test_table_colliding_with_cte_name_is_suffixed_not_the_cte():
    dag = resolved("WITH orders AS (SELECT a FROM orders) SELECT * FROM orders")

    assert dag.nodes["orders"].node_type is CTE
    assert dag.nodes["orders__2"].node_type is SourceTable
    assert list(dag.nodes["orders"].parents) == ["orders__2"]


def test_qualified_tables_with_the_same_name_stay_distinct():
    dag = resolved("SELECT * FROM raw.orders r JOIN stage.orders s ON r.id = s.id")

    assert {node.key for node in dag.origin_nodes} == {"raw.orders", "stage.orders"}


def test_query_without_tables_has_a_single_node_and_no_origins():
    dag = resolved("SELECT 1 AS x")

    assert set(dag.nodes) == {"result"}
    assert dag.origin_nodes == []


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE t SET a = 1",
        "INSERT INTO t SELECT 1",
        "CREATE TABLE x AS SELECT a FROM t1",
        "DELETE FROM t WHERE a = 1",
    ],
)
def test_non_select_statements_are_rejected(sql: str):
    with pytest.raises(UnsupportedQueryError):
        _ = build_dag(sql)


def test_recursive_cte_is_rejected():
    with pytest.raises(UnsupportedQueryError):
        _ = build_dag(
            "WITH RECURSIVE r AS ("
            + "SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 5"
            + ") SELECT * FROM r"
        )


def test_table_function_is_rejected():
    with pytest.raises(UnsupportedQueryError):
        _ = build_dag("SELECT * FROM TABLE(FLATTEN(input => x)) f")


def test_builder_is_reusable_across_queries():
    builder = DAGBuilder()
    first = builder.build("WITH c AS (SELECT a FROM t1) SELECT * FROM c")
    second = builder.build("WITH c AS (SELECT a FROM t1) SELECT * FROM c")

    assert set(first.nodes) == set(second.nodes)
