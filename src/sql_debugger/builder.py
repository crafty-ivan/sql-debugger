"""Build a :class:`~sql_debugger.dag.DAG` from a SQL query.

Parses a query with ``sqlglot`` and walks the scope tree of
``sqlglot.optimizer.scope.build_scope``, creating the DAG to represent
intermediate result sets, with edges connecting sets reading from each other.
"""

# TODO: make a pass over this file to simplify and bring in line with style
from __future__ import annotations

from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp
from sqlglot.optimizer.scope import Scope, ScopeType, build_scope

from sql_debugger.dag import (
    CTE,
    DAG,
    Node,
    Select,
    SetOperation,
    SourceTable,
    Subquery,
)

TERMINAL_KEY = "result"


class UnsupportedQueryError(Exception):
    """Early version error: Raised when a query uses a construct this builder
    does not model.
    
    Should not exist by v0.1.
    """


@dataclass(frozen=True)
class _ScopeKeys:
    """The node keys allocated for a single sqlglot scope.

    Attributes:
        outer: The key other scopes refer to this scope by.
        inner: The key of the node holding the scope's own content.  Differs
            from *outer* only when a set operation is split out of a CTE or
            subquery wrapper.
    """

    outer: str
    inner: str


@dataclass
class _BuildState:
    """Mutable per-build bookkeeping."""

    dialect: str
    dag: DAG = field(default_factory=DAG)
    keys: dict[int, _ScopeKeys] = field(default_factory=dict)
    used_keys: set[str] = field(default_factory=set)
    table_keys: dict[str, str] = field(default_factory=dict)


def _query_expression(scope: Scope) -> exp.Select | exp.SetOperation:
    """Narrow a scope's expression to the two forms this builder models.

    ``Scope.expression`` is typed as the bare ``exp.Expr`` marker class, which
    carries none of the query API.
    """
    expression = scope.expression
    if isinstance(expression, (exp.Select, exp.SetOperation)):
        return expression
    raise UnsupportedQueryError(
        f"Unsupported scope expression {type(expression).__name__}."
    )


def _scope_alias(scope: Scope) -> str:
    """Return the name a scope is introduced under, or ``""`` if anonymous."""
    parent_expression = _query_expression(scope).parent
    if isinstance(parent_expression, (exp.CTE, exp.Subquery)):
        return parent_expression.alias_or_name
    return ""


def _index_of(scope: Scope, candidates: list[Scope]) -> int:
    for index, candidate in enumerate(candidates):
        if candidate is scope:
            return index
    raise UnsupportedQueryError(
        f"Scope {scope.expression.sql()!r} is not reachable from its parent."
    )


class DAGBuilder:
    """Builds a ``DAG`` of intermediate result sets from a SQL query.

    Instances are reusable — every call to ``build`` starts from fresh state.
    """

    def __init__(self, dialect: str = "snowflake") -> None:
        self.dialect: str = dialect

    def build(self, sql: str) -> DAG:
        """Parse *sql* and build the corresponding DAG.

        Args:
            sql: A single SELECT or set-operation statement.

        Returns:
            A mutable ``DAG``.  Call ``resolve()`` on it to validate and obtain
            an immutable ``ResolvedDAG``.

        Raises:
            UnsupportedQueryError: If the statement is not a query, or uses a
                construct outside this builder's scope (table functions,
                recursive CTEs).
        """
        expression = sqlglot.parse_one(sql, dialect=self.dialect)
        if not isinstance(expression, (exp.Select, exp.SetOperation)):
            raise UnsupportedQueryError(
                "Expected a SELECT or set operation, got "
                + f"{type(expression).__name__}."
            )

        for with_clause in expression.find_all(exp.With):
            if with_clause.args.get("recursive"):
                raise UnsupportedQueryError("Recursive CTEs are not supported.")

        # Held for the whole build: id(scope) keys stay valid only while the
        # scope tree is alive.
        root_scope = build_scope(expression)
        if root_scope is None:
            raise UnsupportedQueryError(f"Could not build a scope for: {sql!r}")

        scopes = list(root_scope.traverse())
        for scope in scopes:
            if scope.udtf_scopes or scope.scope_type == ScopeType.UDTF:
                raise UnsupportedQueryError("Table functions are not supported.")

        state = _BuildState(dialect=self.dialect)
        self._allocate_keys(state, root_scope)
        for scope in scopes:
            self._create_nodes(state, scope)
        for scope in scopes:
            self._create_edges(state, scope)
        self._mark_dead(state)

        return state.dag

    def _reserve(self, state: _BuildState, candidate: str) -> str:
        key = candidate
        suffix = 2
        while key in state.used_keys:
            key = f"{candidate}__{suffix}"
            suffix += 1
        state.used_keys.add(key)
        return key

    def _allocate_keys(self, state: _BuildState, scope: Scope) -> None:
        """Allocate node keys for *scope* and its descendants, top-down.

        Keys are derived from the enclosing scope's key, so this cannot reuse
        ``Scope.traverse()`` — that yields post-order, parents last.
        """
        if id(scope) in state.keys:
            return

        outer = self._reserve(state, self._candidate_key(state, scope))
        inner = outer
        if isinstance(scope.expression, exp.SetOperation) and not _is_set_operation_node(
            scope
        ):
            inner = self._reserve(state, f"{outer}__setop")
        state.keys[id(scope)] = _ScopeKeys(outer=outer, inner=inner)

        for child in (
            *scope.cte_scopes,
            *scope.union_scopes,
            *scope.derived_table_scopes,
            *scope.subquery_scopes,
        ):
            self._allocate_keys(state, child)

    def _candidate_key(self, state: _BuildState, scope: Scope) -> str:
        if scope.is_root:
            return TERMINAL_KEY

        parent = scope.parent
        if parent is None:
            raise UnsupportedQueryError(
                f"Non-root scope {scope.expression.sql()!r} has no parent scope."
            )
        parent_key = state.keys[id(parent)].outer

        if scope.scope_type == ScopeType.UNION:
            index = _index_of(scope, parent.union_scopes)
            return f"{parent_key}__branch_{index}"

        alias = _scope_alias(scope)
        if alias:
            return alias

        anonymous = [
            candidate
            for candidate in (*parent.derived_table_scopes, *parent.subquery_scopes)
            if not _scope_alias(candidate)
        ]
        return f"{parent_key}__subquery_{_index_of(scope, anonymous)}"

    def _create_nodes(self, state: _BuildState, scope: Scope) -> None:
        keys = state.keys[id(scope)]
        node_sql = self._fragment_sql(_query_expression(scope))

        if keys.inner != keys.outer:
            _ = state.dag.add_node(SetOperation(keys.inner, sql=node_sql))
            _ = state.dag.add_node(_wrapper_class(scope)(keys.outer, sql=node_sql))
            return

        _ = state.dag.add_node(_node_class(scope)(keys.outer, sql=node_sql))

    def _fragment_sql(self, expression: exp.Select | exp.SetOperation) -> str:
        fragment = expression.copy()
        # The arg key is "with_" in sqlglot 30.x; find(exp.With) is a BFS and
        # would reach into a nested subquery's WITH.
        if "with_" in fragment.args:
            del fragment.args["with_"]
        return fragment.sql(dialect=self.dialect)

    def _create_edges(self, state: _BuildState, scope: Scope) -> None:
        keys = state.keys[id(scope)]

        source_keys: list[str] = []
        for _, source in scope.selected_sources.values():
            if isinstance(source, exp.Table):
                source_key = self._table_key(state, source)
            else:
                source_key = state.keys[id(source)].outer
            if source_key not in source_keys:
                source_keys.append(source_key)

        # WHERE/HAVING subqueries are absent from selected_sources, so the two
        # groups never produce duplicate edges.
        for subquery_scope in scope.subquery_scopes:
            source_keys.append(state.keys[id(subquery_scope)].outer)

        if isinstance(scope.expression, exp.SetOperation):
            for branch in scope.union_scopes:
                source_keys.append(state.keys[id(branch)].outer)

        for source_key in source_keys:
            _add_edge(state.dag, source_key, keys.inner)

        if keys.inner != keys.outer:
            _add_edge(state.dag, keys.inner, keys.outer)

    def _table_key(self, state: _BuildState, table: exp.Table) -> str:
        qualified = ".".join(
            part for part in (table.catalog, table.db, table.name) if part
        ).lower()
        if qualified not in state.table_keys:
            key = self._reserve(state, qualified)
            state.table_keys[qualified] = key
            _ = state.dag.add_node(SourceTable(key, sql=""))
        return state.table_keys[qualified]

    def _mark_dead(self, state: _BuildState) -> None:
        reachable: set[str] = set()
        queue = [TERMINAL_KEY]
        while queue:
            key = queue.pop()
            if key in reachable:
                continue
            reachable.add(key)
            queue.extend(state.dag.nodes[key].parents)

        for key, node in state.dag.nodes.items():
            node.is_dead_node = key not in reachable


def _is_set_operation_node(scope: Scope) -> bool:
    """Whether a set-operation scope maps to a bare ``SetOperation`` node.

    True for the query's root and for a branch of an n-ary set operation —
    neither introduces a name, so neither needs a wrapper node.
    """
    return scope.is_root or scope.scope_type == ScopeType.UNION


def _wrapper_class(scope: Scope) -> type[Node]:
    if scope.scope_type == ScopeType.CTE:
        return CTE
    return Subquery


def _node_class(scope: Scope) -> type[Node]:
    is_set_operation = isinstance(scope.expression, exp.SetOperation)
    if scope.is_root or scope.scope_type == ScopeType.UNION:
        return SetOperation if is_set_operation else Select
    if scope.scope_type == ScopeType.CTE:
        return CTE
    if scope.scope_type in (ScopeType.DERIVED_TABLE, ScopeType.SUBQUERY):
        return Subquery
    raise UnsupportedQueryError(f"Unsupported scope type {scope.scope_type}.")


def _add_edge(dag: DAG, parent_key: str, child_key: str) -> None:
    parent = dag.nodes[parent_key]
    child = dag.nodes[child_key]
    if child_key not in parent.children:
        parent.children.append(child_key)
    if parent_key not in child.parents:
        child.parents.append(parent_key)


def build_dag(sql: str, dialect: str = "snowflake") -> DAG:
    """Build a ``DAG`` of intermediate result sets from *sql*."""
    return DAGBuilder(dialect).build(sql)
