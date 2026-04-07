"""Two-pass DAG representation for step-by-step SQL debugging.

Build phase: construct a mutable DAG of Nodes with string-based references.
Resolution phase: call DAG.resolve() to produce an immutable ResolvedDAG
with validated, object-based references.
"""

from __future__ import annotations

from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class ResolutionError(Exception):
    """Raised when DAG resolution fails validation.

    Attributes:
        errors: A list of human-readable descriptions of every validation
            failure encountered during resolution.
    """

    def __init__(self, errors: list[str]) -> None:
        self.errors: list[str] = errors
        super().__init__(
            f"DAG resolution failed with {len(errors)} error(s):\n"
            + "\n".join(f"  - {e}" for e in errors)
        )


# ---------------------------------------------------------------------------
# Builder-phase nodes
# ---------------------------------------------------------------------------


class Node:
    """A mutable node used during DAG construction.

    Parents and children are stored as plain string keys and resolved into
    object references only when ``DAG.resolve()`` is called.

    Attributes:
        key: Unique identifier for this node — a table name, alias, or the
            reserved key ``"select"`` for the terminal node.
        parents: Keys of nodes that this node depends on (reads from).
        children: Keys of nodes that depend on (read from) this node.
    """

    def __init__(
        self,
        key: str,
        parents: list[str] | None = None,
        children: list[str] | None = None,
    ) -> None:
        self.key: str = key
        self.parents: list[str] = parents if parents is not None else []
        self.children: list[str] = children if children is not None else []


class SourceTable(Node):
    """A node representing an external source table.

    Origin node of the DAG — must have no parents.
    """


class CTE(Node):
    """A node representing a Common Table Expression.

    No structural constraints on parents or children.
    """


class Subquery(Node):
    """A node representing a subquery (aliased or auto-aliased).

    Must have exactly one child — the node in whose scope it appears.
    """


class Select(Node):
    """A node representing a SELECT statement.

    Valid as a terminal node (no children) or as a branch of a
    ``SetOperation``.
    """


class SetOperation(Node):
    """A node representing a set operation (UNION, INTERSECT, EXCEPT, etc.).

    Its parents are the branches (``Select`` or nested ``SetOperation``
    nodes) being combined.  Valid as a terminal node (no children) or as
    an intermediate node (e.g. a UNION inside a CTE or subquery).
    """


# ---------------------------------------------------------------------------
# Resolved (immutable) types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ResolvedNode:
    """An immutable node with live object references to its neighbours.

    Created exclusively by ``DAG.resolve()``.  The ``parents`` and
    ``children`` dicts are mutable containers on a frozen dataclass — their
    *contents* are populated during resolution, but the object identity of
    the dicts (and all other fields) cannot change after init.

    Attributes:
        key: The node's unique identifier (same as the source ``Node.key``).
        parents: Resolved parent nodes keyed by their ``key``.
        children: Resolved child nodes keyed by their ``key``.
        node_type: The original ``Node`` subclass (``SourceTable``, ``CTE``,
            ``Subquery``, ``Select``, or ``SetOperation``).
    """

    key: str
    parents: dict[str, ResolvedNode] = field(default_factory=dict)
    children: dict[str, ResolvedNode] = field(default_factory=dict)
    node_type: type = Node


@dataclass(frozen=True)
class ResolvedDAG:
    """An immutable, fully-validated snapshot of the DAG.

    Produced by ``DAG.resolve()``.  All structural invariants have been
    checked at construction time.

    Attributes:
        nodes: Every resolved node, keyed by ``key``.
        origin_nodes: The subset of nodes whose ``node_type`` is
            ``SourceTable`` (entry points of the DAG).
        terminal_node: The single terminal node — a ``Select`` or
            ``SetOperation`` with no children.
    """

    nodes: dict[str, ResolvedNode]
    origin_nodes: list[ResolvedNode]
    terminal_node: ResolvedNode


# ---------------------------------------------------------------------------
# Mutable DAG builder
# ---------------------------------------------------------------------------


class DAG:
    """Mutable DAG builder.

    Collect ``Node`` instances via ``add_node`` / ``remove_node``, then call
    ``resolve()`` to obtain an immutable ``ResolvedDAG``.  The builder
    remains usable (and independently mutable) after resolution.
    """

    def __init__(self) -> None:
        self.nodes: dict[str, Node] = {}

    def add_node(self, node: Node, overwrite: bool = False) -> Node | None:
        """Add a node to the DAG.

        Args:
            node: The node to insert.
            overwrite: When ``True`` and a node with the same key already
                exists, the existing node is replaced.

        Returns:
            The inserted (or replacement) node on success, or ``None`` if
            the key already exists and *overwrite* is ``False``.
        """
        if node.key in self.nodes and not overwrite:
            return None
        self.nodes[node.key] = node
        return node

    def remove_node(self, key: str) -> Node | None:
        """Remove a node from the DAG by key.

        Args:
            key: The key of the node to remove.

        Returns:
            The removed node, or ``None`` if no node with that key exists.
        """
        return self.nodes.pop(key, None)

    # ---- resolution -----------------------------------------------------

    def resolve(self) -> ResolvedDAG:
        """Validate the DAG and produce an immutable ``ResolvedDAG``.

        Runs every structural check in a single pass, collecting all errors
        rather than failing on the first one.  Does **not** mutate the
        builder — you can continue to add/remove nodes and resolve again.

        Returns:
            A fully-linked, immutable ``ResolvedDAG``.

        Raises:
            ResolutionError: If one or more validation checks fail.  The
                exception's ``errors`` attribute contains a list of every
                problem found.
        """
        errors: list[str] = []

        # -- 1. Create ResolvedNode shells --------------------------------
        resolved: dict[str, ResolvedNode] = {
            key: ResolvedNode(key=key, node_type=type(node))
            for key, node in self.nodes.items()
        }

        # -- 2. Validate reference existence ------------------------------
        for key, node in self.nodes.items():
            for parent_key in node.parents:
                if parent_key not in self.nodes:
                    errors.append(
                        f"Node '{key}' references parent '{parent_key}' "
                        + "which does not exist."
                    )
            for child_key in node.children:
                if child_key not in self.nodes:
                    errors.append(
                        f"Node '{key}' references child '{child_key}' "
                        + "which does not exist."
                    )

        # -- 3. Validate reciprocal links ---------------------------------
        for key, node in self.nodes.items():
            for parent_key in node.parents:
                if parent_key in self.nodes:
                    parent = self.nodes[parent_key]
                    if key not in parent.children:
                        errors.append(
                            f"Node '{key}' lists '{parent_key}' as a parent, "
                            + f"but '{parent_key}' does not list '{key}' as a child."
                        )
            for child_key in node.children:
                if child_key in self.nodes:
                    child = self.nodes[child_key]
                    if key not in child.parents:
                        errors.append(
                            f"Node '{key}' lists '{child_key}' as a child, "
                            + f"but '{child_key}' does not list '{key}' as a parent."
                        )

        # -- 4. Validate type constraints ---------------------------------
        TERMINAL_TYPES = (Select, SetOperation)
        terminal_candidates: list[str] = []

        for key, node in self.nodes.items():
            if isinstance(node, SourceTable) and node.parents:
                errors.append(
                    f"SourceTable '{key}' must have no parents, "
                    + f"but has: {node.parents}."
                )
            if isinstance(node, Subquery) and len(node.children) != 1:
                errors.append(
                    f"Subquery '{key}' must have exactly one child, "
                    + f"but has {len(node.children)}: {node.children}."
                )
            if not node.children:
                terminal_candidates.append(key)

        if len(terminal_candidates) == 0:
            errors.append("DAG must contain exactly one terminal node; found none.")
        elif len(terminal_candidates) > 1:
            errors.append(
                "DAG must contain exactly one terminal node (a node with no "
                + f"children); found {len(terminal_candidates)}: {terminal_candidates}."
            )
        elif not isinstance(self.nodes[terminal_candidates[0]], TERMINAL_TYPES):
            bad = terminal_candidates[0]
            errors.append(
                f"Terminal node '{bad}' is of type "
                + f"'{type(self.nodes[bad]).__name__}', but must be "
                + "Select or SetOperation."
            )

        # -- 5. Cycle detection (Kahn's algorithm) ------------------------
        if not errors:
            in_degree: dict[str, int] = {
                key: len(node.parents) for key, node in self.nodes.items()
            }
            queue: list[str] = [k for k, d in in_degree.items() if d == 0]
            visited_count = 0

            while queue:
                current = queue.pop()
                visited_count += 1
                for child_key in self.nodes[current].children:
                    in_degree[child_key] -= 1
                    if in_degree[child_key] == 0:
                        queue.append(child_key)

            if visited_count != len(self.nodes):
                errors.append(
                    "DAG contains a cycle. Nodes involved in cycles: "
                    + str([k for k, d in in_degree.items() if d > 0])
                )

        # -- 6. Bail on errors --------------------------------------------
        if errors:
            raise ResolutionError(errors)

        # -- 7. Wire up resolved references -------------------------------
        for key, node in self.nodes.items():
            rnode = resolved[key]
            for parent_key in node.parents:
                rnode.parents[parent_key] = resolved[parent_key]
            for child_key in node.children:
                rnode.children[child_key] = resolved[child_key]

        # -- 8. Build ResolvedDAG -----------------------------------------
        origin_nodes = [
            rnode for rnode in resolved.values()
            if rnode.node_type is SourceTable
        ]
        terminal_node = next(
            rnode for rnode in resolved.values()
            if not rnode.children
        )

        return ResolvedDAG(
            nodes=resolved,
            origin_nodes=origin_nodes,
            terminal_node=terminal_node,
        )
