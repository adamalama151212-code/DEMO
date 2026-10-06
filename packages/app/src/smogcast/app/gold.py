"""Read-only access to gold tables for the assistant and the dashboard.

Every query — written by us or by a language model — goes through the same path:
1. ``validate_select`` (guardrails: one SELECT, whitelisted gold tables, enforced LIMIT),
2. table names written as ``gold.<table>`` are rewritten to the physical reference of the current
   environment (locally a Delta path, on Databricks ``smogcast_<env>.gold.<table>``),
3. execution with Spark SQL. The SQL actually run is returned with the rows, so the user always sees it.
"""

from __future__ import annotations

from dataclasses import dataclass

import sqlglot
from sqlglot import exp

from smogcast.app.guardrails import DEFAULT_ALLOWED_TABLES, validate_select


@dataclass
class QueryResult:
    sql: str              # the validated query as written (gold.<table> names) — shown to the user
    rows: list[dict]


class GoldReader:
    def __init__(self, ctx, max_rows: int = 200, allowed: frozenset[str] = DEFAULT_ALLOWED_TABLES):
        self.ctx, self.max_rows, self.allowed = ctx, max_rows, allowed

    def exists(self, table: str) -> bool:
        return self.ctx.storage.exists("gold", table)

    def physical(self, sql: str) -> str:
        """``gold.x`` / ``x`` → the storage reference of this environment (CTE names stay untouched)."""
        tree = sqlglot.parse_one(sql, read="databricks")
        ctes = {c.alias_or_name.lower() for c in tree.find_all(exp.CTE)}

        def swap(node):
            if isinstance(node, exp.Table) and node.name.lower() in self.allowed and node.name.lower() not in ctes:
                ref = sqlglot.parse_one(self.ctx.storage.sql_ref("gold", node.name.lower()), into=exp.Table,
                                        read="databricks")
                if node.args.get("alias"):
                    ref.set("alias", node.args["alias"])
                return ref
            return node

        return tree.transform(swap).sql(dialect="databricks")

    def query(self, sql: str, max_rows: int | None = None) -> QueryResult:
        """``max_rows``: our own dashboard queries may read more rows than a language model is given."""
        safe = validate_select(sql, self.allowed, max_rows or self.max_rows)
        rows = [r.asDict(recursive=True) for r in self.ctx.spark.sql(self.physical(safe)).collect()]
        return QueryResult(safe, rows)

    def schema_text(self) -> str:
        """Columns of every existing whitelisted table — the only schema a language model gets to see."""
        lines = []
        for table in sorted(self.allowed):
            if self.exists(table):
                fields = self.ctx.storage.read("gold", table).schema.fields
                cols = ", ".join(f"{f.name} {f.dataType.simpleString()}" for f in fields)
                lines.append(f"gold.{table}({cols})")
        return "\n".join(lines)
