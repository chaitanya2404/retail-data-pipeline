"""Schema contracts and drift detection.

A contract is the agreement between whoever produces a dataset and everyone downstream of it.
Checking it at ingestion is the difference between "the upstream team renamed a column" being a
loud failure at the front door and being a silently empty dashboard column three days later.

The central idea here is that **not all drift is equal**:

* A *new* column is additive. Nothing downstream referenced it yesterday, so nothing breaks today.
  Failing the pipeline for it trains people to bypass the check.
* A *missing* column is breaking. Every model, report and query referencing it is now wrong.
* A *retyped* column is the worst case, because it usually does not raise anything at all — a
  numeric id arriving as text still loads, still joins to nothing, and quietly drops rows from
  every inner join it participates in.

Treating all three the same produces either a pipeline that cries wolf or one that misses the
failure that actually matters.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Iterable

import pandas as pd

DEFAULT_CONTRACT_PATH = (
    Path(__file__).resolve().parents[3] / "contracts" / "online_retail.contract.json"
)


class TypeKind(str, Enum):
    """Normalised type family.

    Deliberately coarser than a pandas dtype. int64 becoming int32 is not drift anyone cares
    about; int64 becoming object is. Comparing raw dtype strings would flag the first and, on a
    different pandas version, miss the second.
    """

    INTEGER = "integer"
    FLOAT = "float"
    STRING = "string"
    DATETIME = "datetime"
    BOOLEAN = "boolean"
    UNKNOWN = "unknown"

    @classmethod
    def of(cls, dtype: object) -> "TypeKind":
        kind = getattr(dtype, "kind", None)
        mapping = {
            "i": cls.INTEGER,
            "u": cls.INTEGER,
            "f": cls.FLOAT,
            "O": cls.STRING,
            "U": cls.STRING,
            "S": cls.STRING,
            "M": cls.DATETIME,
            "m": cls.DATETIME,
            "b": cls.BOOLEAN,
        }
        if kind in mapping:
            return mapping[kind]

        # pandas extension dtypes (string[python], Int64, boolean) expose no single-char kind.
        name = str(dtype).lower()
        if "datetime" in name or "timestamp" in name:
            return cls.DATETIME
        if "int" in name:
            return cls.INTEGER
        if "float" in name or "decimal" in name:
            return cls.FLOAT
        if "bool" in name:
            return cls.BOOLEAN
        if "str" in name or "object" in name:
            return cls.STRING
        return cls.UNKNOWN


class DriftSeverity(str, Enum):
    BREAKING = "breaking"
    ADDITIVE = "additive"


@dataclass(frozen=True)
class ColumnContract:
    name: str
    type_kind: TypeKind
    nullable: bool = False
    description: str = ""

    @classmethod
    def from_dict(cls, payload: dict) -> "ColumnContract":
        return cls(
            name=payload["name"],
            type_kind=TypeKind(payload["type_kind"]),
            nullable=bool(payload.get("nullable", False)),
            description=payload.get("description", ""),
        )

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "type_kind": self.type_kind.value,
            "nullable": self.nullable,
            "description": self.description,
        }


@dataclass(frozen=True)
class DriftFinding:
    column: str
    kind: str
    severity: DriftSeverity
    detail: str

    def to_dict(self) -> dict:
        return {
            "column": self.column,
            "kind": self.kind,
            "severity": self.severity.value,
            "detail": self.detail,
        }


@dataclass
class DriftReport:
    findings: list[DriftFinding] = field(default_factory=list)

    @property
    def breaking(self) -> list[DriftFinding]:
        return [f for f in self.findings if f.severity is DriftSeverity.BREAKING]

    @property
    def additive(self) -> list[DriftFinding]:
        return [f for f in self.findings if f.severity is DriftSeverity.ADDITIVE]

    @property
    def has_breaking_drift(self) -> bool:
        return bool(self.breaking)

    def to_dict(self) -> dict:
        return {
            "has_breaking_drift": self.has_breaking_drift,
            "breaking_count": len(self.breaking),
            "additive_count": len(self.additive),
            "findings": [f.to_dict() for f in self.findings],
        }

    def summary(self) -> str:
        if not self.findings:
            return "no schema drift detected"
        return f"schema drift: {len(self.breaking)} breaking, {len(self.additive)} additive"


class SchemaDriftError(RuntimeError):
    """Raised when breaking drift is found and the caller asked for enforcement."""

    def __init__(self, report: DriftReport):
        self.report = report
        detail = "; ".join(f"{f.column}: {f.detail}" for f in report.breaking)
        super().__init__(f"breaking schema drift: {detail}")


@dataclass(frozen=True)
class SchemaContract:
    """The expected shape of an incoming dataset."""

    name: str
    version: int
    columns: tuple[ColumnContract, ...]

    @classmethod
    def load(cls, path: Path | str = DEFAULT_CONTRACT_PATH) -> "SchemaContract":
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls(
            name=payload["name"],
            version=int(payload["version"]),
            columns=tuple(ColumnContract.from_dict(c) for c in payload["columns"]),
        )

    def save(self, path: Path | str = DEFAULT_CONTRACT_PATH) -> Path:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(
                {
                    "name": self.name,
                    "version": self.version,
                    "columns": [c.to_dict() for c in self.columns],
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        return target

    @classmethod
    def infer(cls, df: pd.DataFrame, *, name: str, version: int = 1) -> "SchemaContract":
        """Derive a contract from a frame — for bootstrapping a new source, not for enforcement.

        Inferring nullability from one batch is a guess: a column that happens to be complete
        today is marked non-nullable and will fail on the first batch that legitimately has a gap.
        The generated file is meant to be reviewed and corrected by hand before it is trusted.
        """
        return cls(
            name=name,
            version=version,
            columns=tuple(
                ColumnContract(
                    name=str(column),
                    type_kind=TypeKind.of(df[column].dtype),
                    nullable=bool(df[column].isna().any()),
                )
                for column in df.columns
            ),
        )

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)

    def detect_drift(self, df: pd.DataFrame) -> DriftReport:
        """Compare an incoming frame against this contract."""
        report = DriftReport()
        actual = {str(column): df[column] for column in df.columns}

        for column in self.columns:
            if column.name not in actual:
                report.findings.append(
                    DriftFinding(
                        column=column.name,
                        kind="missing_column",
                        severity=DriftSeverity.BREAKING,
                        detail="declared in the contract but absent from the batch",
                    )
                )
                continue

            series = actual[column.name]

            # An all-null column carries no type evidence — pandas types it float64 or object
            # depending on how it was constructed, and reporting either as drift is a false
            # positive. The signal is not lost: if the column is non-nullable, the null check
            # below reports it as breaking anyway, which is the finding that actually matters.
            all_null = bool(series.isna().all()) and len(series) > 0

            observed = TypeKind.of(series.dtype)
            if (
                not all_null
                and observed is not column.type_kind
                and not _is_benign_widening(column.type_kind, observed)
            ):
                report.findings.append(
                    DriftFinding(
                        column=column.name,
                        kind="type_change",
                        severity=DriftSeverity.BREAKING,
                        detail=f"expected {column.type_kind.value}, received {observed.value}",
                    )
                )

            if not column.nullable and series.isna().any():
                null_count = int(series.isna().sum())
                report.findings.append(
                    DriftFinding(
                        column=column.name,
                        kind="unexpected_nulls",
                        severity=DriftSeverity.BREAKING,
                        detail=f"declared non-nullable but {null_count} nulls present",
                    )
                )

        for name in actual:
            if name not in self.column_names:
                report.findings.append(
                    DriftFinding(
                        column=name,
                        kind="new_column",
                        severity=DriftSeverity.ADDITIVE,
                        detail="present in the batch but not declared in the contract",
                    )
                )

        return report


def _is_benign_widening(expected: TypeKind, observed: TypeKind) -> bool:
    """Integer arriving where a float was expected loses nothing and breaks nothing.

    The reverse is not benign: floats landing in an integer column mean either silent truncation
    or a join key that no longer matches.
    """
    return expected is TypeKind.FLOAT and observed is TypeKind.INTEGER


def check_contract(
    df: pd.DataFrame,
    contract: SchemaContract | None = None,
    *,
    enforce: bool = True,
) -> DriftReport:
    """Detect drift and, by default, refuse to continue on breaking findings.

    :param enforce: when False the report is returned regardless, which is what a profiling or
        backfill run wants — it should observe drift without blocking on it.
    """
    contract = contract or SchemaContract.load()
    report = contract.detect_drift(df)
    if enforce and report.has_breaking_drift:
        raise SchemaDriftError(report)
    return report


def columns_of(contract: SchemaContract, *, only: Iterable[str] | None = None) -> list[str]:
    """Contract column names, optionally filtered — used to build expectation suites."""
    if only is None:
        return list(contract.column_names)
    wanted = set(only)
    return [name for name in contract.column_names if name in wanted]
