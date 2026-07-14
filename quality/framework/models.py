from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class Rule:
    """
    Metadata của một Data Quality Rule
    """

    id: str
    name: str
    description: str
    enabled: bool

    layer: str

    severity: str

    owner: str

    type: str

    query: str

    metric: str

    operator: str

    threshold: Any

    tags: list[str] = field(default_factory=list)


@dataclass
class ValidationResult:
    """
    Kết quả của một Rule
    """

    rule_id: str

    rule_name: str

    status: str

    metric_name: str

    actual_value: Any

    expected_value: Any

    operator: str

    severity: str

    execution_time: float

    executed_at: datetime

    message: str


@dataclass
class ValidationReport:
    """
    Tổng hợp kết quả của một lần chạy
    """

    run_id: str

    started_at: datetime

    finished_at: datetime

    duration: float

    total_rules: int

    passed_rules: int

    failed_rules: int

    results: list[ValidationResult]