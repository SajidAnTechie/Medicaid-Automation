from __future__ import annotations

import re
from datetime import date
from typing import Optional

from pydantic import BaseModel, field_validator


class MedicaidRate(BaseModel):
    state_code: str
    dataset_type: str
    procedure_code: str
    modifier: Optional[str] = None
    fee_amount: float
    effective_date: Optional[date] = None
    end_date: Optional[date] = None

    @field_validator("procedure_code")
    @classmethod
    def validate_code(cls, v: str) -> str:
        v = v.strip().upper()
        if not re.match(r"^[A-Z0-9]{4,7}$", v):
            raise ValueError(f"Invalid procedure code format: '{v}'")
        return v

    @field_validator("fee_amount", mode="before")
    @classmethod
    def clean_fee(cls, v) -> float:
        if isinstance(v, str):
            v = v.replace("$", "").replace(",", "").strip()
        return float(v)

    @field_validator("modifier", mode="before")
    @classmethod
    def clean_modifier(cls, v) -> Optional[str]:
        if v is None:
            return None
        v = str(v).strip()
        return v if v else None


# Canonical schema description injected into every LLM prompt
SCHEMA_DESCRIPTION = """
Canonical output fields — all records must use these exact key names:

  procedure_code  (str)   : HCPCS or CPT code, e.g. "D0120" or "99213"
  modifier        (str)   : optional procedure modifier, e.g. "TC" — use null if absent
  fee_amount      (float) : reimbursement rate as a plain number, e.g. 45.50
  effective_date  (str)   : ISO date string YYYY-MM-DD — use null if not present
  end_date        (str)   : ISO date string YYYY-MM-DD — use null if not present

Do NOT include state_code or dataset_type — those are added automatically.
"""
