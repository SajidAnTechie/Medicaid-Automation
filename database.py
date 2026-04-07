import os
import json
from contextlib import contextmanager
from datetime import date, datetime

from sqlalchemy import Boolean, Date, DateTime, Text, UniqueConstraint, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:postgres@localhost:5432/sentinel_state",
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


class Base(DeclarativeBase):
    pass


class StateRegistry(Base):
    __tablename__ = "state_registry"

    id: Mapped[int] = mapped_column(primary_key=True)
    state_name: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    state_home_link: Mapped[str] = mapped_column(Text, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class SourceMetadata(Base):
    __tablename__ = "source_metadata"
    __table_args__ = (UniqueConstraint("state_name", "source_url", name="uq_source_metadata_state_url"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    state_name: Mapped[str] = mapped_column(Text, nullable=False)
    source_name: Mapped[str] = mapped_column(Text, nullable=False)
    source_table_name: Mapped[str] = mapped_column(Text, nullable=False)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    discovered_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    content_type: Mapped[str | None] = mapped_column(Text)
    last_hash: Mapped[str | None] = mapped_column(Text)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    last_extracted_at: Mapped[datetime | None] = mapped_column(DateTime)
    extraction_status: Mapped[str] = mapped_column(Text, nullable=False, default="discovered")
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class MappingColumn(Base):
    __tablename__ = "mapping_column"
    __table_args__ = (
        UniqueConstraint(
            "state_name",
            "source_url",
            "raw_column",
            name="uq_mapping_column_state_source_raw",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    state_name: Mapped[str] = mapped_column(Text, nullable=False)
    source_name: Mapped[str] = mapped_column(Text, nullable=False)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    raw_column: Mapped[str] = mapped_column(Text, nullable=False)
    canonical_column: Mapped[str] = mapped_column(Text, nullable=False)
    confidence: Mapped[float] = mapped_column(nullable=False, default=0.0)
    rationale: Mapped[str | None] = mapped_column(Text)
    approved: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


class AgentMemory(Base):
    """Persistent agent memory: stores observations, decisions, fact-checks across runs."""
    __tablename__ = "agent_memory"
    __table_args__ = (UniqueConstraint("state_name", "agent_id", "memory_key", name="uq_agent_memory_key"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    state_name: Mapped[str] = mapped_column(Text, nullable=False)
    agent_id: Mapped[str] = mapped_column(Text, nullable=False)
    memory_key: Mapped[str] = mapped_column(Text, nullable=False)
    memory_value: Mapped[str] = mapped_column(Text, nullable=False)
    confidence: Mapped[float] = mapped_column(nullable=False, default=0.0)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


class AgentHandoff(Base):
    """Agent-to-agent messages and context passing."""
    __tablename__ = "agent_handoff"

    id: Mapped[int] = mapped_column(primary_key=True)
    state_name: Mapped[str] = mapped_column(Text, nullable=False)
    from_agent: Mapped[str] = mapped_column(Text, nullable=False)
    to_agent: Mapped[str] = mapped_column(Text, nullable=False)
    message_type: Mapped[str] = mapped_column(Text, nullable=False)
    message_body: Mapped[str] = mapped_column(Text, nullable=False)
    priority: Mapped[int] = mapped_column(nullable=False, default=0)
    acknowledged: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime)


class GoldMedicaidRate(Base):
    """Canonical fact table: all states' merged fee schedules with normalized columns."""
    __tablename__ = "gold_medicaid_rates"
    __table_args__ = (
        UniqueConstraint(
            "state_id",
            "dataset_type",
            "procedure_code",
            "modifier",
            "effective_date",
            "row_hash",
            name="uq_gold_rates_version",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    state_id: Mapped[int] = mapped_column(nullable=False)
    state_name: Mapped[str] = mapped_column(Text, nullable=False)
    dataset_type: Mapped[str] = mapped_column(Text, nullable=False)
    procedure_code: Mapped[str] = mapped_column(Text, nullable=False)
    modifier: Mapped[str] = mapped_column(Text, nullable=False, default="")
    description: Mapped[str | None] = mapped_column(Text)
    fee_amount: Mapped[str | None] = mapped_column(Text)
    effective_date: Mapped[str | None] = mapped_column(Text)
    end_date: Mapped[date | None] = mapped_column(Date)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    ingestion_timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    agent_version: Mapped[str] = mapped_column(Text, nullable=False)
    row_hash: Mapped[str] = mapped_column(Text, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class CanonicalColumnMapping(Base):
    """
    Cross-state column normalization: maps raw column names to canonical names.
    Reference standard set by the first state loaded for each dataset_type.
    """
    __tablename__ = "canonical_column_mapping"
    __table_args__ = (
        UniqueConstraint(
            "dataset_type",
            "reference_state",
            "state_name",
            "source_column_name",
            name="uq_canonical_mapping",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    dataset_type: Mapped[str] = mapped_column(Text, nullable=False)
    reference_state: Mapped[str] = mapped_column(Text, nullable=False)  # First state for this dataset_type
    state_name: Mapped[str] = mapped_column(Text, nullable=False)  # State with this source column
    source_column_name: Mapped[str] = mapped_column(Text, nullable=False)  # Raw column name
    canonical_column_name: Mapped[str] = mapped_column(Text, nullable=False)  # Normalized name
    confidence: Mapped[float] = mapped_column(nullable=False, default=0.9)  # LLM confidence
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


@contextmanager
def get_session():
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
