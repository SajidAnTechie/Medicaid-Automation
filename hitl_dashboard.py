import os

import pandas as pd
import streamlit as st
from sqlalchemy import text

from database import engine

CANONICAL_FIELDS = [
    "procedure_code",
    "modifier",
    "description",
    "fee_amount",
    "effective_date",
    "end_date",
]

THRESHOLD = float(os.getenv("ANALYST_CONFIDENCE_THRESHOLD", "85"))

st.set_page_config(page_title="Sentinel HITL Review", layout="wide")
st.title("Sentinel-State HITL Mapping Review")
st.caption("Approve or correct low-confidence mappings.")


def load_pending() -> pd.DataFrame:
    with engine.begin() as connection:
        rows = connection.execute(
            text(
                """
                SELECT
                    id,
                    state_name,
                    source_name,
                    source_url,
                    raw_column,
                    canonical_column,
                    confidence,
                    approved,
                    rationale,
                    updated_at
                FROM mapping_column
                WHERE approved = FALSE
                   OR confidence < :threshold
                ORDER BY updated_at DESC
                LIMIT 500
                """
            ),
            {"threshold": THRESHOLD},
        ).mappings().all()
    return pd.DataFrame(rows)


def apply_review(row_id: int, canonical_column: str, note: str) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE mapping_column
                SET canonical_column = :canonical_column,
                    approved = TRUE,
                    rationale = :rationale,
                    updated_at = NOW()
                WHERE id = :id
                """
            ),
            {
                "id": row_id,
                "canonical_column": canonical_column,
                "rationale": note.strip() or "HITL approved via Streamlit dashboard",
            },
        )


pending_df = load_pending()

if pending_df.empty:
    st.success("No pending review items. All mappings are approved above threshold.")
    st.stop()

st.subheader(f"Pending Rows: {len(pending_df)}")
state_filter = st.selectbox("Filter by state", ["all"] + sorted(pending_df["state_name"].dropna().unique().tolist()))
if state_filter != "all":
    pending_df = pending_df[pending_df["state_name"] == state_filter].copy()

for _, row in pending_df.iterrows():
    with st.container(border=True):
        st.markdown(
            f"**{row['state_name']} | {row['source_name']}**  "
            f"`raw: {row['raw_column']}` -> `proposed: {row['canonical_column']}`  "
            f"(confidence={row['confidence']})"
        )
        col1, col2 = st.columns([2, 3])
        with col1:
            selected = st.selectbox(
                f"Canonical field for row {row['id']}",
                CANONICAL_FIELDS,
                index=CANONICAL_FIELDS.index(row["canonical_column"]) if row["canonical_column"] in CANONICAL_FIELDS else 0,
                key=f"canon_{row['id']}",
            )
        with col2:
            note = st.text_input(
                f"Review note for row {row['id']}",
                value="HITL approved via dashboard",
                key=f"note_{row['id']}",
            )
        if st.button(f"Approve Row {row['id']}", key=f"approve_{row['id']}"):
            apply_review(int(row["id"]), selected, note)
            st.success(f"Row {row['id']} approved")
            st.rerun()
