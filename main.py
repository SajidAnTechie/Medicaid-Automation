import argparse
import json
import os
import time
import uuid

from dotenv import load_dotenv

load_dotenv()  # load .env when running locally

from sqlalchemy import text

from database import Base, SessionLocal, StateRegistry, engine
from graph import build_graph


DEFAULT_STATES = [
    {
        "state_name": "alaska",
        "state_home_link": "https://extranet-sp.dhss.alaska.gov/hcs/medicaidalaska/Provider/Sites/FeeSchedule.html",
    },
    {
        "state_name": "arizona",
        "state_home_link": "https://www.azahcccs.gov/PlansProviders/RatesAndBilling/FFS/DurableMedEquip.html",
    },
]


def wait_for_database(max_retries: int = 30, sleep_seconds: int = 2) -> None:
    for attempt in range(1, max_retries + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("Database is reachable.")
            return
        except Exception as exc:
            print(f"Waiting for database ({attempt}/{max_retries}): {exc}")
            time.sleep(sleep_seconds)
    raise RuntimeError("Database connection could not be established.")


def load_active_states(state_id: int | None = None, run_all: bool = False) -> list[StateRegistry]:
    with SessionLocal() as session:
        query = session.query(StateRegistry).filter(StateRegistry.is_active.is_(True))
        if state_id is not None:
            query = query.filter(StateRegistry.id == state_id)
        elif not run_all:
            return []
        return query.all()


def seed_state_registry() -> None:
    with SessionLocal() as session:
        for item in DEFAULT_STATES:
            session.execute(
                text(
                    """
                    INSERT INTO state_registry (state_name, state_home_link, is_active)
                    VALUES (:state_name, :state_home_link, TRUE)
                    ON CONFLICT (state_name) DO UPDATE
                    SET state_home_link = EXCLUDED.state_home_link,
                        is_active = EXCLUDED.is_active
                    """
                ),
                item,
            )
        session.commit()


def _parse_event_payload(event_json: str) -> dict[str, str] | None:
    if not event_json.strip():
        return None
    try:
        payload = json.loads(event_json)
    except json.JSONDecodeError:
        return None

    # Supports either direct payload or EventBridge-style detail envelope.
    body = payload.get("detail", payload) if isinstance(payload, dict) else {}
    if not isinstance(body, dict):
        return None

    state_name = str(body.get("state_name") or body.get("state") or "").strip().lower()
    state_home_link = str(body.get("state_home_link") or body.get("portal_url") or "").strip()
    dataset_category = str(body.get("dataset_category") or body.get("dataset_type") or "").strip()
    run_id = str(body.get("run_id") or body.get("event_id") or "").strip()
    if not state_name or not state_home_link:
        return None

    return {
        "state_name": state_name,
        "state_home_link": state_home_link,
        "dataset_category": dataset_category,
        "run_id": run_id,
    }


def _upsert_state_from_event(event_payload: dict[str, str]) -> int:
    with SessionLocal() as session:
        row = session.execute(
            text(
                """
                INSERT INTO state_registry (state_name, state_home_link, is_active)
                VALUES (:state_name, :state_home_link, TRUE)
                ON CONFLICT (state_name) DO UPDATE
                SET state_home_link = EXCLUDED.state_home_link,
                    is_active = TRUE
                RETURNING id
                """
            ),
            {
                "state_name": event_payload["state_name"],
                "state_home_link": event_payload["state_home_link"],
            },
        ).fetchone()
        session.commit()
        return int(row[0])


def run_ingestion_cycle(
    state_id: int | None = None,
    run_all: bool = False,
    event_payload: dict[str, str] | None = None,
) -> None:
    wait_for_database()
    Base.metadata.create_all(bind=engine)
    seed_state_registry()

    app = build_graph()
    active_states = load_active_states(state_id=state_id, run_all=run_all)

    if event_payload is not None:
        event_state_id = _upsert_state_from_event(event_payload)
        active_states = load_active_states(state_id=event_state_id, run_all=False)

    if not active_states:
        print("No states selected. Pass --state-id <id> or --all.")
        return

    runtime_mode = os.getenv("RUNTIME_MODE", "local").strip().lower() or "local"

    for state_row in active_states:
        run_id = event_payload.get("run_id", "").strip() if event_payload is not None else ""
        if not run_id:
            run_id = f"{state_row.state_name}-{uuid.uuid4().hex[:10]}"
        initial_state = {
            "state_id": state_row.id,
            "state_name": state_row.state_name,
            "state_home_link": state_row.state_home_link,
            "run_id": run_id,
            "runtime_mode": runtime_mode,
            "dataset_category": (event_payload or {}).get("dataset_category", ""),
            "status": "queued",
            "log": [],
        }
        print(f"Starting ingestion for {state_row.state_name} (run_id={run_id}, mode={runtime_mode})...")
        result = app.invoke(initial_state)

        print(f"Final status for {state_row.state_name}: {result.get('status')}")
        for log_line in result.get("log", []):
            print(f"  - {log_line}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Sentinel-State ingestion")
    parser.add_argument("--state-id", type=int, default=None, help="Active state_registry.id to ingest")
    parser.add_argument("--all", action="store_true", help="Run ingestion for all active states")
    parser.add_argument("--idle", action="store_true", help="Keep the worker container running without ingestion")
    parser.add_argument("--event-json", type=str, default="", help="Event payload JSON (EventBridge style or direct)")
    parser.add_argument("--event-file", type=str, default="", help="Path to JSON payload file")
    return parser.parse_args()


def idle_forever() -> None:
    print("Worker is idle. Run ingestion with: docker compose run --rm worker python main.py --state-id <id>")
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    args = parse_args()
    env_state_id = os.getenv("INGEST_STATE_ID")
    state_id = args.state_id if args.state_id is not None else (int(env_state_id) if env_state_id else None)

    raw_event_json = args.event_json.strip()
    if args.event_file.strip():
        with open(args.event_file.strip(), "r", encoding="utf-8") as fp:
            raw_event_json = fp.read()
    event_payload = _parse_event_payload(raw_event_json)

    if args.idle:
        idle_forever()
    else:
        run_ingestion_cycle(state_id=state_id, run_all=args.all, event_payload=event_payload)
