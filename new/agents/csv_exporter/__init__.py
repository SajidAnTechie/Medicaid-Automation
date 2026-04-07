from .models import CSVExporterInput, CSVExporterOutput


def __getattr__(name: str):
    """Lazy-import the entrypoint module to avoid RuntimeWarning when
    running via ``python -m agents.csv_exporter.raw_csv_exporter``."""
    if name == "get_csv_exporter_agent":
        from .raw_csv_exporter import get_csv_exporter_agent
        return get_csv_exporter_agent
    if name == "run_csv_exporter":
        from .raw_csv_exporter import run_csv_exporter
        return run_csv_exporter
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["get_csv_exporter_agent", "run_csv_exporter",
           "CSVExporterInput", "CSVExporterOutput"]
