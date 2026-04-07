# Hardcoded URL registry — add new states/datasets here.
# file_type: "excel" | "pdf" | "csv"

URL_REGISTRY = [
    {
        "state_code": "FL",
        "dataset_type": "dental",
        "url": "https://ahca.myflorida.com/medicaid/fee_schedules/dental_2024.xlsx",
        "file_type": "excel",
    },
    {
        "state_code": "AL",
        "dataset_type": "physician",
        "url": "https://medicaid.alabama.gov/content/Docs/physician_fee_schedule.pdf",
        "file_type": "pdf",
    },
    {
        "state_code": "TX",
        "dataset_type": "pharmacy",
        "url": "https://www.txhhs.com/medicaid/rates/pharmacy_2024.csv",
        "file_type": "csv",
    },
    # Add more entries as needed:
    # {
    #     "state_code": "CA",
    #     "dataset_type": "dental",
    #     "url": "https://...",
    #     "file_type": "excel",
    # },
]
