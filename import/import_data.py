import os
import math
from datetime import datetime
from decimal import Decimal, InvalidOperation

import pandas as pd
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from bson.decimal128 import Decimal128

DB_NAME = "projectdb"

PATIENTS_FILE = os.environ.get("PATIENTS_FILE", "/app/Data/patients.csv")
ENCOUNTERS_FILE = os.environ.get("ENCOUNTERS_FILE", "/app/Data/encounters.csv")
CLAIMS_FILE = os.environ.get("CLAIMS_FILE", "/app/Data/claims_and_billing.csv")

MONGO_URI = os.environ["MONGO_URI"]
BATCH_SIZE = int(os.environ.get("IMPORT_BATCH_SIZE", "1000"))

EXPECTED_COUNTS = {
    "patients": 60000,
    "encounters": 70000,
    "claims": 70000,
}

def log(msg: str) -> None:
    print(f"[data-import] {msg}", flush=True)

def is_data_already_loaded(db) -> bool:
    counts = {
        "patients": db.patients.count_documents({}),
        "encounters": db.encounters.count_documents({}),
        "claims": db.claims.count_documents({}),
    }

    log(
        "Current collection counts: "
        f"patients={counts['patients']}, "
        f"encounters={counts['encounters']}, "
        f"claims={counts['claims']}"
    )

    if (
        counts["patients"] >= EXPECTED_COUNTS["patients"]
        and counts["encounters"] >= EXPECTED_COUNTS["encounters"]
        and counts["claims"] >= EXPECTED_COUNTS["claims"]
    ):
        log(
            "Data import not needed. Expected minimum counts are already present "
            f"({EXPECTED_COUNTS})."
        )
        return True

    log(
        "Data import is needed because one or more collections are below the expected "
        f"minimum counts ({EXPECTED_COUNTS})."
    )
    return False

def is_missing(value) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if pd.isna(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def clean_string(value):
    if is_missing(value):
        return None
    return str(value).strip()


def parse_date(value):
    if is_missing(value):
        return None
    text = str(value).strip()
    if not text:
        return None

    # set dateFirst as true
    dt = pd.to_datetime(text, errors="coerce", dayfirst=True)
    if pd.isna(dt):
        return None

    # convert pandas Timestamp to python datetime
    return dt.to_pydatetime()


def parse_int(value):
    if is_missing(value):
        return None
    try:
        return int(float(str(value).strip()))
    except (ValueError, TypeError):
        return None


def parse_bool(value):
    if is_missing(value):
        return None

    if isinstance(value, bool):
        return value

    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y", "t"}:
        return True
    if text in {"false", "0", "no", "n", "f"}:
        return False

    return None


def parse_decimal128(value):
    if is_missing(value):
        return None

    text = str(value).strip().replace(",", "")
    try:
        dec = Decimal(text)
        return Decimal128(dec)
    except (InvalidOperation, ValueError):
        return None


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(col).strip() for col in df.columns]
    return df


def read_csv(path: str) -> pd.DataFrame:
    log(f"Reading {path}")
    df = pd.read_csv(path)
    df = normalize_columns(df)
    log(f"Loaded {len(df)} rows from {path}")
    return df


def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def clear_collection(coll):
    result = coll.delete_many({})
    log(f"Cleared {coll.full_name}: deleted {result.deleted_count} documents")


def insert_in_batches(coll, docs, batch_size: int):
    total = len(docs)
    inserted = 0

    for batch in chunked(docs, batch_size):
        try:
            coll.insert_many(batch, ordered=False)
            inserted += len(batch)
            log(f"Inserted {inserted}/{total} into {coll.full_name}")
        except BulkWriteError as e:
            log(f"Bulk write error on {coll.full_name}: {e.details}")
            raise


def transform_patients(df: pd.DataFrame):
    docs = []

    for _, row in df.iterrows():
        doc = {
            "patient_id": clean_string(row.get("patient_id")),
            "first_name": clean_string(row.get("first_name")),
            "last_name": clean_string(row.get("last_name")),
            "dob": parse_date(row.get("dob")),
            "age": parse_int(row.get("age")),
            "gender": clean_string(row.get("gender")),
            "ethnicity": clean_string(row.get("ethnicity")),
            "insurance_type": clean_string(row.get("insurance_type")),
            "marital_status": clean_string(row.get("marital_status")),
            "contact": {
                "address": clean_string(row.get("address")),
                "city": clean_string(row.get("city")),
                "state": clean_string(row.get("state")),
                "zip": clean_string(row.get("zip")),
                "phone": clean_string(row.get("phone")),
                "email": clean_string(row.get("email")),
            },
            "registration_date": parse_date(row.get("registration_date")),
        }

        # keep contact object, even if some fields are null, because validator allows it
        docs.append(doc)

    return docs


def transform_encounters(df: pd.DataFrame):
    docs = []

    for _, row in df.iterrows():
        admission = {
            "admission_type": clean_string(row.get("admission_type")),
            "discharge_date": parse_date(row.get("discharge_date")),
            "length_of_stay": parse_int(row.get("length_of_stay")),
        }

        if all(v is None for v in admission.values()):
            admission = None

        doc = {
            "encounter_id": clean_string(row.get("encounter_id")),
            "patient_id": clean_string(row.get("patient_id")),
            "provider_id": clean_string(row.get("provider_id")),
            "visit_date": parse_date(row.get("visit_date")),
            "visit_type": clean_string(row.get("visit_type")),
            "department": clean_string(row.get("department")),
            "reason_for_visit": clean_string(row.get("reason_for_visit")),
            "diagnosis_code": clean_string(row.get("diagnosis_code")),
            "admission": admission,
            "status": clean_string(row.get("status")),
            "readmitted_flag": parse_bool(row.get("readmitted_flag")),
        }

        docs.append(doc)

    return docs


def transform_claims(df: pd.DataFrame):
    docs = []

    for _, row in df.iterrows():
        claim = {
            "claim_id": clean_string(row.get("claim_id")),
            "claim_billing_date": parse_date(row.get("claim_billing_date")),
            "claim_status": clean_string(row.get("claim_status")),
            "denial_reason": clean_string(row.get("denial_reason")),
        }

        amounts = {
            "billed_amount": parse_decimal128(row.get("billed_amount")),
            "paid_amount": parse_decimal128(row.get("paid_amount")),
        }

        doc = {
            "billing_id": clean_string(row.get("billing_id")),
            "patient_id": clean_string(row.get("patient_id")),
            "encounter_id": clean_string(row.get("encounter_id")),
            "insurance_provider": clean_string(row.get("insurance_provider")),
            "payment_method": clean_string(row.get("payment_method")),
            "claim": claim,
            "amounts": amounts,
        }

        docs.append(doc)

    return docs


def validate_required_ids(docs, collection_name, required_fields):
    bad = 0
    for doc in docs:
        for field in required_fields:
            if doc.get(field) is None:
                bad += 1
                break

    if bad > 0:
        raise ValueError(
            f"{collection_name}: found {bad} documents with missing required id fields: {required_fields}"
        )


def main():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    log("Connected to MongoDB through mongos")

    # checks if data is loaded (no sofisticated check, just if the expected count is there)
    if is_data_already_loaded(db):
        return

    patients_df = read_csv(PATIENTS_FILE)
    encounters_df = read_csv(ENCOUNTERS_FILE)
    claims_df = read_csv(CLAIMS_FILE)
    
    log("Starting data transformation")
    patients_docs = transform_patients(patients_df)
    encounters_docs = transform_encounters(encounters_df)
    claims_docs = transform_claims(claims_df)

    log("Starting id validation")
    validate_required_ids(patients_docs, "patients", ["patient_id"])
    validate_required_ids(encounters_docs, "encounters", ["encounter_id", "patient_id"])
    validate_required_ids(claims_docs, "claims", ["billing_id", "patient_id", "encounter_id"])

    log("Starting collection clearing")
    clear_collection(db.patients)
    clear_collection(db.encounters)
    clear_collection(db.claims)

    log("Starting insert into batches")
    insert_in_batches(db.patients, patients_docs, BATCH_SIZE)
    insert_in_batches(db.encounters, encounters_docs, BATCH_SIZE)
    insert_in_batches(db.claims, claims_docs, BATCH_SIZE)

    log(f"Final counts: patients={db.patients.count_documents({})}, "
        f"encounters={db.encounters.count_documents({})}, "
        f"claims={db.claims.count_documents({})}")

    log("Import finished successfully")


if __name__ == "__main__":
    main()