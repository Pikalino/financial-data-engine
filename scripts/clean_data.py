import hashlib
from pathlib import Path

import pandas as pd

RAW_PATH = Path("data/raw/sample_transactions.csv")
OUTPUT_PATH = Path("data/processed/cleaned_transactions.csv")


def classify_type(description: str, amount: float) -> str:
    desc = str(description).lower()
    if "refund" in desc:
        return "refund"
    if amount > 0:
        return "income"
    if amount < 0:
        return "expense"
    return "unknown"


def classify_category(description: str) -> str:
    desc = str(description).lower()
    if "order" in desc:
        return "sales"
    if "rent" in desc:
        return "rent"
    if "supplies" in desc or "packaging" in desc:
        return "supplies"
    if "delivery" in desc or "courier" in desc:
        return "shipping"
    if "ad" in desc or "boost" in desc or "meta" in desc:
        return "marketing"
    if "refund" in desc:
        return "refund"
    return "unknown"


def build_row_hash(row: pd.Series) -> str:
    raw_string = "|".join(
        [
            str(row["date"]),
            str(row["description"]),
            str(row["amount"]),
            str(row["currency"]),
            str(row["payment_method"]),
            str(row["counterparty"]),
            str(row["type"]),
            str(row["category"]),
        ]
    )
    return hashlib.sha256(raw_string.encode("utf-8")).hexdigest()


def main() -> None:
    df = pd.read_csv(RAW_PATH)

    df.columns = [col.strip().lower() for col in df.columns]

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    text_cols = ["description", "currency", "payment_method", "counterparty"]
    for col in text_cols:
        df[col] = df[col].astype(str).str.strip()

    df["type"] = df.apply(lambda row: classify_type(row["description"], row["amount"]), axis=1)
    df["category"] = df["description"].apply(classify_category)

    df["currency"] = df["currency"].str.upper()
    df["payment_method"] = df["payment_method"].str.lower()
    df["counterparty"] = df["counterparty"].str.lower()
    df["source_file"] = RAW_PATH.name

    df["raw_row_hash"] = df.apply(build_row_hash, axis=1)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    print("Cleaned file saved to:", OUTPUT_PATH)
    print(df)


if __name__ == "__main__":
    main()
