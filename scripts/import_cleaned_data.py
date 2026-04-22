import pandas as pd
from sqlalchemy.exc import IntegrityError

from app.database import SessionLocal
from app.models import Transaction

CLEANED_CSV_PATH = "data/processed/cleaned_transactions.csv"


def main() -> None:
    df = pd.read_csv(CLEANED_CSV_PATH)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    session = SessionLocal()
    inserted_count = 0
    skipped_count = 0

    try:
        for _, row in df.iterrows():
            transaction = Transaction(
                date=row["date"],
                description=row["description"],
                amount=float(row["amount"]),
                currency=row["currency"],
                payment_method=row.get("payment_method"),
                counterparty=row.get("counterparty"),
                type=row["type"],
                category=row["category"],
                source_file=row["source_file"],
                raw_row_hash=row["raw_row_hash"],
            )

            session.add(transaction)

            try:
                session.commit()
                inserted_count += 1
            except IntegrityError:
                session.rollback()
                skipped_count += 1

        print(f"Inserted: {inserted_count}")
        print(f"Skipped duplicates: {skipped_count}")

    finally:
        session.close()


if __name__ == "__main__":
    main()
