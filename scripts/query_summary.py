from sqlalchemy import func
from app.database import SessionLocal
from app.models import Transaction


def main() -> None:
    session = SessionLocal()

    try:
        total_income = session.query(func.sum(Transaction.amount)).filter(Transaction.type == "income").scalar() or 0
        total_expense = session.query(func.sum(Transaction.amount)).filter(Transaction.type == "expense").scalar() or 0
        total_refund = session.query(func.sum(Transaction.amount)).filter(Transaction.type == "refund").scalar() or 0

        print("Finance Summary")
        print("-------------------")
        print(f"Total income:   {total_income}")
        print(f"Total expense:  {total_expense}")
        print(f"Total refund:   {total_refund}")
        print(f"Net profit:     {total_income + total_expense + total_refund}")

    finally:
        session.close()


if __name__ == "__main__":
    main()
