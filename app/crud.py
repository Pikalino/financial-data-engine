from datetime import datetime, timedelta
import statistics

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models import Transaction


def apply_transaction_filters(
    query,
    user_id: int,
    transaction_type: str | None = None,
    category: str | None = None,
    payment_method: str | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
):
    query = query.filter(Transaction.user_id == user_id)

    if transaction_type:
        query = query.filter(Transaction.type == transaction_type)

    if category:
        query = query.filter(Transaction.category == category)

    if payment_method:
        query = query.filter(Transaction.payment_method == payment_method)

    if start_date:
        query = query.filter(Transaction.date >= start_date)

    if end_date:
        end_date_exclusive = end_date + timedelta(days=1)
        query = query.filter(Transaction.date < end_date_exclusive)

    return query


def get_transactions(
    db: Session,
    user_id: int,
    transaction_type: str | None = None,
    category: str | None = None,
    payment_method: str | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
):
    query = db.query(Transaction)
    query = apply_transaction_filters(
        query,
        user_id=user_id,
        transaction_type=transaction_type,
        category=category,
        payment_method=payment_method,
        start_date=start_date,
        end_date=end_date,
    )
    return query.order_by(Transaction.date.asc(), Transaction.id.asc()).all()


def get_import_batch_by_batch_id(db: Session, batch_id: str, user_id: int):
    from app.models import ImportBatch

    return (
        db.query(ImportBatch)
        .filter(
            ImportBatch.batch_id == batch_id,
            ImportBatch.user_id == user_id,
        )
        .first()
    )


def get_transactions_by_import_batch_id(db: Session, import_batch_db_id: int, user_id: int):
    return (
        db.query(Transaction)
        .filter(
            Transaction.import_batch_id == import_batch_db_id,
            Transaction.user_id == user_id,
        )
        .order_by(Transaction.date.asc(), Transaction.id.asc())
        .all()
    )


def get_summary(
    db: Session,
    user_id: int,
    transaction_type: str | None = None,
    category: str | None = None,
    payment_method: str | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
):
    base_query = db.query(Transaction)
    base_query = apply_transaction_filters(
        base_query,
        user_id=user_id,
        transaction_type=transaction_type,
        category=category,
        payment_method=payment_method,
        start_date=start_date,
        end_date=end_date,
    )

    total_income = (
        base_query.filter(Transaction.type == "income")
        .with_entities(func.sum(Transaction.amount))
        .scalar()
        or 0.0
    )
    total_expense = (
        base_query.filter(Transaction.type == "expense")
        .with_entities(func.sum(Transaction.amount))
        .scalar()
        or 0.0
    )
    total_refund = (
        base_query.filter(Transaction.type == "refund")
        .with_entities(func.sum(Transaction.amount))
        .scalar()
        or 0.0
    )

    return {
        "total_income": float(total_income),
        "total_expense": float(total_expense),
        "total_refund": float(total_refund),
        "net_profit": float(total_income + total_expense + total_refund),
    }


def bulk_update_transaction_category(
    db: Session,
    user_id: int,
    transaction_ids: list[int],
    category: str,
):
    if not transaction_ids:
        return 0

    updated_count = (
        db.query(Transaction)
        .filter(
            Transaction.id.in_(transaction_ids),
            Transaction.user_id == user_id,
        )
        .update(
            {"category": category.strip().lower()},
            synchronize_session=False,
        )
    )

    db.commit()
    return updated_count


def get_transaction_by_id(db: Session, transaction_id: int, user_id: int):
    return (
        db.query(Transaction)
        .filter(
            Transaction.id == transaction_id,
            Transaction.user_id == user_id,
        )
        .first()
    )


def update_transaction(
    db: Session,
    transaction,
    description: str,
    amount: float,
    currency: str,
    payment_method: str,
    counterparty: str,
    transaction_type: str,
    category: str,
):
    transaction.description = description.strip()
    transaction.amount = float(amount)
    transaction.currency = currency.strip().upper()
    transaction.payment_method = payment_method.strip().lower()
    transaction.counterparty = counterparty.strip().lower()
    transaction.type = transaction_type.strip().lower()
    transaction.category = category.strip().lower()

    db.commit()
    db.refresh(transaction)
    return transaction


def get_summary_by_category(
    db: Session,
    user_id: int,
    transaction_type: str | None = None,
    payment_method: str | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
):
    query = db.query(
        Transaction.category,
        func.sum(Transaction.amount).label("total_amount"),
    )

    query = apply_transaction_filters(
        query,
        user_id=user_id,
        transaction_type=transaction_type,
        payment_method=payment_method,
        start_date=start_date,
        end_date=end_date,
    )

    results = (
        query.group_by(Transaction.category)
        .order_by(Transaction.category.asc())
        .all()
    )

    return [
        {
            "category": row.category,
            "total_amount": float(row.total_amount),
        }
        for row in results
    ]


def get_recent_import_batches(db: Session, user_id: int, limit: int = 10):
    from app.models import ImportBatch

    return (
        db.query(ImportBatch)
        .filter(ImportBatch.user_id == user_id)
        .order_by(ImportBatch.uploaded_at.desc())
        .limit(limit)
        .all()
    )


def get_summary_by_day(
    db: Session,
    user_id: int,
    transaction_type: str | None = None,
    category: str | None = None,
    payment_method: str | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
):
    day_expr = func.date(Transaction.date)

    query = db.query(
        day_expr.label("date"),
        func.sum(Transaction.amount).label("total_amount"),
    )

    query = apply_transaction_filters(
        query,
        user_id=user_id,
        transaction_type=transaction_type,
        category=category,
        payment_method=payment_method,
        start_date=start_date,
        end_date=end_date,
    )

    results = (
        query.group_by(day_expr)
        .order_by(day_expr.asc())
        .all()
    )

    return [
    {
        "date": str(row.date),
        "total_amount": float(row.total_amount),
    }
    for row in results
]


def get_category_rules(db: Session, user_id: int):
    from app.models import CategoryRule

    return (
        db.query(CategoryRule)
        .filter(CategoryRule.user_id == user_id)
        .order_by(CategoryRule.priority.asc(), CategoryRule.keyword.asc())
        .all()
    )


def create_category_rule(
    db: Session,
    user_id: int,
    keyword: str,
    category: str,
    priority: int = 100,
    is_active: bool = True,
):
    from app.models import CategoryRule

    rule = CategoryRule(
        user_id=user_id,
        keyword=keyword.strip().lower(),
        category=category.strip().lower(),
        priority=priority,
        is_active=is_active,
    )
    db.add(rule)
    db.commit()
    db.refresh(rule)
    return rule


def apply_category_rules_to_all_transactions(db: Session, user_id: int):
    from app.models import CategoryRule

    rules = (
        db.query(CategoryRule)
        .filter(
            CategoryRule.user_id == user_id,
            CategoryRule.is_active == True,
        )
        .order_by(CategoryRule.priority.asc(), CategoryRule.keyword.asc())
        .all()
    )

    transactions = (
        db.query(Transaction)
        .filter(Transaction.user_id == user_id)
        .all()
    )

    updated_count = 0

    for tx in transactions:
        description = (tx.description or "").lower()

        for rule in rules:
            if rule.keyword in description:
                if tx.category != rule.category:
                    tx.category = rule.category
                    updated_count += 1
                break

    db.commit()
    return updated_count


def get_summary_by_month(
    db: Session,
    user_id: int,
    transaction_type: str | None = None,
    category: str | None = None,
    payment_method: str | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
):
    from collections import defaultdict

    query = db.query(Transaction)
    query = apply_transaction_filters(
        query,
        user_id=user_id,
        transaction_type=transaction_type,
        category=category,
        payment_method=payment_method,
        start_date=start_date,
        end_date=end_date,
    )

    transactions = query.all()

    monthly_data = defaultdict(
        lambda: {
            "total_income": 0.0,
            "total_expense": 0.0,
            "total_refund": 0.0,
            "net_profit": 0.0,
        }
    )

    for tx in transactions:
        month_key = tx.date.strftime("%Y-%m")

        if tx.type == "income":
            monthly_data[month_key]["total_income"] += float(tx.amount)
        elif tx.type == "expense":
            monthly_data[month_key]["total_expense"] += float(tx.amount)
        elif tx.type == "refund":
            monthly_data[month_key]["total_refund"] += float(tx.amount)

        monthly_data[month_key]["net_profit"] += float(tx.amount)

    results = []
    for month in sorted(monthly_data.keys()):
        row = monthly_data[month]
        results.append(
            {
                "month": month,
                "total_income": row["total_income"],
                "total_expense": row["total_expense"],
                "total_refund": row["total_refund"],
                "net_profit": row["net_profit"],
            }
        )

    return results


def get_summary_by_payment_method(
    db: Session,
    user_id: int,
    transaction_type: str | None = None,
    category: str | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
):
    query = db.query(
        Transaction.payment_method,
        func.sum(Transaction.amount).label("total_amount"),
    )

    query = apply_transaction_filters(
        query,
        user_id=user_id,
        transaction_type=transaction_type,
        category=category,
        payment_method=None,
        start_date=start_date,
        end_date=end_date,
    )

    results = (
        query.group_by(Transaction.payment_method)
        .order_by(Transaction.payment_method.asc())
        .all()
    )

    return [
        {
            "payment_method": row.payment_method or "unknown",
            "total_amount": float(row.total_amount),
        }
        for row in results
    ]


def get_top_counterparties(
    db: Session,
    user_id: int,
    transaction_type: str | None = None,
    category: str | None = None,
    payment_method: str | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    limit: int = 10,
):
    query = db.query(
        Transaction.counterparty,
        func.sum(Transaction.amount).label("total_amount"),
    )

    query = apply_transaction_filters(
        query,
        user_id=user_id,
        transaction_type=transaction_type,
        category=category,
        payment_method=payment_method,
        start_date=start_date,
        end_date=end_date,
    )

    results = (
        query.group_by(Transaction.counterparty)
        .order_by(func.abs(func.sum(Transaction.amount)).desc())
        .limit(limit)
        .all()
    )

    return [
        {
            "counterparty": row.counterparty or "unknown",
            "total_amount": float(row.total_amount),
        }
        for row in results
    ]


def get_category_trend_by_month(
    db: Session,
    user_id: int,
    transaction_type: str | None = None,
    payment_method: str | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
):
    query = db.query(Transaction)

    query = apply_transaction_filters(
        query,
        user_id=user_id,
        transaction_type=transaction_type,
        category=None,
        payment_method=payment_method,
        start_date=start_date,
        end_date=end_date,
    )

    transactions = query.all()

    trend_map: dict[tuple[str, str], float] = {}

    for tx in transactions:
        month = tx.date.strftime("%Y-%m")
        category = tx.category or "unknown"
        key = (month, category)

        if key not in trend_map:
            trend_map[key] = 0.0

        trend_map[key] += float(tx.amount)

    results = []
    for (month, category), total_amount in sorted(trend_map.items()):
        results.append(
            {
                "month": month,
                "category": category,
                "total_amount": total_amount,
            }
        )

    return results


def get_summary_by_week(
    db: Session,
    user_id: int,
    transaction_type: str | None = None,
    category: str | None = None,
    payment_method: str | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
):
    from collections import defaultdict

    query = db.query(Transaction)
    query = apply_transaction_filters(
        query,
        user_id=user_id,
        transaction_type=transaction_type,
        category=category,
        payment_method=payment_method,
        start_date=start_date,
        end_date=end_date,
    )

    transactions = query.all()

    weekly_data = defaultdict(
        lambda: {
            "total_income": 0.0,
            "total_expense": 0.0,
            "total_refund": 0.0,
            "net_profit": 0.0,
        }
    )

    for tx in transactions:
        iso_year, iso_week, _ = tx.date.isocalendar()
        week_key = f"{iso_year}-W{iso_week:02d}"

        if tx.type == "income":
            weekly_data[week_key]["total_income"] += float(tx.amount)
        elif tx.type == "expense":
            weekly_data[week_key]["total_expense"] += float(tx.amount)
        elif tx.type == "refund":
            weekly_data[week_key]["total_refund"] += float(tx.amount)

        weekly_data[week_key]["net_profit"] += float(tx.amount)

    results = []
    for week in sorted(weekly_data.keys()):
        row = weekly_data[week]
        results.append(
            {
                "week": week,
                "total_income": row["total_income"],
                "total_expense": row["total_expense"],
                "total_refund": row["total_refund"],
                "net_profit": row["net_profit"],
            }
        )

    return results


def get_kpi_summary(
    db: Session,
    user_id: int,
    transaction_type: str | None = None,
    category: str | None = None,
    payment_method: str | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
):
    query = db.query(Transaction)
    query = apply_transaction_filters(
        query,
        user_id=user_id,
        transaction_type=transaction_type,
        category=category,
        payment_method=payment_method,
        start_date=start_date,
        end_date=end_date,
    )

    transactions = query.all()

    transaction_count = len(transactions)
    income_transactions = [tx for tx in transactions if tx.type == "income"]
    expense_transactions = [tx for tx in transactions if tx.type == "expense"]
    refund_transactions = [tx for tx in transactions if tx.type == "refund"]

    total_income = sum(float(tx.amount) for tx in income_transactions)
    total_expense = sum(float(tx.amount) for tx in expense_transactions)
    total_refund = sum(float(tx.amount) for tx in refund_transactions)
    total_net = sum(float(tx.amount) for tx in transactions)

    net_margin = (total_net / total_income) if total_income != 0 else 0.0
    expense_ratio = (abs(total_expense) / total_income) if total_income != 0 else 0.0
    refund_rate = (abs(total_refund) / total_income) if total_income != 0 else 0.0
    average_transaction_amount = (total_net / transaction_count) if transaction_count != 0 else 0.0

    return {
        "transaction_count": transaction_count,
        "income_transaction_count": len(income_transactions),
        "expense_transaction_count": len(expense_transactions),
        "refund_transaction_count": len(refund_transactions),
        "net_margin": net_margin,
        "expense_ratio": expense_ratio,
        "refund_rate": refund_rate,
        "average_transaction_amount": average_transaction_amount,
    }


def get_transactions_extended(
    db: Session,
    user_id: int,
    transaction_type: str | None = None,
    category: str | None = None,
    payment_method: str | None = None,
    counterparty: str | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
):
    query = db.query(Transaction)

    query = apply_transaction_filters(
        query,
        user_id=user_id,
        transaction_type=transaction_type,
        category=category,
        payment_method=payment_method,
        start_date=start_date,
        end_date=end_date,
    )

    if counterparty:
        query = query.filter(Transaction.counterparty == counterparty)

    return query.order_by(Transaction.date.asc(), Transaction.id.asc()).all()


def generate_auto_insights(
    summary: dict,
    kpi_summary: dict,
    category_summary: list[dict],
    payment_method_summary: list[dict],
    top_counterparties: list[dict],
    monthly_summary: list[dict],
    weekly_summary: list[dict],
):
    insights = []

    net_profit = summary.get("net_profit", 0.0)

    if net_profit < 0:
        insights.append("Net profit is negative in the current filtered view.")
    elif net_profit > 0:
        insights.append("Net profit is positive in the current filtered view.")

    refund_rate = kpi_summary.get("refund_rate", 0.0)
    if refund_rate >= 0.2:
        insights.append(f"Refund rate is high at {refund_rate * 100:.2f}% of income.")
    elif refund_rate > 0:
        insights.append(f"Refund rate is {refund_rate * 100:.2f}% of income.")

    expense_ratio = kpi_summary.get("expense_ratio", 0.0)
    if expense_ratio >= 0.8:
        insights.append(f"Expense ratio is heavy at {expense_ratio * 100:.2f}% of income.")

    net_margin = kpi_summary.get("net_margin", 0.0)
    if net_margin >= 0.2:
        insights.append(f"Net margin is strong at {net_margin * 100:.2f}%.")
    elif net_margin < 0:
        insights.append(f"Net margin is negative at {net_margin * 100:.2f}%.")

    if category_summary:
        sorted_categories = sorted(category_summary, key=lambda x: abs(x["total_amount"]), reverse=True)
        top_category = sorted_categories[0]
        insights.append(
            f"Highest-impact category is {top_category['category']} with total amount {top_category['total_amount']:.2f}."
        )

        expense_categories = [x for x in category_summary if x["total_amount"] < 0]
        if expense_categories:
            top_expense_category = sorted(expense_categories, key=lambda x: abs(x["total_amount"]), reverse=True)[0]
            insights.append(
                f"Top expense category is {top_expense_category['category']} at {top_expense_category['total_amount']:.2f}."
            )

    if payment_method_summary:
        top_payment_method = sorted(payment_method_summary, key=lambda x: abs(x["total_amount"]), reverse=True)[0]
        insights.append(
            f"Most money is flowing through {top_payment_method['payment_method']} with total amount {top_payment_method['total_amount']:.2f}."
        )

    if top_counterparties:
        top_counterparty = sorted(top_counterparties, key=lambda x: abs(x["total_amount"]), reverse=True)[0]
        insights.append(
            f"Top counterparty is {top_counterparty['counterparty']} with total amount {top_counterparty['total_amount']:.2f}."
        )

    if len(monthly_summary) >= 2:
        last_month = monthly_summary[-1]
        prev_month = monthly_summary[-2]

        delta = last_month["net_profit"] - prev_month["net_profit"]
        if delta > 0:
            insights.append(
                f"Net profit improved by {delta:.2f} from {prev_month['month']} to {last_month['month']}."
            )
        elif delta < 0:
            insights.append(
                f"Net profit declined by {abs(delta):.2f} from {prev_month['month']} to {last_month['month']}."
            )

    if len(weekly_summary) >= 2:
        last_week = weekly_summary[-1]
        prev_week = weekly_summary[-2]

        delta = last_week["net_profit"] - prev_week["net_profit"]
        if delta > 0:
            insights.append(
                f"Weekly net profit improved by {delta:.2f} from {prev_week['week']} to {last_week['week']}."
            )
        elif delta < 0:
            insights.append(
                f"Weekly net profit declined by {abs(delta):.2f} from {prev_week['week']} to {last_week['week']}."
            )

    if not insights:
        insights.append("No strong insights detected for the current filtered view.")

    return [{"message": msg} for msg in insights[:8]]


def detect_anomalies(
    db: Session,
    user_id: int,
    transaction_type: str | None = None,
    category: str | None = None,
    payment_method: str | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
):
    query = db.query(Transaction)
    query = apply_transaction_filters(
        query,
        user_id=user_id,
        transaction_type=transaction_type,
        category=category,
        payment_method=payment_method,
        start_date=start_date,
        end_date=end_date,
    )

    transactions = [tx for tx in query.all() if not tx.is_ignored]

    if len(transactions) < 3:
        return []

    abs_amounts = [abs(float(tx.amount)) for tx in transactions]

    overall_mean = statistics.mean(abs_amounts)
    overall_std = statistics.pstdev(abs_amounts) if len(abs_amounts) > 1 else 0
    overall_threshold = overall_mean + (2 * overall_std)

    category_map: dict[str, list[float]] = {}
    for tx in transactions:
        cat = tx.category or "unknown"
        category_map.setdefault(cat, []).append(abs(float(tx.amount)))

    category_stats: dict[str, dict[str, float]] = {}
    for cat, values in category_map.items():
        mean_val = statistics.mean(values)
        std_val = statistics.pstdev(values) if len(values) > 1 else 0
        category_stats[cat] = {
            "mean": mean_val,
            "std": std_val,
            "threshold": mean_val + (2 * std_val),
        }

    anomalies = []

    for tx in transactions:
        abs_amount = abs(float(tx.amount))
        reasons = []

        if overall_std > 0 and abs_amount > overall_threshold:
            reasons.append("unusually large compared to overall transactions")

        cat = tx.category or "unknown"
        cat_threshold = category_stats[cat]["threshold"]
        cat_std = category_stats[cat]["std"]

        if cat_std > 0 and abs_amount > cat_threshold:
            reasons.append(f"unusually large within category '{cat}'")

        if reasons:
            anomalies.append(
                {
                    "transaction_id": tx.id,
                    "date": tx.date,
                    "description": tx.description,
                    "amount": float(tx.amount),
                    "category": tx.category,
                    "counterparty": tx.counterparty,
                    "reason": "; ".join(reasons),
                    "is_reviewed": tx.is_reviewed,
                    "is_ignored": tx.is_ignored,
                    "review_note": tx.review_note,
                }
            )

    anomalies.sort(key=lambda x: abs(x["amount"]), reverse=True)
    return anomalies[:20]


def get_forecast_summary(
    weekly_summary: list[dict],
    monthly_summary: list[dict],
):
    if weekly_summary:
        projected_next_week_net = sum(row["net_profit"] for row in weekly_summary) / len(weekly_summary)
    else:
        projected_next_week_net = 0.0

    if monthly_summary:
        projected_next_month_income = sum(row["total_income"] for row in monthly_summary) / len(monthly_summary)
        projected_next_month_expense = sum(row["total_expense"] for row in monthly_summary) / len(monthly_summary)
        projected_next_month_refund = sum(row["total_refund"] for row in monthly_summary) / len(monthly_summary)
        projected_next_month_net = sum(row["net_profit"] for row in monthly_summary) / len(monthly_summary)
    else:
        projected_next_month_income = 0.0
        projected_next_month_expense = 0.0
        projected_next_month_refund = 0.0
        projected_next_month_net = 0.0

    return {
        "projected_next_week_net": projected_next_week_net,
        "projected_next_month_income": projected_next_month_income,
        "projected_next_month_expense": projected_next_month_expense,
        "projected_next_month_refund": projected_next_month_refund,
        "projected_next_month_net": projected_next_month_net,
    }


def get_saved_views(db: Session, user_id: int):
    from app.models import SavedView

    return (
        db.query(SavedView)
        .filter(SavedView.user_id == user_id)
        .order_by(SavedView.name.asc())
        .all()
    )


def create_saved_view(
    db: Session,
    user_id: int,
    name: str,
    filter_type: str | None = None,
    category: str | None = None,
    payment_method: str | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
):
    from app.models import SavedView

    saved_view = SavedView(
        user_id=user_id,
        name=name.strip(),
        filter_type=filter_type or None,
        category=category or None,
        payment_method=payment_method or None,
        start_date=start_date,
        end_date=end_date,
    )
    db.add(saved_view)
    db.commit()
    db.refresh(saved_view)
    return saved_view


def delete_saved_view(db: Session, user_id: int, saved_view_id: int):
    from app.models import SavedView

    saved_view = (
        db.query(SavedView)
        .filter(
            SavedView.id == saved_view_id,
            SavedView.user_id == user_id,
        )
        .first()
    )
    if saved_view:
        db.delete(saved_view)
        db.commit()
        return True
    return False


def get_saved_view_by_name(db: Session, user_id: int, name: str):
    from app.models import SavedView

    return (
        db.query(SavedView)
        .filter(
            SavedView.user_id == user_id,
            SavedView.name == name.strip(),
        )
        .first()
    )


def mark_transaction_reviewed(db: Session, user_id: int, transaction_id: int, reviewed: bool = True):
    tx = (
        db.query(Transaction)
        .filter(
            Transaction.id == transaction_id,
            Transaction.user_id == user_id,
        )
        .first()
    )
    if not tx:
        return None

    tx.is_reviewed = reviewed
    db.commit()
    db.refresh(tx)
    return tx


def mark_transaction_ignored(db: Session, user_id: int, transaction_id: int, ignored: bool = True):
    tx = (
        db.query(Transaction)
        .filter(
            Transaction.id == transaction_id,
            Transaction.user_id == user_id,
        )
        .first()
    )
    if not tx:
        return None

    tx.is_ignored = ignored
    db.commit()
    db.refresh(tx)
    return tx


def update_transaction_note(db: Session, user_id: int, transaction_id: int, note: str):
    tx = (
        db.query(Transaction)
        .filter(
            Transaction.id == transaction_id,
            Transaction.user_id == user_id,
        )
        .first()
    )
    if not tx:
        return None

    tx.review_note = note.strip() if note else ""
    db.commit()
    db.refresh(tx)
    return tx


def get_user_by_username(db: Session, username: str):
    from app.models import User

    return (
        db.query(User)
        .filter(User.username == username.strip())
        .first()
    )


def get_user_by_email(db: Session, email: str):
    from app.models import User

    return (
        db.query(User)
        .filter(User.email == email.strip().lower())
        .first()
    )


def create_user(
    db: Session,
    username: str,
    email: str,
    password_hash: str,
):
    from app.models import User

    user = User(
        username=username.strip(),
        email=email.strip().lower(),
        password_hash=password_hash,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def get_user_by_id(db: Session, user_id: int):
    from app.models import User

    return (
        db.query(User)
        .filter(User.id == user_id)
        .first()
    )