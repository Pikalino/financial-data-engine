import csv
import hashlib
import io
import json
import os
import uuid
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
from fastapi import FastAPI, Request, UploadFile, File, Form, HTTPException
from fastapi.responses import RedirectResponse, StreamingResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from starlette.middleware.sessions import SessionMiddleware

from app.auth import hash_password, verify_password
from app.crud import (
    get_summary,
    get_summary_by_category,
    get_summary_by_day,
    get_transactions,
    get_recent_import_batches,
    get_import_batch_by_batch_id,
    get_transactions_by_import_batch_id,
    get_transaction_by_id,
    update_transaction,
    bulk_update_transaction_category,
    get_category_rules,
    create_category_rule,
    apply_category_rules_to_all_transactions,
    get_summary_by_month,
    get_summary_by_payment_method,
    get_top_counterparties,
    get_category_trend_by_month,
    get_summary_by_week,
    get_kpi_summary,
    get_transactions_extended,
    generate_auto_insights,
    detect_anomalies,
    get_forecast_summary,
    get_saved_views,
    create_saved_view,
    delete_saved_view,
    get_saved_view_by_name,
    mark_transaction_reviewed,
    mark_transaction_ignored,
    update_transaction_note,
    get_user_by_username,
    get_user_by_email,
    create_user,
    get_user_by_id,
)
from app.database import SessionLocal
from app.models import Transaction, ImportBatch
from app.schemas import (
    CategorySummaryResponse,
    MonthlySummaryResponse,
    DailySummaryResponse,
    SummaryResponse,
    TransactionResponse,
    PaymentMethodSummaryResponse,
    CounterpartySummaryResponse,
    CategoryMonthlyTrendResponse,
    WeeklySummaryResponse,
    KpiSummaryResponse,
    AutoInsightResponse,
    AnomalyResponse,
    ForecastResponse,
)
load_dotenv()
app = FastAPI(title="Financial Data Engine")
app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("SESSION_SECRET", "dev-secret-change-this"),
)
app.mount("/static", StaticFiles(directory="app/static"), name="static")

templates = Jinja2Templates(directory="app/templates")

PREVIEW_DIR = Path("/tmp/financial_data_engine_previews")
PREVIEW_DIR.mkdir(parents=True, exist_ok=True)

UPLOAD_DIR = Path("/tmp/financial_data_engine_uploads")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

COLUMN_ALIASES = {
    "date": ["date", "transaction_date", "txn_date", "created_at", "posted_date"],
    "description": ["description", "details", "memo", "narration", "note"],
    "amount": ["amount", "total", "net_amount", "value"],
    "currency": ["currency", "ccy"],
    "payment_method": ["payment_method", "payment", "method"],
    "counterparty": ["counterparty", "merchant", "vendor", "customer", "party"],
}


def get_current_user(request: Request):
    user_id = request.session.get("user_id")
    if not user_id:
        return None

    db = SessionLocal()
    try:
        return get_user_by_id(db, user_id)
    finally:
        db.close()


def require_user(request: Request):
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    return user


def get_db():
    db: Session = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def save_preview_data(data: list[dict]) -> str:
    preview_id = str(uuid.uuid4())
    preview_path = PREVIEW_DIR / f"{preview_id}.json"

    with open(preview_path, "w", encoding="utf-8") as f:
        json.dump(data, f, default=str)

    return preview_id


def load_preview_data(preview_id: str) -> list[dict] | None:
    preview_path = PREVIEW_DIR / f"{preview_id}.json"

    if not preview_path.exists():
        return None

    with open(preview_path, "r", encoding="utf-8") as f:
        return json.load(f)


def delete_preview_data(preview_id: str) -> None:
    preview_path = PREVIEW_DIR / f"{preview_id}.json"
    if preview_path.exists():
        os.remove(preview_path)


def save_uploaded_csv(contents: bytes, original_filename: str) -> str:
    upload_id = str(uuid.uuid4())
    file_path = UPLOAD_DIR / f"{upload_id}.csv"

    with open(file_path, "wb") as f:
        f.write(contents)

    meta_path = UPLOAD_DIR / f"{upload_id}.meta.json"
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump({"original_filename": original_filename}, f)

    return upload_id


def load_uploaded_csv(upload_id: str) -> tuple[bytes | None, str | None]:
    file_path = UPLOAD_DIR / f"{upload_id}.csv"
    meta_path = UPLOAD_DIR / f"{upload_id}.meta.json"

    if not file_path.exists() or not meta_path.exists():
        return None, None

    with open(file_path, "rb") as f:
        contents = f.read()

    with open(meta_path, "r", encoding="utf-8") as f:
        meta = json.load(f)

    return contents, meta.get("original_filename")


def delete_uploaded_csv(upload_id: str) -> None:
    file_path = UPLOAD_DIR / f"{upload_id}.csv"
    meta_path = UPLOAD_DIR / f"{upload_id}.meta.json"

    if file_path.exists():
        os.remove(file_path)
    if meta_path.exists():
        os.remove(meta_path)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized = {}
    used_source_columns = set()
    source_columns = {col.strip().lower(): col for col in df.columns}

    for canonical_name, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            if alias in source_columns and source_columns[alias] not in used_source_columns:
                normalized[source_columns[alias]] = canonical_name
                used_source_columns.add(source_columns[alias])
                break

    return df.rename(columns=normalized)


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

    if "order" in desc or "sale" in desc:
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


def build_row_hash(
    date_value,
    description: str,
    amount: float,
    currency: str,
    payment_method: str,
    counterparty: str,
    tx_type: str,
    category: str,
) -> str:
    raw_string = "|".join(
        [
            str(date_value),
            str(description),
            str(amount),
            str(currency),
            str(payment_method),
            str(counterparty),
            str(tx_type),
            str(category),
        ]
    )
    return hashlib.sha256(raw_string.encode("utf-8")).hexdigest()


@app.get("/")
def home(request: Request):
    user = get_current_user(request)
    if user:
        return RedirectResponse(url="/dashboard", status_code=303)
    return RedirectResponse(url="/login", status_code=303)


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request, error: str | None = None):
    return templates.TemplateResponse(
        request,
        "login.html",
        {"error": error},
    )


@app.post("/login")
async def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
):
    db = SessionLocal()
    try:
        user = get_user_by_username(db, username)

        if not user or not verify_password(password, user.password_hash):
            return templates.TemplateResponse(
                request,
                "login.html",
                {"error": "Invalid username or password"},
                status_code=400,
            )

        request.session["user_id"] = user.id
        return RedirectResponse(url="/dashboard", status_code=303)
    finally:
        db.close()


@app.get("/register", response_class=HTMLResponse)
def register_page(request: Request, error: str | None = None):
    return templates.TemplateResponse(
        request,
        "register.html",
        {"error": error},
    )


@app.post("/register")
async def register_submit(
    request: Request,
    username: str = Form(...),
    email: str = Form(...),
    password: str = Form(...),
):
    db = SessionLocal()
    try:
        if len(password.encode("utf-8")) > 72:
            return templates.TemplateResponse(
                request,
                "register.html",
                {"error": "Password must be 72 bytes or fewer"},
                status_code=400,
            )

        existing_username = get_user_by_username(db, username)
        if existing_username:
            return templates.TemplateResponse(
                request,
                "register.html",
                {"error": "Username already exists"},
                status_code=400,
            )

        existing_email = get_user_by_email(db, email)
        if existing_email:
            return templates.TemplateResponse(
                request,
                "register.html",
                {"error": "Email already exists"},
                status_code=400,
            )

        user = create_user(
            db,
            username=username,
            email=email,
            password_hash=hash_password(password),
        )

        request.session["user_id"] = user.id
        return RedirectResponse(url="/dashboard", status_code=303)
    finally:
        db.close()


@app.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)


@app.get("/transactions", response_model=list[TransactionResponse])
def read_transactions(
    request: Request,
    type: str | None = None,
    category: str | None = None,
    payment_method: str | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
):
    user = require_user(request)
    db = SessionLocal()
    try:
        return get_transactions(
            db,
            user_id=user.id,
            transaction_type=type,
            category=category,
            payment_method=payment_method,
            start_date=start_date,
            end_date=end_date,
        )
    finally:
        db.close()


@app.get("/summary", response_model=SummaryResponse)
def read_summary(
    request: Request,
    type: str | None = None,
    category: str | None = None,
    payment_method: str | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
):
    user = require_user(request)
    db = SessionLocal()
    try:
        return get_summary(
            db,
            user_id=user.id,
            transaction_type=type,
            category=category,
            payment_method=payment_method,
            start_date=start_date,
            end_date=end_date,
        )
    finally:
        db.close()


@app.get("/summary/by-category", response_model=list[CategorySummaryResponse])
def read_summary_by_category(
    request: Request,
    type: str | None = None,
    payment_method: str | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
):
    user = require_user(request)
    db = SessionLocal()
    try:
        return get_summary_by_category(
            db,
            user_id=user.id,
            transaction_type=type,
            payment_method=payment_method,
            start_date=start_date,
            end_date=end_date,
        )
    finally:
        db.close()


@app.get("/summary/by-day", response_model=list[DailySummaryResponse])
def read_summary_by_day(
    request: Request,
    type: str | None = None,
    category: str | None = None,
    payment_method: str | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
):
    user = require_user(request)
    db = SessionLocal()
    try:
        return get_summary_by_day(
            db,
            user_id=user.id,
            transaction_type=type,
            category=category,
            payment_method=payment_method,
            start_date=start_date,
            end_date=end_date,
        )
    finally:
        db.close()


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(
    request: Request,
    type: str | None = None,
    category: str | None = None,
    payment_method: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    upload_inserted: int | None = None,
    upload_skipped: int | None = None,
    upload_invalid: int | None = None,
    upload_error: str | None = None,
):
    user = require_user(request)
    db = SessionLocal()
    try:
        parsed_start_date = datetime.fromisoformat(start_date) if start_date else None
        parsed_end_date = datetime.fromisoformat(end_date) if end_date else None

        summary = get_summary(
            db,
            user_id=user.id,
            transaction_type=type or None,
            category=category or None,
            payment_method=payment_method or None,
            start_date=parsed_start_date,
            end_date=parsed_end_date,
        )

        kpi_summary = get_kpi_summary(
            db,
            user_id=user.id,
            transaction_type=type or None,
            category=category or None,
            payment_method=payment_method or None,
            start_date=parsed_start_date,
            end_date=parsed_end_date,
        )

        category_summary = get_summary_by_category(
            db,
            user_id=user.id,
            transaction_type=type or None,
            payment_method=payment_method or None,
            start_date=parsed_start_date,
            end_date=parsed_end_date,
        )

        daily_summary = get_summary_by_day(
            db,
            user_id=user.id,
            transaction_type=type or None,
            category=category or None,
            payment_method=payment_method or None,
            start_date=parsed_start_date,
            end_date=parsed_end_date,
        )

        weekly_summary = get_summary_by_week(
            db,
            user_id=user.id,
            transaction_type=type or None,
            category=category or None,
            payment_method=payment_method or None,
            start_date=parsed_start_date,
            end_date=parsed_end_date,
        )

        monthly_summary = get_summary_by_month(
            db,
            user_id=user.id,
            transaction_type=type or None,
            category=category or None,
            payment_method=payment_method or None,
            start_date=parsed_start_date,
            end_date=parsed_end_date,
        )

        payment_method_summary = get_summary_by_payment_method(
            db,
            user_id=user.id,
            transaction_type=type or None,
            category=category or None,
            start_date=parsed_start_date,
            end_date=parsed_end_date,
        )

        top_counterparties = get_top_counterparties(
            db,
            user_id=user.id,
            transaction_type=type or None,
            category=category or None,
            payment_method=payment_method or None,
            start_date=parsed_start_date,
            end_date=parsed_end_date,
        )

        category_monthly_trend = get_category_trend_by_month(
            db,
            user_id=user.id,
            transaction_type=type or None,
            payment_method=payment_method or None,
            start_date=parsed_start_date,
            end_date=parsed_end_date,
        )

        transactions = get_transactions(
            db,
            user_id=user.id,
            transaction_type=type or None,
            category=category or None,
            payment_method=payment_method or None,
            start_date=parsed_start_date,
            end_date=parsed_end_date,
        )

        recent_transactions = list(reversed(transactions))[:10]
        import_batches = get_recent_import_batches(db, user_id=user.id)

        auto_insights = generate_auto_insights(
            summary=summary,
            kpi_summary=kpi_summary,
            category_summary=category_summary,
            payment_method_summary=payment_method_summary,
            top_counterparties=top_counterparties,
            monthly_summary=monthly_summary,
            weekly_summary=weekly_summary,
        )

        anomalies = detect_anomalies(
            db,
            user_id=user.id,
            transaction_type=type or None,
            category=category or None,
            payment_method=payment_method or None,
            start_date=parsed_start_date,
            end_date=parsed_end_date,
        )

        forecast_summary = get_forecast_summary(
            weekly_summary=weekly_summary,
            monthly_summary=monthly_summary,
        )

        saved_views = get_saved_views(db, user_id=user.id)

        return templates.TemplateResponse(
            request,
            "dashboard.html",
            {
                "current_user": user,
                "summary": summary,
                "kpi_summary": kpi_summary,
                "category_summary": category_summary,
                "daily_summary": daily_summary,
                "weekly_summary": weekly_summary,
                "monthly_summary": monthly_summary,
                "payment_method_summary": payment_method_summary,
                "top_counterparties": top_counterparties,
                "category_monthly_trend": category_monthly_trend,
                "recent_transactions": recent_transactions,
                "import_batches": import_batches,
                "auto_insights": auto_insights,
                "anomalies": anomalies,
                "forecast_summary": forecast_summary,
                "saved_views": saved_views,
                "filters": {
                    "type": type or None,
                    "category": category or None,
                    "payment_method": payment_method or None,
                    "start_date": parsed_start_date,
                    "end_date": parsed_end_date,
                },
                "upload_inserted": upload_inserted,
                "upload_skipped": upload_skipped,
                "upload_invalid": upload_invalid,
                "upload_error": upload_error,
            },
        )
    finally:
        db.close()

@app.post("/upload", response_class=HTMLResponse)
async def upload_csv(request: Request, file: UploadFile = File(...)):
    require_user(request)
    contents = await file.read()

    try:
        df = pd.read_csv(io.BytesIO(contents))
    except Exception:
        return RedirectResponse(
            url="/dashboard?upload_error=Could not read CSV file",
            status_code=303,
        )

    df.columns = [col.strip() for col in df.columns]
    available_columns = list(df.columns)
    upload_id = save_uploaded_csv(contents, file.filename or "uploaded.csv")

    suggested_mapping = {}

    for canonical_name, aliases in COLUMN_ALIASES.items():
        for col in available_columns:
            if col.strip().lower() in aliases:
                suggested_mapping[canonical_name] = col
                break

    preview_df = df.head(5).copy()

    column_samples = {}
    column_profiles = {}

    for col in available_columns:
        series = preview_df[col].dropna().astype(str).str.strip()
        values = series.tolist()
        column_samples[col] = values

        numeric_count = 0
        date_count = 0

        for value in values:
            try:
                float(value.replace(",", ""))
                numeric_count += 1
            except Exception:
                pass

            try:
                pd.to_datetime(value)
                date_count += 1
            except Exception:
                pass

        total = len(values) if len(values) > 0 else 1

        column_profiles[col] = {
            "sample_count": len(values),
            "numeric_ratio": numeric_count / total,
            "date_ratio": date_count / total,
        }

    return templates.TemplateResponse(
        request,
        "mapping.html",
        {
            "upload_id": upload_id,
            "columns": available_columns,
            "suggested_mapping": suggested_mapping,
            "column_samples": column_samples,
            "column_profiles": column_profiles,
            "mapping_error": None,
        },
    )


@app.get("/download-template")
def download_template(request: Request):
    require_user(request)
    template_csv = """date,description,amount,currency,payment_method,counterparty
2026-04-01,Facebook order #3001,125000,MNT,qpay,customer y
2026-04-02,Office rent,-450000,MNT,bank,landlord
2026-04-03,Packaging supplies,-18000,MNT,cash,supply store
2026-04-04,Refund to customer,-25000,MNT,bank,customer y
2026-04-05,Boosted ad payment,-30000,MNT,card,meta
"""
    return StreamingResponse(
        io.BytesIO(template_csv.encode("utf-8")),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=financial_data_template.csv"},
    )


@app.get("/upload-preview", response_class=HTMLResponse)
def upload_preview(
    request: Request,
    preview_id: str,
    invalid_count: int = 0,
):
    require_user(request)
    preview_data = load_preview_data(preview_id)

    if preview_data is None:
        return RedirectResponse(
            url="/dashboard?upload_error=Preview expired or not found",
            status_code=303,
        )

    preview_rows = preview_data[:10]

    return templates.TemplateResponse(
        request,
        "upload_preview.html",
        {
            "preview_id": preview_id,
            "preview_rows": preview_rows,
            "total_rows": len(preview_data),
            "invalid_count": invalid_count,
        },
    )


@app.post("/confirm-upload")
async def confirm_upload(request: Request, preview_id: str = Form(...)):
    user = require_user(request)
    preview_data = load_preview_data(preview_id)

    if preview_data is None:
        return RedirectResponse(
            url="/dashboard?upload_error=Preview expired or not found",
            status_code=303,
        )

    file_name = preview_data[0].get("source_file", "uploaded.csv") if preview_data else "uploaded.csv"
    batch_uuid = str(uuid.uuid4())

    db = SessionLocal()
    inserted = 0
    skipped = 0

    try:
        batch = ImportBatch(
            batch_id=batch_uuid,
            file_name=file_name,
            uploaded_at=datetime.utcnow(),
            inserted_count=0,
            skipped_count=0,
            invalid_count=0,
            user_id=user.id,
        )
        db.add(batch)
        db.commit()
        db.refresh(batch)

        for row in preview_data:
            transaction = Transaction(
                date=pd.to_datetime(row["date"]),
                description=row["description"],
                amount=float(row["amount"]),
                currency=row["currency"],
                payment_method=row["payment_method"],
                counterparty=row["counterparty"],
                type=row["type"],
                category=row["category"],
                source_file=row["source_file"],
                raw_row_hash=row["raw_row_hash"],
                import_batch_id=batch.id,
                user_id=user.id,
            )

            db.add(transaction)

            try:
                db.commit()
                inserted += 1
            except IntegrityError:
                db.rollback()
                skipped += 1

        batch.inserted_count = inserted
        batch.skipped_count = skipped
        batch.invalid_count = 0
        db.commit()

    finally:
        db.close()

    delete_preview_data(preview_id)

    return RedirectResponse(
        url=f"/dashboard?upload_inserted={inserted}&upload_skipped={skipped}&upload_invalid=0",
        status_code=303,
    )


@app.post("/map-columns", response_class=HTMLResponse)
async def map_columns(
    request: Request,
    upload_id: str = Form(...),
    date_column: str = Form(...),
    description_column: str = Form(...),
    amount_column: str = Form(...),
    currency_column: str = Form(""),
    payment_method_column: str = Form(""),
    counterparty_column: str = Form(""),
):
    require_user(request)
    contents, original_filename = load_uploaded_csv(upload_id)

    if contents is None:
        return RedirectResponse(
            url="/dashboard?upload_error=Uploaded file expired or not found",
            status_code=303,
        )

    try:
        df = pd.read_csv(io.BytesIO(contents))
    except Exception:
        return RedirectResponse(
            url="/dashboard?upload_error=Could not read uploaded CSV",
            status_code=303,
        )

    df.columns = [col.strip() for col in df.columns]
    available_columns = list(df.columns)

    selected_columns = [
        date_column,
        description_column,
        amount_column,
        currency_column,
        payment_method_column,
        counterparty_column,
    ]
    selected_non_empty = [col for col in selected_columns if col]

    if len(selected_non_empty) != len(set(selected_non_empty)):
        column_samples = {}
        column_profiles = {}
        preview_df = df.head(5).copy()

        for col in available_columns:
            series = preview_df[col].fillna("").astype(str).str.strip()
            values = series.tolist()
            column_samples[col] = values

            numeric_count = 0
            date_count = 0

            for value in values:
                if value == "":
                    continue

                try:
                    float(value.replace(",", ""))
                    numeric_count += 1
                except Exception:
                    pass

                try:
                    pd.to_datetime(value)
                    date_count += 1
                except Exception:
                    pass

            total = len(values) if len(values) > 0 else 1

            column_profiles[col] = {
                "sample_count": len(values),
                "numeric_ratio": numeric_count / total,
                "date_ratio": date_count / total,
            }

        return templates.TemplateResponse(
            request,
            "mapping.html",
            {
                "upload_id": upload_id,
                "columns": available_columns,
                "suggested_mapping": {
                    "date": date_column,
                    "description": description_column,
                    "amount": amount_column,
                    "currency": currency_column,
                    "payment_method": payment_method_column,
                    "counterparty": counterparty_column,
                },
                "column_samples": column_samples,
                "column_profiles": column_profiles,
                "mapping_error": "Each target field must map to a different source column.",
            },
        )

    rename_map = {
        date_column: "date",
        description_column: "description",
        amount_column: "amount",
    }

    if currency_column:
        rename_map[currency_column] = "currency"
    if payment_method_column:
        rename_map[payment_method_column] = "payment_method"
    if counterparty_column:
        rename_map[counterparty_column] = "counterparty"

    df = df.rename(columns=rename_map)

    required_columns = ["date", "description", "amount"]
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        missing_text = ",".join(missing_columns)
        return RedirectResponse(
            url=f"/dashboard?upload_error=Missing required mapped columns: {missing_text}",
            status_code=303,
        )

    if "currency" not in df.columns:
        df["currency"] = "MNT"
    if "payment_method" not in df.columns:
        df["payment_method"] = "unknown"
    if "counterparty" not in df.columns:
        df["counterparty"] = "unknown"

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    invalid_rows = df["date"].isna() | df["description"].isna() | df["amount"].isna()
    invalid_count = int(invalid_rows.sum())
    df = df[~invalid_rows].copy()

    text_cols = ["description", "currency", "payment_method", "counterparty"]
    for col in text_cols:
        df[col] = df[col].astype(str).str.strip()

    df["currency"] = df["currency"].str.upper()
    df["payment_method"] = df["payment_method"].str.lower()
    df["counterparty"] = df["counterparty"].str.lower()

    df["type"] = df.apply(lambda row: classify_type(row["description"], row["amount"]), axis=1)
    df["category"] = df["description"].apply(classify_category)
    df["source_file"] = original_filename or "uploaded.csv"
    df["raw_row_hash"] = df.apply(
        lambda row: build_row_hash(
            row["date"],
            row["description"],
            float(row["amount"]),
            row["currency"],
            row["payment_method"],
            row["counterparty"],
            row["type"],
            row["category"],
        ),
        axis=1,
    )

    preview_records = df.to_dict(orient="records")
    preview_id = save_preview_data(preview_records)

    delete_uploaded_csv(upload_id)

    return RedirectResponse(
        url=f"/upload-preview?preview_id={preview_id}&invalid_count={invalid_count}",
        status_code=303,
    )


@app.get("/import-batches/{batch_id}", response_class=HTMLResponse)
def import_batch_detail(request: Request, batch_id: str):
    user = require_user(request)
    db = SessionLocal()
    try:
        batch = get_import_batch_by_batch_id(db, batch_id, user_id=user.id)

        if batch is None:
            return RedirectResponse(
                url="/dashboard?upload_error=Import batch not found",
                status_code=303,
            )

        transactions = get_transactions_by_import_batch_id(db, batch.id, user_id=user.id)

        return templates.TemplateResponse(
            request,
            "import_batch_detail.html",
            {
                "batch": batch,
                "transactions": transactions,
            },
        )
    finally:
        db.close()


@app.get("/transactions/{transaction_id}/edit", response_class=HTMLResponse)
def edit_transaction_page(request: Request, transaction_id: int):
    user = require_user(request)
    db = SessionLocal()
    try:
        transaction = get_transaction_by_id(db, transaction_id, user_id=user.id)

        if transaction is None:
            return RedirectResponse(
                url="/dashboard?upload_error=Transaction not found",
                status_code=303,
            )

        return templates.TemplateResponse(
            request,
            "transaction_edit.html",
            {
                "transaction": transaction,
            },
        )
    finally:
        db.close()


@app.post("/transactions/{transaction_id}/edit")
async def edit_transaction_submit(
    request: Request,
    transaction_id: int,
    description: str = Form(...),
    amount: float = Form(...),
    currency: str = Form(...),
    payment_method: str = Form(...),
    counterparty: str = Form(...),
    type: str = Form(...),
    category: str = Form(...),
):
    user = require_user(request)
    db = SessionLocal()
    try:
        transaction = get_transaction_by_id(db, transaction_id, user_id=user.id)

        if transaction is None:
            return RedirectResponse(
                url="/dashboard?upload_error=Transaction not found",
                status_code=303,
            )

        updated = update_transaction(
            db,
            transaction=transaction,
            description=description,
            amount=amount,
            currency=currency,
            payment_method=payment_method,
            counterparty=counterparty,
            transaction_type=type,
            category=category,
        )

        if updated.import_batch and updated.import_batch.batch_id:
            return RedirectResponse(
                url=f"/import-batches/{updated.import_batch.batch_id}",
                status_code=303,
            )

        return RedirectResponse(
            url="/dashboard",
            status_code=303,
        )
    finally:
        db.close()


@app.post("/import-batches/{batch_id}/bulk-update-category")
async def bulk_update_category(
    request: Request,
    batch_id: str,
    category: str = Form(...),
    transaction_ids: list[int] | None = Form(None),
):
    user = require_user(request)
    db = SessionLocal()
    try:
        batch = get_import_batch_by_batch_id(db, batch_id, user_id=user.id)

        if batch is None:
            return RedirectResponse(
                url="/dashboard?upload_error=Import batch not found",
                status_code=303,
            )

        if not transaction_ids:
            return RedirectResponse(
                url=f"/import-batches/{batch_id}",
                status_code=303,
            )

        bulk_update_transaction_category(
            db,
            user_id=user.id,
            transaction_ids=transaction_ids,
            category=category,
        )

        return RedirectResponse(
            url=f"/import-batches/{batch_id}",
            status_code=303,
        )
    finally:
        db.close()


@app.get("/category-rules", response_class=HTMLResponse)
def category_rules_page(
    request: Request,
    updated_count: int | None = None,
):
    user = require_user(request)
    db = SessionLocal()
    try:
        rules = get_category_rules(db, user_id=user.id)
        return templates.TemplateResponse(
            request,
            "category_rules.html",
            {
                "rules": rules,
                "updated_count": updated_count,
            },
        )
    finally:
        db.close()


@app.post("/category-rules")
async def create_category_rule_submit(
    request: Request,
    keyword: str = Form(...),
    category: str = Form(...),
    priority: int = Form(100),
):
    user = require_user(request)
    db = SessionLocal()
    try:
        create_category_rule(
            db,
            user_id=user.id,
            keyword=keyword,
            category=category,
            priority=priority,
            is_active=True,
        )
        return RedirectResponse(
            url="/category-rules",
            status_code=303,
        )
    finally:
        db.close()


@app.post("/category-rules/apply")
async def apply_category_rules_submit(request: Request):
    user = require_user(request)
    db = SessionLocal()
    try:
        updated_count = apply_category_rules_to_all_transactions(db, user_id=user.id)
        return RedirectResponse(
            url=f"/category-rules?updated_count={updated_count}",
            status_code=303,
        )
    finally:
        db.close()


@app.get("/summary/by-month", response_model=list[MonthlySummaryResponse])
def read_summary_by_month(
    request: Request,
    type: str | None = None,
    category: str | None = None,
    payment_method: str | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
):
    user = require_user(request)
    db = SessionLocal()
    try:
        return get_summary_by_month(
            db,
            user_id=user.id,
            transaction_type=type,
            category=category,
            payment_method=payment_method,
            start_date=start_date,
            end_date=end_date,
        )
    finally:
        db.close()


@app.get("/summary/by-payment-method", response_model=list[PaymentMethodSummaryResponse])
def read_summary_by_payment_method(
    request: Request,
    type: str | None = None,
    category: str | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
):
    user = require_user(request)
    db = SessionLocal()
    try:
        return get_summary_by_payment_method(
            db,
            user_id=user.id,
            transaction_type=type,
            category=category,
            start_date=start_date,
            end_date=end_date,
        )
    finally:
        db.close()


@app.get("/summary/top-counterparties", response_model=list[CounterpartySummaryResponse])
def read_top_counterparties(
    request: Request,
    type: str | None = None,
    category: str | None = None,
    payment_method: str | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
):
    user = require_user(request)
    db = SessionLocal()
    try:
        return get_top_counterparties(
            db,
            user_id=user.id,
            transaction_type=type,
            category=category,
            payment_method=payment_method,
            start_date=start_date,
            end_date=end_date,
        )
    finally:
        db.close()


@app.get("/summary/category-trend-by-month", response_model=list[CategoryMonthlyTrendResponse])
def read_category_trend_by_month(
    request: Request,
    type: str | None = None,
    payment_method: str | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
):
    user = require_user(request)
    db = SessionLocal()
    try:
        return get_category_trend_by_month(
            db,
            user_id=user.id,
            transaction_type=type,
            payment_method=payment_method,
            start_date=start_date,
            end_date=end_date,
        )
    finally:
        db.close()


@app.get("/summary/by-week", response_model=list[WeeklySummaryResponse])
def read_summary_by_week(
    request: Request,
    type: str | None = None,
    category: str | None = None,
    payment_method: str | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
):
    user = require_user(request)
    db = SessionLocal()
    try:
        return get_summary_by_week(
            db,
            user_id=user.id,
            transaction_type=type,
            category=category,
            payment_method=payment_method,
            start_date=start_date,
            end_date=end_date,
        )
    finally:
        db.close()


@app.get("/summary/kpis", response_model=KpiSummaryResponse)
def read_kpi_summary(
    request: Request,
    type: str | None = None,
    category: str | None = None,
    payment_method: str | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
):
    user = require_user(request)
    db = SessionLocal()
    try:
        return get_kpi_summary(
            db,
            user_id=user.id,
            transaction_type=type,
            category=category,
            payment_method=payment_method,
            start_date=start_date,
            end_date=end_date,
        )
    finally:
        db.close()


@app.get("/transactions/view", response_class=HTMLResponse)
def transaction_list_view(
    request: Request,
    type: str | None = None,
    category: str | None = None,
    payment_method: str | None = None,
    counterparty: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
):
    user = require_user(request)
    db = SessionLocal()
    try:
        parsed_start_date = datetime.fromisoformat(start_date) if start_date else None
        parsed_end_date = datetime.fromisoformat(end_date) if end_date else None

        transactions = get_transactions_extended(
            db,
            user_id=user.id,
            transaction_type=type or None,
            category=category or None,
            payment_method=payment_method or None,
            counterparty=counterparty or None,
            start_date=parsed_start_date,
            end_date=parsed_end_date,
        )

        return templates.TemplateResponse(
            request,
            "transaction_list.html",
            {
                "transactions": transactions,
                "filters": {
                    "type": type,
                    "category": category,
                    "payment_method": payment_method,
                    "counterparty": counterparty,
                    "start_date": start_date,
                    "end_date": end_date,
                },
            },
        )
    finally:
        db.close()


@app.get("/summary/insights", response_model=list[AutoInsightResponse])
def read_auto_insights(
    request: Request,
    type: str | None = None,
    category: str | None = None,
    payment_method: str | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
):
    user = require_user(request)
    db = SessionLocal()
    try:
        summary = get_summary(
            db,
            user_id=user.id,
            transaction_type=type,
            category=category,
            payment_method=payment_method,
            start_date=start_date,
            end_date=end_date,
        )
        kpi_summary = get_kpi_summary(
            db,
            user_id=user.id,
            transaction_type=type,
            category=category,
            payment_method=payment_method,
            start_date=start_date,
            end_date=end_date,
        )
        category_summary = get_summary_by_category(
            db,
            user_id=user.id,
            transaction_type=type,
            payment_method=payment_method,
            start_date=start_date,
            end_date=end_date,
        )
        payment_method_summary = get_summary_by_payment_method(
            db,
            user_id=user.id,
            transaction_type=type,
            category=category,
            start_date=start_date,
            end_date=end_date,
        )
        top_counterparties = get_top_counterparties(
            db,
            user_id=user.id,
            transaction_type=type,
            category=category,
            payment_method=payment_method,
            start_date=start_date,
            end_date=end_date,
        )
        monthly_summary = get_summary_by_month(
            db,
            user_id=user.id,
            transaction_type=type,
            category=category,
            payment_method=payment_method,
            start_date=start_date,
            end_date=end_date,
        )
        weekly_summary = get_summary_by_week(
            db,
            user_id=user.id,
            transaction_type=type,
            category=category,
            payment_method=payment_method,
            start_date=start_date,
            end_date=end_date,
        )

        return generate_auto_insights(
            summary=summary,
            kpi_summary=kpi_summary,
            category_summary=category_summary,
            payment_method_summary=payment_method_summary,
            top_counterparties=top_counterparties,
            monthly_summary=monthly_summary,
            weekly_summary=weekly_summary,
        )
    finally:
        db.close()


@app.get("/summary/anomalies", response_model=list[AnomalyResponse])
def read_anomalies(
    request: Request,
    type: str | None = None,
    category: str | None = None,
    payment_method: str | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
):
    user = require_user(request)
    db = SessionLocal()
    try:
        return detect_anomalies(
            db,
            user_id=user.id,
            transaction_type=type,
            category=category,
            payment_method=payment_method,
            start_date=start_date,
            end_date=end_date,
        )
    finally:
        db.close()


@app.get("/summary/forecast", response_model=ForecastResponse)
def read_forecast_summary(
    request: Request,
    type: str | None = None,
    category: str | None = None,
    payment_method: str | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
):
    user = require_user(request)
    db = SessionLocal()
    try:
        weekly_summary = get_summary_by_week(
            db,
            user_id=user.id,
            transaction_type=type,
            category=category,
            payment_method=payment_method,
            start_date=start_date,
            end_date=end_date,
        )

        monthly_summary = get_summary_by_month(
            db,
            user_id=user.id,
            transaction_type=type,
            category=category,
            payment_method=payment_method,
            start_date=start_date,
            end_date=end_date,
        )

        return get_forecast_summary(
            weekly_summary=weekly_summary,
            monthly_summary=monthly_summary,
        )
    finally:
        db.close()


@app.get("/export/transactions")
def export_transactions(
    request: Request,
    type: str | None = None,
    category: str | None = None,
    payment_method: str | None = None,
    counterparty: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
):
    user = require_user(request)
    db = SessionLocal()
    try:
        parsed_start_date = datetime.fromisoformat(start_date) if start_date else None
        parsed_end_date = datetime.fromisoformat(end_date) if end_date else None

        transactions = get_transactions_extended(
            db,
            user_id=user.id,
            transaction_type=type or None,
            category=category or None,
            payment_method=payment_method or None,
            counterparty=counterparty or None,
            start_date=parsed_start_date,
            end_date=parsed_end_date,
        )

        output = io.StringIO()
        writer = csv.writer(output)

        writer.writerow(
            [
                "id",
                "date",
                "description",
                "amount",
                "currency",
                "type",
                "category",
                "payment_method",
                "counterparty",
                "source_file",
            ]
        )

        for tx in transactions:
            writer.writerow(
                [
                    tx.id,
                    tx.date,
                    tx.description,
                    tx.amount,
                    tx.currency,
                    tx.type,
                    tx.category,
                    tx.payment_method,
                    tx.counterparty,
                    tx.source_file,
                ]
            )

        output.seek(0)

        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=transactions_export.csv"},
        )
    finally:
        db.close()


@app.get("/export/summary")
def export_summary(
    request: Request,
    type: str | None = None,
    category: str | None = None,
    payment_method: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
):
    user = require_user(request)
    db = SessionLocal()
    try:
        parsed_start_date = datetime.fromisoformat(start_date) if start_date else None
        parsed_end_date = datetime.fromisoformat(end_date) if end_date else None

        summary = get_summary(
            db,
            user_id=user.id,
            transaction_type=type or None,
            category=category or None,
            payment_method=payment_method or None,
            start_date=parsed_start_date,
            end_date=parsed_end_date,
        )

        weekly_summary = get_summary_by_week(
            db,
            user_id=user.id,
            transaction_type=type or None,
            category=category or None,
            payment_method=payment_method or None,
            start_date=parsed_start_date,
            end_date=parsed_end_date,
        )

        monthly_summary = get_summary_by_month(
            db,
            user_id=user.id,
            transaction_type=type or None,
            category=category or None,
            payment_method=payment_method or None,
            start_date=parsed_start_date,
            end_date=parsed_end_date,
        )

        output = io.StringIO()
        writer = csv.writer(output)

        writer.writerow(["SUMMARY"])
        writer.writerow(["total_income", summary["total_income"]])
        writer.writerow(["total_expense", summary["total_expense"]])
        writer.writerow(["total_refund", summary["total_refund"]])
        writer.writerow(["net_profit", summary["net_profit"]])
        writer.writerow([])

        writer.writerow(["WEEKLY SUMMARY"])
        writer.writerow(["week", "total_income", "total_expense", "total_refund", "net_profit"])
        for row in weekly_summary:
            writer.writerow(
                [
                    row["week"],
                    row["total_income"],
                    row["total_expense"],
                    row["total_refund"],
                    row["net_profit"],
                ]
            )
        writer.writerow([])

        writer.writerow(["MONTHLY SUMMARY"])
        writer.writerow(["month", "total_income", "total_expense", "total_refund", "net_profit"])
        for row in monthly_summary:
            writer.writerow(
                [
                    row["month"],
                    row["total_income"],
                    row["total_expense"],
                    row["total_refund"],
                    row["net_profit"],
                ]
            )

        output.seek(0)

        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=summary_export.csv"},
        )
    finally:
        db.close()


@app.get("/report", response_class=HTMLResponse)
def printable_report(
    request: Request,
    type: str | None = None,
    category: str | None = None,
    payment_method: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
):
    user = require_user(request)
    db = SessionLocal()
    try:
        parsed_start_date = datetime.fromisoformat(start_date) if start_date else None
        parsed_end_date = datetime.fromisoformat(end_date) if end_date else None

        summary = get_summary(
            db,
            user_id=user.id,
            transaction_type=type or None,
            category=category or None,
            payment_method=payment_method or None,
            start_date=parsed_start_date,
            end_date=parsed_end_date,
        )

        kpi_summary = get_kpi_summary(
            db,
            user_id=user.id,
            transaction_type=type or None,
            category=category or None,
            payment_method=payment_method or None,
            start_date=parsed_start_date,
            end_date=parsed_end_date,
        )

        weekly_summary = get_summary_by_week(
            db,
            user_id=user.id,
            transaction_type=type or None,
            category=category or None,
            payment_method=payment_method or None,
            start_date=parsed_start_date,
            end_date=parsed_end_date,
        )

        monthly_summary = get_summary_by_month(
            db,
            user_id=user.id,
            transaction_type=type or None,
            category=category or None,
            payment_method=payment_method or None,
            start_date=parsed_start_date,
            end_date=parsed_end_date,
        )

        payment_method_summary = get_summary_by_payment_method(
            db,
            user_id=user.id,
            transaction_type=type or None,
            category=category or None,
            start_date=parsed_start_date,
            end_date=parsed_end_date,
        )

        top_counterparties = get_top_counterparties(
            db,
            user_id=user.id,
            transaction_type=type or None,
            category=category or None,
            payment_method=payment_method or None,
            start_date=parsed_start_date,
            end_date=parsed_end_date,
        )

        category_summary = get_summary_by_category(
            db,
            user_id=user.id,
            transaction_type=type or None,
            payment_method=payment_method or None,
            start_date=parsed_start_date,
            end_date=parsed_end_date,
        )

        anomalies = detect_anomalies(
            db,
            user_id=user.id,
            transaction_type=type or None,
            category=category or None,
            payment_method=payment_method or None,
            start_date=parsed_start_date,
            end_date=parsed_end_date,
        )

        forecast_summary = get_forecast_summary(
            weekly_summary=weekly_summary,
            monthly_summary=monthly_summary,
        )

        auto_insights = generate_auto_insights(
            summary=summary,
            kpi_summary=kpi_summary,
            category_summary=category_summary,
            payment_method_summary=payment_method_summary,
            top_counterparties=top_counterparties,
            monthly_summary=monthly_summary,
            weekly_summary=weekly_summary,
        )

        return templates.TemplateResponse(
            request,
            "report.html",
            {
                "current_user": user,
                "generated_at": datetime.utcnow(),
                "summary": summary,
                "kpi_summary": kpi_summary,
                "forecast_summary": forecast_summary,
                "auto_insights": auto_insights,
                "anomalies": anomalies,
                "weekly_summary": weekly_summary,
                "monthly_summary": monthly_summary,
                "payment_method_summary": payment_method_summary,
                "top_counterparties": top_counterparties,
                "category_summary": category_summary,
                "filters": {
                    "type": type or None,
                    "category": category or None,
                    "payment_method": payment_method or None,
                    "start_date": parsed_start_date,
                    "end_date": parsed_end_date,
                },
            },
        )
    finally:
        db.close()


@app.post("/saved-views")
async def create_saved_view_submit(
    request: Request,
    name: str = Form(...),
    type: str = Form(""),
    category: str = Form(""),
    payment_method: str = Form(""),
    start_date: str = Form(""),
    end_date: str = Form(""),
):
    user = require_user(request)
    db = SessionLocal()
    try:
        existing = get_saved_view_by_name(db, user_id=user.id, name=name)
        if existing:
            return RedirectResponse(
                url="/dashboard?upload_error=Saved view name already exists",
                status_code=303,
            )

        parsed_start_date = datetime.fromisoformat(start_date) if start_date else None
        parsed_end_date = datetime.fromisoformat(end_date) if end_date else None

        create_saved_view(
            db,
            user_id=user.id,
            name=name,
            filter_type=type or None,
            category=category or None,
            payment_method=payment_method or None,
            start_date=parsed_start_date,
            end_date=parsed_end_date,
        )

        return RedirectResponse(url="/dashboard", status_code=303)
    finally:
        db.close()


@app.post("/saved-views/{saved_view_id}/delete")
async def delete_saved_view_submit(request: Request, saved_view_id: int):
    user = require_user(request)
    db = SessionLocal()
    try:
        delete_saved_view(db, user_id=user.id, saved_view_id=saved_view_id)
        return RedirectResponse(url="/dashboard", status_code=303)
    finally:
        db.close()


@app.post("/transactions/{transaction_id}/review")
async def review_transaction(request: Request, transaction_id: int):
    user = require_user(request)
    db = SessionLocal()
    try:
        tx = mark_transaction_reviewed(db, user_id=user.id, transaction_id=transaction_id, reviewed=True)
        if tx and tx.import_batch and tx.import_batch.batch_id:
            return RedirectResponse(
                url=f"/import-batches/{tx.import_batch.batch_id}",
                status_code=303,
            )
        return RedirectResponse(url="/dashboard", status_code=303)
    finally:
        db.close()


@app.post("/transactions/{transaction_id}/ignore")
async def ignore_transaction(request: Request, transaction_id: int):
    user = require_user(request)
    db = SessionLocal()
    try:
        tx = mark_transaction_ignored(db, user_id=user.id, transaction_id=transaction_id, ignored=True)
        if tx and tx.import_batch and tx.import_batch.batch_id:
            return RedirectResponse(
                url=f"/import-batches/{tx.import_batch.batch_id}",
                status_code=303,
            )
        return RedirectResponse(url="/dashboard", status_code=303)
    finally:
        db.close()


@app.post("/transactions/{transaction_id}/note")
async def save_transaction_note(
    request: Request,
    transaction_id: int,
    note: str = Form(""),
):
    user = require_user(request)
    db = SessionLocal()
    try:
        tx = update_transaction_note(db, user_id=user.id, transaction_id=transaction_id, note=note)
        if tx and tx.import_batch and tx.import_batch.batch_id:
            return RedirectResponse(
                url=f"/import-batches/{tx.import_batch.batch_id}",
                status_code=303,
            )
        return RedirectResponse(url="/dashboard", status_code=303)
    finally:
        db.close()