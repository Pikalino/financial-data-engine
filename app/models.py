from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Boolean, Text, UniqueConstraint
from sqlalchemy.orm import relationship

from app.database import Base


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, nullable=False, index=True)
    email = Column(String, unique=True, nullable=False, index=True)
    password_hash = Column(String, nullable=False)

    transactions = relationship("Transaction", back_populates="user", cascade="all, delete-orphan")
    import_batches = relationship("ImportBatch", back_populates="user", cascade="all, delete-orphan")
    saved_views = relationship("SavedView", back_populates="user", cascade="all, delete-orphan")
    category_rules = relationship("CategoryRule", back_populates="user", cascade="all, delete-orphan")


class ImportBatch(Base):
    __tablename__ = "import_batches"

    id = Column(Integer, primary_key=True, index=True)
    batch_id = Column(String, unique=True, nullable=False, index=True)
    file_name = Column(String, nullable=False)
    uploaded_at = Column(DateTime, nullable=False)
    inserted_count = Column(Integer, nullable=False, default=0)
    skipped_count = Column(Integer, nullable=False, default=0)
    invalid_count = Column(Integer, nullable=False, default=0)

    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)

    user = relationship("User", back_populates="import_batches")
    transactions = relationship("Transaction", back_populates="import_batch", cascade="all, delete-orphan")


class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, index=True)
    date = Column(DateTime, nullable=False, index=True)
    description = Column(String, nullable=False)
    amount = Column(Float, nullable=False)
    currency = Column(String, nullable=False, default="MNT")
    payment_method = Column(String, nullable=True, index=True)
    counterparty = Column(String, nullable=True, index=True)
    type = Column(String, nullable=False, index=True)
    category = Column(String, nullable=False, index=True)
    source_file = Column(String, nullable=True)
    raw_row_hash = Column(String, nullable=False)
    is_reviewed = Column(Boolean, nullable=False, default=False)
    is_ignored = Column(Boolean, nullable=False, default=False)
    review_note = Column(Text, nullable=True)

    import_batch_id = Column(Integer, ForeignKey("import_batches.id"), nullable=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)

    user = relationship("User", back_populates="transactions")
    import_batch = relationship("ImportBatch", back_populates="transactions")

    __table_args__ = (
        UniqueConstraint("user_id", "raw_row_hash", name="uq_transactions_user_raw_row_hash"),
    )


class SavedView(Base):
    __tablename__ = "saved_views"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False, index=True)
    filter_type = Column(String, nullable=True)
    category = Column(String, nullable=True)
    payment_method = Column(String, nullable=True)
    start_date = Column(DateTime, nullable=True)
    end_date = Column(DateTime, nullable=True)

    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)

    user = relationship("User", back_populates="saved_views")


class CategoryRule(Base):
    __tablename__ = "category_rules"

    id = Column(Integer, primary_key=True, index=True)
    keyword = Column(String, nullable=False, index=True)
    category = Column(String, nullable=False, index=True)
    priority = Column(Integer, nullable=False, default=100)
    is_active = Column(Boolean, nullable=False, default=True)

    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)

    user = relationship("User", back_populates="category_rules")