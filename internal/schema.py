"""
Schema definitions and data quality contracts.
"""

from typing import Optional
from datetime import datetime
from decimal import Decimal
from pydantic import BaseModel, Field, validator


class CustomerContract(BaseModel):
    """Schema contract for the dim_customers table."""
    customer_id: str
    name: str
    email: Optional[str] = None
    age: Optional[int] = Field(None, ge=0, le=150)
    city: Optional[str] = None
    country: Optional[str] = None
    favorite_team: Optional[str] = None
    membership_tier: Optional[str] = None
    signup_date: Optional[str] = None

    class Config:
        table_name = "dim_customers"
        expected_row_count = 200
        expected_row_count_threshold = 190


class TransactionContract(BaseModel):
    """Schema contract for the fact_transactions table."""
    transaction_id: str
    customer_id: str
    transaction_date: datetime
    amount_eur: Decimal = Field(..., ge=Decimal("-1000"), le=Decimal("50000"))
    category: str
    merchant: Optional[str] = None

    class Config:
        table_name = "fact_transactions"
        expected_row_count = 2505
        expected_row_count_threshold = 2400
        allow_orphan_fk = False


class SentimentContract(BaseModel):
    """Schema contract for the sentiment table."""
    post_id: str
    user_name: Optional[str] = None
    topic: Optional[str] = None
    sentiment_score: Optional[float] = Field(None, ge=-1.0, le=1.0)
    engagement: Optional[int] = Field(None, ge=0)
    published_at: Optional[str] = None

    class Config:
        table_name = "fact_sentiment"


# SQL Schema Definitions (DDL)

CREATE_STAGING_CUSTOMERS = """
CREATE TABLE IF NOT EXISTS raw_customers (
    _row_id INTEGER PRIMARY KEY,
    customer_id VARCHAR,
    name VARCHAR,
    email VARCHAR,
    age VARCHAR,
    city VARCHAR,
    country VARCHAR,
    signup_date VARCHAR,
    favorite_team VARCHAR,
    membership_tier VARCHAR,
    gender VARCHAR,
    _load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_STAGING_TRANSACTIONS = """
CREATE TABLE IF NOT EXISTS raw_transactions (
    _row_id INTEGER PRIMARY KEY,
    transaction_id VARCHAR,
    customer_id VARCHAR,
    timestamp VARCHAR,
    amount VARCHAR,
    currency VARCHAR,
    category VARCHAR,
    merchant VARCHAR,
    description VARCHAR,
    _load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_STAGING_SENTIMENT = """
CREATE TABLE IF NOT EXISTS raw_sentiment (
    _row_id INTEGER PRIMARY KEY,
    id VARCHAR,
    user VARCHAR,
    source VARCHAR,
    text VARCHAR,
    published_at VARCHAR,
    topic VARCHAR,
    tags VARCHAR,
    sentiment_score VARCHAR,
    engagement VARCHAR,
    _load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_DIM_CUSTOMERS = """
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    email VARCHAR,
    age INTEGER CHECK (age >= 0 AND age <= 150),
    city VARCHAR,
    country VARCHAR,
    favorite_team VARCHAR,
    membership_tier VARCHAR,
    signup_date DATE,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_FACT_TRANSACTIONS = """
CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR NOT NULL,
    transaction_date TIMESTAMP NOT NULL,
    amount_eur DECIMAL(10, 2) NOT NULL,
    category VARCHAR NOT NULL,
    merchant VARCHAR,
    _source_row_id INTEGER,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id)
);
"""

CREATE_FACT_SENTIMENT = """
CREATE TABLE IF NOT EXISTS fact_sentiment (
    post_id VARCHAR PRIMARY KEY,
    user_name VARCHAR,
    topic VARCHAR,
    sentiment_score DECIMAL(3, 2),
    engagement INTEGER,
    published_at TIMESTAMP,
    _source_row_id INTEGER,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_CUSTOMER_PROFILE = """
CREATE TABLE IF NOT EXISTS customer_profile (
    customer_id VARCHAR PRIMARY KEY,
    txn_count INTEGER,
    total_spend DECIMAL(10, 2),
    avg_txn DECIMAL(10, 2),
    last_txn_date DATE,
    match_ticket_count INTEGER,
    sports_affinity_ratio DECIMAL(3, 2),
    avg_days_between_txns DECIMAL(5, 1),
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""
