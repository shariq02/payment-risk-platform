"""
Payment Risk API - FastAPI Service
===================================
Serves risk scores and platform metrics from mart tables.

Endpoints:
  GET /health
  GET /risk/customer/{customer_id}
  GET /risk/seller/{seller_id}/summary
  GET /risk/top-payment-alerts
  GET /risk/top-fulfillment-alerts
  GET /platform/stats

Run:
  uvicorn main:app --reload --port 8000
"""

import sys
from pathlib import Path
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import psycopg2
from psycopg2.extras import RealDictCursor

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config import DATABASE_CONFIG

# ============================================================================
# DATABASE CONNECTION
# ============================================================================

def get_db():
    """Create database connection."""
    return psycopg2.connect(**DATABASE_CONFIG, cursor_factory=RealDictCursor)

# ============================================================================
# PYDANTIC MODELS
# ============================================================================

class HealthResponse(BaseModel):
    status: str
    database: str
    timestamp: datetime

class CustomerRisk(BaseModel):
    customer_unique_id: str
    risk_tier_code: str
    segment_code: str
    total_orders: int
    total_payment_value: float
    avg_payment_value: float
    has_dispute_history: bool
    last_order_ts: Optional[datetime]

class SellerRisk(BaseModel):
    seller_id: str
    risk_tier: str
    seller_reliability_score: float
    is_new_seller: bool
    total_orders_30d: int
    late_delivery_rate_30d: float
    avg_review_score_30d: Optional[float]
    cancellation_rate_30d: float

class PaymentAlert(BaseModel):
    order_id: str
    customer_unique_id: str
    payment_value: float
    payment_type: str
    payment_installments: int
    payment_risk_score: float
    risk_tier_code: str
    order_purchase_ts: datetime

class FulfillmentAlert(BaseModel):
    order_id: str
    seller_id: str
    order_item_count: int
    total_item_price: float
    fulfillment_risk_score: float
    delivery_days: Optional[int]
    is_late_delivery: bool
    order_purchase_ts: datetime

class PlatformStats(BaseModel):
    total_orders: int
    total_customers: int
    total_sellers: int
    avg_payment_risk: float
    avg_fulfillment_risk: float
    high_risk_orders_pct: float
    last_pipeline_run: Optional[datetime]

# ============================================================================
# FASTAPI APP
# ============================================================================

app = FastAPI(
    title="Payment Risk API",
    description="Risk scoring and platform metrics",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# ENDPOINTS
# ============================================================================

@app.get("/health", response_model=HealthResponse)
def health_check():
    """Health check with database connectivity test."""
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
        
        return HealthResponse(
            status="healthy",
            database="connected",
            timestamp=datetime.now()
        )
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database error: {str(e)}")


@app.get("/risk/customer/{customer_id}", response_model=CustomerRisk)
def get_customer_risk(customer_id: str):
    """Get customer risk profile."""
    conn = get_db()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT 
                customer_unique_id,
                risk_tier_code,
                segment_code,
                total_orders,
                total_payment_value,
                avg_payment_value,
                has_dispute_history,
                last_order_ts
            FROM mart.dim_customer
            WHERE customer_unique_id = %s
            AND is_current = true
        """, (customer_id,))
        
        result = cur.fetchone()
        
        if not result:
            raise HTTPException(status_code=404, detail="Customer not found")
        
        return CustomerRisk(**result)
        
    finally:
        cur.close()
        conn.close()


@app.get("/risk/seller/{seller_id}/summary", response_model=SellerRisk)
def get_seller_risk(seller_id: str):
    """Get seller risk summary."""
    conn = get_db()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT 
                seller_id,
                risk_tier,
                seller_reliability_score,
                is_new_seller,
                total_orders_30d,
                late_delivery_rate_30d,
                avg_review_score_30d,
                cancellation_rate_30d
            FROM mart.dim_seller
            WHERE seller_id = %s
            AND is_current = true
        """, (seller_id,))
        
        result = cur.fetchone()
        
        if not result:
            raise HTTPException(status_code=404, detail="Seller not found")
        
        return SellerRisk(**result)
        
    finally:
        cur.close()
        conn.close()


@app.get("/risk/top-payment-alerts", response_model=list[PaymentAlert])
def get_payment_alerts(
    limit: int = Query(20, ge=1, le=100),
    min_risk_score: float = Query(0.7, ge=0.0, le=1.0)
):
    """Get top payment risk alerts."""
    conn = get_db()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT 
                order_id,
                customer_unique_id,
                payment_value,
                payment_type,
                payment_installments,
                payment_risk_score,
                risk_tier_code,
                order_purchase_ts
            FROM mart.fact_order_payments
            WHERE payment_risk_score >= %s
            ORDER BY payment_risk_score DESC, payment_value DESC
            LIMIT %s
        """, (min_risk_score, limit))
        
        results = cur.fetchall()
        return [PaymentAlert(**row) for row in results]
        
    finally:
        cur.close()
        conn.close()


@app.get("/risk/top-fulfillment-alerts", response_model=list[FulfillmentAlert])
def get_fulfillment_alerts(
    limit: int = Query(20, ge=1, le=100),
    min_risk_score: float = Query(0.7, ge=0.0, le=1.0)
):
    """Get top fulfillment risk alerts."""
    conn = get_db()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT 
                order_id,
                seller_id,
                order_item_count,
                total_item_price,
                fulfillment_risk_score,
                delivery_days,
                is_late_delivery,
                order_purchase_ts
            FROM mart.fact_order_fulfillment
            WHERE fulfillment_risk_score >= %s
            ORDER BY fulfillment_risk_score DESC, total_item_price DESC
            LIMIT %s
        """, (min_risk_score, limit))
        
        results = cur.fetchall()
        return [FulfillmentAlert(**row) for row in results]
        
    finally:
        cur.close()
        conn.close()


@app.get("/platform/stats", response_model=PlatformStats)
def get_platform_stats():
    """Get platform-wide statistics."""
    conn = get_db()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT 
                COUNT(DISTINCT p.order_id) as total_orders,
                COUNT(DISTINCT p.customer_unique_id) as total_customers,
                COUNT(DISTINCT f.seller_id) as total_sellers,
                AVG(p.payment_risk_score) as avg_payment_risk,
                AVG(f.fulfillment_risk_score) as avg_fulfillment_risk,
                AVG(CASE WHEN p.payment_risk_score >= 0.7 THEN 1.0 ELSE 0.0 END) as high_risk_orders_pct
            FROM mart.fact_order_payments p
            LEFT JOIN mart.fact_order_fulfillment f ON p.order_id = f.order_id
        """)
        
        result = cur.fetchone()
        
        # Get last pipeline run from dbt metadata if available
        cur.execute("""
            SELECT MAX(dbt_updated_at) as last_run
            FROM mart.dim_customer
        """)
        last_run = cur.fetchone()
        
        return PlatformStats(
            **result,
            last_pipeline_run=last_run['last_run'] if last_run else None
        )
        
    finally:
        cur.close()
        conn.close()
