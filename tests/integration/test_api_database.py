"""
Integration test for API database queries
==========================================
Tests API queries against real database.
"""

import sys
from pathlib import Path
import pytest
import psycopg2

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import DATABASE_CONFIG


@pytest.fixture
def db_conn():
    """Database connection fixture."""
    conn = psycopg2.connect(**DATABASE_CONFIG)
    yield conn
    conn.close()


class TestCustomerRiskQuery:
    """Test customer risk API query."""
    
    def test_customer_query_returns_data(self, db_conn):
        """Customer risk query should return data for valid customer."""
        cur = db_conn.cursor()
        
        cur.execute("""
            SELECT customer_unique_id 
            FROM mart.dim_customer
            WHERE is_current = true
            LIMIT 1
        """)
        customer_id = cur.fetchone()[0]
        
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
        assert result is not None
        assert result[0] == customer_id
        
        cur.close()


class TestSellerRiskQuery:
    """Test seller risk API query."""
    
    def test_seller_query_returns_data(self, db_conn):
        """Seller risk query should return data for valid seller."""
        cur = db_conn.cursor()
        
        cur.execute("""
            SELECT seller_id 
            FROM mart.dim_seller
            WHERE is_current = true
            LIMIT 1
        """)
        seller_id = cur.fetchone()[0]
        
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
        assert result is not None
        assert result[0] == seller_id
        
        cur.close()


class TestPaymentAlertsQuery:
    """Test payment alerts API query."""
    
    def test_payment_alerts_query(self, db_conn):
        """Payment alerts query should return high risk orders."""
        cur = db_conn.cursor()
        
        cur.execute("""
            SELECT 
                order_id,
                customer_unique_id,
                payment_value,
                payment_installments,
                payment_risk_score,
                event_ts
            FROM mart.fact_order_payments
            WHERE payment_risk_score >= 0.7
            ORDER BY payment_risk_score DESC, payment_value DESC
            LIMIT 20
        """)
        
        results = cur.fetchall()
        assert len(results) > 0
        
        for row in results:
            assert row[4] >= 0.7, "All results should have risk score >= 0.7"
        
        cur.close()
    
    def test_payment_alerts_sorted_correctly(self, db_conn):
        """Payment alerts should be sorted by risk score DESC."""
        cur = db_conn.cursor()
        
        cur.execute("""
            SELECT payment_risk_score
            FROM mart.fact_order_payments
            WHERE payment_risk_score >= 0.7
            ORDER BY payment_risk_score DESC
            LIMIT 10
        """)
        
        scores = [row[0] for row in cur.fetchall()]
        assert scores == sorted(scores, reverse=True)
        
        cur.close()


class TestFulfillmentAlertsQuery:
    """Test fulfillment alerts API query."""
    
    def test_fulfillment_alerts_query(self, db_conn):
        """Fulfillment alerts query should return high risk fulfillments."""
        cur = db_conn.cursor()
        
        cur.execute("""
            SELECT 
                order_id,
                seller_id,
                item_price,
                fulfillment_risk_score,
                is_late_delivery,
                event_ts
            FROM mart.fact_order_fulfillment
            WHERE fulfillment_risk_score >= 0.7
            ORDER BY fulfillment_risk_score DESC, item_price DESC
            LIMIT 20
        """)
        
        results = cur.fetchall()
        assert len(results) > 0
        
        for row in results:
            assert row[3] >= 0.7, "All results should have risk score >= 0.7"
        
        cur.close()


class TestPlatformStatsQuery:
    """Test platform stats API query."""
    
    def test_platform_stats_query(self, db_conn):
        """Platform stats query should return aggregated metrics."""
        cur = db_conn.cursor()
        
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
        assert result[0] > 0, "Should have total orders"
        assert result[1] > 0, "Should have total customers"
        assert result[2] > 0, "Should have total sellers"
        assert 0 <= result[3] <= 1, "Avg payment risk should be 0-1"
        assert 0 <= result[4] <= 1, "Avg fulfillment risk should be 0-1"
        assert 0 <= result[5] <= 1, "High risk pct should be 0-1"
        
        cur.close()
