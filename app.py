"""
Web Dashboard & Analytics Server for ETL Data Warehouse
Powered by Tornado with interactive charts, KPIs, SQL runner, and pipeline triggers.
"""

import os
import sys
import json
import sqlite3
import argparse
import subprocess
import tornado.ioloop
import tornado.web

DB_PATH = "data/analytics_warehouse.db"
DEFAULT_PORT = int(os.environ.get("PORT", 8080))


def query_db(sql: str, params: tuple = ()) -> list:
    if not os.path.exists(DB_PATH):
        return []
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()
    except Exception as e:
        print(f"Database query error: {e}")
        return []


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.set_header("Content-Type", "text/html; charset=utf-8")
        template_path = os.path.join(os.path.dirname(__file__), "src", "templates", "index.html")
        with open(template_path, "r", encoding="utf-8") as f:
            self.write(f.read())


class KpiHandler(tornado.web.RequestHandler):
    def get(self):
        sql = """
        SELECT 
            (SELECT COUNT(*) FROM fact_sales) AS total_sales,
            (SELECT COUNT(DISTINCT order_id) FROM fact_sales) AS unique_orders,
            (SELECT COUNT(*) FROM dim_customer) AS total_customers,
            (SELECT COUNT(*) FROM dim_product) AS total_products,
            (SELECT COUNT(*) FROM dim_store) AS total_stores,
            (SELECT ROUND(COALESCE(SUM(net_amount), 0), 2) FROM fact_sales) AS total_revenue,
            (SELECT ROUND(COALESCE(SUM(profit_amount), 0), 2) FROM fact_sales) AS total_profit,
            (SELECT ROUND((COALESCE(SUM(profit_amount), 0) / NULLIF(SUM(net_amount), 0)) * 100, 2) FROM fact_sales) AS margin_pct,
            (SELECT COUNT(*) FROM etl_rejected_records) AS quarantined_records
        """
        data = query_db(sql)
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps(data[0] if data else {}))


class MonthlyMetricsHandler(tornado.web.RequestHandler):
    def get(self):
        sql = """
        SELECT 
            d.year,
            d.month,
            d.month_name,
            COUNT(DISTINCT f.order_id) AS orders,
            ROUND(SUM(f.net_amount), 2) AS revenue,
            ROUND(SUM(f.profit_amount), 2) AS profit
        FROM fact_sales f
        JOIN dim_date d ON f.date_key = d.date_key
        GROUP BY d.year, d.month, d.month_name
        ORDER BY d.year, d.month
        """
        data = query_db(sql)
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps(data))


class CategoryMetricsHandler(tornado.web.RequestHandler):
    def get(self):
        sql = """
        SELECT 
            p.category,
            SUM(f.quantity) AS units_sold,
            ROUND(SUM(f.net_amount), 2) AS revenue,
            ROUND(SUM(f.profit_amount), 2) AS profit,
            ROUND((SUM(f.profit_amount) / NULLIF(SUM(f.net_amount), 0)) * 100, 2) AS margin_pct
        FROM fact_sales f
        JOIN dim_product p ON f.product_key = p.product_key
        GROUP BY p.category
        ORDER BY revenue DESC
        """
        data = query_db(sql)
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps(data))


class RegionalMetricsHandler(tornado.web.RequestHandler):
    def get(self):
        sql = """
        SELECT 
            s.region,
            COUNT(DISTINCT f.order_id) AS orders,
            ROUND(SUM(f.net_amount), 2) AS revenue,
            ROUND(SUM(f.profit_amount), 2) AS profit
        FROM fact_sales f
        JOIN dim_store s ON f.store_key = s.store_key
        GROUP BY s.region
        ORDER BY revenue DESC
        """
        data = query_db(sql)
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps(data))


class PaymentMetricsHandler(tornado.web.RequestHandler):
    def get(self):
        sql = """
        SELECT 
            pm.payment_method,
            COUNT(DISTINCT f.order_id) AS orders,
            ROUND(SUM(f.net_amount), 2) AS volume,
            ROUND(SUM(f.net_amount) / NULLIF(COUNT(DISTINCT f.order_id), 0), 2) AS aov
        FROM fact_sales f
        JOIN dim_payment_method pm ON f.payment_key = pm.payment_key
        GROUP BY pm.payment_method
        ORDER BY volume DESC
        """
        data = query_db(sql)
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps(data))


class TopCustomersHandler(tornado.web.RequestHandler):
    def get(self):
        sql = """
        SELECT 
            c.customer_id,
            c.first_name || ' ' || c.last_name AS customer_name,
            c.email,
            c.customer_segment,
            c.city,
            c.country,
            COUNT(DISTINCT f.order_id) AS orders,
            ROUND(SUM(f.net_amount), 2) AS total_spend,
            ROUND(SUM(f.profit_amount), 2) AS total_profit
        FROM fact_sales f
        JOIN dim_customer c ON f.customer_key = c.customer_key
        GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.customer_segment, c.city, c.country
        ORDER BY total_spend DESC
        LIMIT 15
        """
        data = query_db(sql)
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps(data))


class AuditLogsHandler(tornado.web.RequestHandler):
    def get(self):
        sql = "SELECT * FROM etl_audit_log ORDER BY start_time DESC LIMIT 10"
        data = query_db(sql)
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps(data))


class QuarantinedHandler(tornado.web.RequestHandler):
    def get(self):
        sql = "SELECT * FROM etl_rejected_records ORDER BY rejected_at DESC LIMIT 20"
        data = query_db(sql)
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps(data))


class QueryHandler(tornado.web.RequestHandler):
    def post(self):
        try:
            req = json.loads(self.request.body)
            query = req.get("query", "").strip()

            if not query.lower().startswith(("select", "with", "pragma", "explain")):
                self.set_status(400)
                self.write(json.dumps({"error": "Only SELECT or analytical queries are permitted."}))
                return

            data = query_db(query)
            self.set_header("Content-Type", "application/json")
            self.write(json.dumps({"rows": data, "count": len(data)}))
        except Exception as e:
            self.set_status(500)
            self.write(json.dumps({"error": str(e)}))


class RunPipelineHandler(tornado.web.RequestHandler):
    def post(self):
        try:
            cmd = [sys.executable, "run_pipeline.py", "--full-run"]
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=180
            )
            self.set_header("Content-Type", "application/json")
            self.write(json.dumps({
                "success": proc.returncode == 0,
                "stdout": proc.stdout[-1500:] if proc.stdout else "",
                "stderr": proc.stderr[-500:] if proc.stderr else ""
            }))
        except Exception as e:
            self.set_status(500)
            self.write(json.dumps({"error": str(e)}))


def make_app():
    return tornado.web.Application([
        (r"/", MainHandler),
        (r"/api/kpis", KpiHandler),
        (r"/api/monthly-metrics", MonthlyMetricsHandler),
        (r"/api/category-metrics", CategoryMetricsHandler),
        (r"/api/regional-metrics", RegionalMetricsHandler),
        (r"/api/payment-metrics", PaymentMetricsHandler),
        (r"/api/top-customers", TopCustomersHandler),
        (r"/api/audit-logs", AuditLogsHandler),
        (r"/api/quarantined", QuarantinedHandler),
        (r"/api/query", QueryHandler),
        (r"/api/run-pipeline", RunPipelineHandler),
    ])


def ensure_database():
    """Seeds the database if running for the first time without an existing db."""
    if not os.path.exists(DB_PATH):
        print("Initial database not detected. Running ETL pipeline to initialize warehouse...")
        cmd = [sys.executable, "run_pipeline.py", "--full-run"]
        subprocess.run(cmd, check=True)


def start_server_with_fallback(app, preferred_port: int, max_attempts: int = 10):
    """Binds to preferred_port or automatically falls back to subsequent ports if occupied."""
    current_port = preferred_port
    for attempt in range(max_attempts):
        try:
            app.listen(current_port)
            print(f"ETL Warehouse Analytics Web Server running at http://localhost:{current_port}")
            return current_port
        except OSError as e:
            if "10048" in str(e) or getattr(e, "errno", None) in (98, 10048, 48):
                print(f"[Notice] Port {current_port} is occupied by another process. Trying port {current_port + 1}...")
                current_port += 1
            else:
                raise e
    raise RuntimeError(f"Unable to bind to any available port between {preferred_port} and {preferred_port + max_attempts - 1}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Warehouse Analytics Web Dashboard")
    parser.add_argument("-p", "--port", type=int, default=DEFAULT_PORT, help="Port to bind server (default: 8080)")
    args = parser.parse_args()

    ensure_database()
    web_app = make_app()
    active_port = start_server_with_fallback(web_app, args.port)
    tornado.ioloop.IOLoop.current().start()
