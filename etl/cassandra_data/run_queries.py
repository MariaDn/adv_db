# run_queries.py
import os
import logging
import argparse
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# --- Logging setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class CassandraQueryRunner:
    def __init__(self):
        auth_provider = PlainTextAuthProvider(
            username=os.environ.get("CASSANDRA_USERNAME", "cassandra"),
            password=os.environ.get("CASSANDRA_PASSWORD", "cassandra")
        )
        self.cluster = Cluster(
            [os.environ.get("CASSANDRA_HOST", "localhost")],
            port=int(os.environ.get("CASSANDRA_PORT", 9042)),
            auth_provider=auth_provider
        )
        self.session = self.cluster.connect(os.environ.get("CASSANDRA_KEYSPACE", "ad_analytics"))
        self.output_dir = "results"
        os.makedirs(self.output_dir, exist_ok=True)

    def save_results(self, name, rows):
        rows = list(rows)
        path = os.path.join(self.output_dir, f"{name}.txt")
        with open(path, "w") as f:
            for row in rows:
                f.write(str(row) + "\n")
        logging.info(f"Saved {len(rows)} rows to {path}")

    def run_ctr_per_campaign(self):
        logging.info("Running query: CTR per campaign per day")
        rows = self.session.execute("""
            SELECT campaign_id, event_date, ctr FROM campaign_daily_performance;
        """)
        result = [(r.campaign_id, r.event_date, r.ctr) for r in rows]
        self.save_results("query1_ctr_per_campaign", result)

    def run_top_advertisers(self, date):
        logging.info("Running query: Top 5 advertisers by spend since {date}")
        rows = self.session.execute("""
            SELECT advertiser_id, spend_date, total_spend FROM advertiser_daily_spend
            WHERE spend_date >= %s ALLOW FILTERING;
        """, (date,))
        spend_map = {}
        for r in rows:
            spend_map[r.advertiser_id] = spend_map.get(r.advertiser_id, 0) + float(r.total_spend)
        top5 = sorted(spend_map.items(), key=lambda x: x[1], reverse=True)[:5]
        self.save_results("query2_top_advertisers", top5)

    def run_user_engagement_history(self, user_id):
        logging.info(f"Running query: Last 10 ads seen by user {user_id}")
        rows = self.session.execute("""
            SELECT event_time, campaign_id, ad_clicked FROM user_engagement_history
            WHERE user_id = %s LIMIT 10;
        """, (user_id,))
        result = [(r.event_time, r.campaign_id, r.ad_clicked) for r in rows]
        self.save_results("query3_user_engagement", result)

    def run_top_users_clicks(self, date):
        logging.info("Running query: Top 10 users with most clicks since {date}")
        rows = self.session.execute("""
            SELECT click_date, user_id, clicks FROM user_clicks_daily
            WHERE click_date >= %s ALLOW FILTERING;
        """, (date,))
        click_map = {}
        for r in rows:
            click_map[r.user_id] = click_map.get(r.user_id, 0) + int(r.clicks)
        top10 = sorted(click_map.items(), key=lambda x: x[1], reverse=True)[:10]
        self.save_results("query4_top_users", top10)

    def run_top_advertisers_by_region(self, date, region="USA"):
        logging.info(f"Running query: Top 5 advertisers in region '{region}' since {date}")
        rows = self.session.execute("""
            SELECT region_name, spend_date, advertiser_id, total_spend FROM advertiser_region_spend
            WHERE region_name = %s AND spend_date >= %s ALLOW FILTERING;
        """, (region, date))
        region_spend = {}
        for r in rows:
            region_spend[r.advertiser_id] = region_spend.get(r.advertiser_id, 0) + float(r.total_spend)
        top5_region = sorted(region_spend.items(), key=lambda x: x[1], reverse=True)[:5]
        self.save_results("query5_top_advertisers_usa", top5_region)

    def run_all(self, date):
        try:
            self.run_ctr_per_campaign()
            self.run_top_advertisers(date)
            self.run_user_engagement_history(145276)
            self.run_top_users_clicks(date)
            self.run_top_advertisers_by_region(date, "USA")
            logging.info("All queries executed successfully.")
        except Exception as e:
            logging.exception("Query execution failed: %s", e)


if __name__ == "__main__":
    # It is said to use last 30 days, but we do not have data for this period, so I choose another period with data
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cutoff-date",
        help="Date in format YYYY-MM-DD",
        type=str
    )
    args = parser.parse_args()

    if args.cutoff_date:
        try:
            cutoff_date = datetime.strptime(args.cutoff_date, "%Y-%m-%d").date()
        except ValueError:
            raise ValueError("Uknown format. Please, use YYYY-MM-DD.")
    else:
        cutoff_date = datetime.now().date() - timedelta(days=30)

    runner = CassandraQueryRunner()
    runner.run_all(date=cutoff_date)
