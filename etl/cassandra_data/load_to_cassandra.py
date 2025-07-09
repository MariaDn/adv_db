# load_to_cassandra.py
import logging
import os
from datetime import datetime

import mysql.connector
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


# --- Logging setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class MySQLClient:
    def __init__(self):
        self.conn = mysql.connector.connect(
            host=os.environ.get("MYSQL_HOST", "localhost"),
            port=int(os.environ.get("MYSQL_PORT", 3306)),
            user=os.environ.get("MYSQL_USER", "root"),
            password=os.environ.get("MYSQL_PASSWORD", ""),
            database=os.environ.get("MYSQL_DATABASE", "ad_analytics")
        )
        self.cursor = self.conn.cursor(dictionary=True)


class CassandraClient:
    def __init__(self):
        auth_provider = PlainTextAuthProvider(
            username=os.environ.get("CASSANDRA_USERNAME", "cassandra"),
            password=os.environ.get("CASSANDRA_PASSWORD", "cassandra")
        )
        self.cluster = Cluster([os.environ.get("CASSANDRA_HOST", "localhost")],
                               port=int(os.environ.get("CASSANDRA_PORT", 9042)),
                               auth_provider=auth_provider, 
                               protocol_version=5)
        self.session = self.cluster.connect(os.environ.get("CASSANDRA_KEYSPACE", "ad_analytics"))


class ETLLoader:
    def __init__(self, mysql_client, cassandra_client):
        self.mysql = mysql_client
        self.cassandra = cassandra_client.session

    def load_campaign_daily_performance(self):
        logging.info("Loading campaign_daily_performance")
        self.mysql.cursor.execute("""
            SELECT CampaignID, DATE(Timestamp) as Day,
                COUNT(*) as impressions,
                SUM(WasClicked) as clicks
            FROM AdEvents
            GROUP BY CampaignID, Day
        """)
        for row in self.mysql.cursor:
            impressions = int(row['impressions'] or 0)
            clicks = int(row['clicks'] or 0)
            ctr = (clicks / impressions * 100) if impressions else 0.0

            self.cassandra.execute("""
                INSERT INTO campaign_daily_performance (
                    campaign_id, event_date, impressions, clicks, ctr
                ) VALUES (%s, %s, %s, %s, %s)
            """, (
                int(row['CampaignID']),
                row['Day'],
                impressions,
                clicks,
                ctr
            ))

    def load_advertiser_daily_spend(self):
        logging.info("Loading advertiser_daily_spend")
        self.mysql.cursor.execute("""
            SELECT c.AdvertiserID, DATE(e.Timestamp) as Day,
                   SUM(e.AdCost) as total_spend
            FROM AdEvents e
            JOIN Campaigns c ON e.CampaignID = c.CampaignID
            GROUP BY c.AdvertiserID, Day
        """)
        for row in self.mysql.cursor:
            self.cassandra.execute("""
                INSERT INTO advertiser_daily_spend (advertiser_id, spend_date, total_spend)
                VALUES (%s, %s, %s)
            """, (row['AdvertiserID'], row['Day'], row['total_spend']))

    def load_user_engagement_history(self):
        logging.info("Loading user_engagement_history")
        self.mysql.cursor.execute("""
            SELECT UserID, CampaignID, Timestamp, WasClicked
            FROM AdEvents
        """)
        for row in self.mysql.cursor:
            self.cassandra.execute("""
                INSERT INTO user_engagement_history (user_id, event_time, campaign_id, ad_clicked)
                VALUES (%s, %s, %s, %s)
            """, (row['UserID'], row['Timestamp'], row['CampaignID'], bool(row['WasClicked'])))

    def load_user_clicks_daily(self):
        logging.info("Loading user_clicks_daily")
        self.mysql.cursor.execute("""
            SELECT UserID, DATE(Timestamp) as Day,
                   SUM(WasClicked) as clicks
            FROM AdEvents
            GROUP BY UserID, Day
        """)
        for row in self.mysql.cursor:
            self.cassandra.execute("""
                INSERT INTO user_clicks_daily (click_date, user_id, clicks)
                VALUES (%s, %s, %s)
            """, (row['Day'], row['UserID'], int(row['clicks'])))

    def load_advertiser_region_spend(self):
        logging.info("Loading advertiser_region_spend")
        self.mysql.cursor.execute("""
            SELECT l.LocationName as Region, c.AdvertiserID, DATE(e.Timestamp) as Day,
                   SUM(e.AdCost) as total_spend
            FROM AdEvents e
            JOIN Campaigns c ON e.CampaignID = c.CampaignID
            JOIN Locations l ON c.TargetingLocationID = l.LocationID
            GROUP BY Region, c.AdvertiserID, Day
        """)
        for row in self.mysql.cursor:
            self.cassandra.execute("""
                INSERT INTO advertiser_region_spend (region_name, spend_date, advertiser_id, total_spend)
                VALUES (%s, %s, %s, %s)
            """, (row['Region'], row['Day'], row['AdvertiserID'], row['total_spend']))

    def run_all(self):
        try:
            self.load_campaign_daily_performance()
            self.load_advertiser_daily_spend()
            self.load_user_engagement_history()
            self.load_user_clicks_daily()
            self.load_advertiser_region_spend()
            logging.info("ETL load to Cassandra complete.")
        except Exception as e:
            logging.exception("ETL failed: %s", e)


if __name__ == "__main__":
    mysql_client = MySQLClient()
    cassandra_client = CassandraClient()
    loader = ETLLoader(mysql_client, cassandra_client)
    loader.run_all()
