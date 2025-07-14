import mysql.connector
from config import settings

class MySQLDatabase:
    def __init__(self):
        self.conn = mysql.connector.connect(
            host=settings.MYSQL_HOST,
            port=settings.MYSQL_PORT,
            user=settings.MYSQL_USER,
            password=settings.MYSQL_PASSWORD,
            database=settings.MYSQL_DATABASE
        )
        self.cursor = self.conn.cursor(dictionary=True)

    def fetch_campaign_performance(self, campaign_id: int):
        query = """
        SELECT
            COUNT(*) AS impressions,
            COALESCE(SUM(WasClicked), 0) AS clicks,
            COALESCE(SUM(AdCost), 0.0) AS spend
        FROM AdEvents
        WHERE CampaignID = %s
        """
        self.cursor.execute(query, (campaign_id,))
        row = self.cursor.fetchone()

        if not row:
            return None

        if row['impressions']:
            row['ctr'] = round(row['clicks'] / row['impressions'], 4)
        else:
            row['ctr'] = 0.0
        row['campaign_id'] = campaign_id
        return row

    def fetch_advertiser_spending(self, advertiser_id: int):
        query = """
        SELECT SUM(e.AdCost) AS total_spend
        FROM AdEvents e
        JOIN Campaigns c ON e.CampaignID = c.CampaignID
        WHERE c.AdvertiserID = %s
        """
        self.cursor.execute(query, (advertiser_id,))
        row = self.cursor.fetchone()

        if not row:
            return None

        row['advertiser_id'] = advertiser_id
        return row

    def fetch_user_engagements(self, user_id: int):
        query = """
        SELECT CampaignID AS campaign_id, Timestamp AS event_time, WasClicked AS was_clicked
        FROM AdEvents
        WHERE UserID = %s AND WasClicked = 1
        ORDER BY Timestamp DESC
        LIMIT 10
        """
        self.cursor.execute(query, (user_id,))
        columns = [desc[0] for desc in self.cursor.description]
        rows = self.cursor.fetchall()
        return rows
