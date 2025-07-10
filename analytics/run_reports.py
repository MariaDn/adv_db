import mysql.connector
import pandas as pd
import os

conn = mysql.connector.connect(
    host="mysql", port=3306, user="root", password="root", database="ad_analytics"
)

DATE_FROM = "2024-11-01"
DATE_TO = "2024-11-30"

queries = {
    "campaign_performance_ctr": f"""
        SELECT
            c.CampaignID,
            c.CampaignName,
            a.AdvertiserName,
            COUNT(e.EventID) AS Impressions,
            SUM(e.WasClicked) AS Clicks,
            SUM(e.AdCost) AS TotalSpend,
            ROUND(100.0 * SUM(e.WasClicked) / COUNT(e.EventID), 2) AS CTR_percent
        FROM AdEvents e
        INNER JOIN Campaigns c ON e.CampaignID = c.CampaignID
        INNER JOIN Advertisers a ON c.AdvertiserID = a.AdvertiserID
        WHERE e.Timestamp BETWEEN '{DATE_FROM}' AND '{DATE_TO}'
        GROUP BY c.CampaignID, c.CampaignName, a.AdvertiserName
        ORDER BY CTR_percent DESC
        LIMIT 5;
    """,
    "advertiser_spending": f"""
        SELECT
            a.AdvertiserName,
            COUNT(e.EventID) AS Impressions,
            SUM(e.WasClicked) AS Clicks,
            SUM(e.AdCost) AS TotalSpend,
            SUM(e.AdCost) AS TotalSpent
        FROM AdEvents e
        INNER JOIN Campaigns c ON e.CampaignID = c.CampaignID
        INNER JOIN Advertisers a ON c.AdvertiserID = a.AdvertiserID
        WHERE e.Timestamp BETWEEN '{DATE_FROM}' AND '{DATE_TO}'
        GROUP BY a.AdvertiserName
        ORDER BY TotalSpent DESC
        LIMIT 10;
    """,
    "cost_efficiency": f"""
        SELECT
            c.CampaignID,
            c.CampaignName,
            ROUND(SUM(e.AdCost) / NULLIF(SUM(e.WasClicked), 0), 2) AS CPC,
            ROUND((SUM(e.AdCost) / COUNT(e.EventID)) * 1000, 2) AS CPM
        FROM AdEvents e
        INNER JOIN Campaigns c ON e.CampaignID = c.CampaignID
        WHERE e.Timestamp BETWEEN '{DATE_FROM}' AND '{DATE_TO}'
        GROUP BY c.CampaignID, c.CampaignName
        ORDER BY CPM ASC;
    """,
    "regional_analysis": f"""
        SELECT
            e.Location,
            ROUND(SUM(e.AdRevenue), 2) AS TotalRevenue
        FROM AdEvents e
        WHERE e.Timestamp BETWEEN '{DATE_FROM}' AND '{DATE_TO}' AND e.WasClicked = 1
        GROUP BY e.Location
        ORDER BY TotalRevenue DESC
        LIMIT 10;
    """,
    "user_engagement": f"""
        SELECT
            u.UserID,
            COUNT(e.EventID) AS Clicks
        FROM AdEvents e
        JOIN Users u ON e.UserID = u.UserID
        WHERE e.WasClicked = 1
        GROUP BY u.UserID
        ORDER BY Clicks DESC
        LIMIT 10;
    """,
    "budget_consumption": f"""
        SELECT
            c.CampaignID,
            c.CampaignName,
            c.Budget,
            (c.Budget - c.RemainingBudget) AS Spent,
            ROUND(100.0 * (c.Budget - c.RemainingBudget) / c.Budget, 2) AS PercentSpent
        FROM Campaigns c
        WHERE (c.Budget - c.RemainingBudget) / c.Budget > 0.8
        ORDER BY PercentSpent DESC;
    """,
    "device_performance": f"""
        SELECT
            e.Device,
            COUNT(e.EventID) AS Impressions,
            SUM(e.WasClicked) AS Clicks,
            ROUND(100.0 * SUM(e.WasClicked) / COUNT(e.EventID), 2) AS CTR_percent
        FROM AdEvents e
        GROUP BY e.Device
        ORDER BY CTR_percent DESC;
    """,
}

RESULTS_DIR = "./analytics/results"

try:
    for name, query in queries.items():
        print(name)
        df = pd.read_sql(query, conn)
        print(df)
        df.to_csv(os.path.join(RESULTS_DIR, f"{name}.csv"))
finally:
    conn.close()
