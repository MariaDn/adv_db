CREATE KEYSPACE IF NOT EXISTS ad_analytics
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE ad_analytics;

-- 1. Companies: usefull for Fetch CTR per campaign per day
DROP TABLE IF EXISTS campaign_daily_performance;
CREATE TABLE campaign_daily_performance (
    campaign_id INT,
    event_date DATE,
    impressions INT,
    clicks INT,
    ctr FLOAT,
    PRIMARY KEY ((campaign_id), event_date)
) WITH CLUSTERING ORDER BY (event_date DESC);

-- 2. Spends of advertiser: Top advertisers by ad spend in last 30 days
DROP TABLE IF EXISTS advertiser_daily_spend;
CREATE TABLE advertiser_daily_spend (
    advertiser_id INT,
    spend_date DATE,
    total_spend DECIMAL,
    PRIMARY KEY ((advertiser_id), spend_date)
) WITH CLUSTERING ORDER BY (spend_date DESC);

-- 3. Users interactions: Given UserID, retrieve last 10 ads seen + clicked
DROP TABLE IF EXISTS user_engagement_history;
CREATE TABLE user_engagement_history (
    user_id BIGINT,
    event_time TIMESTAMP,
    campaign_id INT,
    ad_clicked BOOLEAN,
    PRIMARY KEY ((user_id), event_time)
) WITH CLUSTERING ORDER BY (event_time DESC);

-- 4. Clicks: Top 10 users with most ad clicks (last 30 days)
DROP TABLE IF EXISTS user_clicks_daily;
CREATE TABLE user_clicks_daily (
    click_date DATE,
    clicks INT,
    user_id BIGINT,
    PRIMARY KEY ((click_date), clicks, user_id)
) WITH CLUSTERING ORDER BY (clicks DESC);
-- Альтернатива: ((click_date), user_id)

-- 5. Adv region spends: Top 5 advertisers who spent the most in a region (e.g., USA)
DROP TABLE IF EXISTS advertiser_region_spend;
CREATE TABLE advertiser_region_spend (
    region_name TEXT,
    spend_date DATE,
    advertiser_id INT,
    total_spend DECIMAL,
    PRIMARY KEY ((region_name, spend_date), total_spend, advertiser_id)
) WITH CLUSTERING ORDER BY (total_spend DESC);
