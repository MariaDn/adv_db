from pymongo import MongoClient
import json
import os
from datetime import datetime, timedelta

MONGO_HOST = os.environ.get("MONGO_HOST", "localhost")
client = MongoClient(f"mongodb://{MONGO_HOST}:27017/")
db = client["ad_analytics"]
collection = db["users"]

results_dir = "/app/results"
os.makedirs(results_dir, exist_ok=True)

# 1. All ad interactions for a specific user (impressions + clicks)
def query_all_interactions(user_id):
    pipeline = [
        {"$match": {"user_id": user_id}},
        {"$unwind": "$sessions"},
        {"$unwind": "$sessions.impressions"},
        {"$project": {
            "_id": 0,
            "user_id": 1,
            "session_id": "$sessions.session_id",
            "impression": "$sessions.impressions"
        }}
    ]
    return list(collection.aggregate(pipeline))

# 2. Last 5 ad sessions with timestamps and click behavior
def query_last_5_sessions(user_id):
    pipeline = [
        {"$match": {"user_id": user_id}},
        {"$unwind": "$sessions"},
        {"$sort": {"sessions.start_time": -1}},
        {"$limit": 5},
        {"$project": {
            "_id": 0,
            "user_id": 1,
            "session_id": "$sessions.session_id",
            "start_time": "$sessions.start_time",
            "clicks": {
                "$size": {
                    "$filter": {
                        "input": "$sessions.impressions",
                        "as": "imp",
                        "cond": {"$eq": ["$$imp.was_clicked", True]}
                    }
                }
            }
        }}
    ]
    return list(collection.aggregate(pipeline))

# 3. Number of ad clicks per hour per campaign in last 24h for specific advertiser
def query_clicks_last_24h(advertiser_name):
    since = datetime.fromisoformat("2024-11-13T03:01:37") - timedelta(hours=24)
    pipeline = [
        {"$unwind": "$sessions"},
        {"$unwind": "$sessions.impressions"},
        {"$match": {
            "sessions.impressions.was_clicked": True,
            "sessions.impressions.timestamp": {"$gte": since},
            "sessions.impressions.advertiser_name": advertiser_name
        }},
        {"$group": {
            "_id": {
                "campaign": "$sessions.impressions.campaign_name",
                "hour": {"$hour": "$sessions.impressions.timestamp"}
            },
            "clicks": {"$sum": 1}
        }}
    ]
    return list(collection.aggregate(pipeline))

# 4. Users who saw same ad 5+ times and never clicked
def query_ad_fatigue():
    pipeline = [
        {"$unwind": "$sessions"},
        {"$unwind": "$sessions.impressions"},
        {"$group": {
            "_id": {
                "user_id": "$user_id",
                "campaign": "$sessions.impressions.campaign_name"
            },
            "total_impressions": {"$sum": 1},
            "total_clicks": {
                "$sum": {
                    "$cond": ["$sessions.impressions.was_clicked", 1, 0]
                }
            }
        }},
        {"$match": {
            "total_impressions": {"$gte": 5},
            "total_clicks": 0
        }}
    ]
    return list(collection.aggregate(pipeline))

# 5. User's top 3 most engaged ad categories (clicked)
def query_top_categories(user_id):
    pipeline = [
        {"$match": {"user_id": user_id}},
        {"$unwind": "$sessions"},
        {"$unwind": "$sessions.impressions"},
        {"$match": {"sessions.impressions.was_clicked": True}},
        {"$group": {
            "_id": "$sessions.impressions.campaign_name",
            "clicks": {"$sum": 1}
        }},
        {"$sort": {"clicks": -1}},
        {"$limit": 3}
    ]
    return list(collection.aggregate(pipeline))

# Run and save all queries
queries = {
    "1_all_interactions": query_all_interactions(475),
    "2_last_5_sessions": query_last_5_sessions(933),
    "3_clicks_last_24h": query_clicks_last_24h("Advertiser_47"),
    "4_ad_fatigue_users": query_ad_fatigue(),
    "5_top_categories": query_top_categories(436039)
}

for name, data in queries.items():
    with open(os.path.join(results_dir, f"{name}.json"), "w") as f:
        json.dump(data, f, default=str, indent=2)

print("Queries executed and results saved in 'results/'")

