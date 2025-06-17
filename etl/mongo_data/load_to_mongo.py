import os
from pymongo import MongoClient
import pandas as pd

df_users = pd.read_csv("/data/users.csv")
df_campaigns = pd.read_csv("/data/campaigns.csv")

df_events = pd.read_csv("/data/ad_events_short.csv", skiprows=[0], header=None)

df_events.columns = [
    "EventID",
    "AdvertiserName",
    "CampaignName",
    "CampaignStartDate",
    "CampaignEndDate",
    "TargetingAge",
    "TargetingInterest",
    "TargetingCountry",
    "AdSlotSize",
    "UserID",
    "Device",
    "Location",
    "Timestamp",
    "BidAmount",
    "AdCost",
    "WasClicked",
    "ClickTimestamp",
    "AdRevenue",
    "Budget",
    "RemainingBudget",
]
df_events["Timestamp"] = pd.to_datetime(df_events["Timestamp"])
df_events["ClickTimestamp"] = pd.to_datetime(df_events["ClickTimestamp"], errors='coerce')

MONGO_HOST = os.environ.get("MONGO_HOST", "localhost")
client = MongoClient(f"mongodb://{MONGO_HOST}:27017/")
db = client["ad_analytics"]
users_col = db["users"]
users_col.drop()

for user_id, user_group in df_events.groupby("UserID"):
    user_profile = df_users[df_users["UserID"] == user_id]
    if user_profile.empty:
        continue

    profile = user_profile.iloc[0].to_dict()
    interests = profile["Interests"].split(",") if isinstance(profile["Interests"], str) else []

    # Group by session (using device and date)
    user_group["SessionKey"] = user_group["Device"] + "_" + user_group["Timestamp"].dt.date.astype(str)
    sessions = []
    for skey, session_group in user_group.groupby("SessionKey"):
        impressions = []
        for _, row in session_group.iterrows():
            impression = {
                "event_id": row["EventID"],
                "timestamp": row["Timestamp"],
                "campaign_name": row["CampaignName"],
                "device": row["Device"],
                "location": row["Location"],
                "advertiser_name": row["AdvertiserName"],
                "was_clicked": bool(row["WasClicked"]),
                "click_timestamp": row["ClickTimestamp"] if pd.notnull(row["ClickTimestamp"]) else None,
                "ad_cost": row["AdCost"],
                "ad_revenue": row["AdRevenue"]
            }
            impressions.append(impression)
        
        sessions.append({
            "session_id": skey,
            "start_time": session_group["Timestamp"].min(),
            "device": session_group["Device"].iloc[0],
            "impressions": impressions
        })

    user_doc = {
        "user_id": user_id,
        "age": profile["Age"],
        "gender": profile["Gender"],
        "location": profile["Location"],
        "interests": interests,
        "signup_date": profile["SignupDate"],
        "sessions": sessions
    }

    users_col.replace_one({"user_id": user_id}, user_doc, upsert=True)

print("MongoDB populated with structured user engagement data.")
