# etl/transform_load.py

import pandas as pd
import mysql.connector
from mysql.connector import errorcode
import re

DB_CONFIG = {
    "user": "root",
    "password": "root",
    "host": "mysql",
    "port": "3306",
    "database": "ad_analytics",
}


def parse_targeting_criteria(criteria_str):
    """Парсинг TargetingCriteria (Age XX-YY, Country, Interest)"""
    age_min, age_max, country, interests = None, None, None, []

    age_match = re.search(r"Age (\d+)-(\d+)", criteria_str)
    if age_match:
        age_min = int(age_match.group(1))
        age_max = int(age_match.group(2))

    countries = ["USA", "UK", "Germany", "India", "Australia"]
    for c in countries:
        if c in criteria_str:
            country = c
            break

    parts = [part.strip() for part in criteria_str.split(",")]
    for part in parts:
        if not part.startswith("Age") and part not in countries and part != "":
            interests.append(part)

    return age_min, age_max, country, interests


# ------------- Loading CSV ----------------

print("Loading CSV files...")

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

df_users = pd.read_csv("/data/users.csv")
df_campaigns = pd.read_csv("/data/campaigns.csv")

print("Connecting to database...")

try:
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    conn.start_transaction()
    print("Connected to DB. Transaction started.")

    # ------------ Advertisers ----------------
    print("Import to Advertisers")
    advertiser_names = (
        pd.concat([df_events["AdvertiserName"], df_campaigns["AdvertiserName"]])
        .dropna()
        .unique()
        .tolist()
    )
    cursor.executemany(
        "INSERT IGNORE INTO Advertisers (AdvertiserName) VALUES (%s)",
        [(name,) for name in advertiser_names],
    )
    conn.commit()

    cursor.execute("SELECT AdvertiserID, AdvertiserName FROM Advertisers")
    advertiser_map = {
        advertiser_name: advertiser_id
        for advertiser_id, advertiser_name in cursor.fetchall()
    }

    # ------------ Interests ----------------
    print("Import to Interests")
    all_interests = set()

    # from users
    for interests_str in df_users["Interests"].dropna():
        interests = [i.strip() for i in interests_str.split(",")]
        all_interests.update(interests)

    # from campaigns
    for criteria_str in df_campaigns["TargetingCriteria"].dropna():
        _, _, _, interests = parse_targeting_criteria(criteria_str)
        all_interests.update(interests)

    # insert Interests
    cursor.executemany(
        "INSERT IGNORE INTO Interests (Name) VALUES (%s)",
        [(interest,) for interest in all_interests],
    )
    conn.commit()

    cursor.execute("SELECT InterestID, Name FROM Interests")
    interest_map = {name: interest_id for interest_id, name in cursor.fetchall()}

    # ------------ Users ----------------
    print("Import to Users")
    users_data = []
    for _, row in df_users.iterrows():
        users_data.append(
            (
                row["UserID"],
                row["Age"],
                row["Gender"],
                row["Location"],
                row["SignupDate"],
            )
        )
    cursor.executemany(
        """
        INSERT IGNORE INTO Users (UserID, Age, Gender, Location, SignupDate)
        VALUES (%s, %s, %s, %s, %s)
        """,
        users_data,
    )
    conn.commit()

    # ------------ UserInterests ----------------
    print("Import to UserInterests")
    user_interest_values = []
    for _, row in df_users.iterrows():
        user_id = row["UserID"]
        interests = [i.strip() for i in row["Interests"].split(",")]
        for interest in interests:
            interest_id = interest_map.get(interest)
            if interest_id:
                user_interest_values.append((user_id, interest_id))

    cursor.executemany(
        "INSERT IGNORE INTO UserInterests (UserID, InterestID) VALUES (%s, %s)",
        user_interest_values,
    )
    conn.commit()

    # ------------ Campaigns ----------------
    print("Import to Campaigns")
    campaign_map = {}

    for _, row in df_campaigns.iterrows():
        advertiser_id = advertiser_map.get(row["AdvertiserName"])
        age_min, age_max, country, interests = parse_targeting_criteria(
            row["TargetingCriteria"]
        )

        cursor.execute(
            """
            INSERT INTO Campaigns (
                AdvertiserID, CampaignName, CampaignStartDate, CampaignEndDate,
                Budget, RemainingBudget, TargetingAgeMin, TargetingAgeMax, TargetingCountry, AdSlotSize
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                advertiser_id,
                row["CampaignName"],
                row["CampaignStartDate"],
                row["CampaignEndDate"],
                row["Budget"],
                row["RemainingBudget"],
                age_min,
                age_max,
                country,
                row["AdSlotSize"],
            ),
        )
        campaign_id = cursor.lastrowid
        campaign_map[row["CampaignName"]] = campaign_id

        # CampaignInterests
        campaign_interest_values = []
        for interest in interests:
            interest_id = interest_map.get(interest)
            if interest_id:
                campaign_interest_values.append((campaign_id, interest_id))

        cursor.executemany(
            "INSERT IGNORE INTO CampaignInterests (CampaignID, InterestID) VALUES (%s, %s)",
            campaign_interest_values,
        )
    conn.commit()

    # ------------ AdEvents ----------------

    print("Import to AdEvents")
    ad_events_values = []
    skipped_events = 0

    for _, row in df_events.iterrows():
        campaign_id = campaign_map.get(row["CampaignName"])
        if campaign_id is None:
            print(
                f"Skipping AdEvent {row['EventID']} - unknown CampaignName: '{row['CampaignName']}'"
            )
            skipped_events += 1
            continue

        was_clicked = 1 if str(row["WasClicked"]).lower() == "true" else 0
        click_timestamp = (
            row["ClickTimestamp"]
            if pd.notna(row["ClickTimestamp"]) and row["ClickTimestamp"] != ""
            else None
        )

        ad_events_values.append(
            (
                row["EventID"],
                campaign_id,
                row["UserID"],
                row["Device"],
                row["Location"],
                row["Timestamp"],
                row["BidAmount"],
                row["AdCost"],
                row["AdRevenue"],
                was_clicked,
                click_timestamp,
            )
        )

    cursor.executemany(
        """
        INSERT INTO AdEvents (
            EventID, CampaignID, UserID, Device, Location, Timestamp,
            BidAmount, AdCost, AdRevenue, WasClicked, ClickTimestamp
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        ad_events_values,
    )
    conn.commit()

    print(f"Inserted {len(ad_events_values)} AdEvents. Skipped {skipped_events}.")

    # ------------ Done ----------------
    conn.commit()
    print("ETL finished successfully. Transaction committed.")

except mysql.connector.Error as err:
    print(f"Error: {err}")
    conn.rollback()
    print("Transaction rolled back.")

finally:
    if conn.is_connected():
        cursor.close()
        conn.close()
        print("MySQL connection closed.")
