CREATE DATABASE IF NOT EXISTS ad_analytics;

USE ad_analytics;
DROP TABLE IF EXISTS CampaignInterests;
DROP TABLE IF EXISTS UserInterests;
DROP TABLE IF EXISTS AdEvents;
DROP TABLE IF EXISTS Interests;
DROP TABLE IF EXISTS Users;
DROP TABLE IF EXISTS Campaigns;
DROP TABLE IF EXISTS Advertisers;

/*
Advertisers - рекламні агенції, це окрема сутність, що має ID і назву
*/

CREATE TABLE Advertisers (
    AdvertiserID INT AUTO_INCREMENT PRIMARY KEY,
    AdvertiserName VARCHAR(100) UNIQUE NOT NULL
);

/*
Campaigns - рекламна кампанія, яку запускає агенція (має посилання на агенцію), а також поля, що описують цю кампанію, а саме назва,
початок дії, кінець і тд. TargetCrateria розбитий на кілька кремих колонок (відповідно до інформації з csv файлу). Додаємо індекс по 
Advertiser. Виникло питання щодо AdSlotSize, де його зберігати, але після аналізу файлу івентів стало зрозуміло, що хоч AdSlotSize 
і передається там, але він однаковий в межах кампнанії, тому винесений в таблицю кампанії
*/

CREATE TABLE Campaigns (
    CampaignID INT AUTO_INCREMENT PRIMARY KEY,
    AdvertiserID INT,
    CampaignName VARCHAR(100) NOT NULL,
    CampaignStartDate DATE,
    CampaignEndDate DATE,
    Budget DECIMAL(15, 2),
    RemainingBudget DECIMAL(15, 2),
    TargetingAgeMin INT,
    TargetingAgeMax INT,
    TargetingCountry VARCHAR(100),
    AdSlotSize VARCHAR(50),
    FOREIGN KEY (AdvertiserID) REFERENCES Advertisers(AdvertiserID),
    INDEX idx_advertiser_id (AdvertiserID)
);

/*
Users - користувачі та їхні атрибути
*/

CREATE TABLE Users (
    UserID BIGINT PRIMARY KEY,
    Age INT,
    Gender ENUM('Male', 'Female', 'Non-Binary') NOT NULL,
    Location VARCHAR(50),
    SignupDate DATE,
    INDEX idx_location_age (Location, Age),
    INDEX idx_signup_date (SignupDate)

);

/*
Interests - винесла в окрему таблицю, бо вони є і у юзерів і у кампаній, крім того мають з типи таблицями зв'язок багато до багато
(якщо зберігати кожен інтерес окремо, а не суцільним текстом, з яким надалі буде незручно працювати)
*/

CREATE TABLE Interests (
    InterestID INT PRIMARY KEY AUTO_INCREMENT,
    Name VARCHAR(50) UNIQUE
);

/*
AdEvents - подія рекламної кампанії. Має зв'язок з Campaigns, тому додаємо CampaignID, а також з юзером (UserID). Додаємо інформацію, 
що стосуються конкретного івента, а також індекси
*/

CREATE TABLE AdEvents (
    EventID CHAR(36) PRIMARY KEY,
    CampaignID INT NOT NULL,
    UserID BIGINT NOT NULL,
    Device VARCHAR(50),
    Location VARCHAR(100),
    Timestamp DATETIME,
    BidAmount FLOAT,
    AdCost FLOAT,
    AdRevenue FLOAT,
    WasClicked TINYINT(1) NOT NULL DEFAULT 0,
    ClickTimestamp DATETIME NULL,
    FOREIGN KEY (CampaignID) REFERENCES Campaigns(CampaignID),
    FOREIGN KEY (UserID) REFERENCES Users(UserID),
    INDEX idx_campaign_id (CampaignID),
    INDEX idx_user_id (UserID),
    INDEX idx_was_clicked (WasClicked),
    INDEX idx_timestamp (Timestamp),
    INDEX idx_device_timestamp (Device, Timestamp),
    INDEX idx_location_timestamp (Location, Timestamp)
);

CREATE TABLE UserInterests (
    UserID BIGINT,
    InterestID INT,
    PRIMARY KEY (UserID, InterestID),
    FOREIGN KEY (UserID) REFERENCES Users(UserID),
    FOREIGN KEY (InterestID) REFERENCES Interests(InterestID)
);

CREATE TABLE CampaignInterests (
    CampaignID INT,
    InterestID INT,
    PRIMARY KEY (CampaignID, InterestID),
    FOREIGN KEY (CampaignID) REFERENCES Campaigns(CampaignID),
    FOREIGN KEY (InterestID) REFERENCES Interests(InterestID)
);

