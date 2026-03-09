-- ============================================================================
-- STADIA360 デモ基盤セットアップ SQL
-- 目的: 電通 STADIA360 統合マーケティング基盤のデモ用に
--       ダミーデータの生成・セマンティックビューの定義を行う
-- ============================================================================

-- ############################################################################
-- STEP 1: DB / Schema 作成
-- ############################################################################

CREATE DATABASE IF NOT EXISTS KFUKAMORI_GEN_DB;
CREATE SCHEMA IF NOT EXISTS KFUKAMORI_GEN_DB.STADIA360;
USE SCHEMA KFUKAMORI_GEN_DB.STADIA360;

-- ############################################################################
-- STEP 2: テーブル作成
-- ############################################################################

-- 2-1. キャンペーンマスタ（横断紐付けキー）
CREATE OR REPLACE TABLE CAMPAIGNS (
    CAMPAIGN_ID       VARCHAR(20)   PRIMARY KEY,
    CAMPAIGN_NAME     VARCHAR(200)  NOT NULL,
    ADVERTISER        VARCHAR(100)  NOT NULL,
    PRODUCT_CATEGORY  VARCHAR(100)  NOT NULL,
    START_DATE        DATE          NOT NULL,
    END_DATE          DATE          NOT NULL,
    BUDGET_MM         NUMBER(12,2)  NOT NULL,
    TARGET_AREA       VARCHAR(50)   NOT NULL
);

-- 2-2. テレビ視聴ログデータ
CREATE OR REPLACE TABLE TV_VIEWING_LOG (
    VIEWING_ID        NUMBER        AUTOINCREMENT PRIMARY KEY,
    HOUSEHOLD_ID      VARCHAR(20)   NOT NULL,
    CHANNEL           VARCHAR(50)   NOT NULL,
    PROGRAM_NAME      VARCHAR(200)  NOT NULL,
    VIEWING_START     TIMESTAMP_NTZ NOT NULL,
    VIEWING_END       TIMESTAMP_NTZ NOT NULL,
    VIEWING_SECONDS   NUMBER        NOT NULL,
    CM_EXPOSED        BOOLEAN       NOT NULL,
    CAMPAIGN_ID       VARCHAR(20),
    AREA              VARCHAR(50)   NOT NULL,
    DEVICE_TYPE       VARCHAR(20)   NOT NULL, -- 'TV', 'CTV'
    CREATIVE_NAME     VARCHAR(100)            -- CM素材名（CM接触時のみ）
);

-- 2-3. 顧客ロイヤリティデータ
CREATE OR REPLACE TABLE CUSTOMER_LOYALTY (
    CUSTOMER_ID       VARCHAR(20)   PRIMARY KEY,
    HOUSEHOLD_ID      VARCHAR(20)   NOT NULL,
    AGE_GROUP         VARCHAR(20)   NOT NULL,
    GENDER            VARCHAR(10)   NOT NULL,
    AREA              VARCHAR(50)   NOT NULL,
    NPS_SCORE         NUMBER(3,0)   NOT NULL,
    LOYALTY_SEGMENT   VARCHAR(30)   NOT NULL,
    REPEAT_RATE       NUMBER(5,2)   NOT NULL,
    LTV_AMOUNT        NUMBER(12,0)  NOT NULL,
    MEMBER_SINCE      DATE          NOT NULL,
    LAST_PURCHASE_DATE DATE
);

-- 2-4. 態度変容データ
CREATE OR REPLACE TABLE ATTITUDE_CHANGE (
    SURVEY_ID         NUMBER        AUTOINCREMENT PRIMARY KEY,
    CAMPAIGN_ID       VARCHAR(20)   NOT NULL,
    CUSTOMER_ID       VARCHAR(20)   NOT NULL,
    SURVEY_DATE       DATE          NOT NULL,
    AWARENESS_BEFORE  NUMBER(5,2),
    AWARENESS_AFTER   NUMBER(5,2),
    INTEREST_BEFORE   NUMBER(5,2),
    INTEREST_AFTER    NUMBER(5,2),
    CONSIDER_BEFORE   NUMBER(5,2),
    CONSIDER_AFTER    NUMBER(5,2),
    PURCHASE_BEFORE   NUMBER(5,2),
    PURCHASE_AFTER    NUMBER(5,2),
    CM_EXPOSED        BOOLEAN       NOT NULL
);

-- 2-5. サイト来訪データ
CREATE OR REPLACE TABLE SITE_VISIT_LOG (
    SESSION_ID        VARCHAR(40)   PRIMARY KEY,
    CUSTOMER_ID       VARCHAR(20)   NOT NULL,
    VISIT_TIMESTAMP   TIMESTAMP_NTZ NOT NULL,
    REFERRER_TYPE     VARCHAR(30)   NOT NULL,
    REFERRER_DETAIL   VARCHAR(200),
    PAGE_VIEWS        NUMBER        NOT NULL,
    DURATION_SECONDS  NUMBER        NOT NULL,
    CONVERSION_FLAG   BOOLEAN       NOT NULL,
    CONVERSION_TYPE   VARCHAR(50),
    CAMPAIGN_ID       VARCHAR(20),
    DEVICE            VARCHAR(20)   NOT NULL
);

-- 2-6. オフライン購買データ
CREATE OR REPLACE TABLE OFFLINE_PURCHASE (
    PURCHASE_ID       NUMBER        AUTOINCREMENT PRIMARY KEY,
    CUSTOMER_ID       VARCHAR(20)   NOT NULL,
    PURCHASE_DATE     DATE          NOT NULL,
    STORE_NAME        VARCHAR(100)  NOT NULL,
    STORE_AREA        VARCHAR(50)   NOT NULL,
    PRODUCT_CATEGORY  VARCHAR(100)  NOT NULL,
    PRODUCT_NAME      VARCHAR(200)  NOT NULL,
    AMOUNT            NUMBER(12,0)  NOT NULL,
    QUANTITY          NUMBER        NOT NULL,
    CM_EXPOSED        BOOLEAN       NOT NULL,
    CAMPAIGN_ID       VARCHAR(20)
);

-- 2-7. 来店データ
CREATE OR REPLACE TABLE STORE_VISIT_LOG (
    VISIT_ID          NUMBER        AUTOINCREMENT PRIMARY KEY,
    CUSTOMER_ID       VARCHAR(20)   NOT NULL,
    STORE_NAME        VARCHAR(100)  NOT NULL,
    STORE_AREA        VARCHAR(50)   NOT NULL,
    VISIT_DATE        DATE          NOT NULL,
    VISIT_TIME        TIME          NOT NULL,
    STAY_MINUTES      NUMBER        NOT NULL,
    LOCATION_LAT      NUMBER(10,6),
    LOCATION_LON      NUMBER(10,6),
    CM_EXPOSED        BOOLEAN       NOT NULL,
    CAMPAIGN_ID       VARCHAR(20)
);

-- 2-8. アプリダウンロードデータ
CREATE OR REPLACE TABLE APP_DOWNLOAD_LOG (
    DOWNLOAD_ID       NUMBER        AUTOINCREMENT PRIMARY KEY,
    CUSTOMER_ID       VARCHAR(20)   NOT NULL,
    APP_NAME          VARCHAR(100)  NOT NULL,
    DOWNLOAD_DATE     DATE          NOT NULL,
    OS_TYPE           VARCHAR(20)   NOT NULL,
    AD_CHANNEL        VARCHAR(50),
    CM_EXPOSED        BOOLEAN       NOT NULL,
    CAMPAIGN_ID       VARCHAR(20)
);

-- 2-9. アプリ起動データ
CREATE OR REPLACE TABLE APP_LAUNCH_LOG (
    LAUNCH_ID         NUMBER        AUTOINCREMENT PRIMARY KEY,
    CUSTOMER_ID       VARCHAR(20)   NOT NULL,
    APP_NAME          VARCHAR(100)  NOT NULL,
    LAUNCH_TIMESTAMP  TIMESTAMP_NTZ NOT NULL,
    SESSION_SECONDS   NUMBER        NOT NULL,
    FEATURES_USED     VARCHAR(500),
    OS_TYPE           VARCHAR(20)   NOT NULL,
    CAMPAIGN_ID       VARCHAR(20)
);


-- ############################################################################
-- STEP 3: ダミーデータ投入
-- ############################################################################

-- 3-1. キャンペーンマスタ
INSERT INTO CAMPAIGNS VALUES
    ('CMP001', '春の新商品キャンペーン', 'サントリー', '飲料', '2024-04-01', '2024-06-30', 500, '関東'),
    ('CMP002', '夏季プロモーション', 'トヨタ自動車', '自動車', '2024-07-01', '2024-09-30', 800, '全国'),
    ('CMP003', '秋のブランド認知', 'ユニクロ', 'アパレル', '2024-10-01', '2024-12-31', 350, '全国'),
    ('CMP004', '年末年始セール', 'イオン', '小売', '2024-12-01', '2025-01-31', 600, '全国'),
    ('CMP005', '新生活応援', 'KDDI', '通信', '2025-02-01', '2025-03-31', 450, '全国'),
    ('CMP006', 'スキンケア新商品', '資生堂', '化粧品', '2024-05-01', '2024-07-31', 300, '関東'),
    ('CMP007', '保険見直しキャンペーン', '東京海上日動', '保険', '2024-06-01', '2024-08-31', 250, '全国'),
    ('CMP008', 'ゲームアプリ新作', 'バンダイナムコ', 'エンタメ', '2024-08-01', '2024-10-31', 200, '全国');

-- 3-2. テレビ視聴ログ（約5000件をGENERATOR使用）
INSERT INTO TV_VIEWING_LOG (HOUSEHOLD_ID, CHANNEL, PROGRAM_NAME, VIEWING_START, VIEWING_END, VIEWING_SECONDS, CM_EXPOSED, CAMPAIGN_ID, AREA, DEVICE_TYPE, CREATIVE_NAME)
SELECT
    'HH' || LPAD(UNIFORM(1, 500, RANDOM())::VARCHAR, 5, '0') AS HOUSEHOLD_ID,
    CASE UNIFORM(1, 8, RANDOM())
        WHEN 1 THEN '日本テレビ'
        WHEN 2 THEN 'TBS'
        WHEN 3 THEN 'フジテレビ'
        WHEN 4 THEN 'テレビ朝日'
        WHEN 5 THEN 'テレビ東京'
        WHEN 6 THEN 'NHK総合'
        WHEN 7 THEN 'NHK Eテレ'
        ELSE 'ABEMA'
    END AS CHANNEL,
    CASE UNIFORM(1, 10, RANDOM())
        WHEN 1 THEN 'ニュース9'
        WHEN 2 THEN '朝の情報番組'
        WHEN 3 THEN 'バラエティショー'
        WHEN 4 THEN '月曜ドラマ'
        WHEN 5 THEN 'スポーツ中継'
        WHEN 6 THEN 'ドキュメンタリー'
        WHEN 7 THEN '音楽特番'
        WHEN 8 THEN '映画ロードショー'
        WHEN 9 THEN '料理番組'
        ELSE '深夜アニメ'
    END AS PROGRAM_NAME,
    DATEADD('minute',
        UNIFORM(0, 1440, RANDOM()),
        DATEADD('day', UNIFORM(0, 364, RANDOM()), '2024-04-01'::DATE)
    )::TIMESTAMP_NTZ AS VIEWING_START,
    DATEADD('second',
        UNIFORM(300, 7200, RANDOM()),
        VIEWING_START
    ) AS VIEWING_END,
    DATEDIFF('second', VIEWING_START, VIEWING_END) AS VIEWING_SECONDS,
    CASE WHEN UNIFORM(1, 100, RANDOM()) <= 40 THEN TRUE ELSE FALSE END AS CM_EXPOSED,
    CASE WHEN CM_EXPOSED THEN
        CASE UNIFORM(1, 8, RANDOM())
            WHEN 1 THEN 'CMP001' WHEN 2 THEN 'CMP002' WHEN 3 THEN 'CMP003'
            WHEN 4 THEN 'CMP004' WHEN 5 THEN 'CMP005' WHEN 6 THEN 'CMP006'
            WHEN 7 THEN 'CMP007' ELSE 'CMP008'
        END
    ELSE NULL END AS CAMPAIGN_ID,
    CASE UNIFORM(1, 6, RANDOM())
        WHEN 1 THEN '関東' WHEN 2 THEN '関西' WHEN 3 THEN '中部'
        WHEN 4 THEN '北海道' WHEN 5 THEN '九州' ELSE '東北'
    END AS AREA,
    CASE WHEN UNIFORM(1, 100, RANDOM()) <= 75 THEN 'TV' ELSE 'CTV' END AS DEVICE_TYPE,
    CASE WHEN CM_EXPOSED THEN
        CASE UNIFORM(1, 8, RANDOM())
            WHEN 1 THEN '素材A_15秒_商品訴求'
            WHEN 2 THEN '素材B_30秒_ブランド'
            WHEN 3 THEN '素材C_15秒_キャンペーン告知'
            WHEN 4 THEN '素材D_30秒_タレント起用'
            WHEN 5 THEN '素材E_60秒_ストーリー'
            WHEN 6 THEN '素材F_15秒_リマインド'
            WHEN 7 THEN '素材G_30秒_比較訴求'
            ELSE '素材H_60秒_感動系'
        END
    ELSE NULL END AS CREATIVE_NAME
FROM TABLE(GENERATOR(ROWCOUNT => 5000));

-- 3-3. 顧客ロイヤリティ（1000件）
INSERT INTO CUSTOMER_LOYALTY (CUSTOMER_ID, HOUSEHOLD_ID, AGE_GROUP, GENDER, AREA, NPS_SCORE, LOYALTY_SEGMENT, REPEAT_RATE, LTV_AMOUNT, MEMBER_SINCE, LAST_PURCHASE_DATE)
SELECT
    'CUST' || LPAD(SEQ4()::VARCHAR, 6, '0') AS CUSTOMER_ID,
    'HH' || LPAD(UNIFORM(1, 500, RANDOM())::VARCHAR, 5, '0') AS HOUSEHOLD_ID,
    CASE UNIFORM(1, 6, RANDOM())
        WHEN 1 THEN '20代' WHEN 2 THEN '30代' WHEN 3 THEN '40代'
        WHEN 4 THEN '50代' WHEN 5 THEN '60代' ELSE '10代'
    END AS AGE_GROUP,
    CASE WHEN UNIFORM(1, 100, RANDOM()) <= 50 THEN '男性' ELSE '女性' END AS GENDER,
    CASE UNIFORM(1, 6, RANDOM())
        WHEN 1 THEN '関東' WHEN 2 THEN '関西' WHEN 3 THEN '中部'
        WHEN 4 THEN '北海道' WHEN 5 THEN '九州' ELSE '東北'
    END AS AREA,
    UNIFORM(-100, 100, RANDOM()) AS NPS_SCORE,
    CASE
        WHEN NPS_SCORE >= 50 THEN 'プロモーター'
        WHEN NPS_SCORE >= 0  THEN 'パッシブ'
        ELSE 'デトラクター'
    END AS LOYALTY_SEGMENT,
    ROUND(UNIFORM(5, 95, RANDOM()) + UNIFORM(0, 99, RANDOM()) / 100.0, 2) AS REPEAT_RATE,
    UNIFORM(5000, 500000, RANDOM()) AS LTV_AMOUNT,
    DATEADD('day', -UNIFORM(365, 2000, RANDOM()), '2025-03-31'::DATE) AS MEMBER_SINCE,
    DATEADD('day', -UNIFORM(0, 180, RANDOM()), '2025-03-31'::DATE) AS LAST_PURCHASE_DATE
FROM TABLE(GENERATOR(ROWCOUNT => 1000));

-- 3-4. 態度変容（500件）
INSERT INTO ATTITUDE_CHANGE (CAMPAIGN_ID, CUSTOMER_ID, SURVEY_DATE, AWARENESS_BEFORE, AWARENESS_AFTER, INTEREST_BEFORE, INTEREST_AFTER, CONSIDER_BEFORE, CONSIDER_AFTER, PURCHASE_BEFORE, PURCHASE_AFTER, CM_EXPOSED)
SELECT
    CASE UNIFORM(1, 8, RANDOM())
        WHEN 1 THEN 'CMP001' WHEN 2 THEN 'CMP002' WHEN 3 THEN 'CMP003'
        WHEN 4 THEN 'CMP004' WHEN 5 THEN 'CMP005' WHEN 6 THEN 'CMP006'
        WHEN 7 THEN 'CMP007' ELSE 'CMP008'
    END AS CAMPAIGN_ID,
    'CUST' || LPAD(UNIFORM(1, 1000, RANDOM())::VARCHAR, 6, '0') AS CUSTOMER_ID,
    DATEADD('day', UNIFORM(0, 364, RANDOM()), '2024-04-01'::DATE) AS SURVEY_DATE,
    ROUND(UNIFORM(10, 50, RANDOM()) + UNIFORM(0, 99, RANDOM()) / 100.0, 2) AS AWARENESS_BEFORE,
    ROUND(AWARENESS_BEFORE + UNIFORM(0, 30, RANDOM()) + UNIFORM(0, 99, RANDOM()) / 100.0, 2) AS AWARENESS_AFTER,
    ROUND(UNIFORM(5, 35, RANDOM()) + UNIFORM(0, 99, RANDOM()) / 100.0, 2) AS INTEREST_BEFORE,
    ROUND(INTEREST_BEFORE + UNIFORM(0, 25, RANDOM()) + UNIFORM(0, 99, RANDOM()) / 100.0, 2) AS INTEREST_AFTER,
    ROUND(UNIFORM(3, 25, RANDOM()) + UNIFORM(0, 99, RANDOM()) / 100.0, 2) AS CONSIDER_BEFORE,
    ROUND(CONSIDER_BEFORE + UNIFORM(0, 20, RANDOM()) + UNIFORM(0, 99, RANDOM()) / 100.0, 2) AS CONSIDER_AFTER,
    ROUND(UNIFORM(1, 15, RANDOM()) + UNIFORM(0, 99, RANDOM()) / 100.0, 2) AS PURCHASE_BEFORE,
    ROUND(PURCHASE_BEFORE + UNIFORM(0, 15, RANDOM()) + UNIFORM(0, 99, RANDOM()) / 100.0, 2) AS PURCHASE_AFTER,
    CASE WHEN UNIFORM(1, 100, RANDOM()) <= 60 THEN TRUE ELSE FALSE END AS CM_EXPOSED
FROM TABLE(GENERATOR(ROWCOUNT => 500));

-- 3-5. サイト来訪（5000件）
INSERT INTO SITE_VISIT_LOG (SESSION_ID, CUSTOMER_ID, VISIT_TIMESTAMP, REFERRER_TYPE, REFERRER_DETAIL, PAGE_VIEWS, DURATION_SECONDS, CONVERSION_FLAG, CONVERSION_TYPE, CAMPAIGN_ID, DEVICE)
SELECT
    UUID_STRING() AS SESSION_ID,
    'CUST' || LPAD(UNIFORM(1, 1000, RANDOM())::VARCHAR, 6, '0') AS CUSTOMER_ID,
    DATEADD('minute',
        UNIFORM(0, 1440, RANDOM()),
        DATEADD('day', UNIFORM(0, 364, RANDOM()), '2024-04-01'::DATE)
    )::TIMESTAMP_NTZ AS VISIT_TIMESTAMP,
    CASE UNIFORM(1, 6, RANDOM())
        WHEN 1 THEN 'オーガニック検索'
        WHEN 2 THEN 'リスティング広告'
        WHEN 3 THEN 'SNS'
        WHEN 4 THEN 'ディスプレイ広告'
        WHEN 5 THEN 'ダイレクト'
        ELSE 'メール'
    END AS REFERRER_TYPE,
    CASE REFERRER_TYPE
        WHEN 'オーガニック検索' THEN 'Google'
        WHEN 'リスティング広告' THEN 'Google Ads'
        WHEN 'SNS' THEN CASE UNIFORM(1,3,RANDOM()) WHEN 1 THEN 'X' WHEN 2 THEN 'Instagram' ELSE 'LINE' END
        WHEN 'ディスプレイ広告' THEN 'GDN'
        WHEN 'ダイレクト' THEN NULL
        ELSE 'メルマガ'
    END AS REFERRER_DETAIL,
    UNIFORM(1, 30, RANDOM()) AS PAGE_VIEWS,
    UNIFORM(10, 1800, RANDOM()) AS DURATION_SECONDS,
    CASE WHEN UNIFORM(1, 100, RANDOM()) <= 8 THEN TRUE ELSE FALSE END AS CONVERSION_FLAG,
    CASE WHEN CONVERSION_FLAG THEN
        CASE UNIFORM(1, 3, RANDOM())
            WHEN 1 THEN '会員登録' WHEN 2 THEN '資料請求' ELSE '商品購入'
        END
    ELSE NULL END AS CONVERSION_TYPE,
    CASE WHEN UNIFORM(1, 100, RANDOM()) <= 30 THEN
        CASE UNIFORM(1, 8, RANDOM())
            WHEN 1 THEN 'CMP001' WHEN 2 THEN 'CMP002' WHEN 3 THEN 'CMP003'
            WHEN 4 THEN 'CMP004' WHEN 5 THEN 'CMP005' WHEN 6 THEN 'CMP006'
            WHEN 7 THEN 'CMP007' ELSE 'CMP008'
        END
    ELSE NULL END AS CAMPAIGN_ID,
    CASE UNIFORM(1, 3, RANDOM()) WHEN 1 THEN 'PC' WHEN 2 THEN 'スマートフォン' ELSE 'タブレット' END AS DEVICE
FROM TABLE(GENERATOR(ROWCOUNT => 5000));

-- 3-6. オフライン購買（3000件）
INSERT INTO OFFLINE_PURCHASE (CUSTOMER_ID, PURCHASE_DATE, STORE_NAME, STORE_AREA, PRODUCT_CATEGORY, PRODUCT_NAME, AMOUNT, QUANTITY, CM_EXPOSED, CAMPAIGN_ID)
SELECT
    'CUST' || LPAD(UNIFORM(1, 1000, RANDOM())::VARCHAR, 6, '0') AS CUSTOMER_ID,
    DATEADD('day', UNIFORM(0, 364, RANDOM()), '2024-04-01'::DATE) AS PURCHASE_DATE,
    CASE UNIFORM(1, 6, RANDOM())
        WHEN 1 THEN 'イオンモール幕張' WHEN 2 THEN 'ららぽーと豊洲'
        WHEN 3 THEN 'アリオ亀有' WHEN 4 THEN 'グランフロント大阪'
        WHEN 5 THEN 'マークイズ福岡' ELSE 'サッポロファクトリー'
    END AS STORE_NAME,
    CASE UNIFORM(1, 6, RANDOM())
        WHEN 1 THEN '関東' WHEN 2 THEN '関西' WHEN 3 THEN '中部'
        WHEN 4 THEN '北海道' WHEN 5 THEN '九州' ELSE '東北'
    END AS STORE_AREA,
    CASE UNIFORM(1, 8, RANDOM())
        WHEN 1 THEN '飲料' WHEN 2 THEN '食品' WHEN 3 THEN '日用品'
        WHEN 4 THEN '化粧品' WHEN 5 THEN '衣料品' WHEN 6 THEN '家電'
        WHEN 7 THEN '書籍' ELSE '医薬品'
    END AS PRODUCT_CATEGORY,
    CASE PRODUCT_CATEGORY
        WHEN '飲料' THEN CASE UNIFORM(1,3,RANDOM()) WHEN 1 THEN '天然水 550ml' WHEN 2 THEN '緑茶 2L' ELSE 'コーヒー 缶' END
        WHEN '食品' THEN CASE UNIFORM(1,3,RANDOM()) WHEN 1 THEN 'カップ麺' WHEN 2 THEN 'チョコレート' ELSE 'ポテトチップス' END
        WHEN '日用品' THEN CASE UNIFORM(1,3,RANDOM()) WHEN 1 THEN 'シャンプー' WHEN 2 THEN '洗剤' ELSE 'ティッシュ' END
        WHEN '化粧品' THEN CASE UNIFORM(1,3,RANDOM()) WHEN 1 THEN 'ファンデーション' WHEN 2 THEN '化粧水' ELSE 'リップ' END
        WHEN '衣料品' THEN CASE UNIFORM(1,3,RANDOM()) WHEN 1 THEN 'Tシャツ' WHEN 2 THEN 'パンツ' ELSE 'ジャケット' END
        WHEN '家電' THEN CASE UNIFORM(1,3,RANDOM()) WHEN 1 THEN 'イヤホン' WHEN 2 THEN 'モバイルバッテリー' ELSE 'スマートスピーカー' END
        WHEN '書籍' THEN CASE UNIFORM(1,3,RANDOM()) WHEN 1 THEN 'ビジネス書' WHEN 2 THEN '小説' ELSE '雑誌' END
        ELSE CASE UNIFORM(1,3,RANDOM()) WHEN 1 THEN '風邪薬' WHEN 2 THEN 'ビタミン剤' ELSE '目薬' END
    END AS PRODUCT_NAME,
    CASE PRODUCT_CATEGORY
        WHEN '家電' THEN UNIFORM(3000, 30000, RANDOM())
        WHEN '衣料品' THEN UNIFORM(1000, 15000, RANDOM())
        WHEN '化粧品' THEN UNIFORM(500, 8000, RANDOM())
        ELSE UNIFORM(100, 3000, RANDOM())
    END AS AMOUNT,
    UNIFORM(1, 5, RANDOM()) AS QUANTITY,
    CASE WHEN UNIFORM(1, 100, RANDOM()) <= 45 THEN TRUE ELSE FALSE END AS CM_EXPOSED,
    CASE WHEN CM_EXPOSED THEN
        CASE UNIFORM(1, 8, RANDOM())
            WHEN 1 THEN 'CMP001' WHEN 2 THEN 'CMP002' WHEN 3 THEN 'CMP003'
            WHEN 4 THEN 'CMP004' WHEN 5 THEN 'CMP005' WHEN 6 THEN 'CMP006'
            WHEN 7 THEN 'CMP007' ELSE 'CMP008'
        END
    ELSE NULL END AS CAMPAIGN_ID
FROM TABLE(GENERATOR(ROWCOUNT => 3000));

-- 3-7. 来店データ（2000件）
INSERT INTO STORE_VISIT_LOG (CUSTOMER_ID, STORE_NAME, STORE_AREA, VISIT_DATE, VISIT_TIME, STAY_MINUTES, LOCATION_LAT, LOCATION_LON, CM_EXPOSED, CAMPAIGN_ID)
SELECT
    'CUST' || LPAD(UNIFORM(1, 1000, RANDOM())::VARCHAR, 6, '0') AS CUSTOMER_ID,
    CASE UNIFORM(1, 6, RANDOM())
        WHEN 1 THEN 'イオンモール幕張' WHEN 2 THEN 'ららぽーと豊洲'
        WHEN 3 THEN 'アリオ亀有' WHEN 4 THEN 'グランフロント大阪'
        WHEN 5 THEN 'マークイズ福岡' ELSE 'サッポロファクトリー'
    END AS STORE_NAME,
    CASE STORE_NAME
        WHEN 'イオンモール幕張' THEN '関東'
        WHEN 'ららぽーと豊洲' THEN '関東'
        WHEN 'アリオ亀有' THEN '関東'
        WHEN 'グランフロント大阪' THEN '関西'
        WHEN 'マークイズ福岡' THEN '九州'
        ELSE '北海道'
    END AS STORE_AREA,
    DATEADD('day', UNIFORM(0, 364, RANDOM()), '2024-04-01'::DATE) AS VISIT_DATE,
    TIMEADD('minute', UNIFORM(540, 1260, RANDOM()), '00:00:00'::TIME) AS VISIT_TIME,
    UNIFORM(5, 180, RANDOM()) AS STAY_MINUTES,
    CASE STORE_NAME
        WHEN 'イオンモール幕張' THEN 35.6484 + UNIFORM(-10,10,RANDOM())/10000.0
        WHEN 'ららぽーと豊洲' THEN 35.6550 + UNIFORM(-10,10,RANDOM())/10000.0
        WHEN 'アリオ亀有' THEN 35.7680 + UNIFORM(-10,10,RANDOM())/10000.0
        WHEN 'グランフロント大阪' THEN 34.7055 + UNIFORM(-10,10,RANDOM())/10000.0
        WHEN 'マークイズ福岡' THEN 33.5904 + UNIFORM(-10,10,RANDOM())/10000.0
        ELSE 43.0621 + UNIFORM(-10,10,RANDOM())/10000.0
    END AS LOCATION_LAT,
    CASE STORE_NAME
        WHEN 'イオンモール幕張' THEN 140.0234 + UNIFORM(-10,10,RANDOM())/10000.0
        WHEN 'ららぽーと豊洲' THEN 139.7929 + UNIFORM(-10,10,RANDOM())/10000.0
        WHEN 'アリオ亀有' THEN 139.8498 + UNIFORM(-10,10,RANDOM())/10000.0
        WHEN 'グランフロント大阪' THEN 135.4959 + UNIFORM(-10,10,RANDOM())/10000.0
        WHEN 'マークイズ福岡' THEN 130.3987 + UNIFORM(-10,10,RANDOM())/10000.0
        ELSE 141.3544 + UNIFORM(-10,10,RANDOM())/10000.0
    END AS LOCATION_LON,
    CASE WHEN UNIFORM(1, 100, RANDOM()) <= 40 THEN TRUE ELSE FALSE END AS CM_EXPOSED,
    CASE WHEN CM_EXPOSED THEN
        CASE UNIFORM(1, 8, RANDOM())
            WHEN 1 THEN 'CMP001' WHEN 2 THEN 'CMP002' WHEN 3 THEN 'CMP003'
            WHEN 4 THEN 'CMP004' WHEN 5 THEN 'CMP005' WHEN 6 THEN 'CMP006'
            WHEN 7 THEN 'CMP007' ELSE 'CMP008'
        END
    ELSE NULL END AS CAMPAIGN_ID
FROM TABLE(GENERATOR(ROWCOUNT => 2000));

-- 3-8. アプリダウンロード（1000件）
INSERT INTO APP_DOWNLOAD_LOG (CUSTOMER_ID, APP_NAME, DOWNLOAD_DATE, OS_TYPE, AD_CHANNEL, CM_EXPOSED, CAMPAIGN_ID)
SELECT
    'CUST' || LPAD(UNIFORM(1, 1000, RANDOM())::VARCHAR, 6, '0') AS CUSTOMER_ID,
    CASE UNIFORM(1, 5, RANDOM())
        WHEN 1 THEN 'ブランド公式アプリ'
        WHEN 2 THEN 'ECショッピングアプリ'
        WHEN 3 THEN 'ポイントカードアプリ'
        WHEN 4 THEN 'クーポンアプリ'
        ELSE 'ゲームアプリ'
    END AS APP_NAME,
    DATEADD('day', UNIFORM(0, 364, RANDOM()), '2024-04-01'::DATE) AS DOWNLOAD_DATE,
    CASE WHEN UNIFORM(1, 100, RANDOM()) <= 55 THEN 'iOS' ELSE 'Android' END AS OS_TYPE,
    CASE UNIFORM(1, 5, RANDOM())
        WHEN 1 THEN 'テレビCM' WHEN 2 THEN 'SNS広告'
        WHEN 3 THEN 'リスティング広告' WHEN 4 THEN 'ストア検索'
        ELSE '友人紹介'
    END AS AD_CHANNEL,
    CASE WHEN UNIFORM(1, 100, RANDOM()) <= 50 THEN TRUE ELSE FALSE END AS CM_EXPOSED,
    CASE WHEN CM_EXPOSED THEN
        CASE UNIFORM(1, 8, RANDOM())
            WHEN 1 THEN 'CMP001' WHEN 2 THEN 'CMP002' WHEN 3 THEN 'CMP003'
            WHEN 4 THEN 'CMP004' WHEN 5 THEN 'CMP005' WHEN 6 THEN 'CMP006'
            WHEN 7 THEN 'CMP007' ELSE 'CMP008'
        END
    ELSE NULL END AS CAMPAIGN_ID
FROM TABLE(GENERATOR(ROWCOUNT => 1000));

-- 3-9. アプリ起動（3000件）
INSERT INTO APP_LAUNCH_LOG (CUSTOMER_ID, APP_NAME, LAUNCH_TIMESTAMP, SESSION_SECONDS, FEATURES_USED, OS_TYPE, CAMPAIGN_ID)
SELECT
    'CUST' || LPAD(UNIFORM(1, 1000, RANDOM())::VARCHAR, 6, '0') AS CUSTOMER_ID,
    CASE UNIFORM(1, 5, RANDOM())
        WHEN 1 THEN 'ブランド公式アプリ'
        WHEN 2 THEN 'ECショッピングアプリ'
        WHEN 3 THEN 'ポイントカードアプリ'
        WHEN 4 THEN 'クーポンアプリ'
        ELSE 'ゲームアプリ'
    END AS APP_NAME,
    DATEADD('minute',
        UNIFORM(0, 1440, RANDOM()),
        DATEADD('day', UNIFORM(0, 364, RANDOM()), '2024-04-01'::DATE)
    )::TIMESTAMP_NTZ AS LAUNCH_TIMESTAMP,
    UNIFORM(10, 3600, RANDOM()) AS SESSION_SECONDS,
    CASE UNIFORM(1, 5, RANDOM())
        WHEN 1 THEN '商品検索,カート追加'
        WHEN 2 THEN 'クーポン確認,バーコード表示'
        WHEN 3 THEN 'ポイント照会'
        WHEN 4 THEN 'お気に入り閲覧,商品購入'
        ELSE 'ゲームプレイ,ガチャ'
    END AS FEATURES_USED,
    CASE WHEN UNIFORM(1, 100, RANDOM()) <= 55 THEN 'iOS' ELSE 'Android' END AS OS_TYPE,
    CASE WHEN UNIFORM(1, 100, RANDOM()) <= 25 THEN
        CASE UNIFORM(1, 8, RANDOM())
            WHEN 1 THEN 'CMP001' WHEN 2 THEN 'CMP002' WHEN 3 THEN 'CMP003'
            WHEN 4 THEN 'CMP004' WHEN 5 THEN 'CMP005' WHEN 6 THEN 'CMP006'
            WHEN 7 THEN 'CMP007' ELSE 'CMP008'
        END
    ELSE NULL END AS CAMPAIGN_ID
FROM TABLE(GENERATOR(ROWCOUNT => 3000));


-- ############################################################################
-- STEP 4: セマンティックビュー定義（Snowflake Intelligence 用）
-- ############################################################################

CREATE OR REPLACE SEMANTIC VIEW STADIA360_ANALYTICS

    TABLES (
        CAMP AS KFUKAMORI_GEN_DB.STADIA360.CAMPAIGNS
            PRIMARY KEY (CAMPAIGN_ID)
            COMMENT = 'キャンペーンマスタ。広告主、商品カテゴリ、予算、対象エリアを管理',

        TV AS KFUKAMORI_GEN_DB.STADIA360.TV_VIEWING_LOG
            PRIMARY KEY (VIEWING_ID)
            COMMENT = 'テレビ・CTV視聴ログ。世帯単位の視聴行動とCM接触を記録',

        LOYALTY AS KFUKAMORI_GEN_DB.STADIA360.CUSTOMER_LOYALTY
            PRIMARY KEY (CUSTOMER_ID)
            COMMENT = '顧客ロイヤリティ。NPS、リピート率、LTV、セグメント情報',

        ATTITUDE AS KFUKAMORI_GEN_DB.STADIA360.ATTITUDE_CHANGE
            PRIMARY KEY (SURVEY_ID)
            COMMENT = '態度変容データ。CM接触前後の認知・興味・検討・購入意向の変化',

        SITE AS KFUKAMORI_GEN_DB.STADIA360.SITE_VISIT_LOG
            PRIMARY KEY (SESSION_ID)
            COMMENT = 'サイト来訪ログ。流入経路、PV、滞在時間、コンバージョン',

        PURCHASE AS KFUKAMORI_GEN_DB.STADIA360.OFFLINE_PURCHASE
            PRIMARY KEY (PURCHASE_ID)
            COMMENT = 'オフライン購買。店舗での商品購入と金額、CM接触有無',

        STORE AS KFUKAMORI_GEN_DB.STADIA360.STORE_VISIT_LOG
            PRIMARY KEY (VISIT_ID)
            COMMENT = '来店データ。位置情報ベースの店舗来店記録',

        DL AS KFUKAMORI_GEN_DB.STADIA360.APP_DOWNLOAD_LOG
            PRIMARY KEY (DOWNLOAD_ID)
            COMMENT = 'アプリDLログ。ダウンロード経路とCM接触',

        LAUNCH AS KFUKAMORI_GEN_DB.STADIA360.APP_LAUNCH_LOG
            PRIMARY KEY (LAUNCH_ID)
            COMMENT = 'アプリ起動ログ。利用時間と機能利用状況'
    )

    RELATIONSHIPS (
        TV_CAMPAIGN AS TV (CAMPAIGN_ID) REFERENCES CAMP (CAMPAIGN_ID),
        ATTITUDE_CAMPAIGN AS ATTITUDE (CAMPAIGN_ID) REFERENCES CAMP (CAMPAIGN_ID),
        SITE_CAMPAIGN AS SITE (CAMPAIGN_ID) REFERENCES CAMP (CAMPAIGN_ID),
        PURCHASE_CAMPAIGN AS PURCHASE (CAMPAIGN_ID) REFERENCES CAMP (CAMPAIGN_ID),
        STORE_CAMPAIGN AS STORE (CAMPAIGN_ID) REFERENCES CAMP (CAMPAIGN_ID),
        DL_CAMPAIGN AS DL (CAMPAIGN_ID) REFERENCES CAMP (CAMPAIGN_ID),
        LAUNCH_CAMPAIGN AS LAUNCH (CAMPAIGN_ID) REFERENCES CAMP (CAMPAIGN_ID),
        ATTITUDE_CUSTOMER AS ATTITUDE (CUSTOMER_ID) REFERENCES LOYALTY (CUSTOMER_ID),
        SITE_CUSTOMER AS SITE (CUSTOMER_ID) REFERENCES LOYALTY (CUSTOMER_ID),
        PURCHASE_CUSTOMER AS PURCHASE (CUSTOMER_ID) REFERENCES LOYALTY (CUSTOMER_ID),
        STORE_CUSTOMER AS STORE (CUSTOMER_ID) REFERENCES LOYALTY (CUSTOMER_ID),
        DL_CUSTOMER AS DL (CUSTOMER_ID) REFERENCES LOYALTY (CUSTOMER_ID),
        LAUNCH_CUSTOMER AS LAUNCH (CUSTOMER_ID) REFERENCES LOYALTY (CUSTOMER_ID)
    )

    FACTS (
        TV.VIEWING_SECONDS_FACT AS VIEWING_SECONDS
            COMMENT = '視聴秒数',
        LOYALTY.NPS_SCORE_FACT AS NPS_SCORE
            COMMENT = 'NPSスコア（-100から100）',
        LOYALTY.REPEAT_RATE_FACT AS REPEAT_RATE
            COMMENT = 'リピート率',
        LOYALTY.LTV_AMOUNT_FACT AS LTV_AMOUNT
            COMMENT = '顧客生涯価値（円）',
        ATTITUDE.AWARENESS_BEFORE_FACT AS AWARENESS_BEFORE
            COMMENT = '認知率（施策前）',
        ATTITUDE.AWARENESS_AFTER_FACT AS AWARENESS_AFTER
            COMMENT = '認知率（施策後）',
        ATTITUDE.INTEREST_BEFORE_FACT AS INTEREST_BEFORE
            COMMENT = '興味率（施策前）',
        ATTITUDE.INTEREST_AFTER_FACT AS INTEREST_AFTER
            COMMENT = '興味率（施策後）',
        ATTITUDE.CONSIDER_BEFORE_FACT AS CONSIDER_BEFORE
            COMMENT = '検討率（施策前）',
        ATTITUDE.CONSIDER_AFTER_FACT AS CONSIDER_AFTER
            COMMENT = '検討率（施策後）',
        ATTITUDE.PURCHASE_BEFORE_FACT AS PURCHASE_BEFORE
            COMMENT = '購入意向率（施策前）',
        ATTITUDE.PURCHASE_AFTER_FACT AS PURCHASE_AFTER
            COMMENT = '購入意向率（施策後）',
        SITE.PAGE_VIEWS_FACT AS PAGE_VIEWS
            COMMENT = 'ページビュー数',
        SITE.DURATION_SECONDS_FACT AS DURATION_SECONDS
            COMMENT = 'サイト滞在秒数',
        PURCHASE.AMOUNT_FACT AS AMOUNT
            COMMENT = '購買金額（円）',
        PURCHASE.QUANTITY_FACT AS QUANTITY
            COMMENT = '購買数量',
        STORE.STAY_MINUTES_FACT AS STAY_MINUTES
            COMMENT = '店舗滞在時間（分）',
        LAUNCH.SESSION_SECONDS_FACT AS SESSION_SECONDS
            COMMENT = 'アプリセッション秒数'
    )

    DIMENSIONS (
        CAMP.CAMPAIGN_NAME_DIM AS CAMPAIGN_NAME
            COMMENT = 'キャンペーン名',
        CAMP.ADVERTISER_DIM AS ADVERTISER
            COMMENT = '広告主名',
        CAMP.PRODUCT_CATEGORY_DIM AS PRODUCT_CATEGORY
            COMMENT = '商品カテゴリ',
        CAMP.START_DATE_DIM AS START_DATE
            COMMENT = 'キャンペーン開始日',
        CAMP.END_DATE_DIM AS END_DATE
            COMMENT = 'キャンペーン終了日',
        CAMP.TARGET_AREA_DIM AS TARGET_AREA
            COMMENT = '対象エリア',
        TV.CHANNEL_DIM AS CHANNEL
            COMMENT = 'テレビチャンネル名',
        TV.PROGRAM_NAME_DIM AS PROGRAM_NAME
            COMMENT = '番組名',
        TV.AREA_DIM AS AREA
            COMMENT = '視聴エリア',
        TV.DEVICE_TYPE_DIM AS DEVICE_TYPE
            COMMENT = 'デバイス種別（TV/CTV）',
        TV.CM_EXPOSED_DIM AS CM_EXPOSED
            COMMENT = 'CM接触フラグ（テレビ）',
        TV.VIEWING_DATE_DIM AS DATE(VIEWING_START)
            COMMENT = '視聴日',
        LOYALTY.AGE_GROUP_DIM AS AGE_GROUP
            COMMENT = '年齢層',
        LOYALTY.GENDER_DIM AS GENDER
            COMMENT = '性別',
        LOYALTY.LOYALTY_SEGMENT_DIM AS LOYALTY_SEGMENT
            COMMENT = 'ロイヤリティセグメント（プロモーター/パッシブ/デトラクター）',
        LOYALTY.CUSTOMER_AREA_DIM AS AREA
            COMMENT = '顧客居住エリア',
        ATTITUDE.SURVEY_DATE_DIM AS SURVEY_DATE
            COMMENT = 'アンケート実施日',
        ATTITUDE.ATTITUDE_CM_EXPOSED_DIM AS CM_EXPOSED
            COMMENT = 'CM接触フラグ（態度変容）',
        SITE.REFERRER_TYPE_DIM AS REFERRER_TYPE
            COMMENT = '流入経路タイプ',
        SITE.REFERRER_DETAIL_DIM AS REFERRER_DETAIL
            COMMENT = '流入経路詳細',
        SITE.CONVERSION_FLAG_DIM AS CONVERSION_FLAG
            COMMENT = 'コンバージョンフラグ',
        SITE.CONVERSION_TYPE_DIM AS CONVERSION_TYPE
            COMMENT = 'コンバージョン種別',
        SITE.DEVICE_DIM AS DEVICE
            COMMENT = 'サイト来訪デバイス',
        SITE.VISIT_DATE_DIM AS DATE(VISIT_TIMESTAMP)
            COMMENT = 'サイト来訪日',
        PURCHASE.PURCHASE_DATE_DIM AS PURCHASE_DATE
            COMMENT = '購買日',
        PURCHASE.STORE_NAME_DIM AS STORE_NAME
            COMMENT = '購買店舗名',
        PURCHASE.STORE_AREA_DIM AS STORE_AREA
            COMMENT = '購買店舗エリア',
        PURCHASE.PURCHASE_CATEGORY_DIM AS PRODUCT_CATEGORY
            COMMENT = '商品カテゴリ（購買）',
        PURCHASE.PRODUCT_NAME_DIM AS PRODUCT_NAME
            COMMENT = '商品名',
        PURCHASE.PURCHASE_CM_EXPOSED_DIM AS CM_EXPOSED
            COMMENT = 'CM接触フラグ（購買）',
        STORE.STORE_VISIT_DATE_DIM AS VISIT_DATE
            COMMENT = '来店日',
        STORE.STORE_VISIT_STORE_DIM AS STORE_NAME
            COMMENT = '来店店舗名',
        STORE.STORE_VISIT_AREA_DIM AS STORE_AREA
            COMMENT = '来店店舗エリア',
        STORE.STORE_CM_EXPOSED_DIM AS CM_EXPOSED
            COMMENT = 'CM接触フラグ（来店）',
        DL.APP_NAME_DIM AS APP_NAME
            COMMENT = 'アプリ名（DL）',
        DL.DL_DATE_DIM AS DOWNLOAD_DATE
            COMMENT = 'DL日',
        DL.OS_TYPE_DIM AS OS_TYPE
            COMMENT = 'OS種別',
        DL.AD_CHANNEL_DIM AS AD_CHANNEL
            COMMENT = '広告チャネル',
        DL.DL_CM_EXPOSED_DIM AS CM_EXPOSED
            COMMENT = 'CM接触フラグ（DL）',
        LAUNCH.LAUNCH_APP_NAME_DIM AS APP_NAME
            COMMENT = 'アプリ名（起動）',
        LAUNCH.LAUNCH_DATE_DIM AS DATE(LAUNCH_TIMESTAMP)
            COMMENT = 'アプリ起動日',
        LAUNCH.LAUNCH_OS_DIM AS OS_TYPE
            COMMENT = 'OS種別（起動）'
    )

    METRICS (
        -- テレビ視聴
        TV.TOTAL_VIEWING_HOURS AS SUM(TV.VIEWING_SECONDS_FACT) / 3600.0
            COMMENT = '合計視聴時間（時間）',
        TV.CM_EXPOSURE_COUNT AS COUNT_IF(TV.CM_EXPOSED_DIM = TRUE)
            COMMENT = 'CM接触件数',
        TV.CM_EXPOSURE_RATE AS COUNT_IF(TV.CM_EXPOSED_DIM = TRUE) * 100.0 / NULLIF(COUNT(TV.VIEWING_SECONDS_FACT), 0)
            COMMENT = 'CM接触率（%）',
        TV.VIEWING_COUNT AS COUNT(TV.VIEWING_SECONDS_FACT)
            COMMENT = '視聴ログ件数',

        -- 顧客ロイヤリティ
        LOYALTY.AVG_NPS AS AVG(LOYALTY.NPS_SCORE_FACT)
            COMMENT = '平均NPSスコア',
        LOYALTY.AVG_LTV AS AVG(LOYALTY.LTV_AMOUNT_FACT)
            COMMENT = '平均LTV（円）',
        LOYALTY.CUSTOMER_COUNT AS COUNT(LOYALTY.NPS_SCORE_FACT)
            COMMENT = '顧客数',

        -- 態度変容
        ATTITUDE.AVG_AWARENESS_LIFT AS AVG(ATTITUDE.AWARENESS_AFTER_FACT - ATTITUDE.AWARENESS_BEFORE_FACT)
            COMMENT = '平均認知リフト（ポイント）',
        ATTITUDE.AVG_INTEREST_LIFT AS AVG(ATTITUDE.INTEREST_AFTER_FACT - ATTITUDE.INTEREST_BEFORE_FACT)
            COMMENT = '平均興味リフト（ポイント）',
        ATTITUDE.AVG_CONSIDER_LIFT AS AVG(ATTITUDE.CONSIDER_AFTER_FACT - ATTITUDE.CONSIDER_BEFORE_FACT)
            COMMENT = '平均検討リフト（ポイント）',
        ATTITUDE.AVG_PURCHASE_LIFT AS AVG(ATTITUDE.PURCHASE_AFTER_FACT - ATTITUDE.PURCHASE_BEFORE_FACT)
            COMMENT = '平均購入意向リフト（ポイント）',
        ATTITUDE.SURVEY_COUNT AS COUNT(ATTITUDE.AWARENESS_BEFORE_FACT)
            COMMENT = 'アンケート回答数',

        -- サイト来訪
        SITE.TOTAL_PAGE_VIEWS AS SUM(SITE.PAGE_VIEWS_FACT)
            COMMENT = '合計PV数',
        SITE.AVG_DURATION AS AVG(SITE.DURATION_SECONDS_FACT)
            COMMENT = '平均滞在時間（秒）',
        SITE.CONVERSION_COUNT AS COUNT_IF(SITE.CONVERSION_FLAG_DIM = TRUE)
            COMMENT = 'コンバージョン件数',
        SITE.CVR AS COUNT_IF(SITE.CONVERSION_FLAG_DIM = TRUE) * 100.0 / NULLIF(COUNT(SITE.PAGE_VIEWS_FACT), 0)
            COMMENT = 'コンバージョン率（%）',
        SITE.SESSION_COUNT AS COUNT(SITE.PAGE_VIEWS_FACT)
            COMMENT = 'セッション数',

        -- オフライン購買
        PURCHASE.TOTAL_PURCHASE_AMOUNT AS SUM(PURCHASE.AMOUNT_FACT)
            COMMENT = '合計購買金額（円）',
        PURCHASE.AVG_PURCHASE_AMOUNT AS AVG(PURCHASE.AMOUNT_FACT)
            COMMENT = '平均購買金額（円）',
        PURCHASE.PURCHASE_COUNT AS COUNT(PURCHASE.AMOUNT_FACT)
            COMMENT = '購買件数',

        -- 来店
        STORE.AVG_STAY_MINUTES AS AVG(STORE.STAY_MINUTES_FACT)
            COMMENT = '平均滞在時間（分）',
        STORE.STORE_VISIT_COUNT AS COUNT(STORE.STAY_MINUTES_FACT)
            COMMENT = '来店件数',

        -- アプリDL
        DL.DOWNLOAD_COUNT AS COUNT(DL.APP_NAME_DIM)
            COMMENT = 'アプリDL件数',

        -- アプリ起動
        LAUNCH.LAUNCH_COUNT AS COUNT(LAUNCH.SESSION_SECONDS_FACT)
            COMMENT = 'アプリ起動件数',
        LAUNCH.AVG_SESSION_SECONDS AS AVG(LAUNCH.SESSION_SECONDS_FACT)
            COMMENT = '平均アプリ利用時間（秒）'
    )

    COMMENT = 'STADIA360 統合マーケティング基盤のセマンティックビュー。テレビ視聴、態度変容、サイト来訪、オフライン購買、来店、アプリDL/起動のデータを横断的に分析可能';


-- ############################################################################
-- STEP 5: 検証クエリ
-- ############################################################################

-- テーブル件数確認
-- SELECT 'CAMPAIGNS' AS TBL, COUNT(*) AS CNT FROM CAMPAIGNS
-- UNION ALL SELECT 'TV_VIEWING_LOG', COUNT(*) FROM TV_VIEWING_LOG
-- UNION ALL SELECT 'CUSTOMER_LOYALTY', COUNT(*) FROM CUSTOMER_LOYALTY
-- UNION ALL SELECT 'ATTITUDE_CHANGE', COUNT(*) FROM ATTITUDE_CHANGE
-- UNION ALL SELECT 'SITE_VISIT_LOG', COUNT(*) FROM SITE_VISIT_LOG
-- UNION ALL SELECT 'OFFLINE_PURCHASE', COUNT(*) FROM OFFLINE_PURCHASE
-- UNION ALL SELECT 'STORE_VISIT_LOG', COUNT(*) FROM STORE_VISIT_LOG
-- UNION ALL SELECT 'APP_DOWNLOAD_LOG', COUNT(*) FROM APP_DOWNLOAD_LOG
-- UNION ALL SELECT 'APP_LAUNCH_LOG', COUNT(*) FROM APP_LAUNCH_LOG;

-- セマンティックビュー確認
-- DESCRIBE SEMANTIC VIEW STADIA360_ANALYTICS;

-- ############################################################################
-- STEP 7: 利用規約テーブル & Cortex Search Service
-- ############################################################################

-- 7-1. 利用規約テーブル
CREATE OR REPLACE TABLE STADIA360_TERMS (
    SECTION_ID    VARCHAR(10)   PRIMARY KEY,
    SECTION_TITLE VARCHAR(200)  NOT NULL,
    CONTENT       VARCHAR(5000) NOT NULL,
    CATEGORY      VARCHAR(50)   NOT NULL
);

-- 7-2. ダミー利用規約データ投入
INSERT INTO STADIA360_TERMS VALUES
('T001', 'STADIA360 Service Overview',
 'STADIA360 is an integrated marketing analytics platform provided by Dentsu Inc. (hereinafter referred to as "the Company"). This platform enables unified measurement and analysis of advertising effectiveness across television viewing logs, digital site visits, offline purchases, store visits, app downloads/launches, attitude change surveys, and customer loyalty data. By using this service, you agree to abide by these Terms of Service. STADIA360 integrates data from multiple sources including TV viewing behavior (linear and connected TV), online browsing and conversion tracking, point-of-sale transaction records, geolocation-based store visit detection, mobile application engagement metrics, brand lift survey results, and CRM loyalty program data. The platform provides campaign-level cross-channel attribution, funnel analysis from awareness to purchase, and ROI optimization recommendations.',
 'General'),
('T002', 'Definitions and Terminology',
 'In these Terms, the following definitions apply: (1) "User" means any individual or corporate entity that has entered into a service agreement with the Company to use STADIA360. (2) "Campaign" refers to any advertising initiative registered on the platform, identified by a unique Campaign ID, with defined start and end dates, budget allocation, and target area. (3) "Data Subject" refers to any individual whose personal or behavioral data is processed through the platform. (4) "CM Exposure" refers to the detection of a television commercial viewing event through panel-based or automatic content recognition (ACR) measurement. (5) "Conversion" refers to a predefined action taken by a consumer, including but not limited to website visit, product purchase, store visit, or app download. (6) "NPS Score" means the Net Promoter Score calculated from customer surveys on a scale of -100 to 100. (7) "LTV" means Lifetime Value, representing the total predicted revenue from a customer over the entire relationship period.',
 'General'),
('T003', 'Service Usage and Access Rights',
 'Users are granted a non-exclusive, non-transferable license to access and use STADIA360 for the duration of their service agreement. Access credentials (user ID and password) must not be shared with unauthorized personnel. The platform is accessible via web browser (Chrome, Edge, Safari supported) and through Snowflake Intelligence interface. Each user account is assigned a role-based access level: (a) Viewer - can view dashboards and reports, (b) Analyst - can create custom queries and exports, (c) Admin - can manage campaigns, users, and data integrations. Users must not attempt to reverse-engineer, decompile, or extract source code from the platform. API access is available under separate API Terms of Use. Concurrent session limits apply: Standard plan allows 5 simultaneous sessions, Enterprise plan allows 50 simultaneous sessions.',
 'Access'),
('T004', 'Data Collection and Processing',
 'STADIA360 collects and processes the following categories of data: (a) TV Viewing Data - channel, program, time slot, device type, CM exposure flags, and creative identifiers, sourced from panel households and ACR-enabled devices. Approximately 5,000+ viewing records are processed per campaign. (b) Site Visit Data - page views, referrer type (Organic, Paid, Social, Direct, Email), session duration, and conversion flags. (c) Purchase Data - transaction amount, product category, store type, and CM exposure correlation. (d) Store Visit Data - location, visit timestamp, stay duration in minutes, and frequency classification. (e) App Data - download events by OS type (iOS/Android) and application name, plus launch frequency and session metrics. (f) Attitude Change Data - pre/post brand awareness scores, interest levels, consideration scores, and purchase intent on 1-100 scale. (g) Loyalty Data - NPS scores, LTV amounts, loyalty segment classification (Platinum/Gold/Silver/Bronze), and churn risk indicators. All data processing complies with Japan''s Act on Protection of Personal Information (APPI) and applicable data protection regulations.',
 'Data'),
('T005', 'Privacy and Personal Information Protection',
 'The Company handles personal information in accordance with Japan''s Act on Protection of Personal Information (APPI) and the Company''s Privacy Policy. Data anonymization is applied at the individual level: all person-identifiable information is hashed using SHA-256 before ingestion into the STADIA360 platform. TV viewing panel data is collected with explicit opt-in consent from panel households. Cross-device matching uses probabilistic methods and does not rely on deterministic personal identifiers. Users must not attempt to re-identify anonymized data subjects. Data retention periods: TV viewing logs are retained for 2 years, purchase data for 3 years, and loyalty data for the duration of the customer relationship plus 1 year. Users may request data deletion for specific campaigns by submitting a written request to the Company''s Data Protection Officer. The Company maintains SOC 2 Type II and ISO 27001 certifications for its data processing infrastructure.',
 'Privacy'),
('T006', 'Fees, Billing, and Payment Terms',
 'Service fees for STADIA360 are structured as follows: (a) Base Platform Fee - a fixed monthly subscription fee based on the selected plan tier (Standard, Professional, or Enterprise). Standard plan: JPY 500,000/month, Professional plan: JPY 1,200,000/month, Enterprise plan: JPY 3,000,000/month. (b) Data Volume Fee - charged per million records processed beyond the included allowance. Standard includes 10M records/month, Professional includes 50M records/month, Enterprise includes unlimited. Overage rate: JPY 10,000 per million records. (c) Campaign Activation Fee - JPY 100,000 per campaign setup for Standard plan, included in Professional and Enterprise plans. (d) Custom Analysis Fee - bespoke analytical work is charged at JPY 200,000 per analyst-day. Invoices are issued on the first business day of each month for the prior month''s usage. Payment is due within 30 days of invoice date. Late payments incur a 1.5% monthly interest charge. All prices exclude consumption tax (currently 10%). Annual prepayment receives a 10% discount on base platform fees.',
 'Billing'),
('T007', 'Service Level Agreement (SLA)',
 'The Company commits to the following service levels: (a) Platform Availability - 99.9% uptime measured monthly, excluding scheduled maintenance windows (Sundays 02:00-06:00 JST). (b) Data Freshness - TV viewing data updated within 24 hours of broadcast, site visit and purchase data updated within 4 hours, store visit data within 12 hours. (c) Dashboard Response Time - under 5 seconds for standard dashboards with up to 10,000 filtered records. (d) Support Response Time - Critical issues (platform down): 1 hour response, 4 hour resolution. High priority (data quality): 4 hour response, 1 business day resolution. Normal: 1 business day response, 5 business day resolution. (e) Data Accuracy - CM exposure detection accuracy >= 95%, store visit detection accuracy >= 90%. If monthly uptime falls below 99.9%, service credits are issued: 99.0-99.9% = 10% credit, 95.0-99.0% = 25% credit, below 95.0% = 50% credit on that month''s base platform fee.',
 'SLA'),
('T008', 'Intellectual Property Rights',
 'All intellectual property rights in the STADIA360 platform, including but not limited to software, algorithms, user interface design, documentation, and analytical methodologies, are and remain the exclusive property of the Company. Users retain ownership of their own campaign data, creative assets, and custom analytical configurations created within the platform. Analytical reports and insights generated by the platform may be used by Users for their internal business purposes and for reporting to their advertising clients. Users may not (a) claim ownership of platform-generated analytical models, (b) use insights for competitive benchmarking against other Company clients without written consent, (c) publish raw data extracts from the platform without anonymization, or (d) use the STADIA360 brand name or trademarks without prior written approval. Custom dashboards and saved queries created by Users are considered User-generated content and may be exported upon contract termination.',
 'Legal'),
('T009', 'Limitation of Liability and Disclaimer',
 'The Company provides STADIA360 analytics and insights on an "as-is" basis for informational purposes. While the Company uses commercially reasonable efforts to ensure data accuracy, no guarantee is made regarding the completeness, accuracy, or timeliness of any data, analysis, or recommendation provided through the platform. The Company shall not be liable for: (a) business decisions made based on platform analytics, (b) indirect, incidental, or consequential damages including lost profits, (c) damages arising from unauthorized access due to User''s failure to maintain credential security, (d) service interruptions caused by force majeure events, or (e) third-party data source delays or inaccuracies. Maximum aggregate liability in any 12-month period is limited to the total fees paid by User during that period. Users acknowledge that advertising effectiveness measurement involves statistical estimation and inherent uncertainty. CM exposure attribution uses probabilistic matching with stated accuracy levels in the SLA, and results should be interpreted as directional guidance rather than absolute measurements.',
 'Legal'),
('T010', 'Contract Termination and Data Handling',
 'Either party may terminate the service agreement with 90 days written notice. The Company may suspend or terminate service immediately if the User: (a) breaches these Terms and fails to cure within 30 days of notice, (b) becomes insolvent or enters bankruptcy proceedings, (c) uses the platform for illegal purposes, or (d) attempts to compromise platform security. Upon termination: (i) User access is revoked within 24 hours, (ii) User may request export of their campaign data and custom configurations within 30 days of termination, (iii) User data is deleted from the platform within 90 days of termination, except where retention is required by law, (iv) any prepaid fees for unused service periods are refunded on a pro-rata basis, minus any outstanding charges. The Company retains anonymized, aggregated data for benchmarking and platform improvement purposes. Sections regarding intellectual property, limitation of liability, and confidentiality survive termination.',
 'Contract'),
('T011', 'Confidentiality Obligations',
 'Both parties agree to maintain strict confidentiality regarding: (a) all campaign performance data and analytical results, (b) proprietary algorithms and methodologies, (c) business terms and pricing, (d) client lists and campaign strategies. Confidential information may not be disclosed to third parties without prior written consent, except: (i) to professional advisors bound by confidentiality, (ii) as required by law or regulatory authority, (iii) to authorized subcontractors under equivalent confidentiality terms. Employees and contractors accessing the platform must sign individual NDAs. Confidentiality obligations survive for 3 years after contract termination. The Company implements data isolation measures to ensure that one User''s campaign data is never accessible to another User. Role-based access controls and audit logging are enforced at the database level through Snowflake''s native security framework.',
 'Legal'),
('T012', 'Acceptable Use Policy',
 'Users agree to use STADIA360 exclusively for legitimate marketing analytics purposes. Prohibited uses include: (a) attempting to identify or contact individual data subjects, (b) using the platform to collect competitive intelligence about other Company clients, (c) automated scraping or bulk data extraction beyond authorized API limits, (d) uploading malicious code or attempting to compromise platform security, (e) sharing access credentials or creating unauthorized user accounts, (f) using analytics results to discriminate against protected classes of individuals, (g) reselling or sublicensing platform access to third parties. API rate limits: Standard plan 100 requests/hour, Professional 1,000 requests/hour, Enterprise 10,000 requests/hour. Users must report any suspected security incidents to security@stadia360.example.com within 24 hours of discovery. Violation of this policy may result in immediate suspension without prior notice.',
 'Access'),
('T013', 'Data Integration and Third-Party Services',
 'STADIA360 integrates with the following third-party data sources and services: (a) TV Panel Data Providers - Video Research Ltd. and Intage Inc. for household viewing panel data, (b) ACR Technology - Gracenote and TVision for automatic content recognition, (c) Location Data - Agoop and BlogWatcher for anonymized mobile location signals, (d) Payment Data - Macromill and CARD for purchase transaction matching, (e) Cloud Infrastructure - Snowflake Inc. for data warehousing and compute, (f) Survey Platforms - Macromill and Cross Marketing for attitude change surveys. The Company is not responsible for data quality issues originating from third-party providers. Users acknowledge that third-party data may be subject to additional terms and usage restrictions. Integration availability may change with 30 days notice. Custom data integrations can be arranged under separate SOW agreements. The Company conducts annual security assessments of all third-party data providers.',
 'Data'),
('T014', 'Governing Law and Dispute Resolution',
 'These Terms are governed by and construed in accordance with the laws of Japan. Any disputes arising from or relating to these Terms shall first be resolved through good-faith negotiation between the parties. If negotiation fails within 30 days, disputes shall be submitted to binding arbitration under the rules of the Japan Commercial Arbitration Association (JCAA), with the arbitration conducted in Tokyo, Japan, in the Japanese language. Each party bears its own legal costs unless the arbitrator awards costs to the prevailing party. Nothing in these Terms limits either party''s right to seek injunctive relief from a court of competent jurisdiction. The Tokyo District Court shall have exclusive jurisdiction for any court proceedings. These Terms constitute the entire agreement between the parties regarding STADIA360 and supersede all prior agreements and understandings. Amendments require written consent of both parties. If any provision is found unenforceable, the remaining provisions continue in full force and effect. The Company may update these Terms with 60 days notice to Users.',
 'Legal');

-- 7-3. Change Tracking 有効化
ALTER TABLE STADIA360_TERMS SET CHANGE_TRACKING = TRUE;

-- 7-4. Cortex Search Service 作成
CREATE OR REPLACE CORTEX SEARCH SERVICE STADIA360_TERMS_SEARCH
  ON CONTENT
  ATTRIBUTES SECTION_TITLE, CATEGORY
  WAREHOUSE = COMPUTE_WH
  TARGET_LAG = '1 day'
AS (
  SELECT SECTION_ID, SECTION_TITLE, CONTENT, CATEGORY
  FROM STADIA360_TERMS
);

-- 確認
-- SHOW CORTEX SEARCH SERVICES IN SCHEMA KFUKAMORI_GEN_DB.STADIA360;
-- SELECT COUNT(*) FROM STADIA360_TERMS;
