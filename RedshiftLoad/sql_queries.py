import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_reviews_table_drop = "DROP TABLE IF EXISTS staging_reviews;"
staging_hotels_table_drop = "DROP TABLE IF EXISTS staging_hotels;"
reviews_table_drop = "DROP TABLE IF EXISTS reviews;"
hotels_table_drop = "DROP TABLE IF EXISTS hotels;"
hotelreviewsmetadata_table_drop = "DROP TABLE IF EXISTS hotelreviewsmetadata;"
hoteladdresses_table_drop = "DROP TABLE IF EXISTS hoteladdresses;"
date_table_drop = "DROP TABLE IF EXISTS date;"
airports_table_drop = "DROP TABLE IF EXISTS airports;"
countryindicators_table_drop = "DROP TABLE IF EXISTS countryindicators;"

# CREATE TABLES

staging_reviews_table_create= ("""
CREATE TABLE IF NOT EXISTS public.staging_reviews (
    HotelID smallint,
    NumReviews smallint,
    AverageScore real,
    NumRatings smallint,
    ReviewDate date,
    ReviewerNationality varchar(50),
    TotalNumReviewsByReviewer smallint,
    ReviewerScore real,
    NegativeReview varchar(3000),
    ReviewNegativeWordsCount smallint,
    PositiveReview varchar(3000),
    ReviewPositiveWordsCount smallint,
    Tags varchar(256)
);
""")

staging_hotels_table_create = ("""
CREATE TABLE IF NOT EXISTS public.staging_hotels (
    HotelID smallint,
    HotelName varchar(60),
    HotelAddress varchar(256),
    Country varchar(50),
    Phone varchar(20),
    Price varchar(10),
    OriginalHotelName varchar(60),
    GoogleAddress varchar(256),
    Latitude numeric(18,8),
    Longitude numeric(18,8),
    gPlusPlaceId varchar(25),
    NearestAirportID varchar(10),
    MondayHours varchar(30),
    TuesdayHours varchar(30),
    WednesdayHours varchar(30),
    ThursdayHours varchar(30),
    FridayHours varchar(30),
    SaturdayHours varchar(30),
    SundayHours varchar(30)
);
""")

reviews_table_create = ("""
CREATE TABLE IF NOT EXISTS public.reviews (
    review_id bigint identity(0, 1),
    hotel_id smallint NOT NULL,
    reviewer_nationality varchar(50),
    review_date date,
    num_reviews_by_reviewer smallint,
    reviewer_score real,
    negative_review varchar(3000),
    review_negative_words_count smallint,
    positive_review varchar(3000),
    review_positive_words_count smallint,
    tags varchar(256),
    CONSTRAINT reviews_pkey PRIMARY KEY (review_id)
);
""")

hotels_table_create = ("""
CREATE TABLE IF NOT EXISTS public.hotels (
    hotel_id smallint NOT NULL,
    hotel_name varchar(60),
    nearest_airport_id varchar(10),
    country varchar(50),
    phone varchar(20),
    price varchar(10),
    original_hotel_name varchar(60),
    monday_hours varchar(30),
    tuesday_hours varchar(30),
    wednesday_hours varchar(30),
    thursday_hours varchar(30),
    friday_hours varchar(30),
    saturday_hours varchar(30),
    sunday_hours varchar(30),
    CONSTRAINT hotels_pkey PRIMARY KEY (hotel_id)
);

""")

hotelreviewsmetadata_table_create = ("""
CREATE TABLE IF NOT EXISTS public.hotelreviewsmetadata (
    hotel_id smallint NOT NULL,
    total_hotel_reviews smallint,
    total_hotel_ratings smallint,
    average_score real,
    CONSTRAINT hotelreviewsmetadata_pkey PRIMARY KEY (hotel_id)
);
""")

hoteladdresses_table_create = ("""
CREATE TABLE IF NOT EXISTS public.hoteladdresses (
    hoteladdress_id smallint NOT NULL,
    hotel_address varchar(256),
    google_address varchar(256),
    latitude numeric(18,8),
    longitude numeric(18,8),
    country varchar(50),
    gplus_place_id varchar(25),
    CONSTRAINT hoteladdresses_pkey PRIMARY KEY (hoteladdress_id)
);

""")

date_table_create = ("""
CREATE TABLE IF NOT EXISTS public."date"  (
    review_date date NOT NULL,
    "day" int4,
    "week" int4,
    "month" int4,
    "year" int4,
    weekday int4,
    CONSTRAINT time_pkey PRIMARY KEY (review_date)
);
""")

airports_table_create = ("""
CREATE TABLE IF NOT EXISTS public.airports (
    airport_id varchar(10),
    type varchar(20),
    airport_name varchar(100),
    municipality varchar(60),
    country varchar(50),
    iso_country char(2),
    continent char(2),
    iso_region varchar(10),
    latitude numeric(18,8),
    longitude numeric(18,8),
    elevation_in_feet integer,
    gps_code varchar(10),
    iata_code varchar(10),
    local_code varchar(10),
    CONSTRAINT airports_pkey PRIMARY KEY (airport_id)
);
""")

countryindicators_table_create = ("""
CREATE TABLE IF NOT EXISTS public.countryindicators (
    country varchar(50),
    iso_code char(2), 
    tourism_expenditure_millions float,
    tourist_arrivals_thousands float,
    currency varchar(30),
    exchange_rate_end_of_period float,
    gni_per_capita float,
    gdp_per_capita float,
    mobile_phone_subscriptions real,
    net_migration_rate real,
    population numeric(15,5),
    urban_population_percent float,
    hdi_rank integer,
    hdi float,
    internet_users_percent real,
    political_rights_freedom_score real,
    civil_liberties_freedom_score real,
    freedom_status varchar(15),
    democracy_or_not boolean,
    political_regime_type_score float,
    human_rights_score float
);
""")

# STAGING TABLES and Final tables which can be inserted directly

staging_hotels_copy = ("""
COPY staging_hotels FROM '{}hotels.jsonl'
CREDENTIALS 'aws_iam_role={}'
REGION '{}'
JSON '{}JSONPath/hotelsjsonpath.json'
""").format(config['S3']['S3_PATH'], config['IAM_ROLE']['ARN'],
            config['CLUSTER']['DWH_REGION'], config['S3']['S3_PATH'])

staging_reviews_copy = ("""
COPY staging_reviews FROM '{}reviews.jsonl'
CREDENTIALS 'aws_iam_role={}'
REGION '{}'
JSON '{}JSONPath/reviewsjsonpath.json'
""").format(config['S3']['S3_PATH'], config['IAM_ROLE']['ARN'],
            config['CLUSTER']['DWH_REGION'], config['S3']['S3_PATH'])

airports_copy = ("""
COPY airports FROM '{}airports.jsonl'
CREDENTIALS 'aws_iam_role={}'
REGION '{}'
JSON '{}JSONPath/airportsjsonpath.json'
""").format(config['S3']['S3_PATH'], config['IAM_ROLE']['ARN'],
            config['CLUSTER']['DWH_REGION'], config['S3']['S3_PATH'])

countryindicators_copy = ("""
COPY countryindicators FROM '{}{}'
CREDENTIALS 'aws_iam_role={}'
REGION '{}'
JSON 'auto'
""").format(config['S3']['S3_PATH'], 'countryindicators.jsonl', config['IAM_ROLE']['ARN'], config['CLUSTER']['DWH_REGION'])

# FINAL TABLES: DON'T INSERT null values

review_table_insert = ("""
        INSERT INTO reviews (
            hotel_id,
            reviewer_nationality,
            review_date,
            num_reviews_by_reviewer,
            reviewer_score,
            negative_review,
            review_negative_words_count,
            positive_review,
            review_positive_words_count,
            tags
        )
        SELECT
                HotelID, 
                ReviewerNationality, 
                ReviewDate, 
                TotalNumReviewsByReviewer, 
                ReviewerScore, 
                NegativeReview, 
                ReviewNegativeWordsCount, 
                PositiveReview,
                ReviewPositiveWordsCount,
                Tags
           FROM staging_reviews
    """)

hotelreviewsmetadata_table_insert = ("""
         INSERT INTO hotelreviewsmetadata (
            hotel_id,
            total_hotel_reviews,
            total_hotel_ratings,
            average_score
        )
        SELECT distinct (HotelID), NumReviews, NumRatings, AverageScore 
        FROM staging_reviews
    """)

hotels_table_insert = ("""
        INSERT INTO hotels (
            hotel_id,
            hotel_name,
            nearest_airport_id,
            country,
            phone,
            price,
            original_hotel_name,
            monday_hours,
            tuesday_hours,
            wednesday_hours,
            thursday_hours,
            friday_hours,
            saturday_hours,
            sunday_hours
        )
        SELECT distinct (HotelID), HotelName, NearestAirportID, Country, Phone, Price, OriginalHotelName,
                MondayHours, TuesdayHours, WednesdayHours, ThursdayHours, FridayHours, SaturdayHours, SundayHours
        FROM staging_hotels
    """)

hoteladdresses_table_insert = ("""
        INSERT INTO hoteladdresses (
            hoteladdress_id,
            hotel_address,
            google_address,
            latitude,
            longitude,
            country,
            gplus_place_id
        )
        SELECT distinct (HotelID), HotelAddress, GoogleAddress, Latitude, Longitude, Country, gPlusPlaceId
        FROM staging_hotels
    """)

date_table_insert = ("""
        INSERT INTO date (
            review_date,
            day,
            week,
            month,
            year,
            weekday
        )
        SELECT distinct(review_date), extract(day from review_date), extract(week from review_date), 
               extract(month from review_date), extract(year from review_date), extract(dayofweek from review_date)
        FROM reviews
    """)

# QUERY LISTS

create_table_queries = [staging_reviews_table_create, staging_hotels_table_create, reviews_table_create, hotels_table_create, hotelreviewsmetadata_table_create, hoteladdresses_table_create, date_table_create, airports_table_create, countryindicators_table_create]
drop_table_queries = [staging_reviews_table_drop, staging_hotels_table_drop, reviews_table_drop, hotels_table_drop, hotelreviewsmetadata_table_drop, hoteladdresses_table_drop, date_table_drop, airports_table_drop, countryindicators_table_drop]
copy_table_queries = [staging_reviews_copy, staging_hotels_copy, airports_copy, countryindicators_copy]
insert_table_queries = [review_table_insert, hotelreviewsmetadata_table_insert, hotels_table_insert, hoteladdresses_table_insert, date_table_insert]