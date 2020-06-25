CREATE TABLE IF NOT EXISTS public.hoteladdresses (
	hoteladdress_id smallint NOT NULL,
	hotel_address varchar(256),
	google_address varchar(256),
	latitude numeric(18,8),
	longitude numeric(18,8),
    country varchar(50),
    gplus_place_id bigint,
    CONSTRAINT hoteladdresses_pkey PRIMARY KEY (hoteladdress_id)
);

CREATE TABLE IF NOT EXISTS public.reviews (
	review_id bigint identity(0, 1),
	hotel_id smallint NOT NULL,
	reviewer_nationality varchar(50),
	review_date date,
	num_reviews_by_reviewer smallint,
	reviewer_score real,
	negative_review varchar,
	review_negative_words_count smallint,
	positive_review varchar,
    review_positive_words_count smallint,
    tags varchar(100),
	CONSTRAINT reviews_pkey PRIMARY KEY (review_id)
);

CREATE TABLE IF NOT EXISTS public.hotelreviewsmetadata (
	hotel_id small int NOT NULL,
    total_hotel_reviews smallint,
    total_hotel_ratings smallint,
    average_score real,
    CONSTRAINT hotelreviewsmetadata_pkey PRIMARY KEY (hotel_id)
);

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

CREATE TABLE IF NOT EXISTS public.staging_reviews (
	HotelID smallint,
	NumReviews smallint,
	AverageScore varchar(256),
	NumRatings smallint,
	ReviewDate date,
	ReviewerNationality varchar(50),
	TotalNumReviewsByReviewer smallint,
	ReviewerScore real,
	NegativeReview varchar,
	ReviewNegativeWordsCount smallint,
	PositiveReview varchar,
	ReviewPositiveWordsCount smallint,
	Tags varchar(100)
);

CREATE TABLE IF NOT EXISTS public.staging_hotels (
	HotelID smallint NOT NULL,
	HotelName varchar(60),
    HotelAddress varchar(256),
    Country varchar(50),
	Phone varchar(20),
    Price varchar(10),
    OriginalHotelName varchar(60),
    GoogleAddress varchar(256),
    Latitude numeric(18,8),
    Longitude numeric(18,8),
    gPlusPlaceId bigint,
    NearestAirportID varchar(10),
    MondayHours varchar(30),
    TuesdayHours varchar(30),
    WednesdayHours varchar(30),
    ThursdayHours varchar(30),
    FridayHours varchar(30),
    SaturdayHours varchar(30),
    SundayHours varchar(30),
);


CREATE TABLE IF NOT EXISTS public.airports (
	AirportId int4 NOT NULL,
	Type varchar(20),
	AirportName varchar(60),
	Municipality varchar(60),
	Country varchar(50),
    IsoCountry char(2),
    Continent char(2),
    IsoRegion varchar(10),
    Latitude numeric(18,8),
    Longitude numeric(18,8),
    ElevationInFeet integer,
    GpsCode varchar(10),
    IATACode varchar(10),
    LocalCode varchar(10)
	CONSTRAINT airports_pkey PRIMARY KEY (airports_id)
);

CREATE TABLE IF NOT EXISTS public."date"  (
	review_date date NOT NULL,
    "day" int4,
    "week" int4,
    "month" int4,
    "year" int4,
    weekday int4,
    CONSTRAINT time_pkey PRIMARY KEY (review_date)
);

CREATE TABLE IF NOT EXISTS public.countryindicators (
	Country varchar(50),
    ISOCode char(2), 
    TourismExpenditureMillions float,
    TouristArrivalsThousands float,
    Currency varchar(30),
    ExchangeRateEndOfPeriod float,
    GNIPerCapita float,
    GDPPerCapita float,
    MobilePhoneSubscriptions real,
    NetMigrationRate real,
    Population numeric(15,5),
    UrbanPopulationPercent float,
    HDIRank integer,
    HDI float,
    InternetUsersPercent real,
    PoliticalRightsFreedomScore real,
    CivilLibertiesFreedomScore real,
    FreedomStatus varchar(15),
    DemocracyOrNot boolean,
    PoliticalRegimeTypeScore float,
    HumanRightsScore float
);