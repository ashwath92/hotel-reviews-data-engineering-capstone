class SqlQueries:
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
        SELECT distinct HotelID, NumReviews, AverageScore 
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
        SELECT distinct HotelID, HotelName, NearestAirportID, Country, Phone, Price, OriginalHotelName,
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
        SELECT distinct HotelID, HotelAddress, GoogleAddress, Latitude, Longitude, Country, gPlusPlaceId
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
        SELECT review_date, extract(day from review_date), extract(week from review_date), 
               extract(month from review_date), extract(year from review_date), extract(dayofweek from review_date)
        FROM reviews
    """)