from statistics import correlation

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pandas.core.interchange.dataframe_protocol import DataFrame
import sqlite3

"""
Key Questions to Answer:
    Price Distribution: What's the typical price range in each neighborhood for Airbnb listings in NYC?

    Neighborhood Analysis: Which neighborhoods have the most listings and highest prices?

    Room Types: How do different room types (Entire home, Private room, Shared room) compare in price and availability?

    Reviews: What's the relationship between price and number of reviews?
"""


def setup_database(dataframe, db_name="airbnb.db"):
    conn = sqlite3.connect(db_name)
    # Create table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS listings (
            id INTEGER PRIMARY KEY,
            name TEXT,
            host_id INTEGER,
            neighbourhood_group TEXT,
            neighbourhood TEXT,
            latitude REAL,
            longitude REAL,
            room_type INTEGER,
            price REAL,
            minimum_nights INTEGER,
            number_of_reviews INTEGER,
            availability_365 INTEGER
        )
        ''')

    # Insert data
    dataframe.to_sql('listings', conn, if_exists='replace', index=False)
    conn.commit()
    conn.close()
    print(f"Database {db_name} created successfully")


def run_sql_queries(db_path="airbnb.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Average price by neighborhood
    cursor.execute('''
        SELECT neighbourhood, AVG(price) as avg_price, COUNT(*) as listing_count
        FROM listings
        GROUP BY neighbourhood
        ORDER BY avg_price DESC
        ''')
    neighborhood_prices = cursor.fetchall()

    # Top 10 neighborhoods with the most listing and highest prices
    cursor.execute('''
    SELECT 
    neighbourhood AS neighborhood,
    COUNT(*) AS listing_count,
    ROUND(AVG(price), 2) AS avg_price,
    ROUND(MAX(price), 2) AS max_price
    FROM listings
    GROUP BY neighbourhood
    ORDER BY listing_count DESC, avg_price DESC
    LIMIT 10
    ''')
    highest_listing = cursor.fetchall()

    # Room type analysis
    cursor.execute('''
    SELECT 
    CASE room_type
        WHEN 0 THEN 'Entire home/apt'
        WHEN 1 THEN 'Private room'
        WHEN 2 THEN 'Shared room'
    END AS room_type,
    COUNT(*) AS listing_count,
    ROUND(AVG(price), 2) AS avg_price,
    ROUND(AVG(availability_365), 0) AS avg_availability_days,
    ROUND(AVG(number_of_reviews), 1) AS avg_reviews
    FROM listings
    GROUP BY room_type
    ORDER BY avg_price DESC
    ''')
    room_type = cursor.fetchall()

    # Price vs reviews
    cursor.execute('''
    SELECT 
    CASE
        WHEN number_of_reviews = 0 THEN 'No reviews'
        WHEN number_of_reviews BETWEEN 1 AND 5 THEN '1-5 reviews'
        WHEN number_of_reviews BETWEEN 6 AND 20 THEN '6-20 reviews'
        WHEN number_of_reviews BETWEEN 21 AND 50 THEN '21-50 reviews'
        ELSE '50+ reviews'
    END AS review_category,
    COUNT(*) AS listing_count,
    ROUND(AVG(price), 2) AS avg_price,
    ROUND(AVG(availability_365), 0) AS avg_availability_days
    FROM listings
    GROUP BY review_category
    ORDER BY avg_price DESC
    ''')
    price_reviews = cursor.fetchall()
    conn.close()
    return neighborhood_prices, highest_listing, room_type, price_reviews


if __name__ == '__main__':
    uncleaned_df = pd.read_csv("AB_NYC_2019.csv")

    ## 1. Fill missing values in name columns
    uncleaned_df['name'] = uncleaned_df['name'].fillna('Unnamed Listing')
    uncleaned_df['host_name'] = uncleaned_df['host_name'].fillna('Anonymous Host')

    ## 2. Drop the review-related columns
    df = uncleaned_df.drop(['last_review', 'reviews_per_month', 'calculated_host_listings_count'], axis=1)

    ## 3. Changing the room_type to numeric values to see if any correlation between price
    room_type_mapping = {
        'Entire home/apt': 0,
        'Private room': 1,
        'Shared room': 2
    }
    pd.set_option('future.no_silent_downcasting', True)
    df['room_type'] = df['room_type'].replace(room_type_mapping).astype('int8')

    ## 4. Seeing correlation between location(latitude & longitude), price, room type and number of reviews
    df_subset = df[['latitude', 'longitude', 'price', 'room_type', 'number_of_reviews']]
    correlation_matrix = df_subset.corr()
    # print(correlation_matrix)
    '''the correlation matrix doesnt show any meaningful correlation between these columns. I was expecting to see a
    strong correlation between price and room type, or price and location but it looks like its not the case'''

    setup_database(df)

    neighborhood_prices, highest_listing, room_type, price_reviews = run_sql_queries(db_path="airbnb.db")

    df_avg_price = pd.DataFrame(neighborhood_prices, columns=['Neighborhood', 'Avg Price', 'Listings'])
    df_avg_price['Avg Price'] = df_avg_price['Avg Price'].round(2)
    print("\nList of each neighborhood and it's average airbnb price and how many listings in each")
    print(df_avg_price)

    print("\nTop 10 neighborhoods in terms of highest average price and highest amount of listings")
    df_top_neighborhoods = pd.DataFrame(highest_listing,
                                        columns=['Neighborhood', 'Listings', 'Avg Price', 'Max Price'])
    print(df_top_neighborhoods)

    print("\nHow Room type affects prices")
    df_room_types = pd.DataFrame(room_type,
                                 columns=['Room Type', 'Listings', 'Avg Price', 'Avg Availability', 'Avg Reviews'])
    print(df_room_types)

    print("\nPrice vs. Reviews relationship")
    df_price_reviews = pd.DataFrame(price_reviews,
                                    columns=['Review Category', 'Listings', 'Avg Price', 'Avg Availability'])
    print(df_price_reviews)
