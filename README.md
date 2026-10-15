This project analyzes Airbnb listing data for New York City (2019) to uncover pricing trends, neighborhood insights, and property availability patterns. The dataset is cleaned, transformed, and stored in an SQLite database for efficient querying and analysis.

## Key Findings

Based on 48,895 listings. Prices are medians per night unless stated otherwise, because a few extreme listings (up to $10,000/night) push the averages up.

1. **Room type matters more for price than anything else.** An entire home or apartment has a median price of $160, a private room $70 and a shared room $45. Entire homes make up 52% of listings and private rooms 46%. Shared rooms are only 2%.

   ![Median price by room type](images/price_by_room_type.png)

2. **Manhattan and Brooklyn make up 85% of the market, and Manhattan costs much more.** The median is $150 in Manhattan, $90 in Brooklyn, $75 in Queens and Staten Island, and $65 in the Bronx.

   ![Median price by borough](images/price_by_borough.png)

3. **The neighborhoods with the most listings are not the most expensive.** Williamsburg (3,920 listings), Bedford-Stuyvesant (3,714) and Harlem (2,658) have the most supply, at medians of $80–$105. The most expensive neighborhoods are small, high-end areas of Manhattan: Tribeca has a median of $295 across 177 listings.

   ![Neighborhood listings vs median price](images/neighborhood_supply_vs_price.png)

4. **Cheaper listings get more reviews.** Listings with no reviews average $193, while listings with 50+ reviews average $129. Since reviews are a rough measure of how often a listing is booked, this suggests guests prefer lower-priced listings. Overpriced listings may sit unbooked.

   ![Average price by number of reviews](images/price_by_reviews.png)

**Data caveat:** 36% of listings show 0 days of availability, which suggests a large share of them were inactive. There are also 11 listings priced at $0. Neither group was removed for this analysis.

## Charts

The charts above are made with Plotly in `main.py`. Running it saves each chart twice: as a PNG in `images/` and as an interactive HTML file in `charts/` (hover any bar or point for details). The HTML files can be embedded in a website with an `<iframe>`.

An earlier version of the analysis was also built as a Power BI dashboard (`Dashboard.pbix`, requires Power BI Desktop).

## How to Run

```
pip install -r requirements.txt
python main.py
```

This cleans `AB_NYC_2019.csv`, builds the SQLite database (`airbnb.db`), prints the SQL query results and regenerates the charts.

## Key Features

Data Cleaning: Handles missing values, converts categorical data (e.g., room types to numeric), and removes unnecessary columns.

SQLite Integration: Stores processed data in a structured database for easy access and scalability.

Exploratory Analysis: Examines correlations between price, location, room type, and reviews.

## Technologies Used

Python (Pandas, SQLite3)

SQL (Database management)

Data Visualization (Plotly and Power BI)

Goal: Provide insights for travelers, hosts, and real estate analysts on NYC Airbnb trends.
