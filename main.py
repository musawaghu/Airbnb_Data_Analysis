import pandas as pd
import sqlite3
import os
import numpy as np
import plotly.express as px

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


def style_chart(fig, title, x_title, y_title, height):
    # Shared look for every chart: light background, thin gridlines, no legend
    fig.update_layout(
        title=dict(text=title, x=0, xanchor='left', font=dict(size=18, color='#0b0b0b')),
        height=height, showlegend=False,
        plot_bgcolor='#fcfcfb', paper_bgcolor='#fcfcfb',
        font=dict(family='Arial', size=13, color='#52514e'),
        margin=dict(l=20, r=40, t=70, b=50),
        hoverlabel=dict(bgcolor='white', font_size=13),
    )
    fig.update_xaxes(title=x_title, gridcolor='#e6e5e1', zeroline=False)
    fig.update_yaxes(title=y_title, gridcolor='#e6e5e1', zeroline=False)
    return fig


# Runs inside each HTML chart: on narrow screens (phones) the title is split over two lines
# and made smaller so it doesn't run off the edge. {plot_id} is filled in by Plotly.
MOBILE_SCRIPT = """
var gd = document.getElementById('{plot_id}');
var fullTitle = gd.layout.title.text;
var isSmall = null;
function fitToScreen() {
    var small = gd.clientWidth < 560;
    if (small === isSmall) return;  // only relayout when crossing the breakpoint
    isSmall = small;
    var title = fullTitle;
    if (small) {
        var words = fullTitle.split(' ');
        var mid = Math.ceil(words.length / 2);
        title = words.slice(0, mid).join(' ') + '<br>' + words.slice(mid).join(' ');
    }
    Plotly.relayout(gd, {
        'title.text': title,
        'title.font.size': small ? 15 : 18,
        'margin.t': small ? 85 : 70,
        'margin.r': small ? 20 : 40
    });
}
window.addEventListener('resize', fitToScreen);
fitToScreen();
"""


def create_charts(dataframe, png_dir="images", html_dir="charts"):
    # Saves each chart as an interactive HTML file (for the website) and a PNG (for the README)
    os.makedirs(png_dir, exist_ok=True)
    os.makedirs(html_dir, exist_ok=True)
    bar_color = '#2a78d6'

    # Turn room_type back into readable labels for the charts
    data = dataframe.copy()
    data['room_label'] = data['room_type'].map({0: 'Entire home/apt', 1: 'Private room', 2: 'Shared room'})

    charts = {}

    # 1. Median price by room type
    rooms = (data.groupby('room_label')
             .agg(median_price=('price', 'median'), listings=('price', 'size'))
             .sort_values('median_price').reset_index())
    fig = px.bar(rooms, x='median_price', y='room_label', orientation='h', text='median_price',
                 custom_data=['listings'])
    fig.update_traces(hovertemplate='<b>%{y}</b><br>Median price: $%{x:.0f}<br>Listings: %{customdata[0]:,}<extra></extra>')
    charts['price_by_room_type'] = style_chart(fig, 'Entire homes cost more than twice as much as private rooms',
                                               'Median price per night ($)', None, 300)

    # 2. Median price by borough
    boroughs = (data.groupby('neighbourhood_group')
                .agg(median_price=('price', 'median'), listings=('price', 'size'))
                .sort_values('median_price').reset_index())
    fig = px.bar(boroughs, x='median_price', y='neighbourhood_group', orientation='h', text='median_price',
                 custom_data=['listings'])
    fig.update_traces(hovertemplate='<b>%{y}</b><br>Median price: $%{x:.0f}<br>Listings: %{customdata[0]:,}<extra></extra>')
    charts['price_by_borough'] = style_chart(fig, 'Manhattan is the most expensive borough',
                                             'Median price per night ($)', None, 360)

    # 3. Neighborhood supply vs price (only neighborhoods with 50+ listings, so averages are meaningful)
    hoods = (data.groupby(['neighbourhood', 'neighbourhood_group'])
             .agg(listings=('price', 'size'), median_price=('price', 'median'))
             .reset_index())
    hoods = hoods[hoods['listings'] >= 50]
    fig = px.scatter(hoods, x='listings', y='median_price', log_x=True,
                     custom_data=['neighbourhood', 'neighbourhood_group'])
    fig.update_traces(marker=dict(size=10, color=bar_color, opacity=0.75, line=dict(width=1.5, color='#fcfcfb')),
                      hovertemplate='<b>%{customdata[0]}</b> (%{customdata[1]})<br>'
                                    'Listings: %{x:,}<br>Median price: $%{y:.0f}<extra></extra>')
    # Label the busiest and most expensive neighborhoods (offsets in pixels keep labels from overlapping)
    label_offsets = {'Williamsburg': (0, -30), 'Harlem': (-10, 45), 'Bedford-Stuyvesant': (-40, 45), 'Tribeca': (40, -15)}
    for name, (ax, ay) in label_offsets.items():
        row = hoods[hoods['neighbourhood'] == name].iloc[0]
        fig.add_annotation(x=np.log10(row['listings']), y=row['median_price'], text=name,
                           ax=ax, ay=ay, arrowcolor='#8a8984', arrowwidth=1, arrowhead=0,
                           font=dict(size=12, color='#0b0b0b'))
    fig.update_xaxes(tickvals=[50, 100, 200, 500, 1000, 2000, 4000])
    charts['neighborhood_supply_vs_price'] = style_chart(
        fig, 'The busiest neighborhoods are not the most expensive',
        'Number of listings (log scale)', 'Median price per night ($)', 480)

    # 4. Average price by number of reviews
    bins = [-1, 0, 5, 20, 50, float('inf')]
    labels = ['0', '1-5', '6-20', '21-50', '50+']
    data['review_bucket'] = pd.cut(data['number_of_reviews'], bins=bins, labels=labels)
    reviews = (data.groupby('review_bucket', observed=True)
               .agg(avg_price=('price', 'mean'), listings=('price', 'size'))
               .reset_index())
    fig = px.bar(reviews, x='review_bucket', y='avg_price', text='avg_price', custom_data=['listings'])
    fig.update_traces(hovertemplate='<b>%{x} reviews</b><br>Average price: $%{y:.0f}<br>Listings: %{customdata[0]:,}<extra></extra>')
    charts['price_by_reviews'] = style_chart(fig, 'Listings with more reviews tend to be cheaper',
                                             'Number of reviews', 'Average price per night ($)', 420)

    # Bar styling shared by the bar charts: one color, value at the end of each bar
    for name, fig in charts.items():
        fig.update_traces(selector=dict(type='bar'), marker_color=bar_color, marker_cornerradius=4,
                          texttemplate='$%{text:.0f}', textposition='outside', cliponaxis=False,
                          textfont=dict(color='#0b0b0b'))
        fig.update_layout(bargap=0.45)
        # HTML fills the width of its container (iframe or page); toolbar hidden for a cleaner look
        # A fixed div_id keeps the file identical between runs (otherwise Plotly uses a random id)
        fig.write_html(os.path.join(html_dir, f"{name}.html"), include_plotlyjs='cdn', div_id=name,
                       config={'responsive': True, 'displayModeBar': False},
                       default_width='100%', default_height=f"{fig.layout.height}px",
                       post_script=MOBILE_SCRIPT)
        fig.write_image(os.path.join(png_dir, f"{name}.png"), width=760, scale=2)
    print(f"Charts saved to {html_dir}/ and {png_dir}/")


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

    create_charts(df)
