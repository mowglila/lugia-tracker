"""
Listing Info App - Streamlit Web Interface

Displays eBay listing information alongside PriceCharting market values.
Facts only - no recommendations, no deal scores.

Run with: streamlit run listing_info_app.py
"""

import os
import streamlit as st
import pandas as pd
from database import DatabaseManager
from grade_matcher import GradeMatcher


# Page config
st.set_page_config(
    page_title="Listing Info",
    page_icon="",
    layout="wide"
)

# Condition to PSA grade mapping for display
CONDITION_TO_PSA_RANGE = {
    'Gem Mint': 'PSA 9-10',
    'Near Mint': 'PSA 8-9',
    'Excellent': 'PSA 6-7',
    'Very Good': 'PSA 5-6',
    'Light Play': 'PSA 4-5',
    'Good': 'PSA 3-4',
    'Moderate Play': 'PSA 2-3',
    'Heavy Play': 'PSA 1-2',
    'Damaged': '< PSA 1',
}


def get_db_connection():
    """Get database connection."""
    try:
        db_url = st.secrets["DATABASE_URL"]
    except (KeyError, FileNotFoundError):
        db_url = os.getenv('DATABASE_URL')

    if not db_url:
        st.error("DATABASE_URL not found in secrets or environment variables!")
        st.stop()

    try:
        return DatabaseManager(db_url)
    except Exception as e:
        st.error("Failed to connect to database!")
        st.error(f"Error: {str(e)}")
        st.stop()


def get_grade_matcher():
    """Get GradeMatcher with latest market values."""
    db = get_db_connection()

    with db.conn.cursor() as cursor:
        cursor.execute('''
            SELECT
                psa_10_price, psa_9_price, psa_8_price, psa_7_price, psa_6_price,
                psa_5_price, psa_4_price, psa_3_price, psa_2_price, psa_1_price,
                bgs_10_price, bgs_9_5_price,
                cgc_10_pristine_price, cgc_10_price, cgc_9_5_price,
                sgc_10_price, grade_9_5_price, raw_ungraded_price
            FROM market_values
            ORDER BY recorded_at DESC
            LIMIT 1
        ''')
        row = cursor.fetchone()

        if row:
            market_values = {
                'psa_10_price': row[0], 'psa_9_price': row[1], 'psa_8_price': row[2],
                'psa_7_price': row[3], 'psa_6_price': row[4], 'psa_5_price': row[5],
                'psa_4_price': row[6], 'psa_3_price': row[7], 'psa_2_price': row[8],
                'psa_1_price': row[9], 'bgs_10_price': row[10], 'bgs_9_5_price': row[11],
                'cgc_10_pristine_price': row[12], 'cgc_10_price': row[13],
                'cgc_9_5_price': row[14], 'sgc_10_price': row[15],
                'grade_9_5_price': row[16], 'raw_ungraded_price': row[17],
            }
            return GradeMatcher(market_values)

    return None


def get_hidden_listings():
    """Get set of hidden listing item_ids from session state."""
    if 'hidden_listings' not in st.session_state:
        st.session_state.hidden_listings = set()
    return st.session_state.hidden_listings


def hide_listing(item_id):
    """Add a listing to the hidden set."""
    if 'hidden_listings' not in st.session_state:
        st.session_state.hidden_listings = set()
    st.session_state.hidden_listings.add(item_id)


def load_active_listings():
    """Load all active listings with market values, sorted by price difference."""
    db = get_db_connection()

    # Sort by (total_cost - market_value) ascending
    # Listings priced below market value will appear first
    query = """
    WITH latest_market_values AS (
        SELECT
            psa_10_price,
            psa_9_price,
            psa_8_price,
            psa_7_price,
            bgs_10_price,
            cgc_9_5_price,
            raw_ungraded_price
        FROM market_values
        ORDER BY recorded_at DESC
        LIMIT 1
    )
    SELECT
        l.item_id,
        l.title,
        l.grade,
        l.total_cost,
        l.price,
        l.shipping,
        l.condition,
        l.is_graded,
        l.raw_condition,
        l.comparable_grade,
        l.seller_username,
        l.seller_feedback,
        l.listing_type,
        l.is_auction,
        l.url,
        l.image_url,
        l.last_seen,
        CASE l.grade
            WHEN 'PSA 10' THEN mv.psa_10_price
            WHEN 'PSA 9' THEN mv.psa_9_price
            WHEN 'PSA 8' THEN mv.psa_8_price
            WHEN 'PSA 7' THEN mv.psa_7_price
            WHEN 'BGS 10' THEN mv.bgs_10_price
            WHEN 'BGS 9.5' THEN mv.cgc_9_5_price
            WHEN 'BGS 9' THEN mv.psa_9_price
            WHEN 'CGC 10' THEN mv.bgs_10_price
            WHEN 'CGC 9.5' THEN mv.cgc_9_5_price
            WHEN 'CGC 9' THEN mv.psa_9_price
            WHEN 'Raw' THEN mv.raw_ungraded_price
            WHEN 'Ungraded' THEN mv.raw_ungraded_price
            WHEN 'Unknown' THEN mv.raw_ungraded_price
            ELSE NULL
        END as market_value,
        l.total_cost - COALESCE(CASE l.grade
            WHEN 'PSA 10' THEN mv.psa_10_price
            WHEN 'PSA 9' THEN mv.psa_9_price
            WHEN 'PSA 8' THEN mv.psa_8_price
            WHEN 'PSA 7' THEN mv.psa_7_price
            WHEN 'BGS 10' THEN mv.bgs_10_price
            WHEN 'BGS 9.5' THEN mv.cgc_9_5_price
            WHEN 'BGS 9' THEN mv.psa_9_price
            WHEN 'CGC 10' THEN mv.bgs_10_price
            WHEN 'CGC 9.5' THEN mv.cgc_9_5_price
            WHEN 'CGC 9' THEN mv.psa_9_price
            WHEN 'Raw' THEN mv.raw_ungraded_price
            WHEN 'Ungraded' THEN mv.raw_ungraded_price
            WHEN 'Unknown' THEN mv.raw_ungraded_price
            ELSE NULL
        END, 0) as price_diff
    FROM listings l
    CROSS JOIN latest_market_values mv
    WHERE l.is_active = true
    ORDER BY price_diff ASC
    LIMIT 100
    """

    with db.conn.cursor() as cursor:
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()

    if not data:
        return pd.DataFrame()

    return pd.DataFrame(data, columns=columns)


def display_listing_card(listing, grade_matcher):
    """Display a single listing with facts only."""

    col1, col2 = st.columns([1, 2])

    with col1:
        if listing['image_url']:
            st.image(listing['image_url'], use_container_width=True)
        else:
            st.info("No image available")

    with col2:
        # Title
        st.subheader(listing['title'])

        st.markdown("---")

        # Listing Price
        shipping = listing.get('shipping', 0) or 0
        st.write(f"**Listing Price:** ${listing['total_cost']:,.2f}")
        if shipping > 0:
            st.write(f"(${listing['price']:,.2f} + ${shipping:.2f} shipping)")
        else:
            st.write("(Free shipping)")

        # Market Value from PriceCharting
        is_graded = listing.get('is_graded', False)
        raw_condition = listing.get('raw_condition', 'N/A')
        grade = listing.get('grade', 'Unknown')

        market_value = None
        market_label = ""

        if grade_matcher:
            if is_graded and grade and grade != 'Unknown':
                market_value = grade_matcher.get_comparable_market_value(grade)
                market_label = f"Market Value ({grade})"
            elif not is_graded and raw_condition and raw_condition not in ['N/A', 'None', None]:
                market_value = grade_matcher.get_comparable_market_value('Raw', raw_condition)
                psa_range = CONDITION_TO_PSA_RANGE.get(raw_condition, '')
                market_label = f"Market Value ({raw_condition})" if not psa_range else f"Market Value ({raw_condition} / {psa_range})"
            elif listing.get('market_value'):
                market_value = listing['market_value']
                market_label = "Market Value"

        if market_value:
            st.write(f"**{market_label}:** ${market_value:,.2f}")

        st.markdown("---")

        # Listing attributes
        if is_graded:
            st.write(f"**Grade:** {grade}")
            st.write(f"**Condition:** Graded")
        else:
            st.write(f"**Grade:** Ungraded")
            if raw_condition and raw_condition not in ['N/A', 'None', None]:
                st.write(f"**Condition:** {raw_condition}")

        st.write(f"**Seller:** {listing['seller_username']} ({listing['seller_feedback']}% feedback)")
        st.write(f"**Type:** {listing['listing_type']}")

        # eBay link and hide button
        link_col, hide_col = st.columns([3, 1])
        with link_col:
            st.markdown(f"[View on eBay]({listing['url']})")
        with hide_col:
            if st.button("Hide", key=f"hide_{listing['item_id']}"):
                hide_listing(listing['item_id'])
                st.rerun()


def main():
    """Main app."""

    st.title("Listing Info")

    # Sidebar
    st.sidebar.title("Filters")

    # Load listings (sorted by price difference: listing - market value)
    listings_df = load_active_listings()
    grade_matcher = get_grade_matcher()

    # Filter out hidden listings
    hidden = get_hidden_listings()
    if not listings_df.empty and hidden:
        listings_df = listings_df[~listings_df['item_id'].isin(hidden)]

    # Stats
    st.sidebar.markdown("---")
    st.sidebar.title("Stats")
    if not listings_df.empty:
        st.sidebar.metric("Active Listings", len(listings_df))
    else:
        st.sidebar.metric("Active Listings", 0)

    if hidden:
        st.sidebar.metric("Hidden", len(hidden))
        if st.sidebar.button("Show All Hidden"):
            st.session_state.hidden_listings = set()
            st.rerun()

    # Grade filter
    if not listings_df.empty:
        grade_filter = st.sidebar.multiselect(
            "Filter by Grade",
            options=sorted(listings_df['grade'].dropna().unique()),
            default=None
        )

        # Price range filter
        min_price = int(listings_df['total_cost'].min())
        max_price = int(listings_df['total_cost'].max())
        price_range = st.sidebar.slider(
            "Price Range",
            min_value=min_price,
            max_value=max_price,
            value=(min_price, max_price)
        )

        # Apply filters
        filtered_df = listings_df.copy()
        if grade_filter:
            filtered_df = filtered_df[filtered_df['grade'].isin(grade_filter)]
        filtered_df = filtered_df[
            (filtered_df['total_cost'] >= price_range[0]) &
            (filtered_df['total_cost'] <= price_range[1])
        ]
    else:
        filtered_df = listings_df

    # PriceCharting attribution
    st.sidebar.markdown("---")
    st.sidebar.markdown("Market values from [PriceCharting.com](https://www.pricecharting.com)")

    # Main content
    if filtered_df.empty:
        st.info("No active listings found.")
        st.write("Wait for the next tracker run (every 6 hours) to get new listings.")
    else:
        st.write(f"Showing **{len(filtered_df)}** listings")

        # Display listings
        for idx, listing in filtered_df.iterrows():
            with st.container():
                listing_dict = listing.to_dict()
                display_listing_card(listing_dict, grade_matcher)
                st.divider()
                st.write("")


if __name__ == '__main__':
    main()
