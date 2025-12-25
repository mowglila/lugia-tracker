"""
Listing Info App - Streamlit Web Interface

Run with: streamlit run listing_info_app.py
"""

import os
import streamlit as st
import pandas as pd
from datetime import datetime
from database import DatabaseManager
from grade_matcher import GradeMatcher


# Page config
st.set_page_config(
    page_title="Lugia Listing Info",
    page_icon="🎴",
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


# Qualitative feedback options
RATING_REASONS_POSITIVE = [
    "Great price / Good value",
    "Trusted seller / High feedback",
    "Excellent card centering",
    "Low population count (rare)",
    "Clear, detailed photos",
    "Good return policy",
    "Fast/free shipping",
    "Below market average"
]

RATING_REASONS_NEGATIVE = [
    "Price too high",
    "Seller feedback too low",
    "Poor quality photos",
    "Visible damage/wear",
    "Centering looks off",
    "Possibly overgraded",
    "Shipping costs too high",
    "Suspicious listing",
    "Item out of scope (wrong card/not Lugia 1st Ed)"
]

PRIORITY_FACTORS = [
    "Lowest price",
    "Seller reputation",
    "Card condition/centering",
    "Grading company preference",
    "Fast/free shipping",
    "Return policy"
]


def get_db_connection():
    """Get database connection (no cache - always fresh data)."""
    # Try Streamlit secrets first (for cloud deployment), then fall back to env var (for local)
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
        if "password authentication failed" in str(e):
            st.error("Password authentication failed. Please check your DATABASE_URL password in Streamlit secrets.")
            st.info("Get the correct connection string from Supabase: Settings -> Database -> Connection string -> URI (Transaction pooler)")
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


def load_unreviewed_listings():
    """Load listings that haven't been reviewed yet using PriceCharting market values."""
    db = get_db_connection()

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
            WHEN 'BGS 9.5' THEN mv.cgc_9_5_price  -- Use 9.5 generic as proxy
            WHEN 'BGS 9' THEN mv.psa_9_price      -- Use PSA 9 as proxy
            WHEN 'CGC 10' THEN mv.bgs_10_price    -- Use BGS 10 as proxy
            WHEN 'CGC 9.5' THEN mv.cgc_9_5_price
            WHEN 'CGC 9' THEN mv.psa_9_price      -- Use PSA 9 as proxy
            WHEN 'Raw' THEN mv.raw_ungraded_price
            WHEN 'Ungraded' THEN mv.raw_ungraded_price
            WHEN 'Unknown' THEN mv.raw_ungraded_price
            ELSE NULL
        END as avg_price,
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
        END as median_price,
        CASE
            WHEN CASE l.grade
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
            END > 0 THEN
                ROUND(((l.total_cost - CASE l.grade
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
                END) / CASE l.grade
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
                END * 100)::numeric, 1)
            ELSE NULL
        END as percent_vs_avg
    FROM listings l
    CROSS JOIN latest_market_values mv
    LEFT JOIN reviewed_listings rl ON l.item_id = rl.item_id
    WHERE l.is_active = true
        AND (rl.user_action IS NULL OR rl.id IS NULL)
    ORDER BY l.last_seen DESC
    LIMIT 50
    """

    with db.conn.cursor() as cursor:
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()

    if not data:
        return pd.DataFrame()

    return pd.DataFrame(data, columns=columns)


def save_feedback(item_id, title, grade, total_cost, avg_price, percent_vs_avg,
                  action, rating, notes, url, image_url, seller_username,
                  seller_feedback, is_auction, listing_type, condition,
                  rating_reasons, priorities, concerns, is_graded=None):
    """Save user feedback with qualitative data to reviewed_listings table."""
    db = get_db_connection()

    # Extract grading company from grade if graded
    grading_company = None
    if is_graded and grade and grade not in ['Raw', 'Unknown']:
        grade_parts = grade.split()
        grading_company = grade_parts[0] if grade_parts else None

    # Combine qualitative feedback into structured notes
    qualitative_notes = []

    # Add grade and grading company as attributes
    if grade:
        qualitative_notes.append(f"GRADE: {grade}")
    if grading_company:
        qualitative_notes.append(f"GRADING_COMPANY: {grading_company}")
    if is_graded is not None:
        qualitative_notes.append(f"IS_GRADED: {is_graded}")

    if rating_reasons:
        qualitative_notes.append(f"REASONS: {', '.join(rating_reasons)}")

    if priorities:
        qualitative_notes.append(f"PRIORITIES: {', '.join(priorities)}")

    if concerns:
        qualitative_notes.append(f"CONCERNS: {', '.join(concerns)}")

    if notes:
        qualitative_notes.append(f"NOTES: {notes}")

    combined_notes = " | ".join(qualitative_notes)

    # Insert/update reviewed listing
    insert_query = """
    INSERT INTO reviewed_listings (
        item_id, title, grade, total_cost, avg_price, percent_below_avg,
        listing_type, seller_username, seller_feedback, is_auction,
        condition, url, image_url,
        user_action, user_rating, action_timestamp, notes
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, CURRENT_TIMESTAMP, %s
    )
    ON CONFLICT (item_id)
    DO UPDATE SET
        user_action = EXCLUDED.user_action,
        user_rating = EXCLUDED.user_rating,
        action_timestamp = CURRENT_TIMESTAMP,
        notes = EXCLUDED.notes
    """

    with db.conn.cursor() as cursor:
        cursor.execute(insert_query, (
            item_id, title, grade, total_cost, avg_price, percent_vs_avg,
            listing_type, seller_username, seller_feedback, is_auction, condition,
            url, image_url,
            action, rating, combined_notes
        ))

    db.conn.commit()


def display_listing_card(listing):
    """Display a single listing for review with feedback."""

    # Create columns for layout
    col1, col2 = st.columns([1, 2])

    with col1:
        # Show image if available
        if listing['image_url']:
            st.image(listing['image_url'], use_container_width=True)
        else:
            st.info("No image available")

    with col2:
        # Title and basic info
        st.subheader(listing['title'])

        # Extract grade/condition info for proper market value display
        is_graded = listing.get('is_graded', False)
        raw_condition = listing.get('raw_condition', 'N/A')
        grade = listing.get('grade', 'Unknown')
        shipping = listing.get('shipping', 0) or 0

        # Calculate the actual market value based on grade/condition
        grade_matcher = get_grade_matcher()
        actual_market_value = None
        avg_label = ""

        if grade_matcher:
            if is_graded and grade and grade != 'Unknown':
                # For graded cards, get the value for that specific grade
                actual_market_value = grade_matcher.get_comparable_market_value(grade)
                avg_label = f"{grade} Average"
            elif not is_graded and raw_condition and raw_condition not in ['N/A', 'None', None]:
                # For raw cards, get condition-adjusted value
                actual_market_value = grade_matcher.get_comparable_market_value('Raw', raw_condition)
                psa_range = CONDITION_TO_PSA_RANGE.get(raw_condition, '')
                avg_label = f"{raw_condition} / {psa_range} Average" if psa_range else f"{raw_condition} Average"
            elif listing['avg_price']:
                # Fallback to avg_price from query
                actual_market_value = listing['avg_price']
                avg_label = "Market Average"

        # Build the single-row info display
        # Format: Price | Shipping | Graded/Ungraded | Company (if graded) | Grade (if graded) / Condition (if raw) | Average
        info_parts = []

        # Price
        info_parts.append(f"**${listing['total_cost']:,.2f}**")

        # Shipping
        if shipping > 0:
            info_parts.append(f"Shipping: ${shipping:.2f}")
        else:
            info_parts.append("Free Shipping")

        # Graded status
        if is_graded:
            info_parts.append("Graded")
            # Extract grading company and grade value
            grade_parts = grade.split()
            if len(grade_parts) >= 2:
                grading_company = grade_parts[0]
                grade_value = ' '.join(grade_parts[1:])
                info_parts.append(grading_company)
                info_parts.append(grade_value)
            else:
                info_parts.append(grade)
        else:
            info_parts.append("Ungraded")
            # Condition for raw
            if raw_condition and raw_condition not in ['N/A', 'None', None]:
                info_parts.append(raw_condition)

        # Average price
        if actual_market_value:
            info_parts.append(f"{avg_label}: ${actual_market_value:,.2f}")

        # Display as a single row
        st.write(" | ".join(info_parts))

        # Seller info on second line
        st.write(f"**Seller:** {listing['seller_username']} ({listing['seller_feedback']}% feedback) | **Type:** {listing['listing_type']}")

        # Link
        st.markdown(f"[View on eBay]({listing['url']})")

    st.divider()

    # Feedback Section
    st.subheader("Your Feedback")

    # Row 1: Action and Rating
    feedback_col1, feedback_col2 = st.columns(2)

    with feedback_col1:
        action = st.radio(
            "Action",
            options=["purchased", "watching", "interested", "maybe_later", "dismissed"],
            format_func=lambda x: {
                "purchased": "Purchased / Would Purchase",
                "watching": "Watching / Very Interested",
                "interested": "Interested",
                "maybe_later": "Maybe Later",
                "dismissed": "Not Interested"
            }[x],
            key=f"action_{listing['item_id']}",
            horizontal=False
        )

    with feedback_col2:
        rating = st.slider(
            "Listing Quality (1=Bad, 5=Excellent)",
            min_value=1,
            max_value=5,
            value=3,
            key=f"rating_{listing['item_id']}"
        )

    st.divider()

    # Row 2: Qualitative Reasons
    st.subheader("Why this rating?")

    qual_col1, qual_col2 = st.columns(2)

    with qual_col1:
        st.write("**Positive factors:**")
        rating_reasons_positive = st.multiselect(
            "Select all that apply",
            options=RATING_REASONS_POSITIVE,
            key=f"reasons_pos_{listing['item_id']}",
            label_visibility="collapsed"
        )

    with qual_col2:
        st.write("**Negative factors:**")
        rating_reasons_negative = st.multiselect(
            "Select all that apply",
            options=RATING_REASONS_NEGATIVE,
            key=f"reasons_neg_{listing['item_id']}",
            label_visibility="collapsed"
        )

    rating_reasons = rating_reasons_positive + rating_reasons_negative

    st.divider()

    # Additional Notes
    priorities = []  # Keep for compatibility with save_feedback
    concerns = []    # Keep for compatibility with save_feedback

    notes = st.text_area(
        "Additional Notes",
        placeholder="Any other thoughts? Specific observations?",
        key=f"notes_{listing['item_id']}"
    )

    # Submit button
    st.divider()

    # Show summary before submitting
    with st.expander("Review Summary", expanded=False):
        st.write(f"**Action:** {action}")
        st.write(f"**Rating:** {rating}/5 stars")
        if rating_reasons:
            st.write(f"**Reasons:** {', '.join(rating_reasons)}")
        if notes:
            st.write(f"**Notes:** {notes}")

    if st.button("Save Feedback", key=f"submit_{listing['item_id']}", type="primary", use_container_width=True):
        save_feedback(
            item_id=listing['item_id'],
            title=listing['title'],
            grade=listing['grade'],
            total_cost=listing['total_cost'],
            avg_price=listing['avg_price'],
            percent_vs_avg=listing['percent_vs_avg'],
            action=action,
            rating=rating,
            notes=notes,
            url=listing['url'],
            image_url=listing['image_url'],
            seller_username=listing['seller_username'],
            seller_feedback=listing['seller_feedback'],
            is_auction=listing['is_auction'],
            listing_type=listing['listing_type'],
            condition=listing['condition'],
            rating_reasons=rating_reasons,
            priorities=priorities,
            concerns=concerns,
            is_graded=listing.get('is_graded', None)
        )
        st.success("Feedback saved successfully!")
        st.balloons()
        st.rerun()


def main():
    """Main app."""

    st.title("Lugia Listing Info")

    # Sidebar stats
    st.sidebar.title("Stats")

    db = get_db_connection()

    # Get counts
    with db.conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM listings WHERE is_active = true")
        total_active = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM reviewed_listings WHERE user_action IS NOT NULL")
        total_reviewed = cursor.fetchone()[0]

    st.sidebar.metric("Active Listings", total_active)
    st.sidebar.metric("Listings Reviewed", total_reviewed)

    # Main content
    st.header("Review Listings")

    # Load unreviewed listings
    listings_df = load_unreviewed_listings()

    if listings_df.empty:
        st.info("No unreviewed listings! All caught up.")
        st.write("Wait for the next tracker run (every 6 hours) to get new listings.")
    else:
        st.write(f"**{len(listings_df)} listings** waiting for review")

        # Filter options
        with st.expander("Filters"):
            filter_col1, filter_col2 = st.columns(2)

            with filter_col1:
                grade_filter = st.multiselect(
                    "Filter by Grade",
                    options=listings_df['grade'].unique(),
                    default=None
                )

            with filter_col2:
                price_range = st.slider(
                    "Price Range",
                    min_value=0,
                    max_value=int(listings_df['total_cost'].max()),
                    value=(0, int(listings_df['total_cost'].max()))
                )

        # Apply filters
        filtered_df = listings_df.copy()
        if grade_filter:
            filtered_df = filtered_df[filtered_df['grade'].isin(grade_filter)]
        filtered_df = filtered_df[
            (filtered_df['total_cost'] >= price_range[0]) &
            (filtered_df['total_cost'] <= price_range[1])
        ]

        st.write(f"Showing **{len(filtered_df)}** listings")

        # Display listings one by one
        if not filtered_df.empty:
            for idx, listing in filtered_df.iterrows():
                with st.container():
                    # Convert Series to dict to handle None values properly
                    listing_dict = listing.to_dict()
                    display_listing_card(listing_dict)
                    st.divider()
                    st.write("")  # Spacing


if __name__ == '__main__':
    main()
