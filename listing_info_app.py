"""
Card Tracker - Streamlit Web Interface

Displays eBay listing information alongside PriceCharting market data.
Facts only - no recommendations, no deal scores.

Data sources are visually separated per eBay API License Agreement 8.1(b)(2).

Run with: streamlit run listing_info_app.py
"""

import os
import re
import streamlit as st
import pandas as pd
from database import DatabaseManager
from grade_matcher import GradeMatcher


# Page config
st.set_page_config(
    page_title="Card Tracker",
    page_icon="",
    layout="wide"
)

# Condition to PSA grade equivalent mapping
# Used for estimating market value of raw cards based on condition
CONDITION_TO_GRADE = {
    'Gem Mint': 10,
    'Mint': 9,
    'Near Mint': 9,
    'Near mint or better': 9,
    'Excellent': 7,
    'Very Good': 5,
    'Lightly Played': 7,
    'Light Play': 7,
    'Lightly played (Excellent)': 7,
    'Good': 5,
    'Moderate Play': 5,
    'Moderately Played': 5,
    'Heavy Play': 3,
    'Heavily Played': 3,
    'Poor': 1,
    'Damaged': 1,
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
                psa_10_price, psa_9_price, psa_8_price, psa_7_price,
                bgs_10_price, cgc_10_price, cgc_9_5_price, cgc_9_price,
                raw_ungraded_price
            FROM market_values
            ORDER BY recorded_at DESC
            LIMIT 1
        ''')
        row = cursor.fetchone()

        if row:
            market_values = {
                'psa_10_price': row[0], 'psa_9_price': row[1], 'psa_8_price': row[2],
                'psa_7_price': row[3], 'bgs_10_price': row[4], 'cgc_10_price': row[5],
                'cgc_9_5_price': row[6], 'cgc_9_price': row[7], 'raw_ungraded_price': row[8],
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


def load_lugia_listings(db):
    """
    Load Lugia listings from the listings table.
    Rigid match for Lugia #249 via direct product lookup.
    """
    query = """
    SELECT
        l.item_id,
        l.title as card_name,
        l.grade,
        l.total_cost as listing_price,
        l.price,
        l.shipping,
        l.condition,
        l.is_graded,
        l.raw_condition,
        l.url,
        l.image_url,
        l.seller_feedback,
        'lugia' as interest
    FROM listings l
    WHERE l.is_active = true
      AND l.seller_feedback >= 50
      AND l.title NOT ILIKE '%%Choose Your Card%%'
      AND l.title NOT ILIKE '%%Choose Your%%'
      AND l.title NOT ILIKE '%%Pick Your Card%%'
      AND l.title NOT ILIKE '%%Pick Your%%'
      AND l.title NOT ILIKE '%%You Choose%%'
      AND l.title NOT ILIKE '%%U Pick%%'
      AND l.title NOT ILIKE '%%You Pick%%'
    ORDER BY l.total_cost ASC
    """
    try:
        with db.conn.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
        if data:
            return pd.DataFrame(data, columns=columns)
    except Exception as e:
        st.error(f"Error loading Lugia listings: {e}")
    return pd.DataFrame()


def load_big_mover_listings(db):
    """
    Load eBay listings for cards in big_movers.
    Fuzzy match on card_name + set_name from discovered_listings.
    Uses structured card metadata from eBay getItem API.
    """
    query = """
    SELECT
        d.item_id,
        d.card_name,
        d.set_name,
        d.card_number,
        d.grade,
        d.grading_company,
        d.price as listing_price,
        d.price,
        0 as shipping,
        d.condition,
        COALESCE((d.variant_attributes->>'is_graded')::boolean, false) as is_graded,
        d.variant_attributes->>'condition' as raw_condition,
        d.url,
        d.image_url,
        d.variant_attributes,
        d.seller_feedback,
        d.discovered_at,
        'mover' as interest
    FROM discovered_listings d
    INNER JOIN (
        SELECT card_name, set_name, loose_price, volume_7d
        FROM big_movers
        WHERE captured_date = (SELECT MAX(captured_date) FROM big_movers)
    ) b ON (
        d.card_name ILIKE '%%' || REGEXP_REPLACE(b.card_name, ' #.*$', '') || '%%'
        AND d.set_name ILIKE '%%' || REGEXP_REPLACE(b.set_name, '^Pokemon ', '') || '%%'
    )
    WHERE d.is_active = true
      AND d.card_name IS NOT NULL
      AND d.card_name != ''
      AND d.seller_feedback >= 50
      AND (d.is_multi_variation = FALSE OR d.is_multi_variation IS NULL)
      AND SPLIT_PART(d.item_id, '|', 3) = '0'
      AND d.title NOT ILIKE '%%Choose Your Card%%'
      AND d.title NOT ILIKE '%%Choose Your%%'
      AND d.title NOT ILIKE '%%Pick Your Card%%'
      AND d.title NOT ILIKE '%%Pick Your%%'
      AND d.title NOT ILIKE '%%You Choose%%'
      AND d.title NOT ILIKE '%%U Pick%%'
      AND d.title NOT ILIKE '%%You Pick%%'
    ORDER BY b.volume_7d DESC, d.price ASC
    """
    try:
        with db.conn.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
        if data:
            return pd.DataFrame(data, columns=columns)
    except Exception as e:
        st.error(f"Error loading Big Mover listings: {e}")
    return pd.DataFrame()


def load_wishlist_listings(db):
    """
    Load eBay listings for cards in wishlist_demand.
    Fuzzy match on card_name + set_name from discovered_listings.
    Uses structured card metadata from eBay getItem API.
    """
    query = """
    SELECT
        d.item_id,
        d.card_name,
        d.set_name,
        d.card_number,
        d.grade,
        d.grading_company,
        d.price as listing_price,
        d.price,
        0 as shipping,
        d.condition,
        COALESCE((d.variant_attributes->>'is_graded')::boolean, false) as is_graded,
        d.variant_attributes->>'condition' as raw_condition,
        d.url,
        d.image_url,
        d.variant_attributes,
        d.seller_feedback,
        d.discovered_at,
        'demand' as interest
    FROM discovered_listings d
    INNER JOIN (
        SELECT card_name, set_name, ungraded_price, wishlist_count
        FROM wishlist_demand
        WHERE captured_date = (SELECT MAX(captured_date) FROM wishlist_demand)
    ) w ON (
        d.card_name ILIKE '%%' || REGEXP_REPLACE(w.card_name, ' #.*$', '') || '%%'
        AND d.set_name ILIKE '%%' || REGEXP_REPLACE(w.set_name, '^Pokemon ', '') || '%%'
    )
    WHERE d.is_active = true
      AND d.card_name IS NOT NULL
      AND d.card_name != ''
      AND d.seller_feedback >= 50
      AND (d.is_multi_variation = FALSE OR d.is_multi_variation IS NULL)
      AND SPLIT_PART(d.item_id, '|', 3) = '0'
      AND d.title NOT ILIKE '%%Choose Your Card%%'
      AND d.title NOT ILIKE '%%Choose Your%%'
      AND d.title NOT ILIKE '%%Pick Your Card%%'
      AND d.title NOT ILIKE '%%Pick Your%%'
      AND d.title NOT ILIKE '%%You Choose%%'
      AND d.title NOT ILIKE '%%U Pick%%'
      AND d.title NOT ILIKE '%%You Pick%%'
    ORDER BY w.wishlist_count DESC, d.price ASC
    """
    try:
        with db.conn.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
        if data:
            return pd.DataFrame(data, columns=columns)
    except Exception as e:
        st.error(f"Error loading Wishlist listings: {e}")
    return pd.DataFrame()


def load_highend_listings(db):
    """
    Load eBay listings for high-end cards from card_market_candidates.
    Filters for cards with PSA 10 price >= $300.
    Fuzzy match on card_name + set_name from discovered_listings.
    """
    query = """
    SELECT
        d.item_id,
        d.card_name,
        d.set_name,
        d.card_number,
        d.grade,
        d.grading_company,
        d.price as listing_price,
        d.price,
        0 as shipping,
        d.condition,
        COALESCE((d.variant_attributes->>'is_graded')::boolean, false) as is_graded,
        d.variant_attributes->>'condition' as raw_condition,
        d.url,
        d.image_url,
        d.variant_attributes,
        d.seller_feedback,
        d.discovered_at,
        'highend' as interest
    FROM discovered_listings d
    INNER JOIN (
        SELECT card_name, set_name, card_number, psa_10_price
        FROM card_market_candidates
        WHERE is_active = true
          AND psa_10_price >= 300
    ) c ON (
        d.card_name ILIKE '%%' || c.card_name || '%%'
        AND d.set_name ILIKE '%%' || REGEXP_REPLACE(c.set_name, '^Pokemon ', '') || '%%'
    )
    WHERE d.is_active = true
      AND d.card_name IS NOT NULL
      AND d.card_name != ''
      AND d.seller_feedback >= 50
      AND (d.is_multi_variation = FALSE OR d.is_multi_variation IS NULL)
      AND SPLIT_PART(d.item_id, '|', 3) = '0'
      AND d.title NOT ILIKE '%%Choose Your Card%%'
      AND d.title NOT ILIKE '%%Choose Your%%'
      AND d.title NOT ILIKE '%%Pick Your Card%%'
      AND d.title NOT ILIKE '%%Pick Your%%'
      AND d.title NOT ILIKE '%%You Choose%%'
      AND d.title NOT ILIKE '%%U Pick%%'
      AND d.title NOT ILIKE '%%You Pick%%'
    ORDER BY c.psa_10_price DESC, d.price ASC
    """
    try:
        with db.conn.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
        if data:
            return pd.DataFrame(data, columns=columns)
    except Exception as e:
        st.error(f"Error loading High-End listings: {e}")
    return pd.DataFrame()


def extract_pokemon_name(title):
    """
    Extract Pokemon name from eBay listing title.
    E.g., "Pokemon Lugia Neo Genesis 1st Edition #9 PSA 9 Mint" -> "Lugia"
    """
    if not title:
        return None

    # Remove common prefixes/suffixes
    title = re.sub(r'^(Pokemon|Pokémon)\s+', '', title, flags=re.IGNORECASE)

    # Common Pokemon names to look for
    pokemon_names = [
        'Charizard', 'Pikachu', 'Mewtwo', 'Mew', 'Lugia', 'Ho-Oh', 'Blastoise',
        'Venusaur', 'Gengar', 'Dragonite', 'Gyarados', 'Alakazam', 'Machamp',
        'Eevee', 'Umbreon', 'Espeon', 'Rayquaza', 'Groudon', 'Kyogre',
        'Celebi', 'Jirachi', 'Deoxys', 'Arceus', 'Dialga', 'Palkia', 'Giratina',
        'Victini', 'Zekrom', 'Reshiram', 'Kyurem', 'Xerneas', 'Yveltal',
        'Solgaleo', 'Lunala', 'Necrozma', 'Zacian', 'Zamazenta', 'Eternatus',
    ]

    # Check for known Pokemon names
    title_lower = title.lower()
    for name in pokemon_names:
        if name.lower() in title_lower:
            return name

    # Fallback: take first word after removing Pokemon
    words = title.split()
    if words:
        # Skip common prefixes
        skip_words = ['1st', 'edition', 'holo', 'psa', 'bgs', 'cgc', 'sgc', '#', 'neo', 'base', 'set']
        for word in words:
            clean_word = re.sub(r'[^a-zA-Z]', '', word)
            if clean_word and clean_word.lower() not in skip_words and len(clean_word) > 2:
                return clean_word

    return None


def get_pricecharting_info(db, card_name, card_number=None, set_name=None):
    """
    Get PriceCharting info for a card including all available grade prices.
    Uses card_number and set_name for precise matching when available,
    falls back to fuzzy name matching otherwise.

    Matching priority:
    1. card_number + set_name (most precise - both identifiers together)
    2. card_name + card_number (validation)
    3. card_name + set_name (fallback with name)
    4. card_name only (least precise, may match wrong card)

    Returns dict with: product_name, psa_10_price, bgs_10_price, sgc_10_price,
                       raw_price, graded_price, wishlist_count, volume_7d
    """
    result = {
        'product_name': None,       # Card name from PriceCharting
        'console_name': None,       # Set name from PriceCharting
        # All grade prices from PriceCharting
        'raw_price': None,          # loose_price - Raw/Ungraded
        'grade_1_price': None,      # condition-9-price - Grade 1
        'grade_2_price': None,      # condition-10-price - Grade 2
        'grade_3_price': None,      # condition-13-price - Grade 3
        'grade_7_price': None,      # cib-price - Grade 7/7.5
        'grade_8_price': None,      # new-price - Grade 8/8.5
        'grade_9_price': None,      # graded-price - Grade 9
        'grade_9_5_price': None,    # box-only-price - Grade 9.5
        'psa_10_price': None,       # manual-only-price - PSA 10
        'bgs_10_price': None,       # bgs-10-price - BGS 10
        'cgc_10_price': None,       # condition-17-price - CGC 10
        'sgc_10_price': None,       # condition-18-price - SGC 10
        'wishlist_count': None,
        'volume_7d': None,
    }

    if not card_name:
        return result

    # Query to get all price columns from pricecharting_raw
    price_query = """
        SELECT product_name, console_name,
               loose_price, grade_1_price, grade_2_price, grade_3_price,
               grade_7_price, grade_8_price, grade_9_price, grade_9_5_price,
               psa_10_price, bgs_10_price, cgc_10_price, sgc_10_price,
               sales_volume
        FROM pricecharting_raw
        WHERE {where_clause}
          AND import_date = (SELECT MAX(import_date) FROM pricecharting_raw)
        ORDER BY sales_volume DESC NULLS LAST
        LIMIT 1
    """

    def execute_price_query(where_clause, params):
        try:
            with db.conn.cursor() as cursor:
                cursor.execute(price_query.format(where_clause=where_clause), params)
                return cursor.fetchone()
        except Exception:
            return None

    row = None

    # Helper to clean card number - handles formats like "005/025", "#005", "005"
    def clean_card_number(num):
        if not num:
            return None
        # Remove leading # and extract number before / (e.g., "005/025" -> "005")
        num = num.lstrip('#')
        if '/' in num:
            num = num.split('/')[0]
        # Strip leading zeros for matching (e.g., "005" -> "5")
        num_stripped = num.lstrip('0') or '0'
        return num_stripped

    # Priority 1: Match on card_name + card_number + set_name (most precise)
    if card_number and set_name:
        clean_num = clean_card_number(card_number)
        clean_set = set_name.replace('Pokemon ', '').replace('Pokémon ', '').strip()
        row = execute_price_query(
            "product_name ILIKE %s AND (product_name ILIKE %s OR product_name ILIKE %s) AND console_name ILIKE %s",
            (f'%{card_name}%', f'%#{clean_num}%', f'%#{clean_num} %', f'%{clean_set}%')
        )

    # Priority 2: Match on card_name + card_number (validation)
    if row is None and card_number:
        clean_num = clean_card_number(card_number)
        row = execute_price_query(
            "product_name ILIKE %s AND (product_name ILIKE %s OR product_name ILIKE %s)",
            (f'%{card_name}%', f'%#{clean_num}%', f'%#{clean_num} %')
        )

    # Priority 3: Match on card_name + set_name
    if row is None and set_name:
        clean_set = set_name.replace('Pokemon ', '').replace('Pokémon ', '').strip()
        row = execute_price_query(
            "product_name ILIKE %s AND console_name ILIKE %s",
            (f'%{card_name}%', f'%{clean_set}%')
        )

    # Priority 4: Fallback to card name only (least precise, only for specific card types)
    if row is None:
        specific_terms = ['VMAX', 'VMax', 'GX', 'EX', 'ex', 'V ', ' V', 'VSTAR', 'Radiant',
                          'Full Art', 'Illustration Rare', 'Gold', 'Rainbow']
        is_specific = any(term in card_name for term in specific_terms)
        if is_specific:
            row = execute_price_query(
                "product_name ILIKE %s",
                (f'%{card_name}%',)
            )

    # Extract prices from matched row
    if row:
        result['product_name'] = row[0]
        result['console_name'] = row[1]
        result['raw_price'] = row[2]
        result['grade_1_price'] = row[3]
        result['grade_2_price'] = row[4]
        result['grade_3_price'] = row[5]
        result['grade_7_price'] = row[6]
        result['grade_8_price'] = row[7]
        result['grade_9_price'] = row[8]
        result['grade_9_5_price'] = row[9]
        result['psa_10_price'] = row[10]
        result['bgs_10_price'] = row[11]
        result['cgc_10_price'] = row[12]
        result['sgc_10_price'] = row[13]
        result['volume_7d'] = row[14]

    # Get wishlist count - use precise matching when possible
    try:
        with db.conn.cursor() as cursor:
            if card_number:
                cursor.execute("""
                    SELECT wishlist_count
                    FROM wishlist_demand
                    WHERE captured_date = (SELECT MAX(captured_date) FROM wishlist_demand)
                      AND card_name ILIKE %s
                      AND card_name ILIKE %s
                    LIMIT 1
                """, (f'%{card_name}%', f'%{card_number}%'))
                row = cursor.fetchone()
                if row:
                    result['wishlist_count'] = row[0]

            if result['wishlist_count'] is None:
                cursor.execute("""
                    SELECT wishlist_count
                    FROM wishlist_demand
                    WHERE captured_date = (SELECT MAX(captured_date) FROM wishlist_demand)
                      AND card_name ILIKE %s
                    LIMIT 1
                """, (f'%{card_name}%',))
                row = cursor.fetchone()
                if row:
                    result['wishlist_count'] = row[0]
    except Exception:
        pass

    # Get 7-day volume from big_movers if not already found
    if result['volume_7d'] is None:
        try:
            with db.conn.cursor() as cursor:
                if card_number:
                    cursor.execute("""
                        SELECT volume_7d
                        FROM big_movers
                        WHERE captured_date = (SELECT MAX(captured_date) FROM big_movers)
                          AND card_name ILIKE %s
                          AND card_name ILIKE %s
                        LIMIT 1
                    """, (f'%{card_name}%', f'%{card_number}%'))
                    row = cursor.fetchone()
                    if row:
                        result['volume_7d'] = row[0]

                if result['volume_7d'] is None:
                    cursor.execute("""
                        SELECT volume_7d
                        FROM big_movers
                        WHERE captured_date = (SELECT MAX(captured_date) FROM big_movers)
                          AND card_name ILIKE %s
                        LIMIT 1
                    """, (f'%{card_name}%',))
                    row = cursor.fetchone()
                    if row:
                        result['volume_7d'] = row[0]
        except Exception:
            pass

    return result


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
      AND l.seller_feedback >= 50
      AND l.title NOT ILIKE '%%Choose Your Card%%'
      AND l.title NOT ILIKE '%%Choose Your%%'
      AND l.title NOT ILIKE '%%Pick Your Card%%'
      AND l.title NOT ILIKE '%%Pick Your%%'
      AND l.title NOT ILIKE '%%You Choose%%'
      AND l.title NOT ILIKE '%%U Pick%%'
      AND l.title NOT ILIKE '%%You Pick%%'
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


def display_listing_card(listing, grade_matcher, db):
    """
    Display a single listing with eBay and PriceCharting sections visually separated.
    Per eBay API License Agreement 8.1(b)(2): eBay content must be visually isolated.
    """
    col1, col2 = st.columns([1, 2])

    with col1:
        if listing.get('image_url'):
            st.image(listing['image_url'], use_container_width=True)
        else:
            st.info("No image available")

    with col2:
        # ═══════════════════════════════════════════════════════
        # eBay Listing Section
        # ═══════════════════════════════════════════════════════
        st.markdown("### eBay Listing")

        # Card Name - use structured card_name from eBay API if available
        card_name = listing.get('card_name') or listing.get('title', 'Unknown')
        set_name = listing.get('set_name', '')
        card_number = listing.get('card_number', '')

        # Display card info as separate fields
        st.write(f"**Card Name:** {card_name}")
        if card_number:
            st.write(f"**Card Number:** {card_number}")
        if set_name:
            st.write(f"**Set Name:** {set_name}")

        # Seller feedback info (without disclosing seller name)
        seller_feedback = listing.get('seller_feedback')
        if seller_feedback is not None:
            st.write(f"**Seller Feedback:** {seller_feedback}%")

        # Listing Price
        listing_price = listing.get('listing_price') or listing.get('total_cost') or 0
        st.write(f"**Listing Price:** ${listing_price:,.2f}")

        # Get variant attributes
        variant_attrs = listing.get('variant_attributes', {})
        if isinstance(variant_attrs, str):
            import json
            try:
                variant_attrs = json.loads(variant_attrs)
            except (json.JSONDecodeError, TypeError):
                variant_attrs = {}

        # Determine if graded - check multiple sources
        # 1. is_graded field from query
        # 2. variant_attributes.is_graded
        # 3. condition == 'Graded'
        # 4. grade column has a value
        is_graded_field = listing.get('is_graded', False)
        va_is_graded = variant_attrs.get('is_graded', False) if variant_attrs else False
        condition_field = listing.get('condition', '')
        grade_value = listing.get('grade') or (variant_attrs.get('grade') if variant_attrs else None)

        is_graded = is_graded_field or va_is_graded or condition_field == 'Graded' or (grade_value is not None and str(grade_value).replace('.', '').isdigit())

        # Get grading company from multiple sources
        grading_company = listing.get('grading_company') or (variant_attrs.get('grading_company') if variant_attrs else None)
        # Normalize grading company names
        if grading_company:
            if 'PSA' in grading_company or 'Professional Sports Authenticator' in grading_company:
                grading_company = 'PSA'
            elif 'CGC' in grading_company or 'Certified Guaranty' in grading_company:
                grading_company = 'CGC'
            elif 'BGS' in grading_company or 'Beckett' in grading_company:
                grading_company = 'BGS'
            elif 'SGC' in grading_company:
                grading_company = 'SGC'

        # Get condition for raw cards
        raw_condition = listing.get('raw_condition', '')
        detailed_condition = variant_attrs.get('detailed_condition') if variant_attrs else None

        # Filter out non-condition values (these are grading statuses, not actual conditions)
        invalid_conditions = ['Graded', 'Ungraded', 'Used', 'N/A', 'None', '', None,
                              'Nicht bewertet', 'Valutata', 'Clasificada', 'Non gradata', 'Bewertet', 'Usado']

        # Priority: condition_field (d.condition) > detailed_condition > raw_condition
        actual_condition = None
        if condition_field and condition_field not in invalid_conditions:
            actual_condition = condition_field
        elif detailed_condition and detailed_condition not in invalid_conditions:
            actual_condition = detailed_condition
        elif raw_condition and raw_condition not in invalid_conditions:
            actual_condition = raw_condition

        # Display graded vs ungraded info
        if is_graded:
            # Show grading company
            st.write(f"**Grading Company:** {grading_company if grading_company else 'Unknown'}")
            # Show grade
            if grade_value:
                st.write(f"**Grade:** {grade_value}")
            else:
                st.write(f"**Grade:** Unknown")
        else:
            st.write("**Grading Company:** N/A (Ungraded)")
            st.write(f"**Condition:** {actual_condition if actual_condition else 'Not specified'}")

        # Listing discovery time
        discovered_at = listing.get('discovered_at')
        if discovered_at:
            # Format the timestamp nicely
            if hasattr(discovered_at, 'strftime'):
                st.write(f"**Listed:** {discovered_at.strftime('%b %d, %Y at %I:%M %p')}")
            else:
                st.write(f"**Listed:** {discovered_at}")

        # View on eBay and Hide buttons
        link_col, hide_col = st.columns([3, 1])
        with link_col:
            if listing.get('url'):
                st.markdown(f"[View on eBay]({listing['url']})")
        with hide_col:
            if st.button("Hide", key=f"hide_{listing['item_id']}"):
                hide_listing(listing['item_id'])
                st.rerun()

        st.write("")  # Spacing

        # ═══════════════════════════════════════════════════════
        # PriceCharting Information Section
        # ═══════════════════════════════════════════════════════
        st.markdown("### PriceCharting Information")

        # Get PriceCharting info for THIS SPECIFIC CARD
        # Returns dict with product_name, prices, wishlist, volume
        pc_info = get_pricecharting_info(
            db, card_name, card_number=card_number, set_name=set_name
        )

        # Show PriceCharting card info if found
        if pc_info['product_name']:
            st.write(f"**Card Name:** {pc_info['product_name']}")
            # Extract card number from product_name (e.g., "Pikachu #5" -> "5")
            pc_card_number_match = re.search(r'#(\d+)', pc_info['product_name'])
            if pc_card_number_match:
                st.write(f"**Card Number:** {pc_card_number_match.group(1)}")
        if pc_info['console_name']:
            st.write(f"**Card Set:** {pc_info['console_name']}")

        # 1. Wishlist
        if pc_info['wishlist_count']:
            st.write(f"**Wishlist:** {pc_info['wishlist_count']:,}")
        else:
            st.write("**Wishlist:** N/A")

        # 2. 7-Day Volume
        if pc_info['volume_7d']:
            st.write(f"**7-Day Volume:** {pc_info['volume_7d']:,}")
        else:
            st.write("**7-Day Volume:** N/A")

        # 3. Value field - show the relevant price based on listing grade
        market_value = None
        market_label = "Market Value"
        show_value_field = False

        if is_graded and grade_value:
            show_value_field = True
            grade_str = str(grade_value).replace('.0', '')  # Normalize "10.0" to "10"

            # Grade 10 - company-specific pricing
            if grade_str == '10':
                if grading_company == 'PSA':
                    market_value = pc_info['psa_10_price']
                    market_label = "PSA 10 Value"
                elif grading_company == 'BGS':
                    market_value = pc_info['bgs_10_price']
                    market_label = "BGS 10 Value"
                elif grading_company == 'CGC':
                    market_value = pc_info['cgc_10_price']
                    market_label = "CGC 10 Value"
                elif grading_company == 'SGC':
                    market_value = pc_info['sgc_10_price']
                    market_label = "SGC 10 Value"
                else:
                    # Default to PSA 10 if grading company unknown
                    market_value = pc_info['psa_10_price']
                    market_label = "PSA 10 Value"
            # Grade 9.5
            elif grade_str in ['9.5', '9 5']:
                market_value = pc_info['grade_9_5_price']
                market_label = f"Grade 9.5 Value"
            # Grade 9
            elif grade_str == '9':
                market_value = pc_info['grade_9_price']
                market_label = f"Grade 9 Value"
            # Grade 8/8.5
            elif grade_str in ['8', '8.5', '8 5']:
                market_value = pc_info['grade_8_price']
                market_label = f"Grade 8 Value"
            # Grade 7/7.5
            elif grade_str in ['7', '7.5', '7 5']:
                market_value = pc_info['grade_7_price']
                market_label = f"Grade 7 Value"
            # Grade 3 and below
            elif grade_str == '3':
                market_value = pc_info['grade_3_price']
                market_label = f"Grade 3 Value"
            elif grade_str == '2':
                market_value = pc_info['grade_2_price']
                market_label = f"Grade 2 Value"
            elif grade_str == '1':
                market_value = pc_info['grade_1_price']
                market_label = f"Grade 1 Value"
            else:
                # For unlisted grades (4, 5, 6), find nearest available
                try:
                    grade_num = float(grade_str)
                    if grade_num >= 9:
                        market_value = pc_info['grade_9_price']
                    elif grade_num >= 7:
                        market_value = pc_info['grade_7_price']
                    elif grade_num >= 3:
                        market_value = pc_info['grade_3_price']
                    else:
                        market_value = pc_info['grade_1_price']
                    market_label = f"Grade {grade_str} Value (est)"
                except ValueError:
                    market_label = f"Grade {grade_str} Value"
        elif not is_graded:
            show_value_field = True
            # Use condition to determine equivalent grade value
            equivalent_grade = CONDITION_TO_GRADE.get(actual_condition, None)
            if equivalent_grade:
                # Map condition to appropriate grade price
                # PriceCharting has: 1, 2, 3, 7, 8, 9, 9.5, 10 (PSA/BGS/CGC/SGC)
                if equivalent_grade >= 10:
                    market_value = pc_info['psa_10_price']
                    market_label = f"~Grade 10 Value ({actual_condition})"
                elif equivalent_grade >= 9:
                    market_value = pc_info['grade_9_price']
                    market_label = f"~Grade 9 Value ({actual_condition})"
                elif equivalent_grade >= 7:
                    market_value = pc_info['grade_7_price']
                    market_label = f"~Grade 7 Value ({actual_condition})"
                elif equivalent_grade >= 5:
                    # No grade 5 in PriceCharting, use grade 3 as closest lower
                    market_value = pc_info['grade_3_price']
                    market_label = f"~Grade 5 Value ({actual_condition})"
                elif equivalent_grade >= 3:
                    market_value = pc_info['grade_3_price']
                    market_label = f"~Grade 3 Value ({actual_condition})"
                else:
                    market_value = pc_info['grade_1_price']
                    market_label = f"~Grade 1 Value ({actual_condition})"
            else:
                # Fall back to raw price if no condition mapping
                market_value = pc_info['raw_price']
                market_label = "Raw Value"

        if show_value_field:
            if market_value:
                st.write(f"**{market_label}:** ${market_value:,.2f}")
            else:
                st.write(f"**{market_label}:** N/A")

        # 4. Grade Price Table - use prices from the MATCHED CARD
        st.write("")
        st.write("**Grade Prices:**")

        # Build table from the matched card's prices (ordered from highest to lowest grade)
        grade_data = []

        # Gem grades (10s)
        if pc_info['psa_10_price']:
            grade_data.append({"Grade": "PSA 10", "Price": f"${pc_info['psa_10_price']:,.2f}"})
        if pc_info['bgs_10_price']:
            grade_data.append({"Grade": "BGS 10", "Price": f"${pc_info['bgs_10_price']:,.2f}"})
        if pc_info['cgc_10_price']:
            grade_data.append({"Grade": "CGC 10", "Price": f"${pc_info['cgc_10_price']:,.2f}"})
        if pc_info['sgc_10_price']:
            grade_data.append({"Grade": "SGC 10", "Price": f"${pc_info['sgc_10_price']:,.2f}"})

        # High grades (9-9.5)
        if pc_info['grade_9_5_price']:
            grade_data.append({"Grade": "Grade 9.5", "Price": f"${pc_info['grade_9_5_price']:,.2f}"})
        if pc_info['grade_9_price']:
            grade_data.append({"Grade": "Grade 9", "Price": f"${pc_info['grade_9_price']:,.2f}"})

        # Mid grades (7-8)
        if pc_info['grade_8_price']:
            grade_data.append({"Grade": "Grade 8/8.5", "Price": f"${pc_info['grade_8_price']:,.2f}"})
        if pc_info['grade_7_price']:
            grade_data.append({"Grade": "Grade 7/7.5", "Price": f"${pc_info['grade_7_price']:,.2f}"})

        # Low grades (1-3)
        if pc_info['grade_3_price']:
            grade_data.append({"Grade": "Grade 3", "Price": f"${pc_info['grade_3_price']:,.2f}"})
        if pc_info['grade_2_price']:
            grade_data.append({"Grade": "Grade 2", "Price": f"${pc_info['grade_2_price']:,.2f}"})
        if pc_info['grade_1_price']:
            grade_data.append({"Grade": "Grade 1", "Price": f"${pc_info['grade_1_price']:,.2f}"})

        # Raw/Ungraded
        if pc_info['raw_price']:
            grade_data.append({"Grade": "Raw/Ungraded", "Price": f"${pc_info['raw_price']:,.2f}"})

        if grade_data:
            grade_df = pd.DataFrame(grade_data)
            st.dataframe(grade_df, hide_index=True, use_container_width=True)
        else:
            st.write("No price data available")


def main():
    """Main app."""
    st.title("Card Tracker")

    db = get_db_connection()
    grade_matcher = get_grade_matcher()

    # Sidebar - Filters
    st.sidebar.title("Filters")

    # Interest filter
    interest_options = ["Lugia", "Demand", "Mover", "High-End"]
    interest_filter = st.sidebar.multiselect(
        "Interest",
        options=interest_options,
        default=["High-End"]
    )

    # Load data based on interest selection
    all_listings = pd.DataFrame()

    if "Lugia" in interest_filter:
        lugia_df = load_lugia_listings(db)
        if not lugia_df.empty:
            all_listings = pd.concat([all_listings, lugia_df], ignore_index=True)

    if "Mover" in interest_filter:
        big_mover_df = load_big_mover_listings(db)
        if not big_mover_df.empty:
            all_listings = pd.concat([all_listings, big_mover_df], ignore_index=True)

    if "Demand" in interest_filter:
        wishlist_df = load_wishlist_listings(db)
        if not wishlist_df.empty:
            all_listings = pd.concat([all_listings, wishlist_df], ignore_index=True)

    if "High-End" in interest_filter:
        highend_df = load_highend_listings(db)
        if not highend_df.empty:
            all_listings = pd.concat([all_listings, highend_df], ignore_index=True)

    # Filter out hidden listings
    hidden = get_hidden_listings()
    if not all_listings.empty and hidden:
        all_listings = all_listings[~all_listings['item_id'].isin(hidden)]

    # Deduplicate by item_id (same listing can appear in multiple interest categories)
    if not all_listings.empty:
        all_listings = all_listings.drop_duplicates(subset=['item_id'], keep='first')

    # Calculate value difference (PriceCharting value - eBay listing price) for sorting
    if not all_listings.empty:
        def calculate_value_diff(row):
            """Calculate PriceCharting value - listing price for a row."""
            card_name = row.get('card_name') or row.get('title', '')
            card_number = row.get('card_number', '')
            set_name = row.get('set_name', '')
            listing_price = row.get('listing_price') or row.get('total_cost') or 0

            # Get PriceCharting info
            pc_info = get_pricecharting_info(db, card_name, card_number=card_number, set_name=set_name)

            # Determine market value based on grade/condition
            market_value = None

            # Check if graded
            variant_attrs = row.get('variant_attributes', {})
            if isinstance(variant_attrs, str):
                import json
                try:
                    variant_attrs = json.loads(variant_attrs)
                except:
                    variant_attrs = {}

            is_graded = row.get('is_graded', False) or (variant_attrs.get('is_graded', False) if variant_attrs else False)
            grade_value = row.get('grade') or (variant_attrs.get('grade') if variant_attrs else None)
            condition = row.get('condition', '')
            grading_company = row.get('grading_company') or (variant_attrs.get('grading_company') if variant_attrs else None)

            # Normalize grading company
            if grading_company:
                if 'PSA' in str(grading_company) or 'Professional Sports' in str(grading_company):
                    grading_company = 'PSA'
                elif 'CGC' in str(grading_company) or 'Certified Guaranty' in str(grading_company):
                    grading_company = 'CGC'
                elif 'BGS' in str(grading_company) or 'Beckett' in str(grading_company):
                    grading_company = 'BGS'
                elif 'SGC' in str(grading_company):
                    grading_company = 'SGC'

            is_graded = is_graded or condition == 'Graded' or (grade_value is not None and str(grade_value).replace('.', '').isdigit())

            if is_graded and grade_value:
                grade_str = str(grade_value).replace('.0', '')
                if grade_str == '10':
                    if grading_company == 'PSA':
                        market_value = pc_info.get('psa_10_price')
                    elif grading_company == 'BGS':
                        market_value = pc_info.get('bgs_10_price')
                    elif grading_company == 'CGC':
                        market_value = pc_info.get('cgc_10_price')
                    elif grading_company == 'SGC':
                        market_value = pc_info.get('sgc_10_price')
                    else:
                        market_value = pc_info.get('psa_10_price')
                elif grade_str in ['9.5', '9 5']:
                    market_value = pc_info.get('grade_9_5_price')
                elif grade_str == '9':
                    market_value = pc_info.get('grade_9_price')
                elif grade_str in ['8', '8.5']:
                    market_value = pc_info.get('grade_8_price')
                elif grade_str in ['7', '7.5']:
                    market_value = pc_info.get('grade_7_price')
                elif grade_str == '3':
                    market_value = pc_info.get('grade_3_price')
                elif grade_str == '2':
                    market_value = pc_info.get('grade_2_price')
                elif grade_str == '1':
                    market_value = pc_info.get('grade_1_price')
                else:
                    # Estimate for other grades
                    try:
                        grade_num = float(grade_str)
                        if grade_num >= 9:
                            market_value = pc_info.get('grade_9_price')
                        elif grade_num >= 7:
                            market_value = pc_info.get('grade_7_price')
                        elif grade_num >= 3:
                            market_value = pc_info.get('grade_3_price')
                        else:
                            market_value = pc_info.get('grade_1_price')
                    except:
                        pass
            else:
                # Raw card - use condition mapping
                detailed_condition = variant_attrs.get('detailed_condition') if variant_attrs else None
                actual_condition = detailed_condition or condition
                equivalent_grade = CONDITION_TO_GRADE.get(actual_condition, None)
                if equivalent_grade:
                    if equivalent_grade >= 10:
                        market_value = pc_info.get('psa_10_price')
                    elif equivalent_grade >= 9:
                        market_value = pc_info.get('grade_9_price')
                    elif equivalent_grade >= 7:
                        market_value = pc_info.get('grade_7_price')
                    elif equivalent_grade >= 5:
                        market_value = pc_info.get('grade_3_price')
                    elif equivalent_grade >= 3:
                        market_value = pc_info.get('grade_3_price')
                    else:
                        market_value = pc_info.get('grade_1_price')
                else:
                    market_value = pc_info.get('raw_price')

            if market_value and listing_price and market_value > 0:
                value_diff = market_value - listing_price
                percent_delta = (value_diff / market_value) * 100
                return pd.Series({
                    'value_diff': value_diff,
                    'market_value': market_value,
                    'percent_delta': percent_delta
                })
            return pd.Series({
                'value_diff': float('-inf'),
                'market_value': None,
                'percent_delta': None
            })

        # Calculate value_diff, market_value, and percent_delta for each listing
        calc_results = all_listings.apply(calculate_value_diff, axis=1)
        all_listings['value_diff'] = calc_results['value_diff']
        all_listings['market_value'] = calc_results['market_value']
        all_listings['percent_delta'] = calc_results['percent_delta']

        # Filter by value_diff percentage (data quality filter)
        # Only include listings where listing price is 25-50% below market value
        # This filters out likely incorrect matches or mislabeled items
        all_listings = all_listings[
            (all_listings['percent_delta'] >= 25) &
            (all_listings['percent_delta'] <= 50)
        ]

        # Sort by value_diff descending (best deals first) - default sort
        all_listings = all_listings.sort_values('value_diff', ascending=False)

    # Additional filters (only show if we have data)
    if not all_listings.empty:
        # Graded/Ungraded filter
        graded_options = ["All", "Graded Only", "Ungraded Only"]
        graded_filter = st.sidebar.radio(
            "Card Type",
            options=graded_options,
            index=0
        )
        if graded_filter == "Graded Only":
            all_listings = all_listings[all_listings['is_graded'] == True]
        elif graded_filter == "Ungraded Only":
            all_listings = all_listings[all_listings['is_graded'] == False]

        # Grade filter (only show if graded cards exist)
        grades = all_listings['grade'].dropna().unique()
        if len(grades) > 0:
            grade_filter = st.sidebar.multiselect(
                "Grade",
                options=sorted([g for g in grades if g]),
                default=None
            )
            if grade_filter:
                all_listings = all_listings[all_listings['grade'].isin(grade_filter)]

        st.sidebar.markdown("---")

        # Price range filter
        price_col = 'listing_price' if 'listing_price' in all_listings.columns else 'total_cost'
        if price_col in all_listings.columns:
            prices = all_listings[price_col].dropna()
            if len(prices) > 0:
                min_price = int(prices.min())
                max_price = int(prices.max())
                if min_price < max_price:
                    price_range = st.sidebar.slider(
                        "Price Range ($)",
                        min_value=min_price,
                        max_value=max_price,
                        value=(min_price, max_price)
                    )
                    all_listings = all_listings[
                        (all_listings[price_col] >= price_range[0]) &
                        (all_listings[price_col] <= price_range[1])
                    ]

        # Sort options (default sort by value_diff is already applied internally)
        st.sidebar.markdown("---")
        sort_options = ["Newest First", "Price: Low to High", "Price: High to Low"]
        sort_by = st.sidebar.selectbox(
            "Sort By",
            options=sort_options,
            index=None,
            placeholder="Select sort order"
        )
        if sort_by == "Newest First":
            if 'discovered_at' in all_listings.columns:
                all_listings = all_listings.sort_values('discovered_at', ascending=False, na_position='last')
        elif sort_by == "Price: Low to High":
            all_listings = all_listings.sort_values(price_col, ascending=True)
        elif sort_by == "Price: High to Low":
            all_listings = all_listings.sort_values(price_col, ascending=False)
        # When no option selected, keeps default sort by value_diff from line 1076

    # Sidebar - Stats
    st.sidebar.markdown("---")
    st.sidebar.title("Stats")

    if not all_listings.empty:
        st.sidebar.metric("Active Listings", len(all_listings))

        # Count by interest
        if 'interest' in all_listings.columns:
            interest_counts = all_listings['interest'].value_counts()
            for interest, count in interest_counts.items():
                label = interest.title()
                st.sidebar.write(f"{label}: {count}")
    else:
        st.sidebar.metric("Active Listings", 0)

    if hidden:
        st.sidebar.metric("Hidden", len(hidden))
        if st.sidebar.button("Show All Hidden"):
            st.session_state.hidden_listings = set()
            st.rerun()

    # Sidebar - Attribution
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Data Sources**")
    st.sidebar.markdown("Market data from [PriceCharting.com](https://www.pricecharting.com)")
    st.sidebar.markdown("Listings from [eBay](https://www.ebay.com) Browse API")
    st.sidebar.markdown("*For internal business use only*")

    # Main content
    if all_listings.empty:
        st.info("No listings found for selected interests.")
        st.write("Select different interests or wait for the next data refresh.")
    else:
        st.write(f"Showing **{len(all_listings)}** listings")

        # Display listings
        for idx, listing in all_listings.iterrows():
            with st.container():
                listing_dict = listing.to_dict()
                display_listing_card(listing_dict, grade_matcher, db)
                st.divider()
                st.write("")


if __name__ == '__main__':
    main()
