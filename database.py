"""
Database Manager for Lugia Tracker

Handles PostgreSQL database operations for storing and retrieving
eBay listing data, price history, and search run metadata.
"""

import os
from datetime import datetime, date
from typing import Dict, List, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from urllib.parse import urlparse


class DatabaseManager:
    """Manage PostgreSQL database operations."""

    def __init__(self, database_url: str):
        """
        Initialize database connection.

        Args:
            database_url: PostgreSQL connection string (can be from Supabase)
        """
        self.database_url = database_url
        self.conn = None
        self.connect()
        self.init_database()

    def connect(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(self.database_url)
            print("Database connection established")
        except Exception as e:
            print(f"Error connecting to database: {e}")
            raise

    def init_database(self):
        """Create database tables if they don't exist."""
        create_tables_sql = """
        -- Listings table (current snapshot)
        CREATE TABLE IF NOT EXISTS listings (
            id SERIAL PRIMARY KEY,
            item_id TEXT UNIQUE NOT NULL,
            title TEXT NOT NULL,
            grade TEXT,
            price REAL,
            shipping REAL,
            total_cost REAL,
            condition TEXT,
            is_graded BOOLEAN,
            raw_condition TEXT,
            comparable_grade TEXT,
            listing_type TEXT,
            is_auction BOOLEAN,
            seller_username TEXT,
            seller_feedback REAL,
            url TEXT,
            image_url TEXT,
            first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE
        );

        -- Price history table (track changes over time)
        CREATE TABLE IF NOT EXISTS price_history (
            id SERIAL PRIMARY KEY,
            item_id TEXT NOT NULL,
            total_cost REAL,
            price REAL,
            shipping REAL,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Search runs table (track when searches were performed)
        CREATE TABLE IF NOT EXISTS search_runs (
            id SERIAL PRIMARY KEY,
            run_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            total_found INTEGER,
            total_filtered INTEGER,
            total_valid INTEGER,
            api_calls_used INTEGER DEFAULT 1,
            status TEXT,
            error_message TEXT
        );

        -- Market values table (PriceCharting market values over time)
        CREATE TABLE IF NOT EXISTS market_values (
            id SERIAL PRIMARY KEY,
            product_id TEXT NOT NULL,
            product_name TEXT NOT NULL,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            bgs_10_price REAL,
            psa_10_price REAL,
            psa_9_price REAL,
            psa_8_price REAL,
            psa_7_price REAL,
            cgc_10_price REAL,
            cgc_9_5_price REAL,
            cgc_9_price REAL,
            graded_generic_price REAL,
            raw_ungraded_price REAL,
            new_mint_price REAL,
            complete_price REAL,
            sales_volume INTEGER,
            data_source TEXT DEFAULT 'pricecharting',
            UNIQUE(product_id, recorded_at)
        );

        -- Tracked cards table (cards being monitored)
        CREATE TABLE IF NOT EXISTS tracked_cards (
            id SERIAL PRIMARY KEY,
            card_name TEXT NOT NULL,
            set_name TEXT,
            card_number TEXT,
            search_query TEXT NOT NULL,
            pricecharting_id TEXT,
            is_active BOOLEAN DEFAULT TRUE,
            priority INTEGER DEFAULT 5,
            tracking_status TEXT DEFAULT 'active',
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            added_by TEXT DEFAULT 'auto_discovery',
            last_tracked TIMESTAMP,
            notes TEXT,
            UNIQUE(card_name, set_name, card_number)
        );

        -- Card discovery candidates (potential cards to track)
        CREATE TABLE IF NOT EXISTS card_candidates (
            id SERIAL PRIMARY KEY,
            card_name TEXT NOT NULL,
            set_name TEXT,
            discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            listing_count INTEGER,
            avg_price REAL,
            total_sales_volume INTEGER,
            popularity REAL,
            status TEXT DEFAULT 'pending',
            reviewed_at TIMESTAMP,
            notes TEXT,
            UNIQUE(card_name, set_name)
        );

        -- Card market values (card-specific pricing data)
        CREATE TABLE IF NOT EXISTS card_market_values (
            id SERIAL PRIMARY KEY,
            tracked_card_id INTEGER REFERENCES tracked_cards(id) ON DELETE CASCADE,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            psa_10_price REAL,
            psa_9_price REAL,
            psa_8_price REAL,
            psa_7_price REAL,
            psa_6_price REAL,
            psa_5_price REAL,
            psa_4_price REAL,
            psa_3_price REAL,
            psa_2_price REAL,
            psa_1_price REAL,
            bgs_10_price REAL,
            bgs_9_5_price REAL,
            cgc_10_pristine_price REAL,
            cgc_10_price REAL,
            cgc_9_5_price REAL,
            sgc_10_price REAL,
            grade_9_5_price REAL,
            raw_ungraded_price REAL,
            data_source TEXT DEFAULT 'pricecharting'
        );

        -- Add columns to listings table if they don't exist
        ALTER TABLE listings ADD COLUMN IF NOT EXISTS tracked_card_id INTEGER REFERENCES tracked_cards(id) ON DELETE SET NULL;
        ALTER TABLE listings ADD COLUMN IF NOT EXISTS is_graded BOOLEAN;
        ALTER TABLE listings ADD COLUMN IF NOT EXISTS raw_condition TEXT;
        ALTER TABLE listings ADD COLUMN IF NOT EXISTS comparable_grade TEXT;

        -- Discovery runs table (track each discovery execution)
        CREATE TABLE IF NOT EXISTS discovery_runs (
            id SERIAL PRIMARY KEY,
            run_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            total_queries INTEGER,
            total_listings_found INTEGER,
            total_listings_processed INTEGER,
            total_unique_cards INTEGER,
            total_candidates_saved INTEGER,
            status TEXT,
            error_message TEXT,
            execution_time_seconds REAL
        );

        -- Discovery results table (detailed results per card per run)
        CREATE TABLE IF NOT EXISTS discovery_results (
            id SERIAL PRIMARY KEY,
            discovery_run_id INTEGER REFERENCES discovery_runs(id) ON DELETE CASCADE,
            card_name TEXT NOT NULL,
            set_name TEXT,
            card_number TEXT,
            variant_attributes JSONB,
            listing_count INTEGER,
            avg_price REAL,
            min_price REAL,
            max_price REAL,
            popularity REAL,
            search_query TEXT,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Discovered listings table (individual listings that passed filters)
        CREATE TABLE IF NOT EXISTS discovered_listings (
            id SERIAL PRIMARY KEY,
            discovery_run_id INTEGER REFERENCES discovery_runs(id) ON DELETE CASCADE,
            item_id TEXT NOT NULL,
            title TEXT,
            card_name TEXT,
            set_name TEXT,
            card_number TEXT,
            variant_attributes JSONB,
            grade TEXT,
            grading_company TEXT,
            price REAL NOT NULL,
            condition TEXT,
            seller_username TEXT,
            seller_feedback REAL,
            url TEXT,
            image_url TEXT,
            listing_type TEXT,
            is_auction BOOLEAN DEFAULT FALSE,
            discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(item_id, discovery_run_id)
        );

        -- Reviewed listings table (listings reviewed by user in Streamlit app)
        CREATE TABLE IF NOT EXISTS reviewed_listings (
            id SERIAL PRIMARY KEY,
            item_id TEXT UNIQUE NOT NULL,
            title TEXT,
            grade TEXT,
            total_cost REAL,
            avg_price REAL,
            percent_below_avg REAL,
            listing_type TEXT,
            seller_username TEXT,
            seller_feedback REAL,
            is_auction BOOLEAN,
            condition TEXT,
            url TEXT,
            image_url TEXT,
            detected_at TIMESTAMP,
            user_action TEXT,
            user_rating INTEGER,
            action_timestamp TIMESTAMP,
            notes TEXT
        );

        -- Create indexes for better query performance
        CREATE INDEX IF NOT EXISTS idx_listings_item_id ON listings(item_id);
        CREATE INDEX IF NOT EXISTS idx_listings_grade ON listings(grade);
        CREATE INDEX IF NOT EXISTS idx_listings_is_active ON listings(is_active);
        CREATE INDEX IF NOT EXISTS idx_listings_tracked_card ON listings(tracked_card_id);
        CREATE INDEX IF NOT EXISTS idx_price_history_item_id ON price_history(item_id);
        CREATE INDEX IF NOT EXISTS idx_price_history_recorded_at ON price_history(recorded_at);
        CREATE INDEX IF NOT EXISTS idx_search_runs_run_time ON search_runs(run_time);
        CREATE INDEX IF NOT EXISTS idx_market_values_product_id ON market_values(product_id);
        CREATE INDEX IF NOT EXISTS idx_market_values_recorded_at ON market_values(recorded_at);
        CREATE INDEX IF NOT EXISTS idx_tracked_cards_is_active ON tracked_cards(is_active);
        CREATE INDEX IF NOT EXISTS idx_card_candidates_status ON card_candidates(status);
        CREATE INDEX IF NOT EXISTS idx_card_market_values_tracked_card ON card_market_values(tracked_card_id);
        CREATE INDEX IF NOT EXISTS idx_discovery_runs_run_time ON discovery_runs(run_time);
        CREATE INDEX IF NOT EXISTS idx_discovery_results_run_id ON discovery_results(discovery_run_id);
        CREATE INDEX IF NOT EXISTS idx_discovery_results_card ON discovery_results(card_name, set_name);
        CREATE INDEX IF NOT EXISTS idx_discovered_listings_run_id ON discovered_listings(discovery_run_id);
        CREATE INDEX IF NOT EXISTS idx_discovered_listings_item_id ON discovered_listings(item_id);
        CREATE INDEX IF NOT EXISTS idx_discovered_listings_card ON discovered_listings(card_name, set_name);
        CREATE INDEX IF NOT EXISTS idx_discovered_listings_price ON discovered_listings(price);
        CREATE INDEX IF NOT EXISTS idx_reviewed_listings_item_id ON reviewed_listings(item_id);
        CREATE INDEX IF NOT EXISTS idx_reviewed_listings_user_action ON reviewed_listings(user_action);
        """

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(create_tables_sql)
                self.conn.commit()
                print("Database tables initialized")
        except Exception as e:
            print(f"Error initializing database: {e}")
            self.conn.rollback()
            raise

    def save_listing(self, listing: Dict) -> bool:
        """
        Save or update a listing in the database.

        Args:
            listing: Dictionary containing listing data

        Returns:
            True if successful, False otherwise
        """
        insert_sql = """
        INSERT INTO listings (
            item_id, title, grade, price, shipping, total_cost,
            condition, listing_type, is_auction, seller_username,
            seller_feedback, url, image_url,
            is_graded, raw_condition, comparable_grade,
            first_seen, last_seen, is_active
        ) VALUES (
            %(item_id)s, %(title)s, %(grade)s, %(price)s, %(shipping)s, %(total_cost)s,
            %(condition)s, %(listing_type)s, %(is_auction)s, %(seller_username)s,
            %(seller_feedback)s, %(url)s, %(image_url)s,
            %(is_graded)s, %(raw_condition)s, %(comparable_grade)s,
            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, TRUE
        )
        ON CONFLICT (item_id) DO UPDATE SET
            title = EXCLUDED.title,
            grade = EXCLUDED.grade,
            price = EXCLUDED.price,
            shipping = EXCLUDED.shipping,
            total_cost = EXCLUDED.total_cost,
            condition = EXCLUDED.condition,
            listing_type = EXCLUDED.listing_type,
            is_auction = EXCLUDED.is_auction,
            seller_username = EXCLUDED.seller_username,
            seller_feedback = EXCLUDED.seller_feedback,
            url = EXCLUDED.url,
            image_url = EXCLUDED.image_url,
            is_graded = EXCLUDED.is_graded,
            raw_condition = EXCLUDED.raw_condition,
            comparable_grade = EXCLUDED.comparable_grade,
            last_seen = CURRENT_TIMESTAMP,
            is_active = TRUE
        RETURNING id;
        """

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(insert_sql, listing)
                self.conn.commit()

                # Save to price history
                self._save_price_history(listing)

                return True
        except Exception as e:
            print(f"Error saving listing {listing.get('item_id')}: {e}")
            self.conn.rollback()
            return False

    def _save_price_history(self, listing: Dict):
        """Save price snapshot to history table."""
        insert_sql = """
        INSERT INTO price_history (item_id, total_cost, price, shipping)
        VALUES (%(item_id)s, %(total_cost)s, %(price)s, %(shipping)s);
        """

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(insert_sql, listing)
                self.conn.commit()
        except Exception as e:
            print(f"Error saving price history for {listing.get('item_id')}: {e}")
            self.conn.rollback()

    def save_search_run(self, total_found: int, total_filtered: int,
                        total_valid: int, status: str, error_message: str = None):
        """
        Save search run metadata.

        Args:
            total_found: Total raw listings found
            total_filtered: Number of listings filtered out
            total_valid: Number of valid listings saved
            status: 'success' or 'error'
            error_message: Error message if status is 'error'
        """
        insert_sql = """
        INSERT INTO search_runs (
            total_found, total_filtered, total_valid, status, error_message
        ) VALUES (%s, %s, %s, %s, %s)
        RETURNING id;
        """

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(insert_sql, (
                    total_found, total_filtered, total_valid, status, error_message
                ))
                self.conn.commit()
                return cursor.fetchone()[0]
        except Exception as e:
            print(f"Error saving search run: {e}")
            self.conn.rollback()
            return None

    def mark_inactive_listings(self, active_item_ids: List[str]):
        """
        Mark listings as inactive if they're not in the current search results.

        Args:
            active_item_ids: List of item IDs found in current search
        """
        update_sql = """
        UPDATE listings
        SET is_active = FALSE
        WHERE item_id NOT IN %s AND is_active = TRUE;
        """

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(update_sql, (tuple(active_item_ids),))
                self.conn.commit()
                print(f"Marked {cursor.rowcount} listings as inactive")
        except Exception as e:
            print(f"Error marking inactive listings: {e}")
            self.conn.rollback()

    def get_active_listings(self, grade: Optional[str] = None) -> List[Dict]:
        """
        Get all active listings, optionally filtered by grade.

        Args:
            grade: Filter by grade (e.g., 'PSA 10'), None for all grades

        Returns:
            List of listing dictionaries
        """
        if grade:
            query = """
            SELECT * FROM listings
            WHERE is_active = TRUE AND grade = %s
            ORDER BY total_cost ASC;
            """
            params = (grade,)
        else:
            query = """
            SELECT * FROM listings
            WHERE is_active = TRUE
            ORDER BY total_cost ASC;
            """
            params = None

        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            print(f"Error fetching active listings: {e}")
            return []

    def get_price_history(self, item_id: str) -> List[Dict]:
        """
        Get price history for a specific item.

        Args:
            item_id: eBay item ID

        Returns:
            List of price history records
        """
        query = """
        SELECT * FROM price_history
        WHERE item_id = %s
        ORDER BY recorded_at DESC;
        """

        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (item_id,))
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            print(f"Error fetching price history: {e}")
            return []

    def get_recent_search_runs(self, limit: int = 10) -> List[Dict]:
        """
        Get recent search run records.

        Args:
            limit: Number of records to retrieve

        Returns:
            List of search run records
        """
        query = """
        SELECT * FROM search_runs
        ORDER BY run_time DESC
        LIMIT %s;
        """

        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (limit,))
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            print(f"Error fetching search runs: {e}")
            return []

    def save_discovery_run(self, total_queries: int, total_listings_found: int,
                          total_listings_processed: int, total_unique_cards: int,
                          total_candidates_saved: int, status: str,
                          execution_time_seconds: float, error_message: str = None) -> Optional[int]:
        """
        Save discovery run metadata.

        Args:
            total_queries: Number of search queries executed
            total_listings_found: Total raw listings found
            total_listings_processed: Number of listings successfully processed
            total_unique_cards: Number of unique cards discovered
            total_candidates_saved: Number of candidates saved to database
            status: 'success' or 'error'
            execution_time_seconds: Total execution time in seconds
            error_message: Error message if status is 'error'

        Returns:
            Discovery run ID if successful, None otherwise
        """
        insert_sql = """
        INSERT INTO discovery_runs (
            total_queries, total_listings_found, total_listings_processed,
            total_unique_cards, total_candidates_saved, status,
            execution_time_seconds, error_message
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(insert_sql, (
                    total_queries, total_listings_found, total_listings_processed,
                    total_unique_cards, total_candidates_saved, status,
                    execution_time_seconds, error_message
                ))
                self.conn.commit()
                return cursor.fetchone()[0]
        except Exception as e:
            print(f"Error saving discovery run: {e}")
            self.conn.rollback()
            return None

    def save_discovery_result(self, discovery_run_id: int, card_data: Dict) -> bool:
        """
        Save discovery result for a specific card.

        Args:
            discovery_run_id: ID of the discovery run
            card_data: Dictionary containing card discovery data with keys:
                - card_name, set_name, card_number
                - variant_attributes (dict)
                - listing_count, avg_price, min_price, max_price
                - popularity, search_query

        Returns:
            True if successful, False otherwise
        """
        import json

        insert_sql = """
        INSERT INTO discovery_results (
            discovery_run_id, card_name, set_name, card_number,
            variant_attributes, listing_count, avg_price, min_price, max_price,
            popularity, search_query
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(insert_sql, (
                    discovery_run_id,
                    card_data['card_name'],
                    card_data.get('set_name'),
                    card_data.get('card_number'),
                    json.dumps(card_data.get('variant_attributes', {})),
                    card_data.get('listing_count', 0),
                    card_data.get('avg_price', 0.0),
                    card_data.get('min_price', 0.0),
                    card_data.get('max_price', 0.0),
                    card_data.get('popularity', 0.0),
                    card_data.get('search_query', '')
                ))
                self.conn.commit()
                return True
        except Exception as e:
            print(f"Error saving discovery result for {card_data.get('card_name')}: {e}")
            self.conn.rollback()
            return False

    def get_recent_discovery_runs(self, limit: int = 10) -> List[Dict]:
        """
        Get recent discovery run records.

        Args:
            limit: Number of records to retrieve

        Returns:
            List of discovery run records
        """
        query = """
        SELECT * FROM discovery_runs
        ORDER BY run_time DESC
        LIMIT %s;
        """

        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (limit,))
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            print(f"Error fetching discovery runs: {e}")
            return []

    def get_discovery_results(self, discovery_run_id: int) -> List[Dict]:
        """
        Get all discovery results for a specific run.

        Args:
            discovery_run_id: ID of the discovery run

        Returns:
            List of discovery result records
        """
        query = """
        SELECT * FROM discovery_results
        WHERE discovery_run_id = %s
        ORDER BY popularity DESC;
        """

        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (discovery_run_id,))
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            print(f"Error fetching discovery results: {e}")
            return []

    def save_discovered_listing(self, discovery_run_id: int, listing: Dict) -> bool:
        """
        Save an individual discovered listing to the database.

        Args:
            discovery_run_id: ID of the discovery run
            listing: Dictionary containing listing data with keys:
                - item_id, title, card_name, set_name, card_number
                - variant_attributes (dict), grade, grading_company
                - price, condition, seller_username, seller_feedback
                - url, image_url, listing_type, is_auction

        Returns:
            True if successful, False otherwise
        """
        import json

        insert_sql = """
        INSERT INTO discovered_listings (
            discovery_run_id, item_id, title, card_name, set_name, card_number,
            variant_attributes, grade, grading_company, price, condition,
            seller_username, seller_feedback, url, image_url, listing_type, is_auction
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (item_id, discovery_run_id) DO NOTHING
        RETURNING id;
        """

        try:
            variant_attrs = listing.get('variant_attributes', {})
            variant_json = json.dumps(variant_attrs) if variant_attrs else None

            with self.conn.cursor() as cursor:
                cursor.execute(insert_sql, (
                    discovery_run_id,
                    listing.get('item_id'),
                    listing.get('title'),
                    listing.get('card_name'),
                    listing.get('set_name'),
                    listing.get('card_number'),
                    variant_json,
                    listing.get('grade'),
                    listing.get('grading_company'),
                    listing.get('price'),
                    listing.get('condition'),
                    listing.get('seller_username'),
                    listing.get('seller_feedback'),
                    listing.get('url'),
                    listing.get('image_url'),
                    listing.get('listing_type'),
                    listing.get('is_auction', False)
                ))
                self.conn.commit()
                return True
        except Exception as e:
            print(f"Error saving discovered listing {listing.get('item_id')}: {e}")
            self.conn.rollback()
            return False

    def save_discovered_listings_batch(self, discovery_run_id: int, listings: list) -> int:
        """
        Save multiple discovered listings in a batch for efficiency.

        Args:
            discovery_run_id: ID of the discovery run
            listings: List of listing dictionaries

        Returns:
            Number of listings saved successfully
        """
        saved = 0
        for listing in listings:
            if self.save_discovered_listing(discovery_run_id, listing):
                saved += 1
        return saved

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            print("Database connection closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
