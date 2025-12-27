"""
PriceCharting CSV Importer

Downloads daily CSV from PriceCharting and imports into database.
Populates card market data:
1. pricecharting_raw - All CSV data with import date
2. card_market_candidates - Filtered cards (volume >= 50, PSA 10 >= $50)
3. card_market_trends - Price changes over 7/30 days
"""

import os
import csv
import requests
import re
from datetime import date, timedelta
from typing import Optional, List, Tuple
from io import StringIO
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv('ebay.env')

# PriceCharting CSV URL (requires Legendary subscription)
PRICECHARTING_CSV_URL = os.getenv('PRICECHARTING_CSV_URL')

# Filtering thresholds
MIN_SALES_VOLUME = 50
MIN_PSA_10_PRICE = 50.0  # $50


def parse_price(price_str: str) -> Optional[float]:
    """Parse price string like '$123.45' to float."""
    if not price_str:
        return None
    try:
        return float(price_str.replace('$', '').replace(',', ''))
    except (ValueError, AttributeError):
        return None


def parse_date(date_str: str) -> Optional[date]:
    """Parse date string like '1998-06-01' to date object."""
    if not date_str:
        return None
    try:
        return date.fromisoformat(date_str)
    except ValueError:
        return None


def extract_card_info(console_name: str, product_name: str, release_date: Optional[date]):
    """
    Extract set name, card name, card number, and year from PriceCharting data.

    Examples:
    - console_name: "Pokemon Japanese Mega Dream"
    - product_name: "Charizard #6"
    - release_date: 1998-06-01

    Returns: (set_name, card_name, card_number, card_year)
    """
    set_name = console_name
    card_name = product_name
    card_number = None
    card_year = None

    # Extract card number from product name (e.g., "Charizard #6" -> "6")
    number_match = re.search(r'#(\d+(?:/\d+)?)', product_name)
    if number_match:
        card_number = number_match.group(1)
        # Remove number from card name
        card_name = re.sub(r'\s*#\d+(?:/\d+)?\s*', '', product_name).strip()

    # Extract year from release date
    if release_date:
        card_year = release_date.year

    return set_name, card_name, card_number, card_year


class PriceChartingImporter:
    """Import PriceCharting CSV data into database."""

    def __init__(self, db_manager):
        self.db = db_manager
        self.import_date = date.today()

    def download_csv(self) -> Optional[str]:
        """Download CSV from PriceCharting."""
        if not PRICECHARTING_CSV_URL:
            print("ERROR: PRICECHARTING_CSV_URL not set")
            return None

        print(f"Downloading PriceCharting CSV...")
        try:
            response = requests.get(PRICECHARTING_CSV_URL, timeout=120)
            response.raise_for_status()
            print(f"Downloaded {len(response.text):,} bytes")
            return response.text
        except requests.exceptions.RequestException as e:
            print(f"Error downloading CSV: {e}")
            return None

    def import_raw_data(self, csv_text: str) -> int:
        """Import CSV data into pricecharting_raw table using batch inserts."""
        print(f"Importing raw data for {self.import_date}...")

        reader = csv.DictReader(StringIO(csv_text))

        # Collect all rows into batches
        BATCH_SIZE = 5000
        batch: List[Tuple] = []
        imported = 0

        insert_sql = """
        INSERT INTO pricecharting_raw (
            product_id, console_name, product_name,
            loose_price, cib_price, new_price, graded_price,
            box_only_price, manual_only_price, bgs_10_price, sgc_10_price,
            sales_volume, genre, release_date, import_date
        ) VALUES %s
        ON CONFLICT (product_id, import_date) DO UPDATE SET
            console_name = EXCLUDED.console_name,
            product_name = EXCLUDED.product_name,
            loose_price = EXCLUDED.loose_price,
            cib_price = EXCLUDED.cib_price,
            new_price = EXCLUDED.new_price,
            graded_price = EXCLUDED.graded_price,
            box_only_price = EXCLUDED.box_only_price,
            manual_only_price = EXCLUDED.manual_only_price,
            bgs_10_price = EXCLUDED.bgs_10_price,
            sgc_10_price = EXCLUDED.sgc_10_price,
            sales_volume = EXCLUDED.sales_volume,
            genre = EXCLUDED.genre,
            release_date = EXCLUDED.release_date;
        """

        try:
            with self.db.conn.cursor() as cursor:
                for row in reader:
                    batch.append((
                        row.get('id'),
                        row.get('console-name'),
                        row.get('product-name'),
                        parse_price(row.get('loose-price')),
                        parse_price(row.get('cib-price')),
                        parse_price(row.get('new-price')),
                        parse_price(row.get('graded-price')),
                        parse_price(row.get('box-only-price')),
                        parse_price(row.get('manual-only-price')),
                        parse_price(row.get('bgs-10-price')),
                        parse_price(row.get('condition-18-price')),  # SGC 10
                        int(row.get('sales-volume', 0)) if row.get('sales-volume') else None,
                        row.get('genre'),
                        parse_date(row.get('release-date')),
                        self.import_date
                    ))

                    if len(batch) >= BATCH_SIZE:
                        execute_values(cursor, insert_sql, batch)
                        imported += len(batch)
                        print(f"  Imported {imported:,} rows...")
                        batch = []

                # Insert remaining rows
                if batch:
                    execute_values(cursor, insert_sql, batch)
                    imported += len(batch)

                self.db.conn.commit()
                print(f"Imported {imported:,} rows into pricecharting_raw")
                return imported

        except Exception as e:
            print(f"Error importing raw data: {e}")
            self.db.conn.rollback()
            return 0

    def update_candidates(self) -> int:
        """Update card_market_candidates table with filtered cards using batch inserts."""
        print(f"Updating card market candidates (volume >= {MIN_SALES_VOLUME}, PSA 10 >= ${MIN_PSA_10_PRICE})...")

        # Get today's data that meets criteria
        select_sql = """
        SELECT
            product_id, console_name, product_name,
            manual_only_price, graded_price, loose_price,
            sales_volume, release_date
        FROM pricecharting_raw
        WHERE import_date = %s
          AND sales_volume >= %s
          AND manual_only_price >= %s;
        """

        upsert_sql = """
        INSERT INTO card_market_candidates (
            product_id, set_name, card_name, card_number, card_year,
            psa_10_price, psa_9_price, raw_price, sales_volume,
            first_seen, last_updated, is_active
        ) VALUES %s
        ON CONFLICT (product_id) DO UPDATE SET
            set_name = EXCLUDED.set_name,
            card_name = EXCLUDED.card_name,
            card_number = EXCLUDED.card_number,
            card_year = EXCLUDED.card_year,
            psa_10_price = EXCLUDED.psa_10_price,
            psa_9_price = EXCLUDED.psa_9_price,
            raw_price = EXCLUDED.raw_price,
            sales_volume = EXCLUDED.sales_volume,
            last_updated = EXCLUDED.last_updated,
            is_active = TRUE;
        """

        try:
            with self.db.conn.cursor() as cursor:
                cursor.execute(select_sql, (self.import_date, MIN_SALES_VOLUME, MIN_PSA_10_PRICE))
                rows = cursor.fetchall()

                # Build batch of values
                batch = []
                for row in rows:
                    product_id, console_name, product_name, psa_10, psa_9, raw_price, volume, release_date = row
                    set_name, card_name, card_number, card_year = extract_card_info(
                        console_name, product_name, release_date
                    )
                    batch.append((
                        product_id, set_name, card_name, card_number, card_year,
                        psa_10, psa_9, raw_price, volume,
                        self.import_date, self.import_date, True
                    ))

                if batch:
                    execute_values(cursor, upsert_sql, batch)

                # Mark cards not seen today as inactive
                cursor.execute("""
                    UPDATE card_market_candidates
                    SET is_active = FALSE
                    WHERE last_updated < %s;
                """, (self.import_date,))

                self.db.conn.commit()
                print(f"Updated {len(batch):,} card market candidates")
                return len(batch)

        except Exception as e:
            print(f"Error updating candidates: {e}")
            self.db.conn.rollback()
            return 0

    def calculate_trends(self) -> int:
        """Calculate price trends comparing to 7 and 30 days ago using batch inserts."""
        print("Calculating card market trends...")

        date_7d_ago = self.import_date - timedelta(days=7)
        date_30d_ago = self.import_date - timedelta(days=30)

        # Get today's prices with historical comparisons
        select_sql = """
        SELECT
            t.product_id,
            t.console_name,
            t.product_name,
            t.release_date,
            t.manual_only_price as today_price,
            t.sales_volume,
            d7.manual_only_price as price_7d,
            d30.manual_only_price as price_30d
        FROM pricecharting_raw t
        LEFT JOIN pricecharting_raw d7
            ON t.product_id = d7.product_id AND d7.import_date = %s
        LEFT JOIN pricecharting_raw d30
            ON t.product_id = d30.product_id AND d30.import_date = %s
        WHERE t.import_date = %s
          AND t.sales_volume >= %s
          AND t.manual_only_price >= %s;
        """

        upsert_sql = """
        INSERT INTO card_market_trends (
            product_id, set_name, card_name, card_number, card_year,
            trend_date, psa_10_price, psa_10_price_7d_ago, psa_10_price_30d_ago,
            psa_10_change_7d, psa_10_change_30d,
            psa_10_pct_change_7d, psa_10_pct_change_30d, sales_volume
        ) VALUES %s
        ON CONFLICT (product_id, trend_date) DO UPDATE SET
            set_name = EXCLUDED.set_name,
            card_name = EXCLUDED.card_name,
            psa_10_price = EXCLUDED.psa_10_price,
            psa_10_price_7d_ago = EXCLUDED.psa_10_price_7d_ago,
            psa_10_price_30d_ago = EXCLUDED.psa_10_price_30d_ago,
            psa_10_change_7d = EXCLUDED.psa_10_change_7d,
            psa_10_change_30d = EXCLUDED.psa_10_change_30d,
            psa_10_pct_change_7d = EXCLUDED.psa_10_pct_change_7d,
            psa_10_pct_change_30d = EXCLUDED.psa_10_pct_change_30d,
            sales_volume = EXCLUDED.sales_volume;
        """

        try:
            with self.db.conn.cursor() as cursor:
                cursor.execute(select_sql, (
                    date_7d_ago, date_30d_ago, self.import_date,
                    MIN_SALES_VOLUME, MIN_PSA_10_PRICE
                ))
                rows = cursor.fetchall()

                # Build batch of values
                batch = []
                for row in rows:
                    (product_id, console_name, product_name, release_date,
                     today_price, volume, price_7d, price_30d) = row

                    set_name, card_name, card_number, card_year = extract_card_info(
                        console_name, product_name, release_date
                    )

                    # Calculate changes
                    change_7d = None
                    pct_7d = None
                    if today_price and price_7d:
                        change_7d = today_price - price_7d
                        pct_7d = (change_7d / price_7d) * 100 if price_7d else None

                    change_30d = None
                    pct_30d = None
                    if today_price and price_30d:
                        change_30d = today_price - price_30d
                        pct_30d = (change_30d / price_30d) * 100 if price_30d else None

                    batch.append((
                        product_id, set_name, card_name, card_number, card_year,
                        self.import_date, today_price, price_7d, price_30d,
                        change_7d, change_30d, pct_7d, pct_30d, volume
                    ))

                if batch:
                    execute_values(cursor, upsert_sql, batch)

                self.db.conn.commit()
                print(f"Calculated {len(batch):,} card market trends")
                return len(batch)

        except Exception as e:
            print(f"Error calculating card market trends: {e}")
            self.db.conn.rollback()
            return 0

    def run(self) -> dict:
        """Run the full import pipeline."""
        print("=" * 60)
        print("PriceCharting CSV Importer")
        print(f"Import Date: {self.import_date}")
        print("=" * 60)

        # Download CSV
        csv_text = self.download_csv()
        if not csv_text:
            return {'status': 'error', 'message': 'Failed to download CSV'}

        # Import raw data
        raw_count = self.import_raw_data(csv_text)
        if raw_count == 0:
            return {'status': 'error', 'message': 'Failed to import raw data'}

        # Update candidates
        candidates_count = self.update_candidates()

        # Calculate trends
        trends_count = self.calculate_trends()

        print("=" * 60)
        print("Import Complete")
        print(f"  Raw records: {raw_count:,}")
        print(f"  Candidates: {candidates_count:,}")
        print(f"  Trends: {trends_count:,}")
        print("=" * 60)

        return {
            'status': 'success',
            'import_date': str(self.import_date),
            'raw_count': raw_count,
            'candidates_count': candidates_count,
            'trends_count': trends_count
        }


def main():
    """Main entry point."""
    from database import DatabaseManager

    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        print("ERROR: DATABASE_URL not set")
        exit(1)

    db = DatabaseManager(db_url)
    importer = PriceChartingImporter(db)
    result = importer.run()

    if result['status'] == 'error':
        print(f"ERROR: {result.get('message')}")
        exit(1)

    db.close()


if __name__ == '__main__':
    main()
