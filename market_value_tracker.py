"""
Market Value Tracker

Fetches current market values from PriceCharting API and stores them
in the database to build historical price trend data over time.

Run daily via GitHub Actions to track how market values change.
"""

import os
import requests
from datetime import datetime
from typing import Dict, Optional
from dotenv import load_dotenv


# Load environment variables
load_dotenv('.env')
load_dotenv('ebay.env')

# PriceCharting API Configuration
PRICECHARTING_API_KEY = os.getenv('PRICECHARTING_API_KEY')
PRICECHARTING_API_URL = 'https://www.pricecharting.com/api'

# Lugia 1st Edition Product ID
LUGIA_PRODUCT_ID = '2324884'


class MarketValueTracker:
    """Track market values from PriceCharting over time."""

    def __init__(self, db_manager=None):
        self.db_manager = db_manager
        self.api_key = PRICECHARTING_API_KEY

    def fetch_market_values(self, product_id: str) -> Optional[Dict]:
        """
        Fetch current market values for a product from PriceCharting.

        Args:
            product_id: PriceCharting product ID

        Returns:
            Dict with market value data, or None if error
        """
        if not self.api_key:
            print("ERROR: PRICECHARTING_API_KEY not set")
            return None

        url = f'{PRICECHARTING_API_URL}/product'
        params = {
            't': self.api_key,
            'id': product_id
        }

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            if data.get('status') == 'error':
                print(f"API Error: {data.get('error-message')}")
                return None

            return data

        except requests.exceptions.RequestException as e:
            print(f"Error fetching market values: {e}")
            return None

    def parse_market_values(self, data: Dict) -> Optional[Dict]:
        """
        Parse PriceCharting API response into standardized format.

        PriceCharting field mapping for Pokemon Cards (per official docs):
        - manual-only-price: PSA 10
        - box-only-price: Grade 9.5 (generic)
        - graded-price: Grade 9 (generic)
        - new-price: Grade 8/8.5 (generic)
        - cib-price: Grade 7/7.5 (generic)
        - loose-price: Ungraded/Raw
        - bgs-10-price: BGS 10 (Beckett)
        - condition-18-price: SGC 10

        Args:
            data: Raw API response

        Returns:
            Dict with parsed market values (prices in dollars)
        """
        if not data:
            return None

        # Convert prices from cents to dollars
        def to_dollars(cents):
            return cents / 100.0 if cents else None

        return {
            'product_id': data.get('id'),
            'product_name': data.get('product-name'),
            'bgs_10_price': to_dollars(data.get('bgs-10-price')),
            'psa_10_price': to_dollars(data.get('manual-only-price')),
            'psa_9_price': to_dollars(data.get('graded-price')),
            'psa_8_price': to_dollars(data.get('new-price')),
            'psa_7_price': to_dollars(data.get('cib-price')),
            'cgc_10_price': None,  # Not provided by PriceCharting
            'cgc_9_5_price': to_dollars(data.get('box-only-price')),
            'cgc_9_price': None,  # Not provided by PriceCharting
            'graded_generic_price': to_dollars(data.get('graded-price')),
            'raw_ungraded_price': to_dollars(data.get('loose-price')),
            'new_mint_price': to_dollars(data.get('new-price')),
            'complete_price': to_dollars(data.get('cib-price')),
            'sales_volume': int(data.get('sales-volume', 0)) if data.get('sales-volume') else None,
            'recorded_at': datetime.utcnow().isoformat(),
            'data_source': 'pricecharting'
        }

    def save_market_values(self, values: Dict) -> bool:
        """
        Save market values to database.

        Args:
            values: Parsed market value data

        Returns:
            True if successful, False otherwise
        """
        if not self.db_manager:
            print("No database manager available")
            return False

        insert_sql = """
        INSERT INTO market_values (
            product_id, product_name, recorded_at,
            bgs_10_price, psa_10_price, psa_9_price, psa_8_price, psa_7_price,
            cgc_10_price, cgc_9_5_price, cgc_9_price,
            graded_generic_price, raw_ungraded_price, new_mint_price, complete_price,
            sales_volume, data_source
        ) VALUES (
            %(product_id)s, %(product_name)s, %(recorded_at)s,
            %(bgs_10_price)s, %(psa_10_price)s, %(psa_9_price)s, %(psa_8_price)s, %(psa_7_price)s,
            %(cgc_10_price)s, %(cgc_9_5_price)s, %(cgc_9_price)s,
            %(graded_generic_price)s, %(raw_ungraded_price)s, %(new_mint_price)s, %(complete_price)s,
            %(sales_volume)s, %(data_source)s
        )
        ON CONFLICT (product_id, recorded_at) DO NOTHING;
        """

        try:
            with self.db_manager.conn.cursor() as cursor:
                cursor.execute(insert_sql, values)
                self.db_manager.conn.commit()
                return True
        except Exception as e:
            print(f"Error saving market values: {e}")
            self.db_manager.conn.rollback()
            return False

    def run(self, product_id: str = LUGIA_PRODUCT_ID) -> Dict:
        """
        Run the market value tracker.

        Args:
            product_id: PriceCharting product ID to track

        Returns:
            Dict with run statistics
        """
        print("="*60)
        print("Market Value Tracker")
        print("="*60)
        print(f"Fetching market values for product ID: {product_id}")

        # Fetch market values
        raw_data = self.fetch_market_values(product_id)

        if not raw_data:
            return {
                'status': 'error',
                'error_message': 'Failed to fetch market values from PriceCharting'
            }

        # Parse values
        values = self.parse_market_values(raw_data)

        if not values:
            return {
                'status': 'error',
                'error_message': 'Failed to parse market values'
            }

        print(f"\n✅ Fetched market values for: {values['product_name']}")
        print("\nCurrent Market Values:")
        print("-" * 60)

        if values['psa_10_price']:
            print(f"PSA 10:        ${values['psa_10_price']:>10,.2f}")
        if values['psa_9_price']:
            print(f"PSA 9:         ${values['psa_9_price']:>10,.2f}")
        if values['bgs_10_price']:
            print(f"BGS 10:        ${values['bgs_10_price']:>10,.2f}")
        if values['graded_generic_price']:
            print(f"Graded (gen):  ${values['graded_generic_price']:>10,.2f}")
        if values['raw_ungraded_price']:
            print(f"Raw/Ungraded:  ${values['raw_ungraded_price']:>10,.2f}")
        if values['new_mint_price']:
            print(f"New/Mint:      ${values['new_mint_price']:>10,.2f}")

        if values['sales_volume']:
            print(f"\nSales Volume:  {values['sales_volume']} sales")

        # Save to database
        if self.db_manager:
            if self.save_market_values(values):
                print("\n✅ Saved market values to database")
            else:
                return {
                    'status': 'error',
                    'error_message': 'Failed to save to database'
                }
        else:
            print("\n⚠️  Running in test mode (no database)")

        print("="*60 + "\n")

        return {
            'status': 'success',
            'product_id': values['product_id'],
            'product_name': values['product_name'],
            'recorded_at': values['recorded_at'],
            'market_values': values
        }


def main():
    """Main entry point."""

    # Import database manager
    try:
        from database import DatabaseManager
        db_url = os.getenv('DATABASE_URL')
        if not db_url:
            print("WARNING: DATABASE_URL not set. Running in test mode without database.")
            db_manager = None
        else:
            db_manager = DatabaseManager(db_url)
            print("✅ Database connected successfully")
    except ImportError:
        print("WARNING: Database module not found. Running in test mode.")
        db_manager = None

    # Initialize and run tracker
    tracker = MarketValueTracker(db_manager)
    result = tracker.run(LUGIA_PRODUCT_ID)

    # Exit with appropriate code
    if result['status'] == 'error':
        print(f"❌ ERROR: {result.get('error_message')}")
        exit(1)
    else:
        print("✅ Market value tracker completed successfully")
        exit(0)


if __name__ == '__main__':
    main()
