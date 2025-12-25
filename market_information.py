"""
Market Information

Uses PriceCharting market values to identify listings worth reviewing.
Compares active listings against PriceCharting's market data for accurate market analysis.
"""

import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from database import DatabaseManager


class MarketInformation:
    """Analyze listings using PriceCharting market values as baseline."""

    def __init__(self, db_manager):
        self.db = db_manager

    def get_latest_market_values(self) -> Dict[str, float]:
        """
        Get the most recent PriceCharting market values for each grade.

        Returns:
            Dict mapping grade -> market_price
        """
        query = """
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
        """

        market_values = {}
        try:
            with self.db.conn.cursor() as cursor:
                cursor.execute(query)
                row = cursor.fetchone()

                if not row:
                    return {}

                # Map PriceCharting grades to our grade naming
                market_values = {
                    'PSA 10': float(row[0]) if row[0] else None,
                    'PSA 9': float(row[1]) if row[1] else None,
                    'PSA 8': float(row[2]) if row[2] else None,
                    'PSA 7': float(row[3]) if row[3] else None,
                    'BGS 10': float(row[4]) if row[4] else None,
                    'CGC 9.5': float(row[5]) if row[5] else None,
                    'CGC 9': float(row[1]) if row[1] else None,  # Use PSA 9 as proxy
                    'Raw': float(row[6]) if row[6] else None,
                    'Ungraded': float(row[6]) if row[6] else None,
                }

        except Exception as e:
            print(f"Error fetching market values: {e}")

        return market_values

    def review_listings(self, threshold_pct: float = 15.0) -> List[Dict]:
        """
        Review listings by comparing active listings to PriceCharting market values.

        Args:
            threshold_pct: % below market value to flag for review (default 15%)

        Returns:
            List of listings with market comparison metadata
        """
        print(f"Reviewing listings using PriceCharting market values...")

        # Get latest market values
        market_values = self.get_latest_market_values()

        if not market_values:
            print("Warning: No market value data available. Cannot review listings.")
            print("   Run market_value_tracker.py first to collect market data.")
            return []

        print(f"Loaded market values for {len([v for v in market_values.values() if v])} grades")

        # Get active listings
        query = """
        SELECT
            item_id,
            title,
            grade,
            total_cost,
            price,
            shipping,
            condition,
            seller_username,
            seller_feedback,
            listing_type,
            is_auction,
            url,
            image_url
        FROM listings
        WHERE is_active = true
          AND total_cost > 0
        """

        reviewed_listings = []
        try:
            with self.db.conn.cursor() as cursor:
                cursor.execute(query)
                active_listings = cursor.fetchall()

            print(f"Analyzing {len(active_listings)} active listings...")

            for listing in active_listings:
                item_id = listing[0]
                title = listing[1]
                grade = listing[2]
                total_cost = float(listing[3])
                price = float(listing[4])
                shipping = float(listing[5])
                condition = listing[6]
                seller_username = listing[7]
                seller_feedback = listing[8]
                listing_type = listing[9]
                is_auction = listing[10]
                url = listing[11]
                image_url = listing[12]

                # Check if we have market value for this grade
                market_price = market_values.get(grade)

                if not market_price:
                    continue

                # Calculate vs. market value
                percent_vs_market = ((total_cost - market_price) / market_price) * 100

                # Check if it's below threshold
                if percent_vs_market <= -threshold_pct:
                    # Determine price quality
                    if percent_vs_market <= -30:
                        price_quality = 'excellent'
                    elif percent_vs_market <= -20:
                        price_quality = 'great'
                    else:
                        price_quality = 'good'

                    reviewed_listings.append({
                        'item_id': item_id,
                        'title': title,
                        'grade': grade,
                        'total_cost': total_cost,
                        'market_price': market_price,
                        'percent_vs_market': percent_vs_market,
                        'price_quality': price_quality,
                        'seller_username': seller_username,
                        'seller_feedback': seller_feedback,
                        'is_auction': is_auction,
                        'listing_type': listing_type,
                        'condition': condition,
                        'url': url,
                        'image_url': image_url,
                        'reviewed_at': datetime.utcnow().isoformat()
                    })

            print(f"Found {len(reviewed_listings)} listings ({threshold_pct}%+ below market value)")

        except Exception as e:
            print(f"Error reviewing listings: {e}")
            return []

        # Sort by best prices first
        reviewed_listings.sort(key=lambda x: x['percent_vs_market'])

        return reviewed_listings

    def save_reviewed_listings(self, reviewed_listings: List[Dict]) -> int:
        """
        Save reviewed listings to database.

        Returns:
            Number of listings saved
        """
        saved_count = 0

        for listing in reviewed_listings:
            insert_sql = """
            INSERT INTO reviewed_listings (
                item_id, title, grade, total_cost, avg_price, percent_below_avg,
                listing_type, seller_username, seller_feedback, is_auction,
                condition, url, image_url, detected_at
            ) VALUES (
                %(item_id)s, %(title)s, %(grade)s, %(total_cost)s, %(market_price)s,
                %(percent_vs_market)s, %(listing_type)s, %(seller_username)s,
                %(seller_feedback)s, %(is_auction)s, %(condition)s,
                %(url)s, %(image_url)s, %(reviewed_at)s
            )
            ON CONFLICT (item_id) DO UPDATE SET
                avg_price = EXCLUDED.avg_price,
                percent_below_avg = EXCLUDED.percent_below_avg,
                detected_at = EXCLUDED.detected_at
            """

            try:
                with self.db.conn.cursor() as cursor:
                    cursor.execute(insert_sql, listing)
                self.db.conn.commit()
                saved_count += 1
            except Exception as e:
                print(f"Error saving listing {listing['item_id']}: {e}")
                self.db.conn.rollback()

        return saved_count

    def generate_report(self, reviewed_listings: List[Dict]):
        """Print a formatted report of reviewed listings."""

        if not reviewed_listings:
            print("\nNo listings found meeting criteria.")
            return

        print("\n" + "="*80)
        print("MARKET REPORT (Based on PriceCharting Market Values)")
        print("="*80)

        # Group by quality
        excellent = [l for l in reviewed_listings if l['price_quality'] == 'excellent']
        great = [l for l in reviewed_listings if l['price_quality'] == 'great']
        good = [l for l in reviewed_listings if l['price_quality'] == 'good']

        if excellent:
            print(f"\n EXCELLENT PRICES (30%+ below market) - {len(excellent)} found")
            print("-"*80)
            for listing in excellent[:5]:
                print(f"\n  {listing['title'][:70]}...")
                print(f"  Grade: {listing['grade']}")
                print(f"  Asking: ${listing['total_cost']:,.2f}")
                print(f"  Market Value: ${listing['market_price']:,.2f}")
                print(f"  Difference: ${listing['market_price'] - listing['total_cost']:,.2f} ({listing['percent_vs_market']:.1f}%)")
                print(f"  {listing['url']}")

        if great:
            print(f"\n GREAT PRICES (20-30% below market) - {len(great)} found")
            print("-"*80)
            for listing in great[:3]:
                print(f"\n  {listing['title'][:70]}...")
                print(f"  Grade: {listing['grade']} | Asking: ${listing['total_cost']:,.2f} | Market: ${listing['market_price']:,.2f}")
                print(f"  {listing['percent_vs_market']:.1f}% below market | {listing['url']}")

        if good:
            print(f"\n GOOD PRICES (15-20% below market) - {len(good)} found")
            print(f"   (See reviewed_listings table for full list)")

        print("\n" + "="*80)


def main():
    """Run market information review."""

    print("="*80)
    print("MARKET INFORMATION (Using PriceCharting Market Values)")
    print("="*80 + "\n")

    # Connect to database
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        print("ERROR: DATABASE_URL not set")
        return

    db = DatabaseManager(db_url)
    market_info = MarketInformation(db)

    # Review listings (15%+ below market value)
    reviewed_listings = market_info.review_listings(threshold_pct=15.0)

    # Generate report
    market_info.generate_report(reviewed_listings)

    # Save to database
    if reviewed_listings:
        saved_count = market_info.save_reviewed_listings(reviewed_listings)
        print(f"\nSaved {saved_count} listings to database")

    db.close()


if __name__ == '__main__':
    main()
