"""
Card Discovery Engine

Searches eBay for Pokemon cards and stores individual listing details.
No aggregate statistics computed (eBay API compliant).
"""

import os
import time
from typing import Dict, List, Optional
from dotenv import load_dotenv
from database import DatabaseManager

# Load environment
load_dotenv('ebay.env')

# Import eBay API from existing tracker
import sys
sys.path.insert(0, os.path.dirname(__file__))
from lugia_tracker import EbayAPI


class CardDiscoveryEngine:
    """Discover Pokemon cards on eBay and store individual listings."""

    def __init__(self, db: DatabaseManager, ebay: EbayAPI):
        self.db = db
        self.ebay = ebay

    def discover_listings(self, max_listings: int = 2000, price_filter: float = 50.0) -> List[Dict]:
        """
        Search eBay for Pokemon cards and collect individual listing details.

        Args:
            max_listings: Maximum listings to fetch from eBay
            price_filter: Only fetch listings >= this price

        Returns:
            List of individual listing dictionaries
        """
        print(f"\n{'='*60}")
        print("Card Discovery - Individual Listings")
        print(f"{'='*60}\n")

        # Fetch listings from Pokemon TCG category with price filter
        print(f"Searching Pokemon TCG category for listings >= ${price_filter}...")
        listings = self.ebay.search_by_category(
            min_price=price_filter,
            max_results=max_listings,
            query="Pokemon"
        )

        if not listings:
            print("  No listings found")
            return []

        print(f"  Found {len(listings)} listings >= ${price_filter}")

        # Extract individual listing details
        individual_listings = self._extract_listing_details(listings)

        print(f"\nCollected {len(individual_listings)} individual listing details")
        return individual_listings

    def _extract_listing_details(self, listings: List[Dict]) -> List[Dict]:
        """
        Extract details from each listing using the getItem API.

        Args:
            listings: List of eBay search result listings

        Returns:
            List of individual listing detail dictionaries
        """
        individual_listings = []
        processed = 0

        print(f"  Fetching detailed item data for {len(listings)} listings...")

        for listing in listings:
            item_id = listing.get('itemId')
            if not item_id:
                continue

            # Fetch full item details
            details = self.ebay.get_item_details(item_id)
            if not details:
                continue

            # Parse card info from structured API data
            card_info = self._parse_card_from_api_details(details)
            if not card_info:
                continue

            # Get price
            price_data = details.get('price', {})
            if isinstance(price_data, dict):
                price = float(price_data.get('value', 0))
            else:
                price = 0.0

            if price > 0:
                variants = card_info.get('variant_attributes', {})
                individual_listings.append({
                    'item_id': item_id,
                    'title': details.get('title', ''),
                    'card_name': card_info['card_name'],
                    'set_name': card_info['set_name'],
                    'card_number': card_info.get('card_number'),
                    'variant_attributes': variants,
                    'grade': variants.get('grade'),
                    'grading_company': variants.get('grading_company'),
                    'price': price,
                    'condition': variants.get('condition') or details.get('condition'),
                    'seller_username': details.get('seller', {}).get('username'),
                    'seller_feedback': details.get('seller', {}).get('feedbackPercentage'),
                    'url': details.get('itemWebUrl'),
                    'image_url': details.get('image', {}).get('imageUrl'),
                    'listing_type': 'AUCTION' if details.get('currentBidPrice') else 'FIXED_PRICE',
                    'is_auction': bool(details.get('currentBidPrice'))
                })

            processed += 1
            if processed % 20 == 0:
                print(f"    Processed {processed}/{len(listings)}...")
                time.sleep(0.5)  # Rate limiting

        print(f"  Processed {processed} listings")
        return individual_listings

    def _parse_card_from_api_details(self, details: Dict) -> Optional[Dict]:
        """
        Parse card information from eBay API item details.

        Args:
            details: Full item details from getItem API

        Returns:
            Dictionary with card details, or None
        """
        aspects = details.get('localizedAspects', [])

        # Convert aspects list to dictionary
        aspect_dict = {}
        for aspect in aspects:
            name = aspect.get('name', '')
            value = aspect.get('value', '')
            aspect_dict[name] = value

        parsed = {}

        # Extract card name
        parsed['card_name'] = aspect_dict.get('Card Name', aspect_dict.get('Character', None))
        if not parsed['card_name']:
            return None

        # Extract set name
        parsed['set_name'] = aspect_dict.get('Set', None)
        if not parsed['set_name']:
            return None

        # Extract card number
        parsed['card_number'] = aspect_dict.get('Card Number', None)

        # Variant attributes from structured data
        parsed['variant_attributes'] = {
            'is_graded': aspect_dict.get('Graded', '').lower() == 'yes',
            'grade': aspect_dict.get('Grade', None),
            'grading_company': aspect_dict.get('Professional Grader', None),
            'is_holo': 'Holo' in aspect_dict.get('Features', ''),
            'is_reverse_holo': 'Reverse Holo' in aspect_dict.get('Features', ''),
            'is_1st_edition': '1st Edition' in aspect_dict.get('Features', ''),
            'is_shadowless': 'Shadowless' in aspect_dict.get('Features', ''),
            'is_full_art': 'Full Art' in aspect_dict.get('Features', ''),
            'is_alt_art': 'Alternate Art' in aspect_dict.get('Features', '') or 'Alt Art' in aspect_dict.get('Features', ''),
            'is_secret_rare': 'Secret Rare' in aspect_dict.get('Rarity', ''),
            'is_rainbow_rare': 'Rainbow Rare' in aspect_dict.get('Rarity', ''),
            'condition': aspect_dict.get('Condition', details.get('condition', None)),
            'year': aspect_dict.get('Year Manufactured', None),
            'language': aspect_dict.get('Language', 'English'),
            'finish': aspect_dict.get('Finish', None),
        }

        return parsed


def main():
    """Run card discovery."""
    start_time = time.time()

    # Connect to database
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        print("ERROR: DATABASE_URL not set!")
        return

    db = DatabaseManager(db_url)

    # Initialize eBay API
    ebay = EbayAPI(
        app_id=os.getenv('EBAY_APP_ID'),
        cert_id=os.getenv('EBAY_CERT_ID')
    )

    if not ebay.get_access_token():
        print("ERROR: Failed to authenticate with eBay!")
        return

    # Create discovery engine
    discovery = CardDiscoveryEngine(db, ebay)

    try:
        # Get parameters from environment or use defaults
        max_listings = int(os.getenv('MAX_LISTINGS', '2000'))
        price_filter = float(os.getenv('PRICE_FILTER', '50.0'))

        print(f"Discovery parameters:")
        print(f"  Max Listings to Fetch: {max_listings}")
        print(f"  Price Filter: ${price_filter}\n")

        # Discover individual listings
        individual_listings = discovery.discover_listings(
            max_listings=max_listings,
            price_filter=price_filter
        )

        # Save individual listings to database
        if individual_listings:
            print(f"\nSaving {len(individual_listings)} individual listings...")
            saved_count = db.save_discovered_listings_batch(individual_listings)
            print(f"Saved {saved_count} listings")

        execution_time = time.time() - start_time
        print(f"\nTotal execution time: {execution_time:.1f} seconds")

    except Exception as e:
        print(f"\nDiscovery failed: {e}")
        raise

    finally:
        db.close()


if __name__ == '__main__':
    main()
