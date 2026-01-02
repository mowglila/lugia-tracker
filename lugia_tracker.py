"""
Lugia Neo Genesis Holo 1st Edition Tracker

This script tracks Lugia Neo Genesis Holo 1st Edition listings on eBay,
stores results in a PostgreSQL database, and tracks price trends over time.
"""

import os
import re
import time
import requests
from base64 import b64encode
from datetime import datetime
from typing import Dict, List, Optional
from dotenv import load_dotenv
from grade_matcher import GradeMatcher


# Load environment variables
load_dotenv('ebay.env')

# eBay API Configuration
EBAY_APP_ID = os.getenv('EBAY_APP_ID')
EBAY_CERT_ID = os.getenv('EBAY_CERT_ID')
EBAY_OAUTH_URL = 'https://api.ebay.com/identity/v1/oauth2/token'
EBAY_BROWSE_API_URL = 'https://api.ebay.com/buy/browse/v1'
POKEMON_CATEGORY = '183454'

# Search Configuration
LUGIA_QUERY = 'Lugia Neo Genesis 1st Edition Holo'

# Grade classification patterns
GRADE_PATTERNS = {
    'PSA 10': r'PSA\s*10',
    'PSA 9': r'PSA\s*9',
    'PSA 8': r'PSA\s*8',
    'PSA 7': r'PSA\s*7',
    'CGC 10': r'CGC\s*10|CGC\s*Pristine',
    'CGC 9.5': r'CGC\s*9\.5|CGC\s*Gem',
    'CGC 9': r'CGC\s*9(?!\.5)',
    'BGS 10': r'BGS\s*10|Black\s*Label',
    'BGS 9.5': r'BGS\s*9\.5',
    'BGS 9': r'BGS\s*9(?!\.5)',
    'Raw': r'Raw|Ungraded|NM|Near\s*Mint',
}


class EbayAPI:
    """Handle eBay API authentication and requests."""

    def __init__(self, app_id: str, cert_id: str):
        self.app_id = app_id
        self.cert_id = cert_id
        self.access_token = None

    def get_access_token(self) -> Optional[str]:
        """Get OAuth 2.0 access token from eBay API."""
        credentials = f"{self.app_id}:{self.cert_id}"
        b64_credentials = b64encode(credentials.encode()).decode()

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': f'Basic {b64_credentials}'
        }

        data = {
            'grant_type': 'client_credentials',
            'scope': 'https://api.ebay.com/oauth/api_scope'
        }

        try:
            response = requests.post(EBAY_OAUTH_URL, headers=headers, data=data)
            response.raise_for_status()
            token_data = response.json()
            self.access_token = token_data.get('access_token')
            return self.access_token
        except requests.exceptions.RequestException as e:
            print(f"Error getting access token: {e}")
            return None

    def search_listings(self, query: str, limit: int = 200, filter_sold: bool = False) -> List[Dict]:
        """
        Search for listings on eBay.

        Args:
            query: Search query string
            limit: Maximum results to return (max 200)
            filter_sold: If True, only return sold/completed listings

        Returns:
            List of item summary dictionaries
        """
        if not self.access_token:
            print("No access token available")
            return []

        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'X-EBAY-C-MARKETPLACE-ID': 'EBAY_US'
        }

        params = {
            'q': query,
            'category_ids': POKEMON_CATEGORY,
            'limit': min(limit, 200)
        }

        # Add filter for sold listings
        if filter_sold:
            params['filter'] = 'buyingOptions:{FIXED_PRICE|AUCTION},conditions:{USED|NEW}'
            # Note: eBay Browse API doesn't have a direct "sold items" filter
            # We need to use the Finding API for sold listings data
            return self._search_sold_listings(query, limit)

        try:
            response = requests.get(
                f'{EBAY_BROWSE_API_URL}/item_summary/search',
                headers=headers,
                params=params
            )
            response.raise_for_status()
            data = response.json()
            return data.get('itemSummaries', [])
        except requests.exceptions.RequestException as e:
            print(f"Error searching listings: {e}")
            return []

    def search_listings_paginated(self, query: str, max_results: int = 1000,
                                   per_page: int = 200) -> List[Dict]:
        """
        Search for listings with pagination to get more results.

        Args:
            query: Search query string
            max_results: Maximum total results to fetch (default 1000)
            per_page: Results per page (max 200)

        Returns:
            List of all item summary dictionaries across all pages
        """
        if not self.access_token:
            print("No access token available")
            return []

        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'X-EBAY-C-MARKETPLACE-ID': 'EBAY_US'
        }

        all_items = []
        offset = 0
        per_page = min(per_page, 200)  # eBay max is 200

        while offset < max_results:
            params = {
                'q': query,
                'category_ids': POKEMON_CATEGORY,
                'limit': per_page,
                'offset': offset
            }

            try:
                response = requests.get(
                    f'{EBAY_BROWSE_API_URL}/item_summary/search',
                    headers=headers,
                    params=params
                )
                response.raise_for_status()
                data = response.json()

                items = data.get('itemSummaries', [])
                if not items:
                    # No more results
                    break

                all_items.extend(items)

                # Check if we've reached the end
                total_available = data.get('total', 0)
                if offset + len(items) >= total_available:
                    break

                offset += per_page
                time.sleep(0.3)  # Rate limiting between pages

            except requests.exceptions.RequestException as e:
                print(f"Error searching listings (offset {offset}): {e}")
                break

        return all_items[:max_results]

    def search_by_category(self, category_id: str = None, min_price: float = 50.0,
                           max_results: int = 2000, query: str = "Pokemon") -> List[Dict]:
        """
        Search listings by category with price filter.

        This is more efficient than keyword-based search for broad discovery.
        Uses eBay's filter parameter to only return listings >= min_price.

        Args:
            category_id: eBay category ID (default: Pokemon TCG 183454)
            min_price: Minimum price filter in USD
            max_results: Maximum total results to fetch
            query: Optional keyword to narrow results (default: "Pokemon")

        Returns:
            List of item summaries >= min_price
        """
        if not self.access_token:
            print("No access token available")
            return []

        if category_id is None:
            category_id = POKEMON_CATEGORY

        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'X-EBAY-C-MARKETPLACE-ID': 'EBAY_US'
        }

        all_items = []
        offset = 0
        per_page = 200

        # Build price filter for eBay API
        price_filter = f"price:[{min_price}..],priceCurrency:USD"

        while offset < max_results:
            params = {
                'category_ids': category_id,
                'filter': price_filter,
                'limit': per_page,
                'offset': offset,
                'sort': 'newlyListed'  # Sort by newest listings for better price distribution
            }

            # Add query if provided
            if query:
                params['q'] = query

            # Retry logic with exponential backoff for rate limiting
            max_retries = 5
            retry_delay = 2  # Start with 2 seconds

            for attempt in range(max_retries):
                try:
                    response = requests.get(
                        f'{EBAY_BROWSE_API_URL}/item_summary/search',
                        headers=headers,
                        params=params
                    )

                    # Handle rate limiting (429)
                    if response.status_code == 429:
                        if attempt < max_retries - 1:
                            wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                            print(f"  Rate limited (429). Waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                            time.sleep(wait_time)
                            continue
                        else:
                            print(f"  Rate limited after {max_retries} retries. Returning partial results.")
                            return all_items[:max_results]

                    response.raise_for_status()
                    data = response.json()

                    items = data.get('itemSummaries', [])
                    if not items:
                        return all_items[:max_results]

                    all_items.extend(items)

                    # Check if we've reached the end
                    total_available = data.get('total', 0)
                    print(f"  Fetched {len(all_items)}/{total_available} listings >= ${min_price}...")

                    if offset + len(items) >= total_available:
                        return all_items[:max_results]

                    offset += per_page
                    time.sleep(0.5)  # Rate limiting between pages
                    break  # Success, exit retry loop

                except requests.exceptions.RequestException as e:
                    if attempt < max_retries - 1 and '429' in str(e):
                        wait_time = retry_delay * (2 ** attempt)
                        print(f"  Request error (retrying in {wait_time}s): {e}")
                        time.sleep(wait_time)
                        continue
                    print(f"Error searching by category (offset {offset}): {e}")
                    return all_items[:max_results]

        return all_items[:max_results]

    def _search_sold_listings(self, query: str, limit: int = 200) -> List[Dict]:
        """
        Search for SOLD listings using eBay Finding API.

        Note: This uses the Finding API which has different authentication.
        For production, you'd want to use the proper Finding API with App ID.

        Args:
            query: Search query string
            limit: Maximum results

        Returns:
            List of sold item dictionaries
        """
        # eBay Finding API endpoint (public, uses App ID instead of OAuth)
        finding_url = 'https://svcs.ebay.com/services/search/FindingService/v1'

        params = {
            'OPERATION-NAME': 'findCompletedItems',
            'SERVICE-VERSION': '1.0.0',
            'SECURITY-APPNAME': self.app_id,
            'RESPONSE-DATA-FORMAT': 'JSON',
            'REST-PAYLOAD': '',
            'keywords': query,
            'categoryId': POKEMON_CATEGORY,
            'itemFilter(0).name': 'SoldItemsOnly',
            'itemFilter(0).value': 'true',
            'paginationInput.entriesPerPage': min(limit, 100),
            'sortOrder': 'EndTimeSoonest'
        }

        try:
            response = requests.get(finding_url, params=params)
            response.raise_for_status()
            data = response.json()

            # Extract items from Finding API response
            search_result = data.get('findCompletedItemsResponse', [{}])[0]
            items = search_result.get('searchResult', [{}])[0].get('item', [])

            # Convert Finding API format to Browse API-like format
            converted_items = []
            for item in items:
                converted_items.append({
                    'itemId': item.get('itemId', [''])[0],
                    'title': item.get('title', [''])[0],
                    'price': {
                        'value': item.get('sellingStatus', [{}])[0].get('currentPrice', [{}])[0].get('__value__', '0'),
                        'currency': 'USD'
                    },
                    'condition': item.get('condition', [{}])[0].get('conditionDisplayName', [''])[0],
                    'sold': True
                })

            return converted_items

        except Exception as e:
            print(f"Error searching sold listings: {e}")
            return []

    def get_item_details(self, item_id: str) -> Optional[Dict]:
        """
        Get detailed item information including condition descriptors and item specifics.

        Args:
            item_id: eBay item ID (e.g., 'v1|374277497596|0')

        Returns:
            Full item details dict or None if error
        """
        if not self.access_token:
            print("No access token available")
            return None

        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'X-EBAY-C-MARKETPLACE-ID': 'EBAY_US'
        }

        # Retry logic with exponential backoff for rate limiting
        max_retries = 3
        retry_delay = 1

        for attempt in range(max_retries):
            try:
                response = requests.get(
                    f'{EBAY_BROWSE_API_URL}/item/{item_id}',
                    headers=headers
                )

                # Handle rate limiting (429)
                if response.status_code == 429:
                    if attempt < max_retries - 1:
                        wait_time = retry_delay * (2 ** attempt)
                        time.sleep(wait_time)
                        continue
                    return None

                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1 and '429' in str(e):
                    wait_time = retry_delay * (2 ** attempt)
                    time.sleep(wait_time)
                    continue
                return None

        return None


class ListingParser:
    """Parse and validate eBay listings."""

    @staticmethod
    def extract_condition_from_item_details(item_details: Dict) -> Optional[str]:
        """
        Extract detailed condition from item conditionDescriptors or localizedAspects.

        For trading cards, conditionDescriptors may include:
        - Card Condition (e.g., "Near Mint or Better", "Lightly Played (Excellent)")
        - Professional Grader (e.g., "PSA", "CGC")
        - Grade (e.g., "10", "9")

        Args:
            item_details: Full item response from getItem API

        Returns:
            Condition string (e.g., "Near Mint or Better", "Lightly Played") or None
        """
        # Check conditionDescriptors (trading card specific)
        condition_descriptors = item_details.get('conditionDescriptors', [])
        for descriptor in condition_descriptors:
            name = descriptor.get('name', '')

            # Look for "Card Condition" descriptor
            if 'Card Condition' in name:
                values = descriptor.get('values', [])
                if values and len(values) > 0:
                    # Values are dicts with 'content' and optionally 'additionalInfo'
                    # e.g., {'content': 'Heavily played (Poor)', 'additionalInfo': [...]}
                    first_value = values[0]
                    if isinstance(first_value, dict):
                        return first_value.get('content', None)
                    else:
                        return first_value

        # Also check localizedAspects for condition-related attributes (fallback)
        aspects = item_details.get('localizedAspects', [])
        for aspect in aspects:
            name = aspect.get('name', '')
            if name in ['Condition', 'Card Condition', 'Grade']:
                value = aspect.get('value', '')
                if value:
                    return value

        return None

    @staticmethod
    def is_valid_lugia_listing(title: str) -> bool:
        """
        Check if listing is a valid single Lugia card.
        Returns False for multi-card lots or "choose your card" listings.
        """
        title_upper = title.upper()

        # Exclude patterns for multi-card listings
        invalid_patterns = [
            r'CHOOSE\s+YOUR',
            r'CHOOSE\s+CARD',
            r'SELECT\s+CARD',
            r'PICK\s+YOUR',
            r'PICK\s+CARD',
            r'YOU\s+CHOOSE',
            r'YOU\s+PICK',
            r'MULTIPLE\s+CARDS',
            r'MANY\s+CARDS',
            r'VARIOUS\s+CARDS',
            r'\d+X',  # e.g., "5X Cards"
            r'LOT\s+OF',
            r'BULK',
            r'COLLECTION',
            r'COMPLETE\s+SET',
            r'FULL\s+SET',
        ]

        for pattern in invalid_patterns:
            if re.search(pattern, title_upper):
                return False

        # Must contain "LUGIA" to be valid
        if 'LUGIA' not in title_upper:
            return False

        return True

    @staticmethod
    def classify_grade(title: str) -> str:
        """Classify the card grade based on title."""
        title_upper = title.upper()

        for grade, pattern in GRADE_PATTERNS.items():
            if re.search(pattern, title_upper):
                return grade

        return 'Unknown'

    @staticmethod
    def parse_listing(item: Dict, grade_matcher: GradeMatcher = None, item_details: Dict = None) -> Optional[Dict]:
        """
        Parse eBay listing data into structured format.
        Returns None if listing should be filtered out.

        Args:
            item: eBay API item dict (from search)
            grade_matcher: Optional GradeMatcher for extracting grade/condition info
            item_details: Optional detailed item info (from getItem) with conditionDescriptors
        """
        title = item.get('title', '')

        # Filter out invalid listings
        if not ListingParser.is_valid_lugia_listing(title):
            return None

        # Extract price
        price_info = item.get('price', {})
        price = float(price_info.get('value', 0)) if price_info else 0

        # Extract shipping
        shipping_info = item.get('shippingOptions', [{}])[0] if item.get('shippingOptions') else {}
        shipping_cost = shipping_info.get('shippingCost', {})
        shipping = float(shipping_cost.get('value', 0)) if shipping_cost else 0

        # Total cost
        total_cost = price + shipping

        # Extract condition from eBay - basic condition
        ebay_condition = item.get('condition', 'Unknown')

        # Try to get detailed condition from item details if available
        detailed_condition = None
        if item_details:
            detailed_condition = ListingParser.extract_condition_from_item_details(item_details)

        # Use GradeMatcher if available for better grade/condition extraction
        if grade_matcher:
            # Pass detailed condition if available, otherwise fall back to ebay_condition
            condition_to_check = detailed_condition if detailed_condition else ebay_condition
            grade, raw_condition, is_graded = grade_matcher.extract_grade_and_condition(title, condition_to_check)

            # For raw cards, get comparable grade
            comparable_grade = None
            if not is_graded and raw_condition != 'N/A':
                comparable_grade = grade_matcher.get_condition_comparable_grade(raw_condition)
        else:
            # Fallback to old method
            grade = ListingParser.classify_grade(title)
            raw_condition = 'N/A'
            is_graded = grade not in ['Raw', 'Unknown']
            comparable_grade = None

        # Seller info
        seller = item.get('seller', {})
        seller_username = seller.get('username', 'Unknown')
        seller_feedback = seller.get('feedbackPercentage', None)

        # Buying options
        buying_options = item.get('buyingOptions', [])
        is_auction = 'AUCTION' in buying_options
        is_buy_now = 'FIXED_PRICE' in buying_options

        return {
            'item_id': item.get('itemId', ''),
            'title': title,
            'grade': grade,
            'price': price,
            'shipping': shipping,
            'total_cost': total_cost,
            'condition': ebay_condition,
            'is_graded': is_graded,
            'raw_condition': raw_condition,
            'comparable_grade': comparable_grade,
            'is_auction': is_auction,
            'is_buy_now': is_buy_now,
            'listing_type': 'Auction' if is_auction else 'Buy It Now',
            'seller_username': seller_username,
            'seller_feedback': seller_feedback,
            'url': item.get('itemWebUrl', ''),
            'image_url': item.get('image', {}).get('imageUrl', ''),
            'timestamp': datetime.utcnow().isoformat()
        }


class LugiaTracker:
    """Main tracker class to orchestrate the search and storage."""

    def __init__(self, ebay_api: EbayAPI, db_manager):
        self.ebay_api = ebay_api
        self.db_manager = db_manager
        self.parser = ListingParser()

    def run_search(self) -> Dict:
        """
        Execute a search run and return statistics.
        """
        print(f"Starting search at {datetime.utcnow().isoformat()}")

        # Authenticate
        if not self.ebay_api.get_access_token():
            return {
                'status': 'error',
                'error_message': 'Failed to authenticate with eBay API'
            }

        # Load latest market values for grade matching
        grade_matcher = None
        try:
            with self.db_manager.conn.cursor() as cursor:
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
                    grade_matcher = GradeMatcher(market_values)
                    print("✅ Loaded GradeMatcher with latest market values")
        except Exception as e:
            print(f"⚠️  Could not load market values for grade matching: {e}")

        # Search for listings
        print(f"Searching: {LUGIA_QUERY}")
        raw_items = self.ebay_api.search_listings(LUGIA_QUERY, limit=200)

        # Parse and filter listings
        valid_listings = []
        filtered_count = 0

        print(f"Found {len(raw_items)} raw listings from search")
        print("Fetching detailed item information for condition data...")

        for idx, item in enumerate(raw_items):
            # Get detailed item information to extract condition descriptors
            item_id = item.get('itemId', '')
            item_details = None

            if item_id:
                item_details = self.ebay_api.get_item_details(item_id)
                if item_details:
                    print(f"  [{idx+1}/{len(raw_items)}] Got details for {item_id}")
                else:
                    print(f"  [{idx+1}/{len(raw_items)}] Could not fetch details for {item_id}")

            # Parse with detailed information
            parsed = self.parser.parse_listing(item, grade_matcher, item_details)
            if parsed:
                valid_listings.append(parsed)
            else:
                filtered_count += 1

        print(f"\nFiltered out {filtered_count} invalid listings")
        print(f"Valid listings: {len(valid_listings)}")

        # Store in database
        if self.db_manager:
            try:
                self.db_manager.save_search_run(
                    total_found=len(raw_items),
                    total_filtered=filtered_count,
                    total_valid=len(valid_listings),
                    status='success'
                )

                for listing in valid_listings:
                    self.db_manager.save_listing(listing)

                print("Successfully saved to database")
            except Exception as e:
                print(f"Error saving to database: {e}")
                return {
                    'status': 'error',
                    'error_message': f'Database error: {str(e)}',
                    'total_found': len(raw_items),
                    'total_valid': len(valid_listings)
                }

        return {
            'status': 'success',
            'total_found': len(raw_items),
            'total_filtered': filtered_count,
            'total_valid': len(valid_listings),
            'timestamp': datetime.utcnow().isoformat()
        }


def main():
    """Main entry point for the tracker."""
    print("="*60)
    print("Lugia Neo Genesis 1st Edition Tracker")
    print("="*60)

    # Import database manager
    try:
        from database import DatabaseManager
        db_url = os.getenv('DATABASE_URL')
        if not db_url:
            print("WARNING: DATABASE_URL not set. Running in test mode without database.")
            db_manager = None
        else:
            db_manager = DatabaseManager(db_url)
            print("Database connected successfully")
    except ImportError:
        print("WARNING: Database module not found. Running in test mode.")
        db_manager = None

    # Initialize API
    ebay_api = EbayAPI(EBAY_APP_ID, EBAY_CERT_ID)

    # Initialize tracker
    tracker = LugiaTracker(ebay_api, db_manager)

    # Run search
    result = tracker.run_search()

    print("\n" + "="*60)
    print("Search Complete")
    print("="*60)
    print(f"Status: {result['status']}")
    if result['status'] == 'success':
        print(f"Valid listings found: {result['total_valid']}")
    else:
        print(f"Error: {result.get('error_message', 'Unknown error')}")

    return result


if __name__ == '__main__':
    main()
