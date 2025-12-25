"""
Card Discovery Engine

Automatically identifies high-demand Pokemon cards on eBay based on:
- Active listing volume (market depth)
- Price ranges (value threshold)
- Sales velocity (sold listings data)

Generates card candidates for tracking approval.
"""

import os
import re
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
from dotenv import load_dotenv
from database import DatabaseManager

# Load environment
load_dotenv('ebay.env')

# Import eBay API from existing tracker
import sys
sys.path.insert(0, os.path.dirname(__file__))
from lugia_tracker import EbayAPI


class CardDiscoveryEngine:
    """Discover high-demand Pokemon cards on eBay."""

    def __init__(self, db: DatabaseManager, ebay: EbayAPI):
        """
        Initialize card discovery engine.

        Args:
            db: DatabaseManager instance
            ebay: EbayAPI instance
        """
        self.db = db
        self.ebay = ebay

    def discover_cards_by_search(self,
                                  search_queries: List[str],
                                  min_price: float = 100.0,
                                  min_listings: int = 10,
                                  max_cards: int = 50,
                                  listings_per_query: int = 500,
                                  price_filter: float = 50.0) -> List[Dict]:
        """
        Discover cards by searching eBay with broad queries.

        Args:
            search_queries: List of search terms to explore
            min_price: Minimum average price to consider for candidates
            min_listings: Minimum listing count to consider
            max_cards: Maximum number of cards to return
            listings_per_query: Max listings to fetch per query (uses pagination if > 200)
            price_filter: Only getItem for listings >= this price (reduces API calls)

        Returns:
            List of discovered card candidates (includes 'prices' for statistics)
        """
        print(f"\n{'='*60}")
        print("Card Discovery Engine")
        print(f"{'='*60}\n")
        print(f"Settings: {listings_per_query} listings/query, ${price_filter:.0f}+ filter\n")

        all_cards = {}  # card_key -> card_data
        total_listings_searched = 0
        total_getitem_calls = 0

        for query in search_queries:
            print(f"\n🔍 Searching: {query}")
            time.sleep(1)  # Rate limiting

            # Search eBay with pagination for more results
            if listings_per_query > 200:
                results = self.ebay.search_listings_paginated(query, max_results=listings_per_query)
            else:
                results = self.ebay.search_listings(query, limit=listings_per_query)

            if not results:
                print(f"  No results found")
                continue

            total_listings_searched += len(results)
            print(f"  Found {len(results)} listings")

            # Extract unique cards from results (with price pre-filter)
            cards_in_query, getitem_calls = self._extract_cards_from_listings(results, price_filter)
            total_getitem_calls += getitem_calls

            # Merge with all_cards
            for card_key, card_data in cards_in_query.items():
                if card_key in all_cards:
                    # Merge data
                    all_cards[card_key]['listing_count'] += card_data['listing_count']
                    all_cards[card_key]['total_value'] += card_data['total_value']
                    all_cards[card_key]['prices'].extend(card_data['prices'])
                else:
                    all_cards[card_key] = card_data

        print(f"\n📈 API Usage: {total_listings_searched} listings searched, {total_getitem_calls} getItem calls")

        # Calculate demand scores and filter
        candidates = []
        for card_key, card_data in all_cards.items():
            # Calculate average price
            avg_price = card_data['total_value'] / card_data['listing_count']

            # Apply filters
            if avg_price < min_price:
                continue
            if card_data['listing_count'] < min_listings:
                continue

            # Calculate popularity (listing count)
            popularity = self._calculate_popularity(card_data['listing_count'])

            candidate = {
                'card_name': card_data['card_name'],
                'set_name': card_data['set_name'],
                'card_number': card_data.get('card_number'),
                'variant_attributes': card_data.get('variant_attributes', {}),
                'listing_count': card_data['listing_count'],
                'avg_price': avg_price,
                'prices': card_data['prices'],  # Include for statistics
                'popularity': popularity,
                'search_query': self._generate_search_query(card_data)
            }

            candidates.append(candidate)

        # Sort by popularity and limit
        candidates.sort(key=lambda x: x['popularity'], reverse=True)
        top_candidates = candidates[:max_cards]

        print(f"\n📊 Discovered {len(top_candidates)} high-demand cards")
        return top_candidates

    def discover_cards_by_category(self,
                                    min_price: float = 100.0,
                                    min_listings: int = 10,
                                    max_cards: int = 50,
                                    max_listings: int = 2000,
                                    price_filter: float = 50.0) -> tuple:
        """
        Discover cards using category-based search with price filter.

        More efficient than keyword search - fetches all Pokemon TCG cards
        above price threshold in a single paginated query.

        Args:
            min_price: Minimum average price for candidates
            min_listings: Minimum listing count for candidates
            max_cards: Maximum candidates to return
            max_listings: Maximum listings to fetch from eBay
            price_filter: Only fetch listings >= this price (applied at API level)

        Returns:
            Tuple of (candidates list, individual listings list)
        """
        print(f"\n{'='*60}")
        print("Card Discovery Engine (Category Mode)")
        print(f"{'='*60}\n")

        # Fetch listings from Pokemon TCG category with price filter
        print(f"🔍 Searching Pokemon TCG category for listings >= ${price_filter}...")
        listings = self.ebay.search_by_category(
            min_price=price_filter,
            max_results=max_listings,
            query="Pokemon"  # Minimal query to stay in Pokemon cards
        )

        if not listings:
            print("  No listings found")
            return [], []

        print(f"  Found {len(listings)} listings >= ${price_filter}")

        # Extract unique cards (no additional filtering needed - already filtered by API)
        all_cards, getitem_calls, individual_listings = self._extract_cards_from_listings(
            listings, min_price_filter=0  # Already filtered by API
        )

        print(f"\n📈 API Usage: {len(listings)} search results, {getitem_calls} getItem calls")

        # Calculate demand scores and filter
        candidates = []
        for card_key, card_data in all_cards.items():
            if card_data['listing_count'] == 0:
                continue

            avg_price = card_data['total_value'] / card_data['listing_count']

            if avg_price < min_price:
                continue
            if card_data['listing_count'] < min_listings:
                continue

            popularity = self._calculate_popularity(card_data['listing_count'])

            candidate = {
                'card_name': card_data['card_name'],
                'set_name': card_data['set_name'],
                'card_number': card_data.get('card_number'),
                'variant_attributes': card_data.get('variant_attributes', {}),
                'listing_count': card_data['listing_count'],
                'avg_price': avg_price,
                'prices': card_data['prices'],
                'popularity': popularity,
                'search_query': self._generate_search_query(card_data)
            }
            candidates.append(candidate)

        candidates.sort(key=lambda x: x['popularity'], reverse=True)
        top_candidates = candidates[:max_cards]

        print(f"\n📊 Discovered {len(top_candidates)} high-popularity cards")
        print(f"📋 Collected {len(individual_listings)} individual listing details")
        return top_candidates, individual_listings

    def _extract_cards_from_listings(self, listings: List[Dict],
                                       min_price_filter: float = 50.0) -> tuple:
        """
        Extract unique cards from eBay listings using full item details API.

        Pre-filters by price to reduce API calls, then fetches complete item data
        for structured card attributes.

        Args:
            listings: List of eBay listing dictionaries
            min_price_filter: Only fetch details for listings >= this price ($50 default)

        Returns:
            Tuple of (card_dict, getitem_call_count, individual_listings)
        """
        cards = {}
        individual_listings = []  # Store individual listing details
        processed = 0
        skipped_low_price = 0

        # Pre-filter listings by price from search results to reduce getItem calls
        filtered_listings = []
        for listing in listings:
            price_data = listing.get('price', {})
            if isinstance(price_data, dict):
                price = float(price_data.get('value', 0))
            else:
                price = 0.0

            if price >= min_price_filter:
                filtered_listings.append(listing)
            else:
                skipped_low_price += 1

        print(f"\n  Pre-filtered: {len(filtered_listings)} listings >= ${min_price_filter:.0f} (skipped {skipped_low_price} low-price)")
        print(f"  Fetching detailed item data for {len(filtered_listings)} listings...")

        for idx, listing in enumerate(filtered_listings):
            item_id = listing.get('itemId')
            if not item_id:
                continue

            # Fetch full item details for accurate card data
            details = self.ebay.get_item_details(item_id)
            if not details:
                continue

            # Parse card info from structured API data
            card_info = self._parse_card_from_api_details(details)
            if not card_info:
                continue

            # Create unique card key based on all identifying attributes
            card_key = self._generate_card_key(card_info)

            if card_key not in cards:
                cards[card_key] = {
                    'card_name': card_info['card_name'],
                    'set_name': card_info['set_name'],
                    'card_number': card_info.get('card_number'),
                    'variant_attributes': card_info.get('variant_attributes', {}),
                    'listing_count': 0,
                    'total_value': 0.0,
                    'prices': []
                }

            # Add this listing's data
            price_data = details.get('price', {})
            if isinstance(price_data, dict):
                price = float(price_data.get('value', 0))
            else:
                price = 0.0

            if price > 0:
                cards[card_key]['listing_count'] += 1
                cards[card_key]['total_value'] += price
                cards[card_key]['prices'].append(price)

                # Collect individual listing details for database
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
                print(f"    Processed {processed}/{len(filtered_listings)}...")
                time.sleep(0.5)  # Rate limiting

        print(f"  ✅ Processed {processed} listings, found {len(cards)} unique cards")
        return cards, len(filtered_listings), individual_listings

    def _generate_card_key(self, card_info: Dict) -> str:
        """
        Generate unique key for a card that includes all variant attributes.

        This ensures we don't mix:
        - Charizard 4/102 Holo 1st Edition (worth $10,000)
        - Charizard 4/102 Holo Unlimited (worth $500)
        - Charizard 4/102 Non-Holo (worth $50)

        Args:
            card_info: Parsed card information dictionary

        Returns:
            Unique card key string
        """
        key_parts = [
            str(card_info['card_name']),
            str(card_info['set_name']),
            str(card_info.get('card_number') or 'UNKNOWN')
        ]

        # Add variant attributes that make cards distinct
        variants = card_info.get('variant_attributes', {})

        if variants.get('is_1st_edition'):
            key_parts.append('1ST')
        if variants.get('is_shadowless'):
            key_parts.append('SHADOWLESS')
        if variants.get('is_holo'):
            key_parts.append('HOLO')
        if variants.get('is_reverse_holo'):
            key_parts.append('REVERSE')
        if variants.get('is_full_art'):
            key_parts.append('FULLART')
        if variants.get('is_alt_art'):
            key_parts.append('ALTART')
        if variants.get('is_secret_rare'):
            key_parts.append('SECRET')
        if variants.get('is_rainbow_rare'):
            key_parts.append('RAINBOW')

        return '|'.join(key_parts)

    def _parse_card_from_api_details(self, details: Dict) -> Optional[Dict]:
        """
        Parse card information from eBay API item details (localizedAspects).

        This is more reliable than title parsing as it uses structured data.

        Args:
            details: Full item details from getItem API

        Returns:
            Dictionary with card details including variant_attributes, or None
        """
        aspects = details.get('localizedAspects', [])

        # Convert aspects list to dictionary for easier lookup
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
            # Grading info
            'is_graded': aspect_dict.get('Graded', '').lower() == 'yes',
            'grade': aspect_dict.get('Grade', None),
            'grading_company': aspect_dict.get('Professional Grader', None),

            # Card features
            'is_holo': 'Holo' in aspect_dict.get('Features', ''),
            'is_reverse_holo': 'Reverse Holo' in aspect_dict.get('Features', ''),
            'is_1st_edition': '1st Edition' in aspect_dict.get('Features', ''),
            'is_shadowless': 'Shadowless' in aspect_dict.get('Features', ''),
            'is_full_art': 'Full Art' in aspect_dict.get('Features', ''),
            'is_alt_art': 'Alternate Art' in aspect_dict.get('Features', '') or 'Alt Art' in aspect_dict.get('Features', ''),
            'is_secret_rare': 'Secret Rare' in aspect_dict.get('Rarity', ''),
            'is_rainbow_rare': 'Rainbow Rare' in aspect_dict.get('Rarity', ''),

            # Condition for ungraded
            'condition': aspect_dict.get('Condition', details.get('condition', None)),

            # Other useful data
            'year': aspect_dict.get('Year Manufactured', None),
            'language': aspect_dict.get('Language', 'English'),
            'finish': aspect_dict.get('Finish', None),
        }

        return parsed

    def _parse_card_from_title(self, title: str) -> Optional[Dict]:
        """
        Parse comprehensive card information from eBay title.

        Extracts all identifying attributes to ensure we don't mix different card variants.

        Args:
            title: eBay listing title

        Returns:
            Dictionary with card details including variant_attributes, or None
        """
        title_upper = title.upper()
        parsed = {}

        # Known Pokemon TCG sets
        sets = {
            'BASE SET': 'Base Set',
            'SHADOWLESS': 'Base Set Shadowless',
            'JUNGLE': 'Jungle',
            'FOSSIL': 'Fossil',
            'TEAM ROCKET': 'Team Rocket',
            'GYM HEROES': 'Gym Heroes',
            'GYM CHALLENGE': 'Gym Challenge',
            'NEO GENESIS': 'Neo Genesis',
            'NEO DISCOVERY': 'Neo Discovery',
            'NEO REVELATION': 'Neo Revelation',
            'NEO DESTINY': 'Neo Destiny',
            'LEGENDARY COLLECTION': 'Legendary Collection',
            'EXPEDITION': 'Expedition',
            'AQUAPOLIS': 'Aquapolis',
            'SKYRIDGE': 'Skyridge',
            'EX RUBY': 'EX Ruby & Sapphire',
            'EX SAPPHIRE': 'EX Ruby & Sapphire',
            'HIDDEN FATES': 'Hidden Fates',
            'SHINING FATES': 'Shining Fates',
            'EVOLVING SKIES': 'Evolving Skies',
            'CELEBRATIONS': 'Celebrations',
            'BRILLIANT STARS': 'Brilliant Stars',
            'CROWN ZENITH': 'Crown Zenith',
        }

        # Find set name
        parsed['set_name'] = None
        for key, value in sets.items():
            if key in title_upper:
                parsed['set_name'] = value
                break

        if not parsed['set_name']:
            return None

        # Popular Pokemon names
        pokemon_names = [
            'CHARIZARD', 'PIKACHU', 'MEWTWO', 'LUGIA', 'HO-OH', 'RAYQUAZA',
            'BLASTOISE', 'VENUSAUR', 'GYARADOS', 'DRAGONITE', 'ALAKAZAM',
            'GENGAR', 'MACHAMP', 'RAICHU', 'ZAPDOS', 'ARTICUNO', 'MOLTRES',
            'MEW', 'CELEBI', 'ESPEON', 'UMBREON', 'TYPHLOSION', 'FERALIGATR',
            'MEGANIUM', 'TYRANITAR', 'SUICUNE', 'ENTEI', 'RAIKOU',
            'EEVEE', 'SNORLAX', 'LAPRAS', 'JOLTEON', 'FLAREON', 'VAPOREON'
        ]

        # Find Pokemon name
        parsed['card_name'] = None
        for pokemon in pokemon_names:
            if pokemon in title_upper:
                parsed['card_name'] = pokemon.title()
                break

        if not parsed['card_name']:
            return None

        # Extract card number (e.g., "4/102")
        card_num_pattern = re.search(r'(\d+)/(\d+)', title)
        if card_num_pattern:
            parsed['card_number'] = card_num_pattern.group(0)
        else:
            parsed['card_number'] = None

        # Variant attributes that make cards unique
        parsed['variant_attributes'] = {
            'is_holo': 'HOLO' in title_upper and 'REVERSE' not in title_upper,
            'is_reverse_holo': 'REVERSE HOLO' in title_upper or ('REVERSE' in title_upper and 'HOLO' in title_upper),
            'is_1st_edition': '1ST EDITION' in title_upper or '1ST ED' in title_upper,
            'is_shadowless': 'SHADOWLESS' in title_upper,
            'is_unlimited': 'UNLIMITED' in title_upper,
            'is_full_art': 'FULL ART' in title_upper or 'FA' in title_upper,
            'is_alt_art': 'ALT ART' in title_upper or 'ALTERNATE ART' in title_upper,
            'is_secret_rare': 'SECRET' in title_upper,
            'is_rainbow_rare': 'RAINBOW' in title_upper,
            'is_gold': 'GOLD' in title_upper,
            'is_shining': 'SHINING' in title_upper and 'FATES' not in title_upper,
        }

        return parsed

    def _calculate_popularity(self, listing_count: int) -> float:
        """
        Calculate popularity for a card.

        Currently uses listing count as a simple volume metric.

        Args:
            listing_count: Number of active listings

        Returns:
            Popularity (listing count)
        """
        return float(listing_count)

    def _generate_search_query(self, card_data: Dict) -> str:
        """
        Generate optimized eBay search query for a card.

        Args:
            card_data: Card information dictionary

        Returns:
            Search query string
        """
        return f"{card_data['card_name']} {card_data['set_name']} Holo"

    def save_candidates(self, candidates: List[Dict]):
        """
        Save discovered candidates to database.

        Args:
            candidates: List of card candidate dictionaries
        """
        saved = 0
        for candidate in candidates:
            try:
                with self.db.conn.cursor() as cursor:
                    cursor.execute('''
                        INSERT INTO card_candidates
                        (card_name, set_name, listing_count, avg_price, popularity, status)
                        VALUES (%s, %s, %s, %s, %s, 'pending')
                        ON CONFLICT (card_name, set_name)
                        DO UPDATE SET
                            listing_count = EXCLUDED.listing_count,
                            avg_price = EXCLUDED.avg_price,
                            popularity = EXCLUDED.popularity,
                            discovered_at = CURRENT_TIMESTAMP
                    ''', (
                        candidate['card_name'],
                        candidate['set_name'],
                        candidate['listing_count'],
                        candidate['avg_price'],
                        candidate['popularity']
                    ))
                self.db.conn.commit()
                saved += 1
            except Exception as e:
                print(f"Error saving candidate {candidate['card_name']}: {e}")
                self.db.conn.rollback()

        print(f"✅ Saved {saved} candidates to database")


def main():
    """Run card discovery."""
    import time

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
        # Get parameters from environment (for GitHub Actions) or use defaults
        min_price = float(os.getenv('MIN_PRICE', '100.0'))
        min_listings = int(os.getenv('MIN_LISTINGS', '10'))
        max_cards = int(os.getenv('MAX_CARDS', '50'))
        max_listings = int(os.getenv('MAX_LISTINGS', '2000'))
        price_filter = float(os.getenv('PRICE_FILTER', '50.0'))

        print(f"Discovery parameters:")
        print(f"  Min Price: ${min_price}")
        print(f"  Min Listings: {min_listings}")
        print(f"  Max Cards: {max_cards}")
        print(f"  Max Listings to Fetch: {max_listings}")
        print(f"  Price Filter for getItem: ${price_filter}\n")

        # Discover cards using category-based search (more efficient)
        candidates, individual_listings = discovery.discover_cards_by_category(
            min_price=min_price,
            min_listings=min_listings,
            max_cards=max_cards,
            max_listings=max_listings,
            price_filter=price_filter
        )

        # Display results
        print(f"\n{'='*80}")
        print("TOP DISCOVERED CARDS")
        print(f"{'='*80}\n")

        for i, card in enumerate(candidates[:20], 1):
            print(f"{i}. {card['card_name']} ({card['set_name']})")
            print(f"   Listings: {card['listing_count']}")
            print(f"   Avg Price: ${card['avg_price']:,.2f}")
            print(f"   Popularity: {int(card['popularity'])} listings")
            print(f"   Search Query: {card['search_query']}")
            print()

        # Save to database (card_candidates table)
        discovery.save_candidates(candidates)

        # Calculate execution time
        execution_time = time.time() - start_time

        # Save discovery run to database
        run_id = db.save_discovery_run(
            total_queries=1,  # Category-based search uses 1 query
            total_listings_found=max_listings,
            total_listings_processed=max_listings,
            total_unique_cards=len(candidates),
            total_candidates_saved=len(candidates),
            status='success',
            execution_time_seconds=execution_time
        )

        # Save detailed results for each card
        if run_id:
            print(f"\n💾 Saving discovery results to database (run ID: {run_id})...")

            for candidate in candidates:
                # Calculate additional statistics
                prices = candidate.get('prices', [])
                if prices:
                    min_price = min(prices)
                    max_price = max(prices)
                else:
                    min_price = candidate.get('avg_price', 0)
                    max_price = candidate.get('avg_price', 0)

                # Prepare card data for discovery_results table
                card_result = {
                    'card_name': candidate['card_name'],
                    'set_name': candidate.get('set_name'),
                    'card_number': candidate.get('card_number'),
                    'variant_attributes': candidate.get('variant_attributes', {}),
                    'listing_count': candidate['listing_count'],
                    'avg_price': candidate['avg_price'],
                    'min_price': min_price,
                    'max_price': max_price,
                    'popularity': candidate['popularity'],
                    'search_query': candidate['search_query']
                }

                db.save_discovery_result(run_id, card_result)

            print(f"✅ Saved {len(candidates)} discovery results")

            # Save individual listing details
            if individual_listings:
                print(f"💾 Saving {len(individual_listings)} individual listing details...")
                saved_count = db.save_discovered_listings_batch(run_id, individual_listings)
                print(f"✅ Saved {saved_count} individual listings")

        print(f"\n⏱️  Total execution time: {execution_time:.1f} seconds")

    except Exception as e:
        # Save failed run to database (before closing connection)
        try:
            execution_time = time.time() - start_time
            db.save_discovery_run(
                total_queries=1,
                total_listings_found=0,
                total_listings_processed=0,
                total_unique_cards=0,
                total_candidates_saved=0,
                status='error',
                execution_time_seconds=execution_time,
                error_message=str(e)
            )
        except Exception as save_error:
            print(f"Warning: Could not save error to database: {save_error}")

        print(f"\n❌ Discovery failed: {e}")
        raise

    finally:
        db.close()


if __name__ == '__main__':
    main()
