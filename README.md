# Lugia Tracker

Pokemon card tracker

## Components

- `lugia_tracker.py` - Main eBay listing tracker
- `card_discovery.py` - Discovers cards by category
- `market_value_tracker.py` - Fetches PriceCharting market values
- `market_information.py` - Listings and market information
- `listing_info_app.py` - Streamlit web UI for reviewing listings
- `database.py` - PostgreSQL database manager
- `grade_matcher.py` - Maps conditions to PSA grades

## Setup

1. Copy `ebay.env.example` to `ebay.env` and fill in credentials
2. Install dependencies: `pip install -r requirements.txt`
3. Run tracker: `python lugia_tracker.py`
4. Run web UI: `streamlit run listing_info_app.py`

## GitHub Actions

Workflows run automatically:
- `lugia_tracker.yml` - Every 6 hours
- `card-discovery-scheduled.yml` - Weekly
- `market_values.yml` - Daily