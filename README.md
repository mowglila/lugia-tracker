# Listing Tool

A personal tool to display eBay listing information alongside market values from PriceCharting. The tool presents facts without recommendations, calculations, or suggested actions.

## Purpose

- Display active eBay listings for tracked cards
- Display corresponding market values from PriceCharting
- Display listing attributes (grade, condition, seller info)
- Allow sorting and filtering for personal review

## Design Principles

1. **Display facts only** - No calculated percentages, no recommendations
2. **Two data sources, displayed separately**:
   - eBay: listing price, attributes, seller info
   - PriceCharting: market values by grade
3. **No price modeling** - Tool does not suggest what prices should be
4. **Personal use** - Facilitates informed buying decisions on eBay

## Components

- `lugia_tracker.py` - Main eBay listing tracker
- `market_value_tracker.py` - Fetches PriceCharting market values
- `listing_info_app.py` - Streamlit web UI for viewing listings
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
- `market_values.yml` - Daily

## API Compliance

### eBay API

- Content refreshed every 6 hours (within 6-hour freshness requirement)
- Display prices without derived metrics
- No site-wide statistics
- Private database, personal use only
- Tool facilitates eBay purchases

### PriceCharting API

- Requires active Legendary subscription
- App is personal, not public-facing
- Attribution displayed in app
- Market values fetched daily (not more frequently)
- Data not redistributed
