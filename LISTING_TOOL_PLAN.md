# Listing Tool - Project Plan

## Overview

A personal tool to display eBay listing information alongside market values from PriceCharting. The tool presents facts without recommendations, calculations, or suggested actions.

---

## Purpose

- Display active eBay listings for tracked cards
- Display corresponding market values from PriceCharting
- Display listing attributes (grade, condition, seller info)
- Allow sorting and filtering for personal review

---

## Design Principles

1. **Display facts only** - No calculated percentages, no "deal scores," no recommendations
2. **Two data sources, displayed separately**:
   - eBay: listing price, attributes, seller info
   - PriceCharting: market values by grade
3. **No price modeling** - Tool does not suggest what prices should be
4. **Personal use** - Facilitates informed buying decisions on eBay

---

## What the Tool Displays

### From eBay (via Browse API)
- Listing title
- Listing price (item + shipping)
- Grade (extracted from title)
- Condition
- Seller username
- Seller feedback percentage
- Listing type (Auction / Buy It Now)
- Listing URL
- Image

### From PriceCharting (via API)
- Market value for the corresponding grade
- Data source attribution

### Sorting Options
- By listing price (low to high, high to low)
- By grade
- By date added
- Internal sort by price difference (not displayed in UI)

---

## What the Tool Does NOT Do

- Calculate or display percentage differences
- Label listings as "good deal" or "bad deal"
- Recommend actions (buy, watch, skip)
- Generate pricing suggestions
- Derive statistics from eBay data (no averages, medians, etc.)
- Train ML models on listing data
- Share or publish data

---

## eBay API Compliance

| Requirement | Implementation |
|-------------|----------------|
| Content freshness (6 hours) | Tracker runs every 6 hours |
| No price modeling | Display prices without derived metrics |
| No site-wide statistics | Removed `grade_stats` calculations |
| No AI/ML training | No model training on eBay data |
| No redistribution | Private database, personal use only |
| Facilitating eBay use | Tool helps user find listings to purchase |

Reference: https://developer.ebay.com/join/api-license-agreement

---

## PriceCharting API Compliance

| Requirement | Implementation |
|-------------|----------------|
| Paid subscription | Maintain active Legendary subscription |
| Internal use only | App is personal, not public-facing |
| No third-party access | Data not shared with others |
| Attribution | Display "Market values from PriceCharting.com" with link |
| Rate limiting | Fetch market values daily (not more frequently) |
| No redistribution | Price data stays in private database |

**Key restrictions:**
- Price data cannot be used in public-facing applications
- Keep the Streamlit app local/private - do not deploy publicly
- Do not share or publish market value data

Reference: https://www.pricecharting.com/page/terms-of-service

---

## Data Architecture

### eBay Data (listings table)
```
- item_id
- title
- grade
- price, shipping, total_cost
- condition, is_graded, raw_condition
- seller_username, seller_feedback
- listing_type, is_auction
- url, image_url
- first_seen, last_seen, is_active
```

### PriceCharting Data (market_values table)
```
- psa_10_price through psa_1_price
- bgs_10_price, bgs_9_5_price
- cgc_10_pristine_price, cgc_10_price, cgc_9_5_price
- sgc_10_price
- raw_ungraded_price
- recorded_at
```

---

## UI Layout (Streamlit)

For each listing, display:

```
[Image]  Title
         ---------------------------------
         Listing Price: $X,XXX
         Market Value (PSA 9): $X,XXX
         ---------------------------------
         Grade: PSA 9
         Condition: Graded
         Seller: username (XX% feedback)
         Type: Buy It Now
         [View on eBay]
```

- No color coding based on price comparison
- No badges or labels suggesting value
- Market value shown for reference only

---

## Implementation Phases

### Phase 1: Clean Up
- [x] Remove `percent_vs_avg` from UI (never surfaced - kept for internal sort only)
- [x] Remove `grade_stats` table and related code
- [x] Remove any "deal" terminology from codebase (none found in code)
- [x] Update Streamlit app to display facts only

### Phase 2: Simplify Display
- [x] Show listing price and market value side by side
- [x] Display all listing attributes
- [x] Internal sort by price difference (not exposed in UI)
- [x] Remove rating/feedback collection UI
- [x] Add hide button to dismiss listings from view

### Phase 3: Documentation
- [x] Update README to reflect tool purpose
- [x] Document eBay API compliance approach
- [x] Remove references to ML or deal detection (none found in code - only in docs explaining what tool does NOT do)

---

## Future Considerations

- Add more cards to track (beyond Lugia)
- Support multiple marketplaces (TCGPlayer values)
- Price history chart (for PriceCharting data only, not eBay-derived)
- Mobile-friendly display

---

## Resources

- eBay API License Agreement: https://developer.ebay.com/join/api-license-agreement
- PriceCharting API: https://www.pricecharting.com/api-documentation
- Streamlit: https://streamlit.io/
