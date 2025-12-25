"""
Grade Matcher

Intelligently matches card grades and conditions to PriceCharting market values.
Handles PSA 1-10, BGS, CGC scales, and raw card condition mapping.
"""

import re
from typing import Dict, Optional, Tuple


# Expanded grade patterns to catch PSA 1-6 and all variations
GRADE_PATTERNS = {
    # PSA grades (1-10)
    'PSA 10': [r'PSA\s*10', r'PSA\s+GEM\s+MT'],
    'PSA 9': [r'PSA\s*9(?!\.)'],
    'PSA 8': [r'PSA\s*8'],
    'PSA 7': [r'PSA\s*7'],
    'PSA 6': [r'PSA\s*6'],
    'PSA 5': [r'PSA\s*5'],
    'PSA 4': [r'PSA\s*4'],
    'PSA 3': [r'PSA\s*3'],
    'PSA 2': [r'PSA\s*2'],
    'PSA 1': [r'PSA\s*1(?!\d)'],  # Don't match PSA 10

    # BGS grades
    'BGS 10': [r'BGS\s*10', r'BLACK\s+LABEL', r'BECKETT\s+10'],
    'BGS 9.5': [r'BGS\s*9\.5', r'BECKETT\s+9\.5'],
    'BGS 9': [r'BGS\s*9(?!\.)', r'BECKETT\s+9(?!\.)'],
    'BGS 8.5': [r'BGS\s*8\.5', r'BECKETT\s+8\.5'],
    'BGS 8': [r'BGS\s*8(?!\.)', r'BECKETT\s+8(?!\.)'],
    'BGS 7.5': [r'BGS\s*7\.5', r'BECKETT\s+7\.5'],
    'BGS 7': [r'BGS\s*7(?!\.)', r'BECKETT\s+7(?!\.)'],

    # CGC grades
    'CGC 10 Pristine': [r'CGC\s*10\s+PRISTINE', r'CGC\s+PRISTINE\s+10'],
    'CGC 10': [r'CGC\s*10(?!\s+PRISTINE)', r'CGC\s+PERFECT'],
    'CGC 9.5': [r'CGC\s*9\.5', r'CGC\s+GEM\s+MINT'],
    'CGC 9': [r'CGC\s*9(?!\.)', r'CGC\s+MINT'],
    'CGC 8.5': [r'CGC\s*8\.5'],
    'CGC 8': [r'CGC\s*8(?!\.)'],
    'CGC 7.5': [r'CGC\s*7\.5'],
    'CGC 7': [r'CGC\s*7(?!\.)'],

    # SGC grades
    'SGC 10': [r'SGC\s*10'],
    'SGC 9.5': [r'SGC\s*9\.5'],
    'SGC 9': [r'SGC\s*9(?!\.)'],
    'SGC 8': [r'SGC\s*8'],

    # Raw/Ungraded
    'Raw': [r'\bRAW\b', r'UNGRADED', r'NOT\s+GRADED', r'NEVER\s+GRADED'],
}


# Condition patterns for raw cards
CONDITION_PATTERNS = {
    'Gem Mint': [r'GEM\s*MINT', r'MINT\s*\+', r'PRISTINE'],
    'Near Mint': [r'NEAR\s*MINT', r'\bNM\b', r'NM/M', r'NEAR\s*MINT\s*MINT'],
    'Excellent': [r'EXCELLENT', r'\bEX\b', r'EX\+', r'EX/NM'],
    'Very Good': [r'VERY\s*GOOD', r'\bVG\b', r'VG\+', r'VG/EX'],
    'Good': [r'\bGOOD\b', r'\bGD\b'],
    'Light Play': [r'LIGHT\s*PLAY', r'LP', r'LIGHTLY\s*PLAYED'],
    'Moderate Play': [r'MODERATE\s*PLAY', r'MP', r'MODERATELY\s*PLAYED'],
    'Heavy Play': [r'HEAVY\s*PLAY', r'HP', r'HEAVILY\s*PLAYED'],
    'Damaged': [r'DAMAGED', r'\bDMG\b', r'POOR'],
}


class GradeMatcher:
    """Match card grades and conditions to PriceCharting market values."""

    def __init__(self, market_values: Dict[str, float]):
        """
        Initialize with latest market values from PriceCharting.

        Args:
            market_values: Dict with keys like psa_10_price, psa_9_price, etc.
        """
        self.market_values = market_values

    def extract_grade_and_condition(self, title: str, condition: str = None) -> Tuple[str, str, bool]:
        """
        Extract grade and condition from listing title and condition field.

        Args:
            title: Listing title
            condition: eBay condition field (e.g., "Graded", "Ungraded", "Used")

        Returns:
            Tuple of (grade, condition, is_graded)
            - grade: e.g., "PSA 9", "Raw", "Unknown"
            - condition: e.g., "Near Mint", "Excellent", "N/A"
            - is_graded: True if professionally graded, False otherwise
        """
        title_upper = title.upper()

        # First, check for graded cards
        for grade, patterns in GRADE_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, title_upper):
                    is_graded = grade != 'Raw'
                    return grade, 'N/A', is_graded

        # If no grade found, check for raw card conditions in title
        detected_condition = 'N/A'
        for cond, patterns in CONDITION_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, title_upper):
                    detected_condition = cond
                    break
            if detected_condition != 'N/A':
                break

        # Also check the condition field if provided
        if detected_condition == 'N/A' and condition:
            condition_upper = condition.upper()
            for cond, patterns in CONDITION_PATTERNS.items():
                for pattern in patterns:
                    if re.search(pattern, condition_upper):
                        detected_condition = cond
                        break
                if detected_condition != 'N/A':
                    break

        # Determine if it's graded based on condition field
        is_graded = False
        if condition:
            condition_lower = condition.lower()
            is_graded = 'grad' in condition_lower  # Catches "Graded", "Gradée", "Gradata", etc.

        # If graded but no grade found, return Unknown
        if is_graded:
            return 'Unknown', detected_condition, True
        else:
            return 'Raw', detected_condition, False

    def get_comparable_market_value(self, grade: str, condition: str = None) -> Optional[float]:
        """
        Get the most comparable market value for a given grade/condition.

        Uses real PriceCharting calibration data when available.

        Args:
            grade: Card grade (e.g., "PSA 9", "PSA 4", "Raw")
            condition: Raw card condition (e.g., "Near Mint", "Excellent")

        Returns:
            Market value in dollars, or None if no comparable found
        """
        # Direct matches from PriceCharting calibration data
        # Note: Raw is handled separately below to apply condition multipliers
        direct_match = {
            # PSA all grades (1-10)
            'PSA 10': self.market_values.get('psa_10_price'),
            'PSA 9': self.market_values.get('psa_9_price'),
            'PSA 8': self.market_values.get('psa_8_price'),
            'PSA 7': self.market_values.get('psa_7_price'),
            'PSA 6': self.market_values.get('psa_6_price'),
            'PSA 5': self.market_values.get('psa_5_price'),
            'PSA 4': self.market_values.get('psa_4_price'),
            'PSA 3': self.market_values.get('psa_3_price'),
            'PSA 2': self.market_values.get('psa_2_price'),
            'PSA 1': self.market_values.get('psa_1_price'),

            # BGS grades
            'BGS 10': self.market_values.get('bgs_10_price'),
            'BGS 9.5': self.market_values.get('bgs_9_5_price'),

            # CGC grades
            'CGC 10 Pristine': self.market_values.get('cgc_10_pristine_price'),
            'CGC 10': self.market_values.get('cgc_10_price'),
            'CGC 9.5': self.market_values.get('cgc_9_5_price'),

            # SGC
            'SGC 10': self.market_values.get('sgc_10_price'),

            # Generic 9.5
            'Grade 9.5': self.market_values.get('grade_9_5_price'),
        }

        if grade in direct_match and direct_match[grade]:
            return direct_match[grade]

        # Proxy matches for grades not in our calibration data
        # Only calculate proxies for grades we don't have real data for
        psa_9 = self.market_values.get('psa_9_price')
        psa_8 = self.market_values.get('psa_8_price')
        psa_7 = self.market_values.get('psa_7_price')
        raw = self.market_values.get('raw_ungraded_price')

        # BGS proxies (for grades not in calibration data)
        if grade == 'BGS 9' and psa_9:
            return psa_9 * 1.05  # BGS 9 ≈ PSA 9 + 5%

        if grade == 'BGS 8.5' and psa_8 and psa_9:
            return (psa_8 + psa_9) / 2 * 1.05

        if grade == 'BGS 8' and psa_8:
            return psa_8 * 1.05

        if grade in ['BGS 7.5', 'BGS 7'] and psa_7:
            return psa_7 * 1.05

        # CGC proxies (for grades not in calibration data)
        if grade == 'CGC 9' and psa_9:
            return psa_9 * 0.95  # CGC 9 ≈ 95% of PSA 9

        if grade == 'CGC 8.5' and psa_8 and psa_9:
            return (psa_8 + psa_9) / 2 * 0.95

        if grade == 'CGC 8' and psa_8:
            return psa_8 * 0.95

        if grade in ['CGC 7.5', 'CGC 7'] and psa_7:
            return psa_7 * 0.95

        # SGC proxies (for grades not in calibration data)
        if grade == 'SGC 9.5':
            sgc_10 = self.market_values.get('sgc_10_price')
            if sgc_10:
                return sgc_10 * 0.85

        if grade == 'SGC 9' and psa_9:
            return psa_9 * 0.90

        if grade == 'SGC 8' and psa_8:
            return psa_8 * 0.90

        # Raw card condition-to-grade mapping
        if grade == 'Raw' and condition and raw:
            condition_multipliers = {
                'Gem Mint': 1.5,        # Gem Mint raw ≈ PSA 9-10 potential
                'Near Mint': 1.2,       # Near Mint ≈ PSA 8-9 potential
                'Excellent': 0.9,       # Excellent ≈ PSA 6-7 range
                'Very Good': 0.7,       # Very Good ≈ PSA 5-6 range
                'Light Play': 0.6,      # Light Play ≈ PSA 4-5 range
                'Good': 0.5,            # Good ≈ PSA 3-4 range
                'Moderate Play': 0.4,   # Moderate Play ≈ PSA 2-3
                'Heavy Play': 0.3,      # Heavy Play ≈ PSA 1-2
                'Damaged': 0.2,         # Damaged < PSA 1
            }

            multiplier = condition_multipliers.get(condition, 1.0)
            return raw * multiplier

        # If Unknown grade but we know it's graded, use PSA 8 as default
        if grade == 'Unknown' and psa_8:
            return psa_8

        # Last resort: use raw price
        return raw

    def get_condition_comparable_grade(self, condition: str) -> str:
        """
        Get the PSA grade that most closely matches a raw card condition.

        Args:
            condition: Raw card condition (e.g., "Near Mint", "Excellent")

        Returns:
            Comparable PSA grade (e.g., "PSA 8", "PSA 5")
        """
        condition_to_grade = {
            'Gem Mint': 'PSA 9-10',
            'Near Mint': 'PSA 8',
            'Excellent': 'PSA 6-7',
            'Very Good': 'PSA 5',
            'Light Play': 'PSA 4-5',
            'Good': 'PSA 3-4',
            'Moderate Play': 'PSA 2-3',
            'Heavy Play': 'PSA 1-2',
            'Damaged': 'Below PSA 1',
        }

        return condition_to_grade.get(condition, 'Unknown')


def test_grade_matcher():
    """Test the grade matcher with sample data."""
    # Sample market values
    market_values = {
        'psa_10_price': 3668.00,
        'psa_9_price': 3056.83,
        'psa_8_price': 2242.98,
        'psa_7_price': 1360.02,
        'bgs_10_price': 4768.00,
        'cgc_9_5_price': 3595.00,
        'raw_ungraded_price': 1130.12,
    }

    matcher = GradeMatcher(market_values)

    # Test cases
    test_cases = [
        ("2000 POKEMON NEO GENESIS 1ST EDITION LUGIA HOLO PSA 4", "Graded"),
        ("Lugia Holo 9/111 Neo Genesis 2000 Raw Near Mint", "Ungraded"),
        ("PSA 6 Lugia Neo Genesis 1st Edition", "Graded"),
        ("BGS 9 Lugia 1st Edition Neo Genesis", "Graded"),
        ("CGC 10 Lugia Neo Genesis Holo", "Graded"),
        ("Raw Lugia Neo Genesis Excellent Condition", "Used"),
    ]

    print("=== Grade Matcher Test Results ===\n")
    for title, condition in test_cases:
        grade, cond, is_graded = matcher.extract_grade_and_condition(title, condition)
        market_value = matcher.get_comparable_market_value(grade, cond)

        print(f"Title: {title}")
        print(f"  Grade: {grade}")
        print(f"  Condition: {cond}")
        print(f"  Is Graded: {is_graded}")
        print(f"  Market Value: ${market_value:,.2f}" if market_value else "  Market Value: N/A")
        if cond != 'N/A':
            comparable = matcher.get_condition_comparable_grade(cond)
            print(f"  Comparable Grade: {comparable}")
        print()


if __name__ == '__main__':
    test_grade_matcher()
