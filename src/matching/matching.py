import re
import pandas as pd
from tqdm import tqdm

# =========================
# Config
# =========================


KALSHI_CSV = "Data/kalshi_markets.csv"
POLYMARKET_CSV = "Data/polymarket_markets.csv"
OUTPUT_CSV = "Data/candidate_series_matches.csv"
RANDOM_SEED = 42
TEST_MODE = False
TEST_KALSHI_N = 1000
TEST_POLY_N = 1000
MAX_DATE_DIFF_DAYS = 45
MIN_COMBINED_SCORE = 0.35
MIN_SERIES_SCORE = 0.15
REQUIRE_SHARED_CANDIDATE_TOKEN = True

import os
import sys
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.append(project_root)

try:
    from notifications.telegram_bot import notify_arbitrage
except ImportError:
    try:
        from src.notifications.telegram_bot import notify_arbitrage
    except ImportError:
        # If not found, skip notification setup or define as a dummy
        def notify_arbitrage(*args, **kwargs):
            pass

STOPWORDS = {
    "will", "would", "could", "should",
    "any", "anyone", "the", "a", "an",
    "in", "of", "to", "for", "from", "on", "at", "by", "with",
    "be", "is", "are", "was", "were", "been", "being",
    "who", "what", "when", "where", "why", "how",
    "win", "wins", "winner", "winners", "winning", "won",
    "vote", "votes", "voting",
    "candidate", "candidates",
    "election", "elections",
    "presidential", "president",
    "primary", "general",
    "round", "party",
    "control", "lose", "gain",
    "next", "this", "that", "before", "after",
    "first", "second", "third",
    "total", "between", "less", "more", "than",
    "yes", "no",
    "democratic", "republican",
    "nomination", "nominee",
    "house", "seat", "district",
    "contest", "midterm", "midterms"
}


# =========================
# Helpers
# =========================
def ensure_columns(df: pd.DataFrame, cols, fill_value=pd.NA) -> pd.DataFrame:
    for col in cols:
        if col not in df.columns:
            df[col] = fill_value
    return df


def clean_text(value):
    if pd.isna(value):
        return ""
    value = str(value).lower().strip()
    value = normalize_districts(value)
    value = re.sub(r"\s+", " ", value)
    return value


def normalize_districts(text):
    text = str(text).lower()

    # "house ca 9" -> "ca-9"
    text = re.sub(r"\bhouse\s+([a-z]{2})\s*(\d{1,2}|al)\b", r"\1-\2", text)

    # "ca 9" -> "ca-9"
    text = re.sub(r"\b([a-z]{2})\s+(\d{1,2}|al)\b", r"\1-\2", text)

    # "ca - 9" -> "ca-9"
    text = re.sub(r"\b([a-z]{2})\s*-\s*(\d{1,2}|al)\b", r"\1-\2", text)

    return text


def extract_district(text):
    text = clean_text(text)
    m = re.search(r"\b([a-z]{2})-(\d{1,2}|al)\b", text)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    return None


def district_conflict_from_rows(krow, prow):
    d1 = krow.get("district")
    d2 = prow.get("district")
    return bool(d1 and d2 and d1 != d2)


def get_best_district(row):
    for col in ["series_title_clean", "market_title_clean", "candidate_title_clean", "rules_text_clean"]:
        if col in row and isinstance(row[col], str):
            d = extract_district(row[col])
            if d:
                return d
    return None


def tokenize(text):
    text = clean_text(text)

    # only for token comparison, not for district extraction
    text = re.sub(r"\b([a-z]{2})-(\d{1,2}|al)\b", r"\1\2", text)

    words = re.findall(r"[a-z0-9]+", text)

    return {
        w for w in words
        if len(w) > 1 and w not in STOPWORDS
    }


def parse_status(series):
    return series.astype(str).str.lower().str.strip()


def weighted_jaccard(set1: set, set2: set) -> float:
    if not set1 or not set2:
        return 0.0

    intersection = len(set1 & set2)
    union = len(set1 | set2)

    if union == 0:
        return 0.0

    jaccard = intersection / union
    avg_len = (len(set1) + len(set2)) / 2
    length_factor = min(1 + 0.05 * max(avg_len - 4, 0), 1.3)
    overlap_factor = min(1 + 0.1 * intersection, 1.5)

    score = jaccard * length_factor * overlap_factor
    return min(score, 1.0)


def normalize_candidate_market_title(text):
    text = clean_text(text)

    # remove common wrappers without destroying the candidate name
    text = re.sub(r"^if\s+", "", text)
    text = re.sub(r"^will\s+", "", text)
    text = re.sub(r"^who\s+will\s+", "", text)
    text = re.sub(r"^who\s+", "", text)

    # trim everything after a win-style verb
    text = re.sub(r"\b(wins?|won)\b.*$", "", text)

    # trim after common election boilerplate if still present
    text = re.sub(r"\b(for|in)\b\s+(the\s+)?\d{4}\b.*$", "", text)
    text = re.sub(r"\b(primary|general|election|nomination|nominee)\b.*$", "", text)

    text = re.sub(r"\?", "", text)
    text = re.sub(r"[^a-z0-9\s\-]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text


def first_candidate_token(text):
    toks = [t for t in text.split() if t not in STOPWORDS]
    return toks[0] if toks else ""


def shares_candidate_token(set1, set2):
    return len(set1 & set2) > 0



# Load / clean
def load_and_clean_data():
    print("\n=== LOADING CSV FILES ===")
    kalshi_df = pd.read_csv(KALSHI_CSV)
    polymarket_df = pd.read_csv(POLYMARKET_CSV)

    print(f"Loaded Kalshi rows: {len(kalshi_df)}")
    print(f"Loaded Polymarket rows: {len(polymarket_df)}")

    kalshi_df = ensure_columns(
        kalshi_df,
        [
            "series_ticker", "series_title",
            "market_ticker", "market_title",
            "status", "close_time", "rules_text"
        ],
        fill_value=""
    )

    polymarket_df = ensure_columns(
        polymarket_df,
        [
            "series_ticker", "series_title",
            "market_ticker", "market_title",
            "group_item_title",
            "status", "close_time", "rules_text"
        ],
        fill_value=""
    )

    print("\n=== CLEANING DATAFRAMES ===")

    kalshi_df["close_time"] = pd.to_datetime(kalshi_df["close_time"], errors="coerce", utc=True)
    polymarket_df["close_time"] = pd.to_datetime(polymarket_df["close_time"], errors="coerce", utc=True)

    kalshi_df["status"] = parse_status(kalshi_df["status"])
    polymarket_df["status"] = parse_status(polymarket_df["status"])

    kalshi_before = len(kalshi_df)
    polymarket_before = len(polymarket_df)

    # adjust if your Kalshi status universe differs
    kalshi_df = kalshi_df[~kalshi_df["status"].isin(["finalized", "settled"])].copy()
    polymarket_df = polymarket_df[polymarket_df["status"] == "active"].copy()

    print(f"Kalshi rows after status filter: {len(kalshi_df)} (removed {kalshi_before - len(kalshi_df)})")
    print(f"Polymarket rows after status filter: {len(polymarket_df)} (removed {polymarket_before - len(polymarket_df)})")

    kalshi_df["series_title_clean"] = kalshi_df["series_title"].apply(clean_text)
    kalshi_df["market_title_clean"] = kalshi_df["market_title"].apply(clean_text)
    kalshi_df["rules_text_clean"] = kalshi_df["rules_text"].apply(clean_text)

    polymarket_df["series_title_clean"] = polymarket_df["series_title"].apply(clean_text)
    polymarket_df["market_title_clean"] = polymarket_df["market_title"].apply(clean_text)
    polymarket_df["rules_text_clean"] = polymarket_df["rules_text"].apply(clean_text)

    # Prefer group_item_title when available; often cleaner for candidate submarkets
    polymarket_df["candidate_title_source"] = polymarket_df["group_item_title"].replace("", pd.NA).fillna(
        polymarket_df["market_title"]
    )

    kalshi_df["candidate_title_clean"] = kalshi_df["market_title"].apply(normalize_candidate_market_title)
    polymarket_df["candidate_title_clean"] = polymarket_df["candidate_title_source"].apply(normalize_candidate_market_title)

    return kalshi_df, polymarket_df


# =========================
# Build market tables
# =========================
def build_market_tables(kalshi_df, polymarket_df):
    print("\n=== BUILDING UNIQUE MARKET TABLES ===")

    kalshi_markets = (
        kalshi_df[
            [
                "series_ticker", "series_title", "series_title_clean",
                "market_ticker", "market_title", "market_title_clean",
                "candidate_title_clean",
                "rules_text", "rules_text_clean",
                "close_time", "status"
            ]
        ]
        .drop_duplicates(subset=["market_ticker"])
        .reset_index(drop=True)
    )

    polymarket_markets = (
        polymarket_df[
            [
                "series_ticker", "series_title", "series_title_clean",
                "market_ticker", "market_title", "market_title_clean",
                "group_item_title",
                "candidate_title_source", "candidate_title_clean",
                "rules_text", "rules_text_clean",
                "close_time", "status"
            ]
        ]
        .drop_duplicates(subset=["market_ticker"])
        .reset_index(drop=True)
    )

    print(f"Unique Kalshi markets: {len(kalshi_markets)}")
    print(f"Unique Polymarket markets: {len(polymarket_markets)}")

    print("Pre-tokenizing columns...")
    kalshi_markets["candidate_tokens"] = kalshi_markets["candidate_title_clean"].apply(tokenize)
    kalshi_markets["series_tokens"] = kalshi_markets["series_title_clean"].apply(tokenize)
    
    polymarket_markets["candidate_tokens"] = polymarket_markets["candidate_title_clean"].apply(tokenize)
    polymarket_markets["series_tokens"] = polymarket_markets["series_title_clean"].apply(tokenize)

    print("Pre-computing districts...")
    kalshi_markets["district"] = kalshi_markets.apply(get_best_district, axis=1)
    polymarket_markets["district"] = polymarket_markets.apply(get_best_district, axis=1)

    return kalshi_markets, polymarket_markets



# Matching

def generate_candidate_matches(kalshi_markets, polymarket_markets):
    print("\n=== GENERATING CANDIDATE MARKET MATCHES ===")

    matches = []
    pairs_checked = 0
    pairs_after_district = 0
    pairs_after_date = 0
    pairs_after_candidate_prefilter = 0

    poly_records = polymarket_markets.to_dict("records")

    poly_by_district = {}
    poly_no_district = []

    for prow in poly_records:
        d = prow.get("district")
        if d:
            poly_by_district.setdefault(d, []).append(prow)
        else:
            poly_no_district.append(prow)

    all_poly_rows = poly_records

    progress = tqdm(
        kalshi_markets.iterrows(),
        total=len(kalshi_markets),
        desc="Matching Kalshi markets"
    )

    for idx, krow in progress:
        kd = krow.get("district")

        if kd and kd in poly_by_district:
            candidate_pool = poly_by_district[kd] + poly_no_district
        else:
            candidate_pool = all_poly_rows

        for prow in candidate_pool:
            pairs_checked += 1

            if district_conflict_from_rows(krow, prow):
                continue
            pairs_after_district += 1

            if pd.notna(krow["close_time"]) and pd.notna(prow["close_time"]):
                date_diff_days = abs((krow["close_time"] - prow["close_time"]).days)
                if date_diff_days > MAX_DATE_DIFF_DAYS:
                    continue
            else:
                date_diff_days = None
            pairs_after_date += 1

            if REQUIRE_SHARED_CANDIDATE_TOKEN:
                if not shares_candidate_token(
                    krow["candidate_tokens"],
                    prow["candidate_tokens"]
                ):
                    continue
            pairs_after_candidate_prefilter += 1

            market_score = weighted_jaccard(
                krow["candidate_tokens"],
                prow["candidate_tokens"]
            )

            series_score = weighted_jaccard(
                krow["series_tokens"],
                prow["series_tokens"]
            )

            if series_score < MIN_SERIES_SCORE:
                continue

            score = 0.75 * market_score + 0.25 * series_score

            if score < MIN_COMBINED_SCORE:
                continue

            shared_candidate_words = sorted(krow["candidate_tokens"] & prow["candidate_tokens"])
            shared_series_words = sorted(krow["series_tokens"] & prow["series_tokens"])

            match_entry = {
                "kalshi_series_ticker": krow["series_ticker"],
                "kalshi_series": krow["series_title"],
                "kalshi_market_ticker": krow["market_ticker"],
                "kalshi_market": krow["market_title"],
                "kalshi_candidate_title_clean": krow["candidate_title_clean"],
                "kalshi_district": krow.get("district"),
                "kalshi_rules_text": krow.get("rules_text", ""),
                "kalshi_rules_text_clean": krow.get("rules_text_clean", ""),
                "kalshi_close_time": str(krow["close_time"]) if pd.notna(krow["close_time"]) else "",

                "polymarket_series_ticker": prow["series_ticker"],
                "polymarket_series": prow["series_title"],
                "polymarket_market_ticker": prow["market_ticker"],
                "polymarket_market": prow["market_title"],
                "polymarket_group_item_title": prow.get("group_item_title", ""),
                "polymarket_candidate_title_clean": prow["candidate_title_clean"],
                "polymarket_district": prow.get("district"),
                "polymarket_rules_text": prow.get("rules_text", ""),
                "polymarket_rules_text_clean": prow.get("rules_text_clean", ""),
                "polymarket_close_time": str(prow["close_time"]) if pd.notna(prow["close_time"]) else "",

                "market_score": round(market_score, 4),
                "series_score": round(series_score, 4),
                "combined_score": round(score, 4),
                "date_diff_days": date_diff_days,
                "shared_candidate_words": ", ".join(shared_candidate_words),
                "shared_series_words": ", ".join(shared_series_words),
            }

            # matches.append(match_entry) # Moved out of loop for clarity in some versions, but here it's fine.
            matches.append(match_entry)

        if (idx + 1) % 25 == 0:
            progress.set_postfix({
                "pairs": pairs_checked,
                "date_ok": pairs_after_date,
                "prefilter_ok": pairs_after_candidate_prefilter,
                "matches": len(matches),
            })

    print(f"Pairs checked: {pairs_checked}")
    print(f"Pairs after district filter: {pairs_after_district}")
    print(f"Pairs after date filter: {pairs_after_date}")
    print(f"Pairs after candidate prefilter: {pairs_after_candidate_prefilter}")

    matches_df = pd.DataFrame(matches)
    if matches_df.empty:
        print("No matches found.")
        return matches_df

    matches_df = matches_df.sort_values(
        by=["combined_score", "market_score", "series_score", "date_diff_days"],
        ascending=[False, False, False, True]
    ).reset_index(drop=True)

    print(f"Matches found: {len(matches_df)}")
    print(matches_df.head(20).to_string())

    return matches_df

def main():
    print("=== STARTING CANDIDATE MATCHING SCRIPT ===")

    kalshi_df, polymarket_df = load_and_clean_data()
    kalshi_markets, polymarket_markets = build_market_tables(kalshi_df, polymarket_df)
    if TEST_MODE:
        kalshi_markets = kalshi_markets.sample(
                n=min(TEST_KALSHI_N, len(kalshi_markets)),
                random_state=RANDOM_SEED
            ).copy()
        polymarket_markets = polymarket_markets.sample(
                n=min(TEST_POLY_N, len(polymarket_markets)),
                random_state=RANDOM_SEED
            ).copy()
        print(f"TEST MODE: using {len(kalshi_markets)} Kalshi rows and {len(polymarket_markets)} Polymarket rows")
        
    matches_df = generate_candidate_matches(kalshi_markets, polymarket_markets)

    print("\n=== WRITING OUTPUT ===")
    matches_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"Wrote {len(matches_df)} rows to {OUTPUT_CSV}")

    print("\n=== DONE ===")


if __name__ == "__main__":
    main()