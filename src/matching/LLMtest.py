import time
from datetime import datetime, UTC

import pandas as pd
from pandas.errors import EmptyDataError
from groq import Groq

# =========================
# Configuration
# =========================

GROQ_API_KEYS = [
    "key1",
    "key2",
    "key3",
    "key4",
    "key5",
]

GROQ_MODEL = "openai/gpt-oss-120b"

INPUT_CSV = r"Data/semantic_matches.csv"
OUTPUT_MATCHED_CSV = r"Data/predicted_equivalent_markets.csv"
OUTPUT_ALL_PREDICTIONS_CSV = r"Data/llm_all_predictions.csv"
OUTPUT_PROGRESS_CSV = r"Data/llm_progress_checkpoint.csv"

SAVE_EVERY_N_ROWS = 25
MAX_RETRIES_PER_KEY = 2
REQUEST_SLEEP_SECONDS = 0.0
RESUME_FROM_CHECKPOINT = True

REASONING_EFFORT = "low"
MAX_COMPLETION_TOKENS = 300


# =========================
# Helpers
# =========================

def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def get_client(api_key: str) -> Groq:
    return Groq(api_key=api_key)

#- If one contract can resolve YES in situations where the other cannot, output 0 ??

def build_messages(contract_a: str, contract_b: str, title_a: str, title_b: str):
    prompt = f"""You are labeling whether two prediction market contracts are equivalent.

Definition:
Definition:
Equivalent means both contracts resolve YES on the same real-world event set.

Decision rules:
- Output 1 if equivalent.
- Output 0 if not equivalent.
- Identical contracts should be 1.
- Minor wording differences alone do not matter. (1)
- Name variants for the same person count as the same person.
- A meeting between X and Y is the same as a meeting between Y and X.
- Tiny timing differences like timezone boundaries or Dec 31 vs Jan 1 wording can be ignored.
- Approval-rating contracts must use exactly the same threshold, otherwise 0.
- Physical presence in a country is not the same as holding state power there.
- Do not explain.
- The following dates/wording are equal in terms of the contracts: "Before 2027" == "In 2026" == "by 2027".  "Before May 1" == "By April 31"
- Output exactly one character: 0 or 1.

Examples:

Example 1
A title: Will Mamdani raise the minimum wage to $30 before 2027?
B title: Will Mamdani raise the minimum wage to $30 before 2027?
Answer: 1

Example 2
A title: Will Donald Trump meet Vladimir Putin in 2026?
B title: Will Vladimir Putin meet Trump in 2026?
Answer: 1

Example 3
A title: Will Donald Trump visit India before July 1, 2026?
B title: Will Donald Trump visit India by June 30, 2026?
Answer: 1

Example 4
A title: Will María Corina Machado visit Venezuela before May 1, 2026?
B title: Will María Corina Machado visit Venezuela by March 31, 2026?
Answer: 0

Example 5
A title: Will Emmanuel Macron visit Venezuela before May 1, 2026?
B title: Will Keir Starmer visit Venezuela before May 1, 2026?
Answer: 0


Now classify these:

Contract A:
Title: {title_a}
Rules: {contract_a}

Contract B:
Title: {title_b}
Rules: {contract_b}

Answer with exactly one character."""
    return [{"role": "user", "content": prompt}]


def parse_binary_response(response_text: str):
    if response_text is None:
        return None

    text = response_text.strip()
    if not text:
        return None

    if text == "0":
        return 0
    if text == "1":
        return 1

    first = text[0]
    if first == "0":
        return 0
    if first == "1":
        return 1

    return None


def get_response(row_k, row_p, title_k, title_p, client: Groq) -> str:
    messages = build_messages(str(row_k), str(row_p), str(title_k), str(title_p))

    completion = client.chat.completions.create(
        model=GROQ_MODEL,
        messages=messages,
        temperature=0,
        max_completion_tokens=MAX_COMPLETION_TOKENS,
        reasoning_effort=REASONING_EFFORT,
        include_reasoning=False,
    )

    message = completion.choices[0].message
    content = getattr(message, "content", None)

    if content is None:
        return ""
    return str(content).strip()


def process_row(i: int, df: pd.DataFrame, client: Groq):
    title_k = str(df.loc[i, "kalshi_candidate_title_clean"])
    title_p = str(df.loc[i, "polymarket_candidate_title_clean"])
    row_k = df.loc[i, "kalshi_rules_text"]
    row_p = df.loc[i, "polymarket_rules_text"]

    if pd.isna(row_k) or pd.isna(row_p):
        print(f"Row {i}: skipped because of missing rules text")
        return None, None

    start = time.perf_counter()
    raw_output = get_response(row_k, row_p, title_k, title_p, client=client)
    pred = parse_binary_response(raw_output)
    elapsed = time.perf_counter() - start

    prediction_text = {
        1: "Equivalent",
        0: "Not Equivalent",
        None: "Unparsed / Invalid",
    }[pred]

    now = utc_now_iso()

    result = {
        "row": i,
        "llm_binary": pred,
        "prediction_label": prediction_text,
        "raw_model_output": raw_output,
        "kalshi_series_ticker": df.loc[i, "kalshi_series_ticker"],
        "kalshi_market_ticker": df.loc[i, "kalshi_market_ticker"],
        "kalshi_series_name": df.loc[i, "kalshi_candidate_title_clean"],
        "kalshi_market": df.loc[i, "kalshi_market"],
        "polymarket_series_ticker": df.loc[i, "polymarket_series_ticker"],
        "polymarket_market_ticker": df.loc[i, "polymarket_market_ticker"],
        "polymarket_series_name": df.loc[i, "polymarket_candidate_title_clean"],
        "polymarket_market": df.loc[i, "polymarket_market"],
        "semantic_score": df.loc[i, "semantic_score"] if "semantic_score" in df.columns else None,
        "processed_at_utc": now,
    }

    matched = None
    if pred == 1:
        matched = {
            "row": i,
            "kalshi_series_ticker": df.loc[i, "kalshi_series_ticker"],
            "kalshi_market_ticker": df.loc[i, "kalshi_market_ticker"],
            "kalshi_series_name": df.loc[i, "kalshi_candidate_title_clean"],
            "kalshi_market": df.loc[i, "kalshi_market"],
            "polymarket_series_ticker": df.loc[i, "polymarket_series_ticker"],
            "polymarket_market_ticker": df.loc[i, "polymarket_market_ticker"],
            "polymarket_series_name": df.loc[i, "polymarket_candidate_title_clean"],
            "polymarket_market": df.loc[i, "polymarket_market"],
            "semantic_score": df.loc[i, "semantic_score"] if "semantic_score" in df.columns else None,
            "matched_at_utc": now,
        }

    print(f"\nRow {i}")
    print(f"Raw model output: {raw_output!r}")
    print(f"Predicted: {prediction_text}")
    print(f"Kalshi title: {title_k}")
    print(f"Polymarket title: {title_p}")
    print(f"Prompt {i} took {elapsed:.2f} seconds")

    return result, matched


def save_progress(all_predictions, matched_markets, next_row: int, reason: str):
    pd.DataFrame(all_predictions).to_csv(OUTPUT_ALL_PREDICTIONS_CSV, index=False)
    pd.DataFrame(matched_markets).to_csv(OUTPUT_MATCHED_CSV, index=False)
    pd.DataFrame([{
        "next_row_to_process": next_row,
        "saved_at_utc": utc_now_iso(),
        "reason": reason,
        "num_predictions_saved": len(all_predictions),
        "num_matches_saved": len(matched_markets),
    }]).to_csv(OUTPUT_PROGRESS_CSV, index=False)

    print("\nProgress saved.")
    print(f"Next row to process: {next_row}")
    print(f"Reason: {reason}")


def safe_read_csv(path):
    try:
        return pd.read_csv(path)
    except (FileNotFoundError, EmptyDataError):
        return None


def load_existing_progress():
    all_predictions = []
    matched_markets = []
    start_row = 0

    checkpoint_df = safe_read_csv(OUTPUT_PROGRESS_CSV)
    if checkpoint_df is not None and not checkpoint_df.empty and "next_row_to_process" in checkpoint_df.columns:
        start_row = int(checkpoint_df.loc[0, "next_row_to_process"])

    all_predictions_df = safe_read_csv(OUTPUT_ALL_PREDICTIONS_CSV)
    if all_predictions_df is not None and not all_predictions_df.empty:
        all_predictions = all_predictions_df.to_dict(orient="records")

    matched_df = safe_read_csv(OUTPUT_MATCHED_CSV)
    if matched_df is not None and not matched_df.empty:
        matched_markets = matched_df.to_dict(orient="records")

    return start_row, all_predictions, matched_markets


def match_markets():
    if not GROQ_API_KEYS:
        raise ValueError("GROQ_API_KEYS is empty.")

    df = pd.read_csv(INPUT_CSV)
    print(df.columns.tolist())

    if RESUME_FROM_CHECKPOINT:
        start_row, all_predictions, matched_markets = load_existing_progress()
        if start_row > 0:
            print(f"Resuming from row {start_row}")
    else:
        start_row = 0
        all_predictions = []
        matched_markets = []

    i = start_row
    key_index = 0

    while i < len(df):
        row_done = False
        keys_tried = 0

        while keys_tried < len(GROQ_API_KEYS):
            api_key = GROQ_API_KEYS[key_index]
            client = get_client(api_key)
            success_on_this_key = False

            for attempt in range(MAX_RETRIES_PER_KEY):
                try:
                    result, matched = process_row(i, df, client)

                    if result is not None:
                        all_predictions.append(result)
                    if matched is not None:
                        matched_markets.append(matched)

                    row_done = True
                    success_on_this_key = True
                    i += 1

                    if i % SAVE_EVERY_N_ROWS == 0:
                        save_progress(
                            all_predictions,
                            matched_markets,
                            i,
                            f"periodic save every {SAVE_EVERY_N_ROWS} rows"
                        )

                    if REQUEST_SLEEP_SECONDS > 0:
                        time.sleep(REQUEST_SLEEP_SECONDS)

                    break

                except Exception as e:
                    print(f"Row {i}: key #{key_index + 1}, attempt {attempt + 1} failed: {repr(e)}")

            if success_on_this_key:
                break

            key_index = (key_index + 1) % len(GROQ_API_KEYS)
            keys_tried += 1

        if not row_done:
            save_progress(all_predictions, matched_markets, i, "all API keys exhausted before completion")
            print("\nStopped early because all API keys failed.")
            return

    save_progress(all_predictions, matched_markets, i, "completed successfully")
    print("\nFinished processing all rows.")
    print(f"Number of predicted equivalent pairs: {len(matched_markets)}")


if __name__ == "__main__":
    match_markets()