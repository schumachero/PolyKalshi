import pandas as pd
from transformers import AutoModelForCausalLM, AutoTokenizer
from openai import OpenAI
import torch
import time
from datetime import datetime


PRE_PROMPT = """You are a binary classifier for prediction market contract equivalence.

Task:
Determine whether two contracts resolve based on the same underlying real-world event.

Rules:
- Output only one character: 0 or 1
- Output 1 if the contracts are equivalent.
- Output 0 if the contracts are not equivalent.
- Ignore issuance date.
- Ignore tiny time differences such as minutes, timezone boundaries, and Dec 31 vs Jan 1 wording.
- Different time windows alone do not automatically mean not equivalent.
- A meeting between X and Y is the same people as a meeting between Y and X.
- Obvious name variants are the same person, e.g. Trump = Donald Trump, Putin = Vladimir Putin.
- Physical presence in a country is not the same as holding state power there.
- Focus only on whether the YES-resolution event is the same.
- Do not explain your answer.
- Output exactly one character: 0 or 1.
"""

TOKENS = False
HF_TOKEN = "HF"


def parse_binary_response(response: str):
    text = response.strip()
    if text == "0":
        return 0
    if text == "1":
        return 1

    for ch in text:
        if ch == "0":
            return 0
        if ch == "1":
            return 1
    return None


def build_prompt(contract_a: str, contract_b: str, title_a: str, title_b: str):
    prompt = f"""Contract A:
Title: {title_a}
Rules: {contract_a}

Contract B:
Title: {title_b}
Rules: {contract_b}

Output exactly one character: 0 or 1."""
    return [
        {"role": "system", "content": PRE_PROMPT},
        {"role": "user", "content": prompt}
    ]


def load_model():
    model_name = "Qwen/Qwen2.5-3B-Instruct"
    model = AutoModelForCausalLM.from_pretrained(
        model_name,
        torch_dtype=torch.float16 if torch.cuda.is_available() else torch.float32,
        device_map="auto"
    )
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    print("hf_device_map:", getattr(model, "hf_device_map", None))
    return model, tokenizer


def get_response(row_k, row_p, title_k, title_p, model=None, tokenizer=None):
    messages = build_prompt(str(row_k), str(row_p), title_k, title_p)

    if TOKENS:
        client = OpenAI(
            base_url="https://router.huggingface.co/v1",
            api_key=HF_TOKEN,
        )
        completion = client.chat.completions.create(
            model="Qwen/Qwen3.5-35B-A3B:novita",
            messages=messages,
        )
        return completion.choices[0].message.content.strip()

    text = tokenizer.apply_chat_template(
        messages,
        tokenize=False,
        add_generation_prompt=True
    )

    model_inputs = tokenizer(text, return_tensors="pt")

    if torch.cuda.is_available():
        model_inputs = {k: v.to("cuda") for k, v in model_inputs.items()}

    generated_ids = model.generate(
        **model_inputs,
        max_new_tokens=3,
        do_sample=False,
        pad_token_id=tokenizer.eos_token_id
    )

    if torch.cuda.is_available():
        torch.cuda.synchronize()

    new_tokens = generated_ids[:, model_inputs["input_ids"].shape[1]:]
    return tokenizer.batch_decode(new_tokens, skip_special_tokens=True)[0].strip()


def process_row(i, df, model=None, tokenizer=None):
    title_k = str(df.loc[i, "kalshi_candidate_title_clean"])
    title_p = str(df.loc[i, "polymarket_candidate_title_clean"])
    row_k = df.loc[i, "kalshi_rules_text"]
    row_p = df.loc[i, "polymarket_rules_text"]

    if pd.isna(row_k) or pd.isna(row_p):
        print(f"Row {i}: skipped because of missing rules text")
        return None, None

    start = time.perf_counter()
    response = get_response(row_k, row_p, title_k, title_p, model=model, tokenizer=tokenizer)
    pred = parse_binary_response(response)
    end = time.perf_counter()

    prediction_text = {
        1: "Equivalent",
        0: "Not Equivalent",
        None: "Unparsed / Invalid"
    }[pred]

    result = {
        "row": i,
        "llm_binary": pred,
        "kalshi_market_ticker": df.loc[i, "kalshi_market_ticker"],
        "kalshi_series_ticker": df.loc[i, "kalshi_series_ticker"],
        "kalshi_market": df.loc[i, "kalshi_market"],
        "polymarket_market_ticker": df.loc[i, "polymarket_market_ticker"],
        "polymarket_series_ticker": df.loc[i, "polymarket_series_ticker"],
        "polymarket_market": df.loc[i, "polymarket_market"],
        "matched_at": datetime.utcnow().isoformat()
    }

    matched = None
    if pred == 1:
        matched = {
            "row": i,
            "kalshi_market_ticker": df.loc[i, "kalshi_market_ticker"],
            "kalshi_series_ticker": df.loc[i, "kalshi_series_ticker"],
            "kalshi_market": df.loc[i, "kalshi_market"],
            "polymarket_market_ticker": df.loc[i, "polymarket_market_ticker"],
            "polymarket_series_ticker": df.loc[i, "polymarket_series_ticker"],
            "polymarket_market": df.loc[i, "polymarket_market"],
            "matched_at": datetime.utcnow().isoformat()
        }

    print(f"\nRow {i}")
    print(f"Raw model output: {response!r}")
    print(f"Predicted: {prediction_text}")
    print(f"Kalshi title: {title_k}")
    print(f"Polymarket title: {title_p}")
    print(f"Prompt {i} took {end - start:.2f} seconds")

    return result, matched


def match_markets():
    print("torch:", torch.__version__)
    print("cuda available:", torch.cuda.is_available())
    print("gpu:", torch.cuda.get_device_name(0) if torch.cuda.is_available() else "no gpu")

    df = pd.read_csv(r"Data/candidate_series_matches.csv")
    print(df.columns.tolist())

    matched_markets = []
    all_predictions = []

    model = None
    tokenizer = None
    if not TOKENS:
        model, tokenizer = load_model()

    for i in range(len(df)):
        result, matched = process_row(i, df, model=model, tokenizer=tokenizer)

        if result is not None:
            all_predictions.append(result)
        if matched is not None:
            matched_markets.append(matched)

    # all_predictions_df = pd.DataFrame(all_predictions)
    # all_predictions_df.to_csv("Data/llm_all_predictions.csv", index=False)

    matched_markets_df = pd.DataFrame(matched_markets)
    matched_markets_df.to_csv("Data/predicted_equivalent_markets.csv", index=False)

    print("\nSaved all predictions to Data/llm_all_predictions.csv")
    print("Saved predicted equivalent markets to Data/predicted_equivalent_markets.csv")
    print(f"Number of predicted equivalent pairs: {len(matched_markets)}")

    if len(matched_markets) > 0:
        print("\nFirst predicted equivalent match:")
        print(matched_markets[0])


if __name__ == "__main__":
    match_markets()