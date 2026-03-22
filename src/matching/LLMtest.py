import pandas as pd
from transformers import AutoModelForCausalLM, AutoTokenizer
from openai import OpenAI
import torch
import time
def parse_binary_response(response: str):
    text = response.strip()
    if text == "0":
        return 0
    if text == "1":
        return 1
    return None
predictions = []

print("torch:", torch.__version__)
print("cuda available:", torch.cuda.is_available())
print("gpu:", torch.cuda.get_device_name(0) if torch.cuda.is_available() else "no gpu")

df = pd.read_csv(r"Data/candidate_series_matches.csv")
print(df.columns.tolist())
print(df[["kalshi_candidate_title_clean","polymarket_candidate_title_clean"]])
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
"""
TOKENS = False
HF_TOKEN = "HF"

if not TOKENS:
    model_name = "Qwen/Qwen2.5-3B-Instruct"

    model = AutoModelForCausalLM.from_pretrained(
        model_name,
        torch_dtype=torch.float16 if torch.cuda.is_available() else torch.float32,
        device_map="auto"
    )
    tokenizer = AutoTokenizer.from_pretrained(model_name)

    print("hf_device_map:", getattr(model, "hf_device_map", None))


def build_prompt(contract_a: str, contract_b: str, title_a, title_b) -> list[dict]:
    prompt = f"""Contract A:
    Title: {df.loc[i, "kalshi_market"]}
    Rules: {row_k}

    Contract B:
    Title: {df.loc[i, "polymarket_market"]}
    Rules: {row_p}

    Answer with only 0 or 1."""
    return [
        {"role": "system", "content": PRE_PROMPT},
        {"role": "user", "content": prompt}
    ]


for i in range(0, 30):
    title_p = df.loc[i, "polymarket_candidate_title_clean"]
    title_k = df.loc[i, "kalshi_candidate_title_clean"]
    row_k = df.loc[i, "kalshi_rules_text"]
    row_p = df.loc[i, "polymarket_rules_text"]

    if pd.isna(row_k) or pd.isna(row_p):
        print(f"Row {i}: skipped because of missing rules text")
        continue

    start = time.perf_counter()

    if TOKENS:
        client = OpenAI(
            base_url="https://router.huggingface.co/v1",
            api_key=HF_TOKEN,
        )

        completion = client.chat.completions.create(
            model="Qwen/Qwen3.5-35B-A3B:novita",
            messages=build_prompt(str(row_k), str(row_p)),
        )

        response = completion.choices[0].message.content
        print(f"Row {i}")
        print(response)

    else:
        messages = build_prompt(str(row_k), str(row_p), str(title_k), str(title_p))

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
            max_new_tokens=50,
            do_sample=False,
            pad_token_id=tokenizer.eos_token_id
        )

        if torch.cuda.is_available():
            torch.cuda.synchronize()

        new_tokens = generated_ids[:, model_inputs["input_ids"].shape[1]:]
        response = tokenizer.batch_decode(new_tokens, skip_special_tokens=True)[0].strip()

        print(f"Row {i}")
        print(response)
        print(f"\nActual titles: kalshi: {title_k}, \n polymarket: {title_p}")
    pred = parse_binary_response(response)
    predictions.append({
        "row": i,
        "llm_binary": pred,
        "kalshi_title": df.loc[i, "kalshi_market"],
        "polymarket_title": df.loc[i, "polymarket_market"],
        "raw_response": response,
    })
    end = time.perf_counter()
    print(f"\nPrompt {i} took {end - start:.2f} seconds\n")
print(predictions[0])