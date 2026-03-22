import pandas as pd
import torch
from sentence_transformers import SentenceTransformer, util

# Keep the model instance outside so we don't reload it every time
_model = None

def get_model():
    global _model
    if _model is None:
        _model = SentenceTransformer('all-MiniLM-L6-v2')
    return _model

def generate_semantic_matches(kalshi_df, polymarket_df, title_col_kalshi='market_title', title_col_poly='market_title', threshold=0.40, top_k=5, max_date_diff=45):
    """
    Finds matches strictly based on cosine similarity of the given title columns.
    Returns a DataFrame ranked by semantic similarity.
    """
    model = get_model()
    
    # Ensure no NaN
    kalshi_df = kalshi_df.copy()
    polymarket_df = polymarket_df.copy()
    
    # Filter closed/finished markets
    if "status" in kalshi_df.columns:
        kalshi_df["status"] = kalshi_df["status"].astype(str).str.lower().str.strip()
        kalshi_df = kalshi_df[~kalshi_df["status"].isin(["finalized", "settled"])]
        
    if "status" in polymarket_df.columns:
        polymarket_df["status"] = polymarket_df["status"].astype(str).str.lower().str.strip()
        polymarket_df = polymarket_df[polymarket_df["status"] == "active"]

    # Convert close_time to datetime
    if "close_time" in kalshi_df.columns:
        kalshi_df["close_time"] = pd.to_datetime(kalshi_df["close_time"], errors='coerce')
    if "close_time" in polymarket_df.columns:
        polymarket_df["close_time"] = pd.to_datetime(polymarket_df["close_time"], errors='coerce')

    kalshi_df[title_col_kalshi] = kalshi_df[title_col_kalshi].fillna("")
    polymarket_df[title_col_poly] = polymarket_df[title_col_poly].fillna("")
    
    k_titles = kalshi_df[title_col_kalshi].tolist()
    p_titles = polymarket_df[title_col_poly].tolist()
    
    if not k_titles or not p_titles:
        return pd.DataFrame()
        
    print(f"Computing embeddings for {len(k_titles)} Kalshi titles and {len(p_titles)} Polymarket titles...")
    k_embeds = model.encode(k_titles, convert_to_tensor=True)
    p_embeds = model.encode(p_titles, convert_to_tensor=True)
    
    # Compute cosine similarities
    cosine_scores = util.cos_sim(k_embeds, p_embeds)
    
    matches_list = []
    
    for i in range(len(k_titles)):
        top_results = torch.topk(cosine_scores[i], k=min(top_k, len(p_titles)))
        for score, idx in zip(top_results[0], top_results[1]):
            s_val = score.item()
            idx_val = idx.item()
            if s_val >= threshold:
                k_row = kalshi_df.iloc[i]
                p_row = polymarket_df.iloc[idx_val]
                
                # Filter by date
                if pd.notna(k_row.get("close_time")) and pd.notna(p_row.get("close_time")):
                    date_diff = abs((k_row["close_time"] - p_row["close_time"]).days)
                    if date_diff > max_date_diff:
                        continue
                
                match_dict = {
                    "kalshi_series_ticker": k_row.get("series_ticker", ""),
                    "kalshi_market_ticker": k_row.get("market_ticker", ""),
                    "kalshi_market_title": k_titles[i],
                    "kalshi_rules": k_row.get("rules_text", ""),
                    "polymarket_series_ticker": p_row.get("series_ticker", ""),
                    "polymarket_market_ticker": p_row.get("market_ticker", ""),
                    "polymarket_market_title": p_titles[idx_val],
                    "polymarket_rules": p_row.get("rules_text", ""),
                    "semantic_score": round(s_val, 4)
                }
                matches_list.append(match_dict)
                
    matches_df = pd.DataFrame(matches_list)
    if not matches_df.empty:
        matches_df.sort_values(by="semantic_score", ascending=False, inplace=True)
        
    return matches_df

def rescore_existing_matches(matches_df, title_col_kalshi='kalshi_market', title_col_poly='polymarket_market', threshold=None):
    """
    Takes an existing matches dataframe (like from matching.py) and appends a 'semantic_score'.
    Optionally drops rows below 'threshold'.
    """
    if matches_df.empty:
        return matches_df
        
    model = get_model()
    
    # Ensure titles are strings
    k_titles = matches_df[title_col_kalshi].fillna("").astype(str).tolist()
    p_titles = matches_df[title_col_poly].fillna("").astype(str).tolist()
    
    print(f"Computing semantic scores for {len(k_titles)} existing match pairs...")
    k_embeds = model.encode(k_titles, convert_to_tensor=True)
    p_embeds = model.encode(p_titles, convert_to_tensor=True)
    
    # We only need the diagonal (pairwise) similarity since the rows are already paired
    # Using element-wise multiplication sum
    scores = (k_embeds * p_embeds).sum(dim=-1).cpu().tolist()
    
    scored_df = matches_df.copy()
    scored_df['semantic_score'] = [round(s, 4) for s in scores]
    
    if threshold is not None:
        scored_df = scored_df[scored_df['semantic_score'] >= threshold]
        
    if "combined_score" in scored_df.columns:
        scored_df.sort_values(by=["combined_score", "semantic_score"], ascending=[False, False], inplace=True)
    else:
        scored_df.sort_values(by="semantic_score", ascending=False, inplace=True)
        
    return scored_df

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Semantic Matching for Kalshi and Polymarket")
    parser.add_argument("--mode", choices=["standalone", "rescore"], default="standalone", 
                        help="standalone: compute matches from base CSVs. rescore: enhance existing matches CSV.")
    parser.add_argument("--k_csv", default="Data/kalshi_markets.csv")
    parser.add_argument("--p_csv", default="Data/polymarket_markets.csv")
    parser.add_argument("--matches_csv", default="Data/candidate_series_matches.csv")
    parser.add_argument("--out_csv", default="Data/semantic_matches.csv")
    parser.add_argument("--threshold", type=float, default=0.40)
    
    args = parser.parse_args()
    
    if args.mode == "standalone":
        try:
            k_df = pd.read_csv(args.k_csv)
            p_df = pd.read_csv(args.p_csv)
            out_df = generate_semantic_matches(k_df, p_df, threshold=args.threshold)
            out_df.to_csv(args.out_csv, index=False)
            print(f"Wrote {len(out_df)} standalone semantic matches to {args.out_csv}")
        except FileNotFoundError:
            print(f"Error: Could not find base CSV files '{args.k_csv}' or '{args.p_csv}'.")
    elif args.mode == "rescore":
        try:
            m_df = pd.read_csv(args.matches_csv)
            out_df = rescore_existing_matches(m_df, threshold=args.threshold)
            out_df.to_csv(args.out_csv, index=False)
            print(f"Wrote {len(out_df)} rescored matches to {args.out_csv}")
        except FileNotFoundError:
            print(f"Error: Could not find matches CSV '{args.matches_csv}'.")

if __name__ == "__main__":
    main()
