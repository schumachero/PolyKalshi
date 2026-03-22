import pytest
import pandas as pd
from src.matching.semantic_matching import generate_semantic_matches, rescore_existing_matches

def test_generate_semantic_matches():
    k_data = pd.DataFrame([
        {"market_ticker": "K1", "series_ticker": "KS1", "market_title": "Will Trump win the election in 2024?", "rules_text": "Rules A"},
        {"market_ticker": "K2", "series_ticker": "KS2", "market_title": "Will the sun rise tomorrow?", "rules_text": "Rules B"}
    ])
    p_data = pd.DataFrame([
        {"market_ticker": "P1", "series_ticker": "PS1", "market_title": "Donald Trump wins 2024 Presidential Election", "rules_text": "Rules C"},
        {"market_ticker": "P2", "series_ticker": "PS2", "market_title": "Will it rain tomorrow in NY?", "rules_text": "Rules D"}
    ])
    
    df = generate_semantic_matches(k_data, p_data, threshold=0.10)
    
    assert not df.empty
    
    # The trump match should be the highest
    trump_match = df[(df['kalshi_market_ticker'] == 'K1') & (df['polymarket_market_ticker'] == 'P1')]
    assert not trump_match.empty
    assert trump_match.iloc[0]['semantic_score'] > 0.50

def test_rescore_existing_matches():
    matches = pd.DataFrame([
        {
            "kalshi_market": "trump win election",
            "polymarket_market": "donald trump wins election",
            "combined_score": 0.5,
            "kalshi_market_ticker": "K1",
            "polymarket_market_ticker": "P1"
        },
        {
            "kalshi_market": "sun rise",
            "polymarket_market": "rain ny",
            "combined_score": 0.2,
            "kalshi_market_ticker": "K2",
            "polymarket_market_ticker": "P2"
        }
    ])
    
    rescored = rescore_existing_matches(matches, threshold=0.1)
    
    assert 'semantic_score' in rescored.columns
    assert len(rescored) > 0
    # Higher combined_score or semantic_score should maintain rank
    assert rescored.iloc[0]['kalshi_market_ticker'] == 'K1'
