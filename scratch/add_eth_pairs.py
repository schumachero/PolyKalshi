import os
import re
import pandas as pd
from datetime import datetime

poly_slugs = [
    ("Will Ethereum reach $4,000 in April?", "will-ethereum-reach-4000-in-april-2026"),
    ("Will Ethereum reach $3,800 in April?", "will-ethereum-reach-3800-in-april-2026"),
    ("Will Ethereum reach $3,400 in April?", "will-ethereum-reach-3400-in-april-2026"),
    ("Will Ethereum dip to $1,600 in April?", "will-ethereum-dip-to-1600-in-april-2026"),
    ("Will Ethereum dip to $600 in April?", "will-ethereum-dip-to-600-in-april-2026"),
    ("Will Ethereum dip to $400 in April?", "will-ethereum-dip-to-400-in-april-2026"),
    ("Will Ethereum reach $3,000 in April?", "will-ethereum-reach-3000-in-april-2026"),
    ("Will Ethereum reach $2,600 in April?", "will-ethereum-reach-2600-in-april-2026"),
    ("Will Ethereum reach $2,400 in April?", "will-ethereum-reach-2400-in-april-2026"),
    ("Will Ethereum reach $2,800 in April?", "will-ethereum-reach-2800-in-april-2026"),
    ("Will Ethereum reach $2,200 in April?", "will-ethereum-reach-2200-in-april-2026"),
    ("Will Ethereum dip to $1,400 in April?", "will-ethereum-dip-to-1400-in-april-2026"),
    ("Will Ethereum dip to $1,000 in April?", "will-ethereum-dip-to-1000-in-april-2026"),
    ("Will Ethereum dip to $200 in April?", "will-ethereum-dip-to-200-in-april-2026"),
    ("Will Ethereum reach $3,600 in April?", "will-ethereum-reach-3600-in-april-2026"),
    ("Will Ethereum reach $3,200 in April?", "will-ethereum-reach-3200-in-april-2026"),
    ("Will Ethereum dip to $2,000 in April?", "will-ethereum-dip-to-2000-in-april-2026"),
    ("Will Ethereum dip to $1,800 in April?", "will-ethereum-dip-to-1800-in-april-2026"),
    ("Will Ethereum dip to $800 in April?", "will-ethereum-dip-to-800-in-april-2026"),
    ("Will Ethereum dip to $1,200 in April?", "will-ethereum-dip-to-1200-in-april-2026")
]

csv_path = 'Data/tracked_pairs_btc_apr.csv'
df = pd.read_csv(csv_path)

rows = []

for title, slug in poly_slugs:
    # Extract price
    m = re.search(r'\$(\d+),?(\d*)', title)
    if not m:
        continue
    price = int(m.group(1).replace(',', '') + m.group(2).replace(',', ''))
    price_cents = price * 100
    
    if "reach" in title:
        k_ticker = f"KXETHMAXMON-ETH-26APR30-{price_cents}"
        k_title = f"Will ETH trimmed mean be above ${float(price):.2f} by 11:59 PM ET on Apr 30, 2026?"
        notes = f"ETH reach ${price} April 2026"
    else:
        k_ticker = f"KXETHMINMON-ETH-26APR30-{price_cents}"
        k_title = f"Will ETH trimmed mean be below ${float(price):.2f} by 11:59 PM ET on Apr 30, 2026?"
        notes = f"ETH dip below ${price} April 2026"
        
    pair_id = f"{k_ticker}__{slug}"
    
    row = {
        'pair_id': pair_id,
        'active': True,
        'kalshi_ticker': k_ticker,
        'kalshi_title': k_title,
        'kalshi_side_held': "",
        'kalshi_quantity': "",
        'polymarket_ticker': slug,
        'polymarket_title': title,
        'polymarket_side_held': "",
        'polymarket_quantity': "",
        'close_time': '2026-05-01T03:59:59Z',
        'match_score': 1.0,
        'max_position_per_pair_usd': 100.0,
        'min_profit_pct': 1.0,
        'min_liquidity_usd': 50.0,
        'cooldown_minutes': 30,
        'notes': notes
    }
    rows.append(row)

df_new = pd.DataFrame(rows)
df_final = pd.concat([df, df_new], ignore_index=True)
df_final.to_csv(csv_path, index=False)
print(f"Added {len(rows)} ETH pairs to {csv_path}")
