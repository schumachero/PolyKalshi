import pandas as pd
import numpy as np

df=pd.read_csv(r"Data/candidate_series_matches.csv")
#print(df.columns)
df_k=pd.read_csv(r"Data/kalshi_markets.csv")
df_p=pd.read_csv(r"Data/polymarket_markets.csv")
#print(df_k.head())
#print(df_p.head())
df_compare=df.filter(items=["kalshi_series","polymarket_series","score","date_diff_days"])
df_rules=df.filter(items=["polymarket_rules_text","kalshi_rules_text"])
#print(df_rules.head())
#print(df_compare.head())



#print(df_k.columns)


#df_compare=df.filter(items=["kalshi_series","polymarket_series","score","date_diff_days"])
#df_rules=df.filter(items=["polymarket_rules_text","kalshi_rules_text"])
#print(df_compare)
#print(df_rules)
#print(df_rules["polymarket_rules_text"][15])
#print(df_rules["kalshi_rules_text"][15])

# Cleaned up: data is now pre-cleaned by APIs

print(df_compare.head(30))
df["kalshi_rules_extracted"] = df["kalshi_rules_text"]
row = df.loc[3, ["kalshi_series", "polymarket_series",
                  "polymarket_rules_text", "kalshi_rules_extracted"]]
print(row["kalshi_series"])
print(row["polymarket_series"])
print()
print("POLYMARKET RULES:")
print(row["polymarket_rules_text"])
print()
print("KALSHI RULES:")
print(row["kalshi_rules_extracted"])