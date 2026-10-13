import pandas as pd
p = "include/data/nammamart/orders.csv"
df = pd.read_csv(p, dtype=str, keep_default_na=False)
m = df["store_id"] == "S01"
df.loc[m, "unit_price"] = (pd.to_numeric(df.loc[m, "unit_price"]) * 100).round(2).astype(str)
df.loc[m, "order_amount"] = (pd.to_numeric(df.loc[m, "order_amount"]) * 100).round(2).astype(str)
df.to_csv(p, index=False)
print(f"inflated {m.sum()} S01 rows by 100x")