import glob
import pandas as pd
import pyarrow.dataset as ds

t = pd.read_csv("Strategy_1_TransactionsHistory.csv", dtype=str)
t = t[t["Leg Number"].str.startswith("LEG")].copy()
t["date"] = pd.to_datetime(t["Entry Time"].str[:11], format="%d %b %Y")
t["strike"] = t["Scrip Detail"].str.extract(r"^(\d+)")[0].astype(float)
t["option_type"] = t["Scrip Detail"].str.extract(r"(CE|PE)")[0]
t["expiry"] = pd.to_datetime(
    t["Scrip Detail"].str.extract(r"(\d{2} \w{3} \d{2})$")[0], format="%d %b %y")
keys = t[["date", "strike", "option_type", "expiry"]].drop_duplicates()
keys = keys[keys.date >= "2024-10-01"]
strikes = keys.strike.unique().tolist()

out = []
for f in sorted(glob.glob("NIFTY_*.parquet")):
    print("reading", f)
    tbl = ds.dataset(f).to_table(
        filter=ds.field("strike").isin(strikes),
        columns=["date", "timestamp", "expiry", "strike", "option_type",
                 "open", "high", "low", "close", "volume"])
    df = tbl.to_pandas()
    df["date"] = pd.to_datetime(df["date"])
    df["expiry"] = pd.to_datetime(df["expiry"])
    out.append(df.merge(keys, on=["date", "strike", "option_type", "expiry"]))

res = pd.concat(out)
print(len(res), "rows,", res.date.nunique(), "days")
res.to_parquet("my_strikes_1min.parquet")