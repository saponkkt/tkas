import pandas as pd

path = r"C:\Users\User\Desktop\Validation_TAS\TAS\Lion Air\LGS\1 sep\SL788_3c009538.csv"
df = pd.read_csv(path)
alt = pd.to_numeric(df.get("altitude"), errors="coerce")
mask = (alt >= 10000) & (alt <= 20000)
idx = df.index[mask].tolist()

if idx:
    print("First row (1-based):", idx[0] + 1)
    print("Last row  (1-based):", idx[-1] + 1)
    print("Count rows in range:", len(idx))
else:
    print("No rows with 10000 <= altitude <= 20000")