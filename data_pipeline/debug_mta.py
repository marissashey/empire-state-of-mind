"""tempfile for debugging mta data"""

import pandas as pd
from pathlib import Path

# find the most recent terminal_otp file
data_dir = Path("data/raw/mta")
otp_files = list(data_dir.glob("terminal_otp_*.csv"))

if otp_files:
    latest = sorted(otp_files)[-1]
    print(f"reading: {latest}")
    
    df = pd.read_csv(latest)
    
    print(f"\nshape: {df.shape}")
    print(f"\ncolumns:\n{df.columns.tolist()}")
    print(f"\nfirst few rows:")
    print(df.head())
    print(f"\ndata types:")
    print(df.dtypes)
    print(f"\nsample values from each column:")
    for col in df.columns:
        sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else "all null"
        print(f"  {col}: {sample}")
else:
    print("no terminal_otp files found")