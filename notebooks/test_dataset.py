import pandas as pd

print("STARTING SCRIPT...")

df = pd.read_csv("data/movies_metadata.csv", low_memory=False)

print("\nColumns:")
print(df.columns)

print("\nSample titles:")
print(df[['title']].head())

wb_df = df[df['production_companies'].str.contains("Warner Bros", na=False)]

print("\nWB movie count:", len(wb_df))
print(wb_df[['title']].head())

wb_df = wb_df[['id', 'title']].dropna()
wb_df.to_csv("data/wb_movies.csv", index=False)

print("\n✅ Clean dataset saved!")