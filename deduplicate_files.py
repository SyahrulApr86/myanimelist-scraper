import pandas as pd
import re

print("="*80)
print("DEDUPLICATING FILES")
print("="*80)

# ==========================================
# 1. Deduplicate mal_all_season_anime.csv
# ==========================================
print("\n1. Processing mal_all_season_anime.csv...")
df_all_season = pd.read_csv("mal_all_season_anime.csv")
print(f"   Original rows: {len(df_all_season)}")

# Extract anime_id from URL
def extract_anime_id(url):
    match = re.search(r'/anime/(\d+)', url)
    return int(match.group(1)) if match else None

df_all_season['anime_id'] = df_all_season['url'].apply(extract_anime_id)

# Remove duplicates (keep first occurrence)
df_all_season_dedup = df_all_season.drop_duplicates(subset='anime_id', keep='first')

# Keep only title and url columns
df_all_season_output = df_all_season_dedup[['title', 'url']].copy()

print(f"   After dedup: {len(df_all_season_output)} unique anime")
print(f"   Removed: {len(df_all_season) - len(df_all_season_output)} duplicates")

# Save to new file
output_file_1 = "mal_all_season_anime_dedup.csv"
df_all_season_output.to_csv(output_file_1, index=False)
print(f"   ✓ Saved to: {output_file_1}")

# ==========================================
# 2. Deduplicate mal_anime_merged_dedup.csv
# ==========================================
print("\n2. Processing mal_anime_merged_dedup.csv...")
df_merged = pd.read_csv("mal_anime_merged_dedup.csv")
print(f"   Original rows: {len(df_merged)}")

# Remove duplicates based on myanimelist_id
df_merged_dedup = df_merged.drop_duplicates(subset='myanimelist_id', keep='first')

# Remove csv_index column
columns_to_keep = [col for col in df_merged_dedup.columns if col != 'csv_index']
df_merged_output = df_merged_dedup[columns_to_keep].copy()

print(f"   After dedup: {len(df_merged_output)} unique anime")
print(f"   Removed: {len(df_merged) - len(df_merged_output)} duplicates")

# Save to new file
output_file_2 = "mal_anime_merged_final.csv"
df_merged_output.to_csv(output_file_2, index=False)
print(f"   ✓ Saved to: {output_file_2}")

# ==========================================
# Summary
# ==========================================
print("\n" + "="*80)
print("SUMMARY")
print("="*80)
print(f"\nFile 1: {output_file_1}")
print(f"  - Unique anime: {len(df_all_season_output)}")
print(f"  - Columns: {list(df_all_season_output.columns)}")

print(f"\nFile 2: {output_file_2}")
print(f"  - Unique anime: {len(df_merged_output)}")
print(f"  - Columns: {len(df_merged_output.columns)} columns")
print(f"  - Removed 'csv_index' column")

print("\n" + "="*80)
print("✓ Done!")
print("="*80)
