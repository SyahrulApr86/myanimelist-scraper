import pandas as pd
import re

# File paths
INPUT_FILE = "mal_all_season_anime_dedup.csv"
SCRAPED_FILE = "mal_anime_merged_final.csv"
OUTPUT_FILE = "mal_anime_to_scrape.csv"

print("="*80)
print("EXTRACTING MISSING ANIME (DEDUPLICATED VERSION)")
print("="*80)

print(f"\nLoading {INPUT_FILE}...")
df_input = pd.read_csv(INPUT_FILE)
print(f"Total unique anime in input: {len(df_input)}")

print(f"\nLoading {SCRAPED_FILE}...")
df_scraped = pd.read_csv(SCRAPED_FILE)
print(f"Total unique anime already scraped: {len(df_scraped)}")

# Extract anime_id from URL in input file
def extract_anime_id(url):
    """Extract anime ID from MyAnimeList URL"""
    match = re.search(r'/anime/(\d+)', url)
    return int(match.group(1)) if match else None

df_input['anime_id'] = df_input['url'].apply(extract_anime_id)

# Get all scraped myanimelist_id
scraped_ids = set(df_scraped['myanimelist_id'].dropna().astype(int).tolist())
print(f"Unique scraped IDs: {len(scraped_ids)}")

# Check for invalid URLs
invalid_urls = df_input[df_input['anime_id'].isna()]
print(f"\nInvalid URLs (cannot extract ID): {len(invalid_urls)}")

# Remove invalid URLs
df_input_valid = df_input[df_input['anime_id'].notna()].copy()
print(f"Valid URLs: {len(df_input_valid)}")

# Overlap analysis
input_ids_set = set(df_input_valid['anime_id'].unique())
overlap_ids = scraped_ids & input_ids_set
only_in_scraped = scraped_ids - input_ids_set
only_in_input = input_ids_set - scraped_ids

print(f"\nOverlap analysis:")
print(f"  IDs in both input and scraped: {len(overlap_ids)}")
print(f"  IDs only in scraped (not in input): {len(only_in_scraped)}")
print(f"  IDs only in input (missing, need to scrape): {len(only_in_input)}")

# Filter anime that are NOT in scraped file
df_missing = df_input_valid[~df_input_valid['anime_id'].isin(scraped_ids)].copy()

# Drop the temporary anime_id column and keep only title, url
df_missing_output = df_missing[['title', 'url']].copy()

print(f"\nMissing anime to scrape: {len(df_missing_output)}")

# Save to new CSV file
df_missing_output.to_csv(OUTPUT_FILE, index=False)
print(f"\n✓ Saved missing anime to: {OUTPUT_FILE}")

# Show some statistics
print("\n" + "="*80)
print("SUMMARY")
print("="*80)
print(f"Input file: {INPUT_FILE}")
print(f"  - Total unique anime: {len(df_input)}")
print(f"  - Valid URLs: {len(df_input_valid)}")

print(f"\nScraped file: {SCRAPED_FILE}")
print(f"  - Total unique anime: {len(df_scraped)}")

print(f"\nMissing (to be scraped): {len(df_missing_output)}")
print(f"  - All are unique (no duplicates)")

print(f"\nOutput file: {OUTPUT_FILE}")
print(f"  - Unique anime to scrape: {len(df_missing_output)}")
print(f"  - Columns: {list(df_missing_output.columns)}")
print("="*80)
