import requests
from bs4 import BeautifulSoup
import csv
import os

# ==========================================
# KONFIGURASI
# ==========================================
SEASON_URL = "https://myanimelist.net/anime/season/2025/summer"
OUTPUT_FILE = "mal_anime_2025_summer.csv"

# ==========================================
# SCRAPER
# ==========================================
def scrape_anime_season(url):
    headers = {"User-Agent": "Mozilla/5.0"}
    res = requests.get(url, headers=headers)
    if res.status_code != 200:
        raise Exception(f"Gagal mengambil halaman musim: {res.status_code}")

    soup = BeautifulSoup(res.text, "html.parser")
    results = []

    # Setiap judul anime di MyAnimeList musim punya <h2 class="h2_anime_title">
    for h2 in soup.select("h2.h2_anime_title a[href*='/anime/']"):
        title = h2.get_text(strip=True)
        link = h2["href"].strip()
        results.append({"title": title, "url": link})

    return results


# ==========================================
# SIMPAN KE CSV
# ==========================================
def save_to_csv(data, filename):
    write_header = not os.path.exists(filename)
    with open(filename, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["title", "url"])
        if write_header:
            writer.writeheader()
        for row in data:
            writer.writerow(row)
    print(f"Disimpan ke {filename} ({len(data)} judul)")


# ==========================================
# MAIN
# ==========================================
if __name__ == "__main__":
    print(f"Mengambil daftar anime dari {SEASON_URL}")
    data = scrape_anime_season(SEASON_URL)
    save_to_csv(data, OUTPUT_FILE)
    print("Selesai.")
