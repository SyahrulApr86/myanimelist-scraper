import requests
from bs4 import BeautifulSoup
import csv
import os
import time
import random

# ==========================================
# KONFIGURASI
# ==========================================
INPUT_FILE = "mal_season_links.csv"        # hasil dari scrape archive
OUTPUT_FILE = "mal_all_season_anime.csv"   # output utama
MAX_RETRIES = 10                            # percobaan ulang jika kosong
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) Gecko/20100101 Firefox/130.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
]

# ==========================================
# UTILITAS
# ==========================================
def read_season_links(filename):
    """Baca daftar link musim dari CSV"""
    seasons = []
    with open(filename, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get("name")
            url = row.get("url")
            if url:
                seasons.append({"name": name, "url": url})
    return seasons


def scrape_anime_from_season(season_name, url):
    """Ambil daftar anime dari halaman musim"""
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    res = requests.get(url, headers=headers)
    if res.status_code != 200:
        print(f"[{season_name}] Gagal diambil ({res.status_code})")
        return []

    soup = BeautifulSoup(res.text, "html.parser")
    results = []
    for h2 in soup.select("h2.h2_anime_title a[href*='/anime/']"):
        title = h2.get_text(strip=True)
        link = h2["href"].strip()
        results.append({
            "season": season_name,
            "title": title,
            "url": link
        })
    return results


def append_to_csv(data, filename):
    """Langsung tulis hasil ke CSV (append per season)"""
    write_header = not os.path.exists(filename)
    with open(filename, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["season", "title", "url"])
        if write_header:
            writer.writeheader()
        for row in data:
            writer.writerow(row)


# ==========================================
# MAIN LOOP
# ==========================================
if __name__ == "__main__":
    seasons = read_season_links(INPUT_FILE)
    print(f"Total musim ditemukan: {len(seasons)}\n")

    for season in seasons:
        name = season["name"]
        url = season["url"]

        success = False
        for attempt in range(1, MAX_RETRIES + 1):
            print(f"[{name}] Mengambil (percobaan {attempt})...")
            anime_list = scrape_anime_from_season(name, url)

            if len(anime_list) > 0:
                append_to_csv(anime_list, OUTPUT_FILE)
                print(f"[{name}] {len(anime_list)} judul ditemukan dan disimpan.\n")
                success = True
                break
            else:
                print(f"[{name}] Tidak ditemukan judul. Ulangi setelah jeda...")
                time.sleep(random.uniform(0.1, 1))

        if not success:
            print(f"[{name}] Gagal mendapatkan data setelah {MAX_RETRIES} percobaan.\n")

        # jeda acak antar musim agar aman
        time.sleep(random.uniform(0.1, 1))

    print(f"Selesai. Semua hasil disimpan di {OUTPUT_FILE}")
