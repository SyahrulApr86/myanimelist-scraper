import requests
from bs4 import BeautifulSoup
import csv
import os
import time
import random

# ==========================================
# KONFIGURASI
# ==========================================
INPUT_FILE = "mal_season_links.csv"
OUTPUT_FILE = "mal_all_season_anime.csv"

START_INDEX = 393   # indeks baris yang mau dijadikan titik mulai (0 = dari awal)
MAX_RETRIES = 10
REQUEST_TIMEOUT = 10

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) Gecko/20100101 Firefox/130.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
]


def read_season_links(filename):
    seasons = []
    with open(filename, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            seasons.append({
                "name": row["name"].strip(),
                "url": row["url"].strip()
            })
    return seasons


def scrape_anime_from_season(season_name, url):
    """Ambil daftar anime dari halaman musim dengan retry jika hasil kosong"""
    for attempt in range(1, MAX_RETRIES + 1):
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        try:
            res = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)

            if res.status_code == 200:
                soup = BeautifulSoup(res.text, "html.parser")
                results = [
                    {
                        "season": season_name,
                        "title": h2.get_text(strip=True),
                        "url": h2["href"].strip()
                    }
                    for h2 in soup.select("h2.h2_anime_title a[href*='/anime/']")
                ]

                if results:
                    print(f"[{season_name}] Percobaan {attempt}: {len(results)} judul ditemukan.")
                    return results
                else:
                    print(f"[{season_name}] Percobaan {attempt}: hasil kosong, retry...")
                    time.sleep(random.uniform(1, 3))
                    continue  # ulangi attempt

            else:
                print(f"[{season_name}] Status {res.status_code}, retry...")
                time.sleep(random.uniform(1, 3))

        except Exception as e:
            print(f"[{season_name}] Error ({type(e).__name__}), retry...")
            time.sleep(random.uniform(2, 5))

    print(f"[{season_name}] Gagal setelah {MAX_RETRIES} percobaan (hasil tetap kosong).")
    return []  # tetap kembalikan kosong setelah limit



def append_to_csv(data, filename):
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

    print(f"Total season dalam file: {len(seasons)}")
    print(f"Mulai scraping dari index {START_INDEX}: {seasons[START_INDEX]['name']}")
    print()

    for i in range(START_INDEX, len(seasons)):

        season_name = seasons[i]["name"]
        season_url = seasons[i]["url"]
        print(f"[{i}] {season_name}: mulai scrape")

        anime_list = scrape_anime_from_season(season_name, season_url)

        append_to_csv(anime_list, OUTPUT_FILE)
        print(f"[{season_name}] {len(anime_list)} judul disimpan.\n")

        time.sleep(random.uniform(0.5, 2.0))

    print("Selesai.")
