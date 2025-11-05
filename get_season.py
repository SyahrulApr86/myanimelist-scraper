import requests
from bs4 import BeautifulSoup
import csv
import os

URL = "https://myanimelist.net/anime/season/archive"
OUTPUT_FILE = "mal_season_links.csv"

def scrape_season_archive():
    headers = {"User-Agent": "Mozilla/5.0"}
    res = requests.get(URL, headers=headers)
    if res.status_code != 200:
        raise Exception(f"Gagal mengambil halaman archive: {res.status_code}")

    soup = BeautifulSoup(res.text, "html.parser")
    results = []

    # Ambil semua <table> dengan class anime-seasonal-byseason
    tables = soup.select("table.anime-seasonal-byseason")

    for table in tables:
        for a in table.select("a[href*='/anime/season/']"):
            season_name = a.get_text(strip=True)
            season_url = a["href"].strip()
            if season_name and season_url:
                results.append({"name": season_name, "url": season_url})

    return results

def save_to_csv(data, filename):
    write_header = not os.path.exists(filename)
    with open(filename, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["name", "url"])
        if write_header:
            writer.writeheader()
        for row in data:
            writer.writerow(row)
    print(f"Disimpan ke {filename} ({len(data)} entri)")

if __name__ == "__main__":
    print(f"Mengambil data musim dari {URL}")
    data = scrape_season_archive()
    save_to_csv(data, OUTPUT_FILE)
    print("Selesai.")
