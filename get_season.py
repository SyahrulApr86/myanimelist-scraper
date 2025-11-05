import requests
from bs4 import BeautifulSoup
import csv
import os
import random
import time
import pandas as pd

# ==========================================
# KONFIGURASI
# ==========================================
ARCHIVE_URL = "https://myanimelist.net/anime/season/archive"
OUTPUT_FILE = "mal_season_links.csv"
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) Gecko/20100101 Firefox/130.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
]


# ==========================================
# SCRAPER
# ==========================================
def scrape_season_links():
    """Ambil semua link musim dari halaman archive"""
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    res = requests.get(ARCHIVE_URL, headers=headers)
    if res.status_code != 200:
        raise Exception(f"Gagal mengambil halaman archive: {res.status_code}")

    soup = BeautifulSoup(res.text, "html.parser")
    links = []

    # Cari semua <a> yang mengarah ke /anime/season/<tahun>/<musim>
    for a in soup.select("a[href*='/anime/season/']"):
        href = a.get("href")
        name = a.get_text(strip=True)
        if href and name and "/anime/season/" in href:
            links.append({"name": name, "url": href})

    print(f"Total ditemukan {len(links)} season (termasuk kemungkinan duplikat).")
    return links


def save_unique_to_csv(data, filename):
    """Simpan hasil, hilangkan duplikat"""
    write_header = not os.path.exists(filename)

    # Tambahkan data baru
    with open(filename, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["name", "url"])
        if write_header:
            writer.writeheader()
        for row in data:
            writer.writerow(row)

    # Hilangkan duplikat di CSV
    df = pd.read_csv(filename)
    before = len(df)
    df = df.drop_duplicates(subset=["name", "url"], keep="first").sort_values(by="url", ascending=False)
    after = len(df)
    df.to_csv(filename, index=False, encoding="utf-8")

    print(f"Hapus {before - after} duplikat, total unik sekarang: {after}")


# ==========================================
# MAIN
# ==========================================
if __name__ == "__main__":
    print(f"Mengambil daftar season dari {ARCHIVE_URL} ...")
    data = scrape_season_links()
    save_unique_to_csv(data, OUTPUT_FILE)
    print(f"Selesai ✅ Data tersimpan di: {os.path.abspath(OUTPUT_FILE)}")
