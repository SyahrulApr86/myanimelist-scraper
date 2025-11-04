import requests
from bs4 import BeautifulSoup
import json
import csv
import os
import re
import random
import time

# ==========================================
# KONFIGURASI
# ==========================================
START_ID = 35001
END_ID = 40000
MAX_CONSECUTIVE_404 = 1000
OUTPUT_FILE = "mal_anime_35001_40000.csv"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) Gecko/20100101 Firefox/130.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
]


# ==========================================
# SCRAPER FUNGSI
# ==========================================
def get_characters(anime_url: str, headers):
    characters_url = anime_url.rstrip("/") + "/characters"
    print(f"Mengambil karakter dari {characters_url}")

    res = requests.get(characters_url, headers=headers)
    if res.status_code != 200:
        print(f"Gagal mengambil karakter ({res.status_code})")
        return []

    soup = BeautifulSoup(res.text, "html.parser")
    characters = []

    for char_link in soup.select("a[href*='/character/'] h3.h3_character_name"):
        parent_a = char_link.find_parent("a", href=True)
        if not parent_a:
            continue
        char_url = parent_a["href"]
        char_name = char_link.get_text(strip=True)
        match_id = re.search(r"/character/(\d+)", char_url)
        char_id = int(match_id.group(1)) if match_id else None

        characters.append({
            "id": char_id,
            "name": char_name,
            "url": char_url
        })
    return characters


def scrape_myanimelist(anime_id: int, headers):
    url = f"https://myanimelist.net/anime/{anime_id}"
    res = requests.get(url, headers=headers)
    if res.status_code == 404:
        print(f"{anime_id} not found")
        return None
    if res.status_code != 200:
        print(f"Gagal ambil ID {anime_id} ({res.status_code})")
        return None

    soup = BeautifulSoup(res.text, "html.parser")

    canonical_tag = soup.find("meta", property="og:url") or soup.find("link", rel="canonical")
    canonical_url = canonical_tag.get("content") if canonical_tag else url

    title_tag = soup.select_one("h1.title-name, h1.title")
    title = title_tag.get_text(strip=True) if title_tag else None
    description_tag = soup.find("p", itemprop="description")
    description = description_tag.get_text(strip=True) if description_tag else None

    img = soup.select_one("div.leftside img")
    image_url = img.get("data-src") or img.get("src") if img else None

    def extract_section_between(start_text, end_text=None):
        data = {}
        start = soup.find("h2", string=start_text)
        if not start:
            return data
        for sib in start.find_all_next():
            if sib.name == "h2" and (end_text is None or sib.get_text(strip=True) == end_text):
                break
            if sib.name == "div" and "spaceit_pad" in sib.get("class", []):
                label = sib.find("span", class_="dark_text")
                if not label:
                    continue
                key = label.get_text(strip=True).replace(":", "")
                label.extract()
                anchors = [a.get_text(strip=True) for a in sib.find_all("a")]
                val = ", ".join(anchors) if anchors else sib.get_text(strip=True)
                data[key] = val
        return data

    info = extract_section_between("Information", "Statistics")

    score_tag = soup.select_one('[itemprop="aggregateRating"] [itemprop="ratingValue"]')
    score = score_tag.get_text(strip=True) if score_tag else None

    ranked_div = soup.find("div", {"data-id": "info2"})
    rank = None
    if ranked_div:
        for sup in ranked_div.find_all("sup"):
            sup.decompose()
        text = ranked_div.get_text(" ", strip=True)
        m = re.search(r"#\s*([\d,]+)", text)
        if m:
            rank = f"#{m.group(1)}"

    pop_tag = soup.find("span", string=re.compile("Popularity:"))
    popularity, members, favorites = None, None, None
    if pop_tag:
        text = pop_tag.find_parent("div").get_text(" ", strip=True)
        m = re.search(r"#([\d,]+)", text)
        popularity = f"#{m.group(1)}" if m else None

    for div in soup.select("div.spaceit_pad"):
        txt = div.get_text(" ", strip=True)
        if txt.startswith("Members:"):
            members = re.sub(r"[^0-9,]", "", txt)
        elif txt.startswith("Favorites:"):
            favorites = re.sub(r"[^0-9,]", "", txt)

    premiered = info.get("Premiered")
    released_season, released_year = None, None
    if premiered:
        s = re.search(r"(Winter|Spring|Summer|Fall)", premiered, re.IGNORECASE)
        y = re.search(r"(\d{4})", premiered)
        if s:
            released_season = s.group(1).capitalize()
        if y:
            released_year = int(y.group(1))

    characters = get_characters(canonical_url, headers)

    flat = {
        "myanimelist_id": anime_id,
        "title": title,
        "description": description,
        "image": image_url,
        "Type": info.get("Type"),
        "Episodes": info.get("Episodes"),
        "Status": info.get("Status"),
        "Premiered": premiered,
        "Released_Season": released_season,
        "Released_Year": released_year,
        "Source": info.get("Source"),
        "Genres": info.get("Genres"),
        "Themes": info.get("Themes"),
        "Demographic": info.get("Demographic"),
        "Duration": info.get("Duration"),
        "Rating": info.get("Rating"),
        "Score": score,
        "Ranked": rank,
        "Popularity": popularity,
        "Members": members,
        "Favorites": favorites,
        "characters": characters,
        "source_url": canonical_url,
    }
    return flat


def append_to_csv(data, filename):
    row = {k: json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else v for k, v in data.items()}
    write_header = not os.path.exists(filename)
    with open(filename, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        if write_header:
            writer.writeheader()
        writer.writerow(row)


# ==========================================
# MAIN LOOP
# ==========================================
if __name__ == "__main__":
    consecutive_404 = 0

    print(f"Memulai scraping dari ID={START_ID} sampai {END_ID} ...")
    for anime_id in range(START_ID, END_ID + 1):
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        data = scrape_myanimelist(anime_id, headers)
        if data:
            append_to_csv(data, OUTPUT_FILE)
            consecutive_404 = 0
        # else:
        #     consecutive_404 += 1
        #     if consecutive_404 >= MAX_CONSECUTIVE_404:
        #         print(f"Berhenti: {MAX_CONSECUTIVE_404} anime berturut-turut tidak ditemukan.")
        #         break
        time.sleep(random.uniform(0.2, 0.5))
    print("Selesai.")
