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
START_ID = 1          # ganti sesuai kebutuhan
END_ID = 5000         # misal 1–5000
MAX_CONSECUTIVE_404 = 100
OUTPUT_FILE = "mal_anime_auto_scrape.csv"

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
    # Removed verbose print - only print on error

    res = requests.get(characters_url, headers=headers)
    if res.status_code != 200:
        # Hanya print kalau error
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
        # Silent 404, akan di-handle di caller
        return None
    if res.status_code != 200:
        # Hanya print kalau error bukan 404
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

    # Cek singular/plural untuk field yang bisa berbeda
    genres = info.get("Genres") or info.get("Genre")
    themes = info.get("Themes") or info.get("Theme")
    studios = info.get("Studios") or info.get("Studio")
    producers = info.get("Producers") or info.get("Producer")

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
        "Genres": genres,
        "Themes": themes,
        "Studios": studios,
        "Producers": producers,
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
# RETRY LOGIC
# ==========================================

# Mapping singular <-> plural untuk field yang bisa berubah
FIELD_VARIANTS = {
    'Genre': 'Genres',
    'Genres': 'Genre',
    'Theme': 'Themes',
    'Themes': 'Theme',
    'Studio': 'Studios',
    'Studios': 'Studio',
    'Producer': 'Producers',
    'Producers': 'Producer',
}

def check_null_values(data):
    """
    Cek apakah ada nilai null/None/empty pada kolom penting.
    Return list of kolom yang null.
    """
    # Kolom yang BENAR-BENAR boleh null (karena tidak semua anime punya)
    # End_year: untuk anime ongoing yang belum selesai
    optional_fields = ['End_year']

    # Kolom yang boleh bernilai "Unknown" (untuk anime ongoing/incomplete)
    can_be_unknown = ['Episodes', 'Status', 'Premiered', 'Released_Season', 'Released_Year']

    null_fields = []
    for key, value in data.items():
        # Skip kolom opsional
        if key in optional_fields:
            continue

        # Cek null/None/empty
        if value is None or value == '':
            null_fields.append(key)
        elif value == 'Unknown':
            # Untuk field tertentu, "Unknown" itu valid
            if key not in can_be_unknown:
                null_fields.append(key)

        # Cek untuk list kosong termasuk characters
        if isinstance(value, list) and len(value) == 0:
            if key == 'characters':
                # Characters kosong juga dianggap null, perlu retry
                null_fields.append(key)

    return null_fields


def fix_singular_plural_fields(data):
    """
    Cek apakah field yang null punya versi singular/plural.
    Jika ada, copy nilainya. Return list field yang berhasil di-fix.
    """
    fixed_fields = []

    for field_name, variant_name in FIELD_VARIANTS.items():
        # Cek jika field null tapi variant-nya ada nilai
        if field_name in data and (data[field_name] is None or data[field_name] == '' or data[field_name] == 'Unknown'):
            if variant_name in data and data[variant_name] is not None and data[variant_name] != '' and data[variant_name] != 'Unknown':
                # Copy dari variant
                data[field_name] = data[variant_name]
                fixed_fields.append(f"{field_name} ← {variant_name}")

    return fixed_fields


def scrape_with_retry(anime_id, max_retries=5):
    """
    Scrape anime dengan retry untuk mengisi field yang null.
    Hanya update field yang null, tidak re-scrape semua.
    """
    # Scrape pertama kali
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    data = scrape_myanimelist(anime_id, headers)

    if not data:
        # Anime tidak ditemukan (404)
        return None

    # Print URL anime yang sedang di-scrape
    url = data.get('source_url', f'https://myanimelist.net/anime/{anime_id}')
    print(f"{url}", end=" ")

    # Cek null values
    null_fields = check_null_values(data)

    if not null_fields:
        # Tidak ada null, berhasil!
        return data

    # Ada field yang null, coba fix dengan singular/plural dulu
    fixed = fix_singular_plural_fields(data)
    if fixed:
        print(f"\n  ✓ Fixed singular/plural: {fixed}", end=" ")
        # Cek lagi setelah di-fix
        null_fields = check_null_values(data)
        if not null_fields:
            return data

    # Field yang memang bisa tidak ada (semi-optional)
    semi_optional_fields = {'Premiered', 'Released_Season', 'Released_Year', 'Demographic', 'Themes'}

    # Cek apakah null fields hanya berisi semi-optional fields
    only_semi_optional = all(field in semi_optional_fields for field in null_fields)

    # Tentukan max retries
    actual_max_retries = 3 if only_semi_optional else max_retries

    if only_semi_optional:
        print(f"\n  ⚠ Found null fields (semi-optional): {null_fields} → retry max 3x")
    else:
        print(f"\n  ⚠ Found null fields: {null_fields}")

    for attempt in range(1, actual_max_retries + 1):
        print(f"  → Retry attempt {attempt}/{actual_max_retries}...")
        time.sleep(random.uniform(0.1, .5))  # Delay lebih lama untuk retry

        # Scrape ulang
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        new_data = scrape_myanimelist(anime_id, headers)

        if not new_data:
            print(f"  ✗ Retry gagal (error scraping)")
            break

        # Update hanya field yang null
        updated_fields = []
        for field in null_fields:
            # Special handling untuk characters (list)
            if field == 'characters':
                if field in new_data and isinstance(new_data[field], list) and len(new_data[field]) > 0:
                    # Characters sekarang ada isinya
                    data[field] = new_data[field]
                    updated_fields.append(field)
            else:
                # Field biasa
                if field in new_data and new_data[field] is not None and new_data[field] != '' and new_data[field] != 'Unknown':
                    # Field yang tadinya null sekarang ada nilainya
                    data[field] = new_data[field]
                    updated_fields.append(field)

        if updated_fields:
            print(f"  ✓ Berhasil update: {updated_fields}", end=" ")

        # Coba fix singular/plural lagi dari hasil retry
        fixed = fix_singular_plural_fields(data)
        if fixed:
            print(f"\n  ✓ Fixed singular/plural: {fixed}", end=" ")

        # Cek lagi apakah masih ada yang null
        null_fields = check_null_values(data)

        if not null_fields:
            print(f"\n  ✓ Semua field terisi setelah {attempt} retries", end=" ")
            return data

    # Masih ada yang null setelah max retries
    if null_fields:
        print(f"\n  ✗ Max retries reached, masih ada null: {null_fields}", end=" ")

    return data


# ==========================================
# MAIN LOOP
# ==========================================
if __name__ == "__main__":
    consecutive_404 = 0

    print(f"Memulai scraping dari ID={START_ID} sampai {END_ID} ...")
    print()

    for anime_id in range(START_ID, END_ID + 1):
        print(f"[ID {anime_id}] ", end="")

        data = scrape_with_retry(anime_id, max_retries=5)

        if data:
            append_to_csv(data, OUTPUT_FILE)
            consecutive_404 = 0
            print("→ ✓ Saved")
        else:
            consecutive_404 += 1
            print("✗ Not found (404)")
            if consecutive_404 >= MAX_CONSECUTIVE_404:
                print(f"\nBerhenti: {MAX_CONSECUTIVE_404} anime berturut-turut tidak ditemukan.")
                break

        time.sleep(random.uniform(0.2, 0.5))

    print("\n" + "="*80)
    print("Selesai!")
    print("="*80)