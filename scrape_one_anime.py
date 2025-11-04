import requests
from bs4 import BeautifulSoup
import json
import csv
import os
import re
from urllib.parse import urljoin


def get_characters(anime_url: str):
    """Ambil daftar karakter dari halaman /characters"""
    characters_url = anime_url.rstrip("/") + "/characters"
    print(f"🧩 Mengambil karakter dari {characters_url}")

    headers = {"User-Agent": "Mozilla/5.0"}
    res = requests.get(characters_url, headers=headers)
    if res.status_code != 200:
        print(f"⚠️ Tidak dapat mengambil halaman karakter ({res.status_code})")
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


def scrape_myanimelist(anime_ref: str):
    """
    anime_ref bisa berupa:
      - URL lengkap (https://myanimelist.net/anime/59027/Spy_x_Family_Season_3)
      - atau hanya ID (59027)
    """
    # Tentukan URL
    if anime_ref.isdigit():
        url = f"https://myanimelist.net/anime/{anime_ref}"
    else:
        url = anime_ref

    headers = {"User-Agent": "Mozilla/5.0"}
    res = requests.get(url, headers=headers)
    if res.status_code != 200:
        raise Exception(f"Gagal mengambil data: {res.status_code}")

    soup = BeautifulSoup(res.text, "html.parser")

    # Ambil canonical URL sebenarnya (kalau ada)
    canonical_tag = soup.find("meta", property="og:url") or soup.find("link", rel="canonical")
    canonical_url = canonical_tag.get("content") if canonical_tag else url

    # 🆔 Ambil MyAnimeList ID dari canonical URL
    match_id = re.search(r"/anime/(\d+)", canonical_url)
    mal_id = int(match_id.group(1)) if match_id else None

    # 🖼️ Gambar
    img = soup.select_one("div.leftside img")
    image_url = img.get("data-src") or img.get("src") if img else None

    # 🏷️ Judul & deskripsi
    title = (soup.select_one("h1.title-name, h1.title") or {}).get_text(strip=True)
    description = (soup.find("p", itemprop="description") or {}).get_text(strip=True)

    # 🧹 Bersihkan span tersembunyi agar tidak duplikat
    for span in soup.select('span[itemprop="genre"], span[itemprop="theme"], span[itemprop="demographic"]'):
        span.decompose()

    # Helper: ekstraksi antar dua <h2>
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
                if key in ["Genres", "Themes", "Demographic"]:
                    anchors = [a.get_text(strip=True) for a in sib.find_all("a")]
                    val = ", ".join(anchors)
                else:
                    val = sib.get_text(strip=True)
                data[key] = val
        return data

    # 🧩 Sections
    alt_titles = extract_section_between("Alternative Titles", "Information")
    info = extract_section_between("Information", "Statistics")

    # 📊 Statistics
    stats = {}
    score_tag = soup.select_one('[itemprop="aggregateRating"] [itemprop="ratingValue"]')
    if score_tag:
        stats["Score"] = score_tag.get_text(strip=True)

    ranked_div = soup.find("div", {"data-id": "info2"})
    if ranked_div:
        for sup in ranked_div.find_all("sup"):
            sup.decompose()
        text = ranked_div.get_text(" ", strip=True)
        m = re.search(r"#\s*([\d,]+)", text)
        if m:
            stats["Ranked"] = f"#{m.group(1)}"

    stats_section = extract_section_between("Statistics", "Available At")
    for key, val in stats_section.items():
        if key in ("Score", "Ranked"):
            continue
        if key == "Popularity":
            m = re.search(r"#\s*([\d,]+)", val)
            if m:
                val = f"#{m.group(1)}"
        elif key in ("Members", "Favorites"):
            m = re.search(r"([\d,]+)", val)
            if m:
                val = m.group(1)
        stats[key] = val

    # 🌐 External Links
    links = [
        {"name": (ext.select_one(".caption") or ext).get_text(strip=True), "url": ext.get("href")}
        for ext in soup.select("div.external_links a.link")
    ]

    # 📺 Streaming Platforms
    streams = [
        {
            "platform": s.get("title") or (s.select_one(".caption").get_text(strip=True) if s.select_one(".caption") else ""),
            "url": s.get("href"),
        }
        for s in soup.select(".broadcast-item.available")
    ]

    # 🔗 Related Entries (hanya anime)
    related_entries = []
    for entry in soup.select("div.related-entries div.entry.borderClass"):
        link_tag = entry.select_one("a[href*='/anime/']")
        if not link_tag:
            continue
        rel_text = entry.select_one(".relation")
        title_tag = entry.select_one(".title a")
        if not title_tag:
            continue
        rel_text_clean = rel_text.get_text(" ", strip=True) if rel_text else ""
        match_type = re.search(r"\(([^)]+)\)", rel_text_clean)
        rel_type = match_type.group(1) if match_type else None
        rel_label = rel_text_clean.split("(")[0].strip()

        link_url = link_tag.get("href")
        id_match = re.search(r"/anime/(\d+)", link_url)
        rel_id = int(id_match.group(1)) if id_match else None

        related_entries.append({
            "relation": rel_label,
            "type": rel_type,
            "title": title_tag.get_text(strip=True),
            "id": rel_id,
            "url": link_url
        })

    # 🔄 Flatten semua section
    flat_data = {
        "myanimelist_id": mal_id,
        "title": title,
        "description": description,
        "image": image_url,
        **alt_titles,
        **info,
        **stats,
        "external_links": links,
        "streaming_platforms": streams,
        "related_entries": related_entries,
        "source_url": canonical_url,
    }

    # 🗓️ Released Season & Year
    premiered = flat_data.get("Premiered")
    if premiered:
        season_match = re.search(r"(Winter|Spring|Summer|Fall)", premiered, re.IGNORECASE)
        year_match = re.search(r"(\d{4})", premiered)
        flat_data["Released_Season"] = season_match.group(1).capitalize() if season_match else None
        flat_data["Released_Year"] = int(year_match.group(1)) if year_match else None
    else:
        flat_data["Released_Season"] = None
        flat_data["Released_Year"] = None

    # 🧩 Tambahkan karakter dari halaman /characters
    flat_data["characters"] = get_characters(canonical_url)

    return flat_data


def save_json(data, filename):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"✅ JSON tersimpan di: {os.path.abspath(filename)}")


def save_csv_summary(data, filename):
    serialized_data = {
        k: json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else v
        for k, v in data.items()
    }
    fieldnames = list(serialized_data.keys())
    write_header = not os.path.exists(filename)
    with open(filename, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerow(serialized_data)
    print(f"✅ CSV tersimpan di: {os.path.abspath(filename)} (termasuk kolom characters JSON)")


if __name__ == "__main__":
    anime_ref = "7"
    print(f"🔍 Mengambil data dari {anime_ref}")
    data = scrape_myanimelist(anime_ref)
    save_json(data, f"anime_{data['myanimelist_id']}_with_characters.json")
    save_csv_summary(data, "anime_summary_with_characters.csv")
    print("🚀 Selesai! Termasuk daftar karakter di kolom 'characters'.")
