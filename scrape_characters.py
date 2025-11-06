import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import re
import json
import random
import time
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Load environment variables
load_dotenv()

# ==========================================
# KONFIGURASI
# ==========================================
INPUT_CSV = "mal_characters.csv"
OUTPUT_FILE = "mal_characters_detailed.csv"

# Configuration from .env file
START_INDEX = int(os.getenv("START_INDEX", "0"))
END_INDEX = int(os.getenv("END_INDEX", "-1"))
NUM_WORKERS = 5

# Proxy configuration
USE_PROXY = False
PROXY_HOST = os.getenv("PROXY_HOST", "")
PROXY_USER = os.getenv("PROXY_USER", "")
PROXY_PASS = os.getenv("PROXY_PASS", "")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) Gecko/20100101 Firefox/130.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
]

# Thread-safe locks
csv_lock = threading.Lock()
print_lock = threading.Lock()


# ==========================================
# HELPER FUNCTIONS
# ==========================================
def get_proxies():
    """Generate proxy configuration for requests"""
    if not USE_PROXY:
        return None

    proxy_url = f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}"
    return {
        'http': proxy_url,
        'https': proxy_url
    }


def scrape_character(character_id, url):
    """Scrape character details from MyAnimeList character page"""
    proxies = get_proxies()
    headers = {"User-Agent": random.choice(USER_AGENTS)}

    # Try up to 3 times with different user agents
    for attempt in range(3):
        try:
            if attempt > 0:
                # Wait a bit before retry
                time.sleep(random.uniform(0.2, 1))
                headers = {"User-Agent": random.choice(USER_AGENTS)}

            res = requests.get(url, headers=headers, proxies=proxies, timeout=30)
        except requests.exceptions.RequestException:
            if attempt == 2:
                return None, 0
            continue

        if res.status_code != 200:
            if attempt == 2:
                return None, res.status_code
            continue

        # Check if we got a valid character page
        if 'normal_header' not in res.text and attempt < 2:
            continue  # Retry if we didn't get the expected content

        break  # Success or final attempt

    soup = BeautifulSoup(res.text, "html.parser")

    # Extract full name and alternate name from header
    # Try different selectors
    header = soup.find("h2", class_="normal_header")
    if not header:
        # Try alternative selector with style
        header = soup.find("h2", style=lambda x: x and "height: 15px" in x)

    if not header:
        # Try any h2 with normal_header class (without style requirement)
        headers = soup.find_all("h2")
        for h in headers:
            if 'normal_header' in str(h.get('class', [])):
                header = h
                break

    if not header:
        # Last resort: find h2 that contains the character name
        # Extract character name from URL
        url_match = re.search(r'/character/\d+/([^/]+)', url)
        if url_match:
            char_name_url = url_match.group(1).replace('_', ' ')
            # Try to find h2 containing this name
            for h in soup.find_all("h2"):
                if char_name_url.lower() in h.get_text().lower().replace('_', ' '):
                    header = h
                    break

    if not header:
        # Debug: print first 500 chars of HTML
        print(f"\n  ⚠ Header not found. HTML preview: {res.text[:500]}")
        return None, 200

    # Get full name (text before <span>)
    full_name = ""
    if header.contents:
        # Get first text node
        for content in header.contents:
            if isinstance(content, str):
                full_name = content.strip()
                break

    # Get alternate name (inside parentheses in <small>)
    alternate_name = ""
    small_tag = header.find("small")
    if small_tag:
        text = small_tag.get_text(strip=True)
        # Extract text inside parentheses
        match = re.search(r'\(([^)]+)\)', text)
        if match:
            alternate_name = match.group(1)

    # Extract attributes (everything after the header until next major element)
    data = {
        "character_id": character_id,
        "full_name": full_name,
        "alternate_name": alternate_name,
    }

    # Get HTML content after h2 header
    # Strategy: The attributes and description are direct siblings after the h2 tag
    # They are text nodes and <br> tags, not wrapped in any parent element

    # Method 1: Get the parent and find content after h2
    parent = header.parent

    # Convert parent to string to work with raw HTML
    parent_html = str(parent)

    # Find the h2 tag position in parent HTML
    h2_html = str(header)
    h2_start = parent_html.find(h2_html)

    if h2_start == -1:
        # Fallback: try getting next siblings
        content_parts = []
        for sibling in header.next_siblings:
            if sibling.name in ['div', 'table', 'h2', 'h3']:
                # Check if it's an ad div we should skip
                if sibling.name == 'div':
                    classes = sibling.get('class', [])
                    if classes:
                        class_str = ' '.join(classes)
                        if 'sUaidzctQfngSNMH' in class_str or 'ad-' in class_str:
                            continue
                # Otherwise stop at major sections
                break
            content_parts.append(str(sibling))

        content_after_h2 = ''.join(content_parts)
    else:
        # Extract everything after the h2 closing tag
        h2_end = h2_start + len(h2_html)
        remaining_html = parent_html[h2_end:]

        # Find next major section (div with ads, table, another h2, etc)
        # Look for the FIRST occurrence of any major element
        matches = []

        # Find div with ad class
        div_match = re.search(r'<div[^>]*sUaidzctQfngSNMH[^>]*>', remaining_html, re.IGNORECASE)
        if div_match:
            matches.append(div_match.start())

        # Find table
        table_match = re.search(r'<table', remaining_html, re.IGNORECASE)
        if table_match:
            matches.append(table_match.start())

        # Find next h2 or h3
        header_match = re.search(r'<h[23]', remaining_html, re.IGNORECASE)
        if header_match:
            matches.append(header_match.start())

        # Find normal_header div
        normal_header_match = re.search(r'<div[^>]*normal_header[^>]*>', remaining_html, re.IGNORECASE)
        if normal_header_match:
            matches.append(normal_header_match.start())

        # Use the earliest match as cutoff point
        if matches:
            cutoff = min(matches)
            content_after_h2 = remaining_html[:cutoff]
        else:
            content_after_h2 = remaining_html

    # First, manually replace <br> tags with newlines in the raw HTML
    # This avoids issues with BeautifulSoup truncating content
    content_after_h2 = re.sub(r'<br\s*/?>', '\n', content_after_h2, flags=re.IGNORECASE)

    # Now parse with BeautifulSoup to clean remaining HTML
    temp_soup = BeautifulSoup(content_after_h2, 'html.parser')

    # Handle spoiler divs - extract their content but mark as spoiler
    for spoiler_div in temp_soup.find_all('div', class_='spoiler'):
        # Find the spoiler content span
        spoiler_content = spoiler_div.find('span', class_='spoiler_content')
        if spoiler_content:
            # Get the text inside spoiler, removing any buttons
            for button in spoiler_content.find_all('input'):
                button.decompose()
            spoiler_text = spoiler_content.get_text(separator=' ').strip()
            # Replace the entire spoiler div with the spoiler text inline
            # Don't add newlines, just continue the value
            spoiler_div.replace_with(f' {spoiler_text}')
        else:
            # If no spoiler content span, just remove the div
            spoiler_div.decompose()

    # Get the text content
    text_content = temp_soup.get_text()

    # Split by newlines and process
    lines = text_content.split('\n')

    # Parse attributes and description
    # Collect all attributes as a list of dict
    attributes = []
    description_lines = []
    in_description = False

    for line in lines:
        line = line.strip()

        # Skip empty lines
        if not line:
            continue

        # Check if line has colon (potential attribute)
        if ':' in line and not in_description:
            parts = line.split(':', 1)
            if len(parts) == 2:
                key = parts[0].strip()
                value = parts[1].strip()

                # Only treat as attribute if key is short (< 50 chars)
                if len(key) < 50 and value:
                    # Add to attributes list
                    attributes.append({
                        "name": key,
                        "value": value
                    })
                else:
                    # Long key or no value -> start of description
                    in_description = True
                    description_lines.append(line)
        else:
            # No colon or already in description mode -> part of description
            in_description = True
            description_lines.append(line)

    # Add attributes as JSON string
    if attributes:
        data['attributes'] = json.dumps(attributes, ensure_ascii=False)

    # Combine description lines
    if description_lines:
        description = ' '.join(description_lines).strip()
        # Only add if not too short (to avoid adding junk)
        if len(description) > 20:
            data['description'] = description

    return data, 200


def append_to_csv(data, filename):
    """Thread-safe CSV writing with fixed columns"""
    with csv_lock:
        # Define fixed columns order
        columns = ['character_id', 'full_name', 'alternate_name', 'name', 'url', 'attributes', 'description']

        # Ensure all columns exist in data
        for col in columns:
            if col not in data:
                data[col] = None

        # Only keep the defined columns
        data_clean = {col: data[col] for col in columns}

        # Check if file exists
        file_exists = os.path.exists(filename)

        if file_exists:
            # Append to existing file
            df_existing = pd.read_csv(filename)
            df_new = pd.DataFrame([data_clean])
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            df_combined.to_csv(filename, index=False)
        else:
            # Create new file with fixed columns
            df_new = pd.DataFrame([data_clean], columns=columns)
            df_new.to_csv(filename, index=False)


class FailureCounter:
    """Thread-safe failure counter"""
    def __init__(self):
        self.count = 0
        self.lock = threading.Lock()

    def increment(self):
        with self.lock:
            self.count += 1
            return self.count

    def reset(self):
        with self.lock:
            self.count = 0

    def get(self):
        with self.lock:
            return self.count


failure_counter = FailureCounter()
MAX_CONSECUTIVE_FAILURES = 20


def process_character(idx, character_id, name, url):
    """Worker function to process one character"""
    with print_lock:
        print(f"\n[Index {idx} | ID {character_id}] {name[:30]}... ", end="", flush=True)

    data, status_code = scrape_character(character_id, url)

    if data and status_code == 200:
        # Add original name from CSV
        data['name'] = name
        data['url'] = url

        append_to_csv(data, OUTPUT_FILE)
        failure_counter.reset()
        with print_lock:
            print("→ ✓ Saved")
        return True, status_code
    else:
        fail_count = failure_counter.increment()
        with print_lock:
            print(f"✗ Failed (status: {status_code})")

        # Check for consecutive failures
        if fail_count >= MAX_CONSECUTIVE_FAILURES:
            with print_lock:
                print(f"\n⚠ WARNING: {MAX_CONSECUTIVE_FAILURES} consecutive failures!")
                print(f"⚠ Possible IP block. Sleeping for 10 seconds...")
            time.sleep(10)
            with print_lock:
                print(f"⚠ Resuming scraping...\n")
            failure_counter.reset()

        return False, status_code


# ==========================================
# MAIN
# ==========================================
if __name__ == "__main__":
    # Load characters CSV
    print(f"Loading {INPUT_CSV}...")
    df = pd.read_csv(INPUT_CSV)

    # Determine end index
    total_rows = len(df)
    end_idx = total_rows if END_INDEX == -1 else min(END_INDEX, total_rows)

    # Slice dataframe
    df_slice = df.iloc[START_INDEX:end_idx]

    # Check which characters already scraped
    existing_ids = set()
    if os.path.exists(OUTPUT_FILE):
        print(f"Output file exists. Checking already scraped characters...")
        try:
            df_output = pd.read_csv(OUTPUT_FILE)
            if 'character_id' in df_output.columns:
                existing_ids = set(df_output['character_id'].dropna().astype(int).tolist())
                print(f"Found {len(existing_ids)} already scraped characters")
        except Exception as e:
            print(f"Warning: Could not read output file: {e}")

    # Filter tasks
    all_tasks = [(idx, row['character_id'], row['name'], row['url'])
                 for idx, row in df_slice.iterrows()]
    tasks = [(idx, cid, name, url) for idx, cid, name, url in all_tasks
             if cid not in existing_ids]

    print(f"\nRange: index {START_INDEX} to {end_idx} ({len(df_slice)} characters total)")
    print(f"Already scraped: {len(existing_ids)} characters")
    print(f"To be scraped: {len(tasks)} characters")
    print(f"Running with {NUM_WORKERS} parallel workers")
    if USE_PROXY:
        print(f"Using proxy: {PROXY_HOST}")
    print()

    if len(tasks) == 0:
        print("✓ All characters in range already scraped!")
        exit(0)

    # Run parallel scraping
    success_count = 0
    failed_count = 0

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = {executor.submit(process_character, idx, cid, name, url): (idx, cid)
                   for idx, cid, name, url in tasks}

        for future in as_completed(futures):
            try:
                success, status_code = future.result()
                if success:
                    success_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                idx, cid = futures[future]
                with print_lock:
                    print(f"[Index {idx} | ID {cid}] ✗ Exception: {e}")
                failed_count += 1

            time.sleep(random.uniform(0.1, 0.3))

    print("\n" + "="*80)
    print(f"Selesai! Attempted to scrape {len(tasks)} characters")
    print(f"Success: {success_count} | Failed: {failed_count}")
    print(f"Total in output file now: {len(existing_ids) + success_count}")
    print("="*80)
