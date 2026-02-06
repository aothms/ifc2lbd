import os
import requests
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
import hashlib
import secrets
import re

DOWNLOAD_DIR = "resources/ontologies/downloaded"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

def extract_prefix_from_ttl(content):
    match = re.search(r'@prefix\s+([a-zA-Z0-9_-]+):\s+<', content)
    if match:
        return match.group(1)
    match = re.search(r'PREFIX\s+([a-zA-Z0-9_-]+):\s+<', content)
    if match:
        return match.group(1)
    return None

def get_unique_path(filename, uri, prefix=None):
    if prefix:
        prefix6 = prefix[:6]
    else:
        prefix6 = hashlib.sha1(uri.encode('utf-8')).hexdigest()[:6]
    rand12 = secrets.token_hex(6)
    new_filename = f"{prefix6}_{rand12}_{filename}"
    out_path = os.path.join(DOWNLOAD_DIR, new_filename)
    while os.path.exists(out_path):
        rand12 = secrets.token_hex(6)
        new_filename = f"{prefix6}_{rand12}_{filename}"
        out_path = os.path.join(DOWNLOAD_DIR, new_filename)
    return out_path

def try_download(url, uri_for_hash):
    try:
        print(f"  Trying: {url}")
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        filename = os.path.basename(urlparse(url).path)
        content_type = r.headers.get('Content-Type', '')
        prefix = None
        if filename.endswith('.ttl') or 'text' in content_type or 'turtle' in content_type:
            try:
                text = r.text
                prefix = extract_prefix_from_ttl(text)
            except Exception:
                prefix = None
        out_path = get_unique_path(filename, uri_for_hash, prefix)
        with open(out_path, "wb") as f:
            f.write(r.content)
        return True, None, out_path
    except Exception as e:
        print(f"  Failed: {e}")
        return False, str(e), None

# Test with https://purl.org/fisa#
uri = "https://purl.org/fisa#"
print(f"Testing: {uri}\n")

try:
    r = requests.get(uri, timeout=5, allow_redirects=True)
    print(f"Final URL after redirects: {r.url}")
    print(f"Status: {r.status_code}\n")
    
    # If 404 and URL changed, try base path
    if r.status_code == 404 and r.url != uri:
        print(f"Redirect led to 404, trying base path...")
        base_path = r.url.rsplit('/', 1)[0] + '/'
        print(f"Trying: {base_path}")
        r = requests.get(base_path, timeout=10, allow_redirects=True)
        print(f"New URL: {r.url}")
        print(f"Status: {r.status_code}\n")
    else:
        r.raise_for_status()
    
    soup = BeautifulSoup(r.text, "html.parser")
    base_url_for_links = r.url
    
    print("Looking for .ttl links...")
    ttl_links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.endswith(".ttl"):
            full_url = urljoin(base_url_for_links, href)
            ttl_links.append(full_url)
    
    print(f"Found {len(ttl_links)} .ttl link(s)")
    for full_url in ttl_links:
        print(f"Trying: {full_url}")
        ok, err, out_path = try_download(full_url, uri)
        if ok:
            print(f"\nSUCCESS! Downloaded to: {os.path.basename(out_path)}")
            break
        else:
            print(f"Failed: {err}")
            
except Exception as e:
    print(f"Error: {e}")
