import os
import requests
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
import threading
import hashlib
import secrets
import csv
import re

FAIL_LOG = "resources/ontologies/scrap_fail.log"
DOWNLOAD_DIR = "resources/ontologies/downloaded"
RETRY_SUCCESS_LOG = "resources/ontologies/retry_success.log"
RETRY_FAIL_LOG = "resources/ontologies/retry_fail.log"
CSV_PATH = "resources/ontologies/retry_scrap_results.csv"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Extract URIs from fail log
uris = []
with open(FAIL_LOG, "r", encoding="utf-8") as f:
    for line in f:
        match = re.match(r'(https?://[^\s|]+)', line)
        if match:
            uris.append(match.group(1))

successes = []
fails = []
csv_rows = []

def extract_ontology_from_html(html_content, base_url, uri):
    """Extract ontology data from HTML documentation and convert to TTL"""
    from bs4 import BeautifulSoup
    import re
    
    soup = BeautifulSoup(html_content, 'html.parser')
    ttl_lines = []
    
    # Extract namespace/prefix from the page
    namespace_uri = None
    prefix = None
    
    # Look for the ontology namespace in various places
    # Check for "Namespace:" or "Ontology IRI:"
    for dt in soup.find_all(['dt', 'strong', 'b']):
        text = dt.get_text().strip().lower()
        if 'namespace' in text or 'ontology iri' in text:
            dd = dt.find_next_sibling()
            if dd:
                namespace_uri = dd.get_text().strip()
                break
    
    # Try to extract from class/property URIs
    if not namespace_uri:
        links = soup.find_all('a', href=True)
        for link in links:
            href = link['href']
            if href.startswith('http') and '#' in href:
                namespace_uri = href.split('#')[0] + '#'
                break
    
    # Fallback to the URI being scraped
    if not namespace_uri:
        namespace_uri = uri.rstrip('#') + '#'
    
    # Extract prefix (last part of domain or from page)
    if 'purl.org/' in namespace_uri:
        prefix = namespace_uri.split('purl.org/')[-1].split('#')[0].split('/')[0]
    elif 'w3id.org/' in namespace_uri:
        prefix = namespace_uri.split('w3id.org/')[-1].split('#')[0].split('/')[0]
    else:
        prefix = 'onto'
    
    # Start TTL with prefixes
    ttl_lines.append(f"@prefix {prefix}: <{namespace_uri}> .")
    ttl_lines.append("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .")
    ttl_lines.append("@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .")
    ttl_lines.append("@prefix owl: <http://www.w3.org/2002/07/owl#> .")
    ttl_lines.append("")
    
    # Ontology declaration
    ontology_iri = namespace_uri.rstrip('#')
    ttl_lines.append(f"<{ontology_iri}> a owl:Ontology .")
    ttl_lines.append("")
    
    # Extract classes
    class_section = soup.find('h4', string=re.compile(r'classes', re.I))
    if class_section:
        class_list = class_section.find_next('ul')
        if class_list:
            for li in class_list.find_all('li'):
                link = li.find('a', href=True)
                if link:
                    href = link['href']
                    if href.startswith('#'):
                        class_name = href[1:]
                        full_uri = namespace_uri + class_name
                        label = link.get_text().strip()
                        ttl_lines.append(f"{prefix}:{class_name} a owl:Class ;")
                        ttl_lines.append(f"    rdfs:label \"{label}\" .")
                        ttl_lines.append("")
    
    # Extract properties (object properties and data properties)
    for prop_type in ['Object Properties', 'Data Properties', 'properties']:
        prop_section = soup.find('h4', string=re.compile(prop_type, re.I))
        if prop_section:
            prop_list = prop_section.find_next('ul')
            if prop_list:
                for li in prop_list.find_all('li'):
                    link = li.find('a', href=True)
                    if link:
                        href = link['href']
                        if href.startswith('#'):
                            prop_name = href[1:]
                            full_uri = namespace_uri + prop_name
                            label = link.get_text().strip()
                            prop_class = "owl:ObjectProperty" if 'object' in prop_type.lower() else "owl:DatatypeProperty" if 'data' in prop_type.lower() else "rdf:Property"
                            ttl_lines.append(f"{prefix}:{prop_name} a {prop_class} ;")
                            ttl_lines.append(f"    rdfs:label \"{label}\" .")
                            ttl_lines.append("")
    
    return '\n'.join(ttl_lines) if len(ttl_lines) > 10 else None

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
        return False, str(e), None

def discover_and_download(uri, max_timeout=60):
    parsed = urlparse(uri)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    candidates = []
    path_base = parsed.path.rstrip('/')
    for ext in [".ttl", ".rdf", ".owl", ".nt"]:
        if path_base:
            candidates.append(urljoin(base_url, path_base + ext))
        if '/' in path_base:
            last = path_base.split('/')[-1]
            candidates.append(urljoin(base_url, path_base + f"/{last}{ext}"))
            candidates.append(urljoin(base_url, f"{last}{ext}"))
        for name in ["ontology", "index", "bot", "main", "core"]:
            candidates.append(urljoin(base_url, path_base + f"/{name}{ext}"))
    
    print(f"\n[DEBUG] Processing {uri}")
    print(f"[DEBUG] Generated {len(candidates)} candidates (first 5): {candidates[:5]}")

    result = {'done': False}
    def run():
        for candidate in candidates:
            ok, err, out_path = try_download(candidate, uri)
            if ok:
                successes.append(f"{uri} -> {candidate} -> {os.path.basename(out_path)}")
                print(f"SUCCESS: {uri} -> {candidate} -> {os.path.basename(out_path)}")
                csv_rows.append((uri, os.path.basename(out_path)))
                result['done'] = True
                return
        try:
            r = requests.get(uri, timeout=10, allow_redirects=True)
            r.raise_for_status()
        except requests.HTTPError as e:
            # If the redirect leads to a 404 (like ontology.xml), try the base path
            if r.status_code == 404 and r.url != uri:
                print(f"Redirect to {r.url} failed (404), trying base path...")
                # Extract base path without filename
                base_path = r.url.rsplit('/', 1)[0] + '/'
                try:
                    r = requests.get(base_path, timeout=10, allow_redirects=True)
                    r.raise_for_status()
                except Exception as e2:
                    fails.append(f"{uri} | HTML fetch failed: {e}")
                    print(f"FAIL: {uri} | HTML fetch failed: {e}")
                    result['done'] = True
                    return
            else:
                fails.append(f"{uri} | HTML fetch failed: {e}")
                print(f"FAIL: {uri} | HTML fetch failed: {e}")
                result['done'] = True
                return
        except Exception as e:
            fails.append(f"{uri} | HTML fetch failed: {e}")
            print(f"FAIL: {uri} | HTML fetch failed: {e}")
            result['done'] = True
            return
        
        try:
            soup = BeautifulSoup(r.text, "html.parser")
            meta = soup.find('meta', attrs={'http-equiv': lambda x: x and x.lower() == 'refresh'})
            if meta and 'content' in meta.attrs:
                match = re.search(r'url=(.+)', meta['content'], re.IGNORECASE)
                if match:
                    redirect_url = match.group(1).strip().strip('"').strip("'")
                    redirect_url = urljoin(r.url, redirect_url)
                    print(f"Meta-refresh detected, following to: {redirect_url}")
                    r = requests.get(redirect_url, timeout=10, allow_redirects=True)
                    r.raise_for_status()
                    soup = BeautifulSoup(r.text, "html.parser")
            
            # Use r.url as the base URL (this is the final URL after all redirects)
            base_url_for_links = r.url
            print(f"Parsing HTML from: {base_url_for_links}")
            
            found = False
            # 1. First, search specifically for .ttl links
            ttl_links = []
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if href.endswith(".ttl"):
                    full_url = urljoin(base_url_for_links, href)
                    ttl_links.append(full_url)
            
            if ttl_links:
                print(f"Found {len(ttl_links)} .ttl link(s)")
                for full_url in ttl_links:
                    print(f"Trying .ttl: {full_url}")
                    ok, err, out_path = try_download(full_url, uri)
                    if ok:
                        successes.append(f"{uri} -> {full_url} -> {os.path.basename(out_path)}")
                        print(f"SUCCESS: {uri} -> {full_url} -> {os.path.basename(out_path)}")
                        csv_rows.append((uri, os.path.basename(out_path)))
                        found = True
                        result['done'] = True
                        return
                    else:
                        print(f"  Failed: {err}")
            
            # 2. If no .ttl found, try .rdf, .owl, .nt (but NEVER .xml)
            if not found:
                other_links = []
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if any(href.endswith(ext) for ext in [".rdf", ".owl", ".nt"]):
                        full_url = urljoin(base_url_for_links, href)
                        other_links.append(full_url)
                
                if other_links:
                    print(f"Found {len(other_links)} other ontology link(s)")
                    for full_url in other_links:
                        print(f"Trying: {full_url}")
                        ok, err, out_path = try_download(full_url, uri)
                        if ok:
                            successes.append(f"{uri} -> {full_url} -> {os.path.basename(out_path)}")
                            print(f"SUCCESS: {uri} -> {full_url} -> {os.path.basename(out_path)}")
                            csv_rows.append((uri, os.path.basename(out_path)))
                            found = True
                            result['done'] = True
                            return
                        else:
                            print(f"  Failed: {err}")
            
            # 3. Only fail if no ontology file was found
            if not found:
                # Try to extract ontology data from HTML and save as TTL
                print(f"No download links worked, attempting to scrape ontology from HTML...")
                ttl_content = extract_ontology_from_html(r.text, base_url_for_links, uri)
                
                if ttl_content:
                    print(f"Successfully scraped ontology data from HTML")
                    ttl_filename = get_unique_path("scraped_ontology.ttl", uri)
                    with open(ttl_filename, "w", encoding="utf-8") as f:
                        f.write(ttl_content)
                    successes.append(f"{uri} -> [Scraped from HTML] -> {os.path.basename(ttl_filename)}")
                    print(f"SUCCESS (Scraped): {uri} -> {os.path.basename(ttl_filename)}")
                    csv_rows.append((uri, os.path.basename(ttl_filename)))
                    result['done'] = True
                    return
                else:
                    fails.append(f"{uri} | Could not scrape ontology data from HTML.")
                    print(f"FAIL: {uri} | Could not scrape ontology data from HTML.")
        except Exception as e:
            fails.append(f"{uri} | HTML parse error: {e}")
            print(f"FAIL: {uri} | HTML parse error: {e}")
        result['done'] = True

    thread = threading.Thread(target=run)
    thread.daemon = True
    thread.start()
    thread.join(timeout=max_timeout)
    if thread.is_alive():
        fails.append(f"{uri} | Timeout after {max_timeout} seconds.")
        print(f"FAIL: {uri} | Timeout after {max_timeout} seconds.")

for uri in uris:
    discover_and_download(uri, max_timeout=30)

with open(RETRY_SUCCESS_LOG, "w", encoding="utf-8") as f:
    for line in successes:
        f.write(line + "\n")

with open(RETRY_FAIL_LOG, "w", encoding="utf-8") as f:
    for line in fails:
        f.write(line + "\n")

with open(CSV_PATH, "w", encoding="utf-8", newline="") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["uri", "filename"])
    for row in csv_rows:
        writer.writerow(row)

print(f"\nDone. Success: {len(successes)}, Fail: {len(fails)}.")
print(f"See {RETRY_SUCCESS_LOG} and {RETRY_FAIL_LOG} for details.")
print(f"CSV output written to {CSV_PATH}")
