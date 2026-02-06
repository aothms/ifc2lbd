import requests
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
import re

def extract_ontology_from_html(html_content, base_url, uri):
    """Extract ontology data from HTML documentation and convert to TTL"""
    soup = BeautifulSoup(html_content, 'html.parser')
    ttl_lines = []
    
    # Extract namespace/prefix from the page
    namespace_uri = None
    prefix = None
    
    # Look for the ontology namespace in various places
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
    
    print(f"Extracted namespace: {namespace_uri}")
    
    # Extract prefix
    if 'purl.org/' in namespace_uri:
        prefix = namespace_uri.split('purl.org/')[-1].split('#')[0].split('/')[0]
    elif 'w3id.org/' in namespace_uri:
        prefix = namespace_uri.split('w3id.org/')[-1].split('#')[0].split('/')[0]
    else:
        prefix = 'onto'
    
    print(f"Using prefix: {prefix}")
    
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
        print(f"Found classes section")
        class_list = class_section.find_next('ul')
        if class_list:
            class_count = 0
            for li in class_list.find_all('li'):
                link = li.find('a', href=True)
                if link:
                    href = link['href']
                    if href.startswith('#'):
                        class_name = href[1:]
                        label = link.get_text().strip()
                        ttl_lines.append(f"{prefix}:{class_name} a owl:Class ;")
                        ttl_lines.append(f"    rdfs:label \"{label}\" .")
                        ttl_lines.append("")
                        class_count += 1
            print(f"Extracted {class_count} classes")
    
    # Extract properties
    for prop_type in ['Object Properties', 'Data Properties', 'properties']:
        prop_section = soup.find('h4', string=re.compile(prop_type, re.I))
        if prop_section:
            print(f"Found {prop_type} section")
            prop_list = prop_section.find_next('ul')
            if prop_list:
                prop_count = 0
                for li in prop_list.find_all('li'):
                    link = li.find('a', href=True)
                    if link:
                        href = link['href']
                        if href.startswith('#'):
                            prop_name = href[1:]
                            label = link.get_text().strip()
                            prop_class = "owl:ObjectProperty" if 'object' in prop_type.lower() else "owl:DatatypeProperty" if 'data' in prop_type.lower() else "rdf:Property"
                            ttl_lines.append(f"{prefix}:{prop_name} a {prop_class} ;")
                            ttl_lines.append(f"    rdfs:label \"{label}\" .")
                            ttl_lines.append("")
                            prop_count += 1
                print(f"Extracted {prop_count} {prop_type}")
    
    return '\n'.join(ttl_lines) if len(ttl_lines) > 10 else None

# Test with fisa
uri = "https://purl.org/fisa#"
print(f"Testing HTML scraping with: {uri}\n")

r = requests.get(uri, timeout=5, allow_redirects=True)
print(f"Final URL: {r.url}\n")

ttl_content = extract_ontology_from_html(r.text, r.url, uri)

if ttl_content:
    print(f"\nSuccess! Generated {len(ttl_content.split(chr(10)))} lines of TTL")
    print("\nFirst 30 lines:")
    print('\n'.join(ttl_content.split('\n')[:30]))
    
    # Save to file
    with open("resources/ontologies/downloaded/test_fisa_scraped.ttl", "w", encoding="utf-8") as f:
        f.write(ttl_content)
    print("\nSaved to test_fisa_scraped.ttl")
else:
    print("Failed to extract ontology")
