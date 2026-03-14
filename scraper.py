#!/usr/bin/env python3
"""
MARRAKECH IMMOBILIEN-SCRAPER v4
- Extrahiert Bilder/Thumbnails
- Zuverlaessige URL-Erfassung
- Kontaktdaten
pip install requests beautifulsoup4
"""

import requests
from bs4 import BeautifulSoup
import json, time, random, re, argparse, hashlib
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Optional
from pathlib import Path

BUDGET_MIN_MAD = 1_000_000
BUDGET_MAX_MAD = 2_300_000
MIN_ROOMS = 3
MIN_BEDROOMS = 2

PREFERRED_NEIGHBORHOODS = [
    "targa", "palmeraie", "agdal", "tamansourt", "massira",
    "m'hamid", "mhamid", "izdihar", "amerchich", "tassoultant",
    "route de l'ourika", "route ourika", "route de fes", "route fes",
    "route de casablanca", "route casablanca", "sidi ghanem",
    "route d'amizmiz", "saada", "semlalia", "camp el ghoul",
    "hay mohammadi", "marjane", "annakhil", "tamesna",
]

NO_GO_KEYWORDS = ["riad", "riyad", "rez-de-chaussee", "rez de chaussee"]
MELKIA_KEYWORDS = ["melkia", "melk"]
TITRE_FONCIER_KEYWORDS = ["titre foncier", "tf", "titre"]

MIN_DELAY = 1.0
MAX_DELAY = 2.0
MAX_PAGES = 3

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

@dataclass
class Listing:
    id: str = ""
    title: str = ""
    source: str = ""
    url: str = ""
    price_mad: Optional[int] = None
    price_eur: Optional[int] = None
    area_sqm: Optional[int] = None
    rooms: Optional[int] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[int] = None
    floor: Optional[str] = None
    neighborhood: str = ""
    city: str = "Marrakech"
    property_type: str = "Apartment"
    price_per_sqm_mad: Optional[int] = None
    has_terrace: Optional[bool] = None
    has_pool: Optional[bool] = None
    has_parking: Optional[bool] = None
    has_elevator: Optional[bool] = None
    is_ground_floor: Optional[bool] = None
    is_riad: Optional[bool] = None
    is_new_build: Optional[bool] = None
    ownership_type: str = "Unbekannt"
    condition: str = "Unbekannt"
    description: str = ""
    contact_phone: str = ""
    contact_name: str = ""
    image: str = ""
    images: list = field(default_factory=list)
    scraped_at: str = ""

    def finalize(self):
        if not self.id and self.url:
            self.id = hashlib.md5(self.url.encode()).hexdigest()[:12]
        if not self.scraped_at:
            self.scraped_at = datetime.now().isoformat()
        if self.price_mad and self.area_sqm and self.area_sqm > 0:
            self.price_per_sqm_mad = int(self.price_mad / self.area_sqm)
        if self.price_mad and not self.price_eur:
            self.price_eur = int(self.price_mad / 10.8)
        if self.images and not self.image:
            self.image = self.images[0]
        return self

def delay():
    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

def fetch(url, session):
    for attempt in range(3):
        try:
            resp = session.get(url, headers=HEADERS, timeout=20)
            if resp.status_code == 200: return resp
            if resp.status_code == 429: time.sleep(15)
            elif resp.status_code in (403, 406): return None
        except requests.RequestException:
            time.sleep(5)
    return None

def parse_price(text):
    if not text: return None
    text = re.sub(r'[MADHsDh\u20ac\s\.\,\xa0]', '', text)
    m = re.search(r'(\d{6,8})', text)
    if m:
        val = int(m.group(1))
        if 100_000 <= val <= 50_000_000: return val
    return None

def extract_number(text, mn=0, mx=9999):
    if not text: return None
    m = re.search(r'(\d+)', str(text))
    if m:
        v = int(m.group(1))
        return v if mn <= v <= mx else None
    return None

def extract_images(soup, domain=""):
    imgs = []
    for img in soup.select("img[src], img[data-src], img[data-lazy-src]"):
        src = img.get("data-src") or img.get("data-lazy-src") or img.get("src") or ""
        if not src: continue
        if src.startswith("//"): src = "https:" + src
        elif src.startswith("/") and domain: src = domain + src
        sl = src.lower()
        if any(x in sl for x in ["logo","icon","avatar","placeholder","pixel","spacer","banner","ad-"]): continue
        if any(x in sl for x in [".jpg",".jpeg",".png",".webp"]):
            if len(src) > 20:
                imgs.append(src)
    # OG image
    og = soup.select_one("meta[property='og:image']")
    if og and og.get("content"):
        imgs.insert(0, og["content"])
    return list(dict.fromkeys(imgs))[:5]

def detect_from_text(text):
    t = text.lower()
    r = {}
    m = re.search(r'(\d{2,4})\s*m[^a-z]', t)
    if m:
        v = int(m.group(1))
        if 20 < v < 1000: r['area_sqm'] = v
    m = re.search(r'(\d)\s*(?:pi[e\u00e8]ces?|pcs?)\b', t)
    if m: r['rooms'] = int(m.group(1))
    m = re.search(r'(\d)\s*(?:chambres?|chbr?|ch\.)\b', t)
    if m: r['bedrooms'] = int(m.group(1))
    m = re.search(r'(\d)\s*(?:salles?\s*de\s*bain|sdb)', t)
    if m: r['bathrooms'] = int(m.group(1))
    if any(w in t for w in ["rez-de-chauss", "rdc"]):
        r['floor'] = "RDC"; r['is_ground_floor'] = True
    else:
        m = re.search(r'(\d+)\s*(?:[e\u00e8]me|er|e)?\s*[e\u00e9]tage', t)
        if m: r['floor'] = m.group(1) + ". Etage"; r['is_ground_floor'] = False
    r['has_terrace'] = any(w in t for w in ["terrasse", "balcon", "rooftop", "toit terrasse"])
    r['has_pool'] = any(w in t for w in ["piscine", "pool"])
    r['has_parking'] = any(w in t for w in ["parking", "garage", "sous-sol", "stationnement"])
    r['has_elevator'] = any(w in t for w in ["ascenseur"])
    r['is_new_build'] = any(w in t for w in ["neuf", "nouvelle construction", "livraison 202", "jamais habit"])
    if any(w in t for w in TITRE_FONCIER_KEYWORDS): r['ownership_type'] = "Titre Foncier"
    elif any(w in t for w in MELKIA_KEYWORDS): r['ownership_type'] = "Melkia"
    if any(w in t for w in ["neuf", "jamais habit"]): r['condition'] = "Neu"
    elif any(w in t for w in ["rnov", "renov", "refait"]): r['condition'] = "Renoviert"
    elif "bon tat" in t: r['condition'] = "Gut"
    r['is_riad'] = any(w in t for w in ["riad", "riyad"])
    for nb in PREFERRED_NEIGHBORHOODS:
        if nb in t: r['neighborhood'] = nb.title(); break
    phones = re.findall(r'(?:\+212|0)[\s.-]?[5-7][\s.-]?\d{2}[\s.-]?\d{2}[\s.-]?\d{2}[\s.-]?\d{2}', t)
    if phones: r['contact_phone'] = phones[0].strip()
    return r

def apply_detected(listing, detected):
    for key, val in detected.items():
        current = getattr(listing, key, None)
        if current is None or current == "" or current == "Unbekannt":
            setattr(listing, key, val)

# ═══════════════════════════════════════
# MUBAWAB
# ═══════════════════════════════════════

def scrape_mubawab(session, max_pages=MAX_PAGES):
    print(f"\n  MUBAWAB")
    listings = []
    for page in range(1, max_pages + 1):
        url = f"https://www.mubawab.ma/fr/st/marrakech/appartements-a-vendre:p:{page}"
        resp = fetch(url, session)
        if not resp: continue
        soup = BeautifulSoup(resp.text, "html.parser")
        cards = soup.select("li.listingBox") or soup.select("div[class*='listingBox']") or soup.select("div[class*='adItem']") or soup.select("a[href*='/fr/marrakech/']")
        print(f"    Seite {page}: {len(cards)} Karten")
        for card in cards:
            try:
                l = Listing(source="Mubawab")
                link = card if card.name == 'a' else card.select_one("a[href]")
                if link:
                    href = link.get("href", "")
                    if not href.startswith("http"): href = "https://www.mubawab.ma" + href
                    l.url = href
                for sel in ["h2","h3","[class*='title']","a"]:
                    el = card.select_one(sel)
                    if el and el.get_text(strip=True): l.title = el.get_text(strip=True); break
                for sel in ["[class*='price']","[class*='prix']","span.priceTag"]:
                    el = card.select_one(sel)
                    if el: l.price_mad = parse_price(el.get_text()); break
                img = card.select_one("img[src], img[data-src]")
                if img:
                    src = img.get("data-src") or img.get("src") or ""
                    if ".jpg" in src or ".jpeg" in src or ".png" in src or ".webp" in src:
                        l.image = src if src.startswith("http") else "https://www.mubawab.ma" + src
                apply_detected(l, detect_from_text(card.get_text(" ", strip=True)))
                for sel in ["[class*='location']","[class*='adresse']"]:
                    el = card.select_one(sel)
                    if el and not l.neighborhood: l.neighborhood = el.get_text(strip=True); break
                if l.title and len(l.title) > 3: listings.append(l)
            except: pass
        if not cards: break
        delay()
    seen = set()
    unique = [l for l in listings if l.url and l.url not in seen and not seen.add(l.url)]
    listings = unique
    print(f"    {len(listings)} unique, lade Details...")
    for i, l in enumerate(listings):
        if not l.url: continue
        if i % 20 == 0: print(f"    Detail {i+1}/{len(listings)}...")
        _mubawab_detail(l, session)
        delay()
    print(f"    Mubawab fertig: {len(listings)}")
    return listings

def _mubawab_detail(listing, session):
    resp = fetch(listing.url, session)
    if not resp: return
    soup = BeautifulSoup(resp.text, "html.parser")
    full_text = soup.get_text(" ", strip=True)
    if not listing.price_mad:
        for sel in ["[class*='price']","[class*='prix']","h3[class*='price']"]:
            el = soup.select_one(sel)
            if el: listing.price_mad = parse_price(el.get_text()); break
    for sel in ["[class*='description']","[class*='blockParagraph']","div.detailDesc","div[class*='more-text']"]:
        el = soup.select_one(sel)
        if el: listing.description = el.get_text(" ", strip=True); break
    if not listing.images:
        listing.images = extract_images(soup, "https://www.mubawab.ma")
    for el in soup.select("li, span, div, td"):
        t = el.get_text(strip=True)
        if not t or len(t) > 100: continue
        tl = t.lower()
        if "pi" in tl and "ce" in tl and not listing.rooms: listing.rooms = extract_number(t, 1, 20)
        elif "chambre" in tl and not listing.bedrooms: listing.bedrooms = extract_number(t, 1, 10)
        elif ("salle" in tl or "sdb" in tl) and not listing.bathrooms: listing.bathrooms = extract_number(t, 1, 10)
        elif "m" in tl and ("2" in tl or "\u00b2" in tl) and not listing.area_sqm:
            v = extract_number(t, 20, 1000)
            if v: listing.area_sqm = v
    for sel in ["[class*='phone']","[class*='tel']","a[href^='tel:']"]:
        el = soup.select_one(sel)
        if el and not listing.contact_phone:
            href = el.get("href","")
            if href.startswith("tel:"): listing.contact_phone = href.replace("tel:","")
            else:
                txt = el.get_text(strip=True)
                if re.search(r'\d{8,}', txt.replace(" ","")): listing.contact_phone = txt
    for sel in ["[class*='agent']","[class*='seller']"]:
        el = soup.select_one(sel)
        if el and not listing.contact_name:
            name = el.get_text(strip=True)
            if 2 < len(name) < 50 and not re.search(r'\d{5}', name): listing.contact_name = name
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
            if isinstance(data, dict):
                listing.title = listing.title or data.get("name", "")
                listing.description = listing.description or data.get("description", "")
                if data.get("floorSize") and isinstance(data["floorSize"], dict):
                    listing.area_sqm = listing.area_sqm or extract_number(str(data["floorSize"].get("value", "")), 20, 1000)
                listing.rooms = listing.rooms or extract_number(str(data.get("numberOfRooms", "")), 1, 20)
                if data.get("image"):
                    img_data = data["image"]
                    if isinstance(img_data, str) and not listing.image: listing.image = img_data
                    elif isinstance(img_data, list) and not listing.image and img_data: listing.image = img_data[0] if isinstance(img_data[0], str) else img_data[0].get("url","")
        except: pass
    apply_detected(listing, detect_from_text(f"{listing.title} {listing.description} {full_text}"))
    listing.finalize()

# ═══════════════════════════════════════
# AVITO
# ═══════════════════════════════════════

def scrape_avito(session, max_pages=MAX_PAGES):
    print(f"\n  AVITO")
    listings = []
    for page in range(1, max_pages + 1):
        url = f"https://www.avito.ma/fr/marrakech/appartements-%C3%A0_vendre?o={page}"
        resp = fetch(url, session)
        if not resp: continue
        soup = BeautifulSoup(resp.text, "html.parser")
        for script in soup.find_all("script"):
            text = script.string or ""
            if '__NEXT_DATA__' in text:
                try:
                    m = re.search(r'__NEXT_DATA__\s*=\s*(\{.+?\})\s*;?\s*</script>', text, re.DOTALL)
                    if m: _avito_parse_json(json.loads(m.group(1)), listings)
                except: pass
            if '"listingId"' in text or ('"subject"' in text and '"price"' in text):
                try: _avito_parse_json(json.loads(re.search(r'(\{.*\})', text, re.DOTALL).group(1)), listings)
                except: pass
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string)
                if isinstance(data, dict) and data.get("@type") == "ItemList":
                    for item in data.get("itemListElement", []):
                        i = item.get("item", item)
                        l = Listing(source="Avito", title=i.get("name",""), url=i.get("url",""))
                        if i.get("offers"): l.price_mad = parse_price(str(i["offers"].get("price","")))
                        if i.get("image"):
                            img = i["image"]
                            l.image = img if isinstance(img, str) else (img[0] if isinstance(img, list) and img else "")
                        if l.title: listings.append(l)
            except: pass
        for card in soup.select("a[href*='/fr/marrakech/']"):
            href = card.get("href","")
            if "appartement" not in href.lower(): continue
            if not href.startswith("http"): href = "https://www.avito.ma" + href
            if any(l.url == href for l in listings): continue
            l = Listing(source="Avito", url=href)
            el = card.select_one("p, span, h2, h3")
            l.title = (el.get_text(strip=True) if el else card.get_text(strip=True))[:200]
            pel = card.select_one("[class*='price']")
            if pel: l.price_mad = parse_price(pel.get_text())
            img = card.select_one("img[src], img[data-src]")
            if img:
                src = img.get("data-src") or img.get("src") or ""
                if any(x in src for x in [".jpg",".jpeg",".png",".webp"]): l.image = src
            apply_detected(l, detect_from_text(card.get_text(" ", strip=True)))
            if l.title and len(l.title) > 5: listings.append(l)
        print(f"    Seite {page}: {len(listings)} bisher")
        delay()
    seen = set()
    unique = [l for l in listings if (l.url or l.title) and (l.url or l.title) not in seen and not seen.add(l.url or l.title)]
    listings = unique
    print(f"    {len(listings)} unique, lade Details...")
    for i, l in enumerate(listings):
        if not l.url: continue
        if i % 20 == 0: print(f"    Detail {i+1}/{len(listings)}...")
        _avito_detail(l, session)
        delay()
    print(f"    Avito fertig: {len(listings)}")
    return listings

def _avito_parse_json(data, listings):
    if isinstance(data, dict):
        if data.get("subject") or (data.get("title") and data.get("price")):
            l = Listing(source="Avito")
            l.title = data.get("subject", data.get("title", ""))
            l.url = data.get("url", "")
            l.price_mad = parse_price(str(data.get("price", data.get("priceValue", ""))))
            p = data.get("params", data.get("attributes", {}))
            if isinstance(p, dict):
                l.rooms = extract_number(str(p.get("rooms","")), 1, 20)
                l.area_sqm = extract_number(str(p.get("surface", p.get("size",""))), 20, 1000)
                l.bedrooms = extract_number(str(p.get("bedrooms","")), 1, 10)
            loc = data.get("location")
            if isinstance(loc, dict): l.neighborhood = loc.get("name", loc.get("label",""))
            elif isinstance(loc, str): l.neighborhood = loc
            phone = data.get("phone", data.get("phoneNumber", ""))
            if phone: l.contact_phone = str(phone)
            imgs = data.get("images", data.get("photos", []))
            if isinstance(imgs, list):
                for im in imgs[:5]:
                    if isinstance(im, str): l.images.append(im)
                    elif isinstance(im, dict): l.images.append(im.get("url", im.get("src", "")))
            if l.title: listings.append(l)
        for v in data.values():
            if isinstance(v, (dict, list)): _avito_parse_json(v, listings)
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, (dict, list)): _avito_parse_json(item, listings)

def _avito_detail(listing, session):
    resp = fetch(listing.url, session)
    if not resp: return
    soup = BeautifulSoup(resp.text, "html.parser")
    full_text = soup.get_text(" ", strip=True)
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
            if isinstance(data, dict):
                listing.title = listing.title or data.get("name","")
                listing.description = listing.description or data.get("description","")
                if data.get("offers"): listing.price_mad = listing.price_mad or parse_price(str(data["offers"].get("price","")))
                if data.get("image") and not listing.image:
                    img = data["image"]
                    listing.image = img if isinstance(img, str) else ""
        except: pass
    for script in soup.find_all("script"):
        text = script.string or ""
        if '"price"' in text or '"surface"' in text:
            for m in re.findall(r'\{[^{}]*"(?:price|surface|rooms)"[^{}]*\}', text):
                try:
                    d = json.loads(m)
                    listing.price_mad = listing.price_mad or parse_price(str(d.get("price","")))
                    listing.area_sqm = listing.area_sqm or extract_number(str(d.get("surface","")), 20, 1000)
                    listing.rooms = listing.rooms or extract_number(str(d.get("rooms","")), 1, 20)
                except: pass
        if '"phone"' in text or '"phoneNumber"' in text:
            for m in re.findall(r'"(?:phone|phoneNumber)"\s*:\s*"([^"]+)"', text):
                if not listing.contact_phone and len(m) >= 8: listing.contact_phone = m
    if not listing.images:
        listing.images = extract_images(soup, "https://www.avito.ma")
    if not listing.price_mad:
        for sel in ["[class*='price']","[data-testid*='price']"]:
            el = soup.select_one(sel)
            if el: listing.price_mad = parse_price(el.get_text()); break
    if not listing.description:
        for sel in ["[class*='description']","[class*='body']"]:
            el = soup.select_one(sel)
            if el: listing.description = el.get_text(" ", strip=True); break
    for sel in ["[class*='phone']","a[href^='tel:']","button[class*='phone']"]:
        el = soup.select_one(sel)
        if el and not listing.contact_phone:
            href = el.get("href","")
            if href.startswith("tel:"): listing.contact_phone = href.replace("tel:","")
    apply_detected(listing, detect_from_text(f"{listing.title} {listing.description} {full_text}"))
    listing.finalize()

# ═══════════════════════════════════════
# SAROUTY
# ═══════════════════════════════════════

def scrape_sarouty(session, max_pages=MAX_PAGES):
    print(f"\n  SAROUTY")
    listings = []
    for page in range(1, max_pages + 1):
        url = "https://www.sarouty.ma/acheter/marrakech/appartements-a-vendre/" if page == 1 else f"https://www.sarouty.ma/acheter/marrakech/appartements-a-vendre/?page={page}"
        resp = fetch(url, session)
        if not resp: continue
        soup = BeautifulSoup(resp.text, "html.parser")
        cards = soup.select("[class*='listingCard']") or soup.select("[class*='property-card']") or soup.select("article") or soup.select("div[class*='result']") or soup.select("div[class*='card']")
        if not cards:
            cards = [l for l in soup.select("a[href]") if any(w in l.get('href','').lower() for w in ['appartement','vendre','acheter'])]
        print(f"    Seite {page}: {len(cards)} Karten")
        for card in cards:
            try:
                l = Listing(source="Sarouty")
                link = card if card.name == 'a' else card.select_one("a[href]")
                if link:
                    href = link.get("href","")
                    if not href.startswith("http"): href = "https://www.sarouty.ma" + href
                    l.url = href
                el = card.select_one("h2, h3, [class*='title'], a")
                if el: l.title = el.get_text(strip=True)
                pel = card.select_one("[class*='price']")
                if pel: l.price_mad = parse_price(pel.get_text())
                img = card.select_one("img[src], img[data-src]")
                if img:
                    src = img.get("data-src") or img.get("src") or ""
                    if any(x in src for x in [".jpg",".jpeg",".png",".webp"]): l.image = src if src.startswith("http") else "https://www.sarouty.ma" + src
                apply_detected(l, detect_from_text(card.get_text(" ", strip=True)))
                if l.title and len(l.title) > 5: listings.append(l)
            except: pass
        if not cards: break
        delay()
    seen = set()
    unique = [l for l in listings if (l.url or l.title) and (l.url or l.title) not in seen and not seen.add(l.url or l.title)]
    listings = unique
    print(f"    {len(listings)} unique, lade Details...")
    for i, l in enumerate(listings):
        if not l.url: continue
        if i % 20 == 0: print(f"    Detail {i+1}/{len(listings)}...")
        _sarouty_detail(l, session)
        delay()
    print(f"    Sarouty fertig: {len(listings)}")
    return listings

def _sarouty_detail(listing, session):
    resp = fetch(listing.url, session)
    if not resp: return
    soup = BeautifulSoup(resp.text, "html.parser")
    full_text = soup.get_text(" ", strip=True)
    if not listing.price_mad:
        for sel in ["[class*='price']","[class*='prix']"]:
            el = soup.select_one(sel)
            if el: listing.price_mad = parse_price(el.get_text()); break
    if not listing.description:
        for sel in ["[class*='description']","[class*='text']"]:
            el = soup.select_one(sel)
            if el: listing.description = el.get_text(" ", strip=True); break
    if not listing.images:
        listing.images = extract_images(soup, "https://www.sarouty.ma")
    for sel in ["[class*='phone']","a[href^='tel:']"]:
        el = soup.select_one(sel)
        if el and not listing.contact_phone:
            href = el.get("href","")
            if href.startswith("tel:"): listing.contact_phone = href.replace("tel:","")
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
            if isinstance(data, dict):
                listing.title = listing.title or data.get("name","")
                listing.description = listing.description or data.get("description","")
                if data.get("floorSize") and isinstance(data["floorSize"], dict):
                    listing.area_sqm = listing.area_sqm or extract_number(str(data["floorSize"].get("value", "")), 20, 1000)
                listing.rooms = listing.rooms or extract_number(str(data.get("numberOfRooms", "")), 1, 20)
                if data.get("image") and not listing.image:
                    listing.image = data["image"] if isinstance(data["image"], str) else ""
        except: pass
    apply_detected(listing, detect_from_text(f"{listing.title} {listing.description} {full_text}"))
    listing.finalize()

# ═══════════════════════════════════════
# FILTER
# ═══════════════════════════════════════

def apply_gates(listings):
    passed, rejected = [], []
    for l in listings:
        reason = None
        if l.price_mad is not None:
            if l.price_mad < BUDGET_MIN_MAD: reason = f"Preis zu niedrig: {l.price_mad:,}"
            elif l.price_mad > BUDGET_MAX_MAD: reason = f"Preis zu hoch: {l.price_mad:,}"
        else: reason = "Kein Preis"
        if not reason and l.rooms and l.rooms < MIN_ROOMS and (not l.bedrooms or l.bedrooms < MIN_BEDROOMS):
            reason = f"Zu wenig Zimmer"
        if not reason and l.is_ground_floor: reason = "Erdgeschoss"
        if not reason and l.is_riad: reason = "Riad"
        if not reason and l.ownership_type == "Melkia": reason = "Melkia"
        if not reason:
            full = (l.title + " " + l.description).lower()
            for kw in NO_GO_KEYWORDS:
                if kw in full: reason = f"No-Go: {kw}"; break
        if reason: rejected.append({"title": l.title[:60], "url": l.url, "reason": reason})
        else: passed.append(l)
    return passed, rejected

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--portal", choices=["avito","mubawab","sarouty","all"], default="all")
    parser.add_argument("--output", "-o", default="data/latest_raw.json")
    parser.add_argument("--pages", type=int, default=MAX_PAGES)
    parser.add_argument("--raw", action="store_true")
    args = parser.parse_args()
    print(f"\n  MARRAKECH SCRAPER v4 | {args.portal.upper()} | {args.pages} Seiten\n")
    session = requests.Session()
    all_listings = []
    scrapers = {"avito": scrape_avito, "mubawab": scrape_mubawab, "sarouty": scrape_sarouty}
    portals = scrapers.keys() if args.portal == "all" else [args.portal]
    for portal in portals:
        try: all_listings.extend(scrapers[portal](session, args.pages))
        except Exception as ex:
            print(f"  FEHLER {portal}: {ex}")
            import traceback; traceback.print_exc()
    seen = set()
    unique = []
    for l in all_listings:
        k = l.url or f"{l.title}_{l.price_mad}"
        if k not in seen: seen.add(k); unique.append(l)
    if args.raw: passed, rejected = unique, []
    else: passed, rejected = apply_gates(unique)
    with_url = len([l for l in passed if l.url])
    with_img = len([l for l in passed if l.image])
    print(f"\n  ERGEBNIS: {len(passed)} qualifiziert, {len(rejected)} abgelehnt")
    print(f"  Mit URL: {with_url}/{len(passed)} | Mit Bild: {with_img}/{len(passed)}")
    output = {
        "meta": {"scraped_at": datetime.now().isoformat(), "total_found": len(passed)+len(rejected), "passed_gates": len(passed), "rejected": len(rejected)},
        "listings": [asdict(l) for l in passed],
        "rejected_log": rejected,
    }
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(output, ensure_ascii=False, indent=2))
    print(f"  Gespeichert: {out}\n")

if __name__ == "__main__":
    main()
