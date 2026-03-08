#!/usr/bin/env python3
"""
MARRAKECH IMMOBILIEN-SCRAPER
=============================
Lean & robust. Scrapt Avito, Mubawab, Sarouty.
Filtert hart. Gibt sauberes JSON aus.

Nutzung:
    python scraper.py                  # Alle Portale, Standard-Filter
    python scraper.py --portal avito   # Nur Avito
    python scraper.py --output leads.json
    python scraper.py --raw            # Ohne Filter (alle Ergebnisse)

Abhängigkeiten:
    pip install requests beautifulsoup4
"""

import requests
from bs4 import BeautifulSoup
import json
import time
import random
import re
import argparse
import hashlib
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Optional
from pathlib import Path

# ═══════════════════════════════════════════════════════════
# CONFIG — Hier eure Kriterien anpassen
# ═══════════════════════════════════════════════════════════

BUDGET_MIN_MAD = 1_000_000
BUDGET_MAX_MAD = 2_300_000
MIN_ROOMS = 3
MIN_BEDROOMS = 2  # Alternative: 2 Schlafzimmer reicht auch

PREFERRED_NEIGHBORHOODS = [
    "targa", "palmeraie", "agdal", "tamansourt", "massira",
    "m'hamid", "mhamid", "izdihar", "amerchich", "tassoultant",
    "route de l'ourika", "route ourika", "route de fes", "route fes",
    "route de casablanca", "route casablanca", "sidi ghanem",
    "route d'amizmiz", "saada", "semlalia", "camp el ghoul",
    "hay mohammadi", "marjane", "annakhil", "tamesna",
]

NO_GO_KEYWORDS = ["riad", "riyad", "rez-de-chaussée", "rez de chaussée", "rdc"]
MELKIA_KEYWORDS = ["melkia", "melk"]
TITRE_FONCIER_KEYWORDS = ["titre foncier", "tf", "titré"]

# Delays (Sekunden) — respektvoll gegenüber den Servern
MIN_DELAY = 1.5
MAX_DELAY = 3.0
MAX_PAGES = 5  # Seiten pro Portal

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


# ═══════════════════════════════════════════════════════════
# DATA MODEL
# ═══════════════════════════════════════════════════════════

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
    images: list = field(default_factory=list)
    scraped_at: str = ""
    raw_attributes: dict = field(default_factory=dict)

    def __post_init__(self):
        if not self.id and self.url:
            self.id = hashlib.md5(self.url.encode()).hexdigest()[:12]
        if not self.scraped_at:
            self.scraped_at = datetime.now().isoformat()
        if self.price_mad and self.area_sqm and self.area_sqm > 0:
            self.price_per_sqm_mad = int(self.price_mad / self.area_sqm)
        if self.price_mad and not self.price_eur:
            self.price_eur = int(self.price_mad / 10.8)


# ═══════════════════════════════════════════════════════════
# UTILS
# ═══════════════════════════════════════════════════════════

def polite_delay():
    """Respektvolle Pause zwischen Requests."""
    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))


def fetch(url, session):
    """Robuster GET mit Retries."""
    for attempt in range(3):
        try:
            resp = session.get(url, headers=HEADERS, timeout=15)
            if resp.status_code == 200:
                return resp
            if resp.status_code == 429:
                print(f"  ⏳ Rate limit, warte 10s...")
                time.sleep(10)
            elif resp.status_code == 403:
                print(f"  ⛔ Zugriff verweigert: {url}")
                return None
            else:
                print(f"  ⚠ Status {resp.status_code}: {url}")
        except requests.RequestException as e:
            print(f"  ⚠ Fehler (Versuch {attempt+1}): {e}")
            time.sleep(3)
    return None


def parse_price(text):
    """Extrahiert Preis in MAD aus Text."""
    if not text:
        return None
    text = text.strip().replace("\xa0", " ").replace(" ", "")
    # "1 450 000 DH" / "1.450.000" / "1,450,000"
    text = text.replace(".", "").replace(",", "").replace(" ", "")
    nums = re.findall(r'\d+', text)
    if nums:
        val = int(nums[0])
        # Plausibilitäts-Check: Preise in MAD sind typisch 100k-10M
        if val < 1000:
            val *= 10000  # z.B. "145" → 1.450.000? Nein, zu unsicher
            return None
        if 100_000 <= val <= 50_000_000:
            return val
    return None


def parse_number(text):
    """Extrahiert eine Zahl aus Text."""
    if not text:
        return None
    nums = re.findall(r'\d+', str(text))
    return int(nums[0]) if nums else None


def detect_amenities(text):
    """Erkennt Ausstattung aus Beschreibung."""
    t = text.lower()
    return {
        "has_terrace": any(w in t for w in ["terrasse", "balcon", "rooftop", "toit"]),
        "has_pool": any(w in t for w in ["piscine", "pool"]),
        "has_parking": any(w in t for w in ["parking", "garage", "sous-sol", "stationnement"]),
        "has_elevator": any(w in t for w in ["ascenseur", "elevator"]),
        "is_new_build": any(w in t for w in ["neuf", "nouvelle construction", "livraison 2024",
                                              "livraison 2025", "livraison 2026"]),
    }


def detect_floor(text):
    """Erkennt Etage."""
    t = text.lower()
    if any(w in t for w in ["rez-de-chaussée", "rez de chaussée", "rdc"]):
        return "RDC"
    m = re.search(r'(\d+)\s*(?:ème|er|e)?\s*(?:étage|etage)', t)
    if m:
        return f"{m.group(1)}. Etage"
    return None


def detect_ownership(text):
    """Erkennt Eigentumslage."""
    t = text.lower()
    if any(w in t for w in TITRE_FONCIER_KEYWORDS):
        return "Titre Foncier"
    if any(w in t for w in MELKIA_KEYWORDS):
        return "Melkia"
    return "Unbekannt"


# ═══════════════════════════════════════════════════════════
# SCRAPER: AVITO
# ═══════════════════════════════════════════════════════════

def scrape_avito(session, max_pages=MAX_PAGES):
    """Scrapt Avito.ma Immobilien-Listings in Marrakesch."""
    listings = []
    base = "https://www.avito.ma/fr/marrakech/appartements-%C3%A0_vendre"

    print(f"\n{'='*50}")
    print(f"🔍 AVITO — Starte Scraping...")
    print(f"{'='*50}")

    for page in range(1, max_pages + 1):
        url = f"{base}?o={page}" if page > 1 else base
        print(f"\n  📄 Seite {page}/{max_pages}: {url}")

        resp = fetch(url, session)
        if not resp:
            continue

        soup = BeautifulSoup(resp.text, "html.parser")

        # Avito listing cards — sie nutzen verschiedene Selektoren
        cards = soup.select("a[href*='/fr/marrakech/'][href*='appartement']")
        if not cards:
            cards = soup.select("div[class*='listing'] a, div[class*='item'] a, li[class*='item'] a")

        # Auch JSON-LD oder Script-Tags checken (Avito lädt teils per JS)
        scripts = soup.find_all("script", type="application/ld+json")
        for script in scripts:
            try:
                data = json.loads(script.string)
                if isinstance(data, list):
                    for item in data:
                        if item.get("@type") in ["Product", "Offer", "RealEstateListing"]:
                            listing = _avito_from_jsonld(item)
                            if listing:
                                listings.append(listing)
                elif isinstance(data, dict) and "itemListElement" in data:
                    for item in data["itemListElement"]:
                        listing = _avito_from_jsonld(item.get("item", item))
                        if listing:
                            listings.append(listing)
            except (json.JSONDecodeError, TypeError):
                pass

        # Fallback: HTML parsen
        seen_urls = {l.url for l in listings}
        for card in cards:
            href = card.get("href", "")
            if not href or href in seen_urls:
                continue
            if not href.startswith("http"):
                href = "https://www.avito.ma" + href

            listing = Listing(source="Avito", url=href)

            # Titel
            title_el = card.select_one("p, span, h2, h3, [class*='title']")
            if title_el:
                listing.title = title_el.get_text(strip=True)
            elif card.get("title"):
                listing.title = card["title"]

            # Preis
            price_el = card.select_one("[class*='price'], [class*='Prix']")
            if price_el:
                listing.price_mad = parse_price(price_el.get_text())

            seen_urls.add(href)
            if listing.title:
                listings.append(listing)

        found = len([c for c in cards if c.get("href")])
        print(f"  → {found} Karten gefunden")

        if found == 0:
            print("  → Keine weiteren Ergebnisse, stoppe.")
            break

        polite_delay()

    # Detail-Seiten abrufen für vollständige Infos
    print(f"\n  📋 {len(listings)} Listings gefunden, lade Details...")
    for i, listing in enumerate(listings):
        if not listing.url:
            continue
        print(f"  🔎 Detail {i+1}/{len(listings)}: {listing.title[:50]}...")
        _avito_enrich(listing, session)
        polite_delay()

    print(f"\n  ✅ Avito fertig: {len(listings)} Inserate")
    return listings


def _avito_from_jsonld(data):
    """Extrahiert Listing aus JSON-LD Daten."""
    try:
        listing = Listing(source="Avito")
        listing.title = data.get("name", "")
        listing.url = data.get("url", "")
        offers = data.get("offers", {})
        if isinstance(offers, dict):
            listing.price_mad = parse_price(str(offers.get("price", "")))
        listing.description = data.get("description", "")
        return listing if listing.title else None
    except Exception:
        return None


def _avito_enrich(listing, session):
    """Lädt Detail-Seite und reichert Listing an."""
    resp = fetch(listing.url, session)
    if not resp:
        return

    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text(" ", strip=True).lower()

    # Preis
    if not listing.price_mad:
        price_el = soup.select_one("[class*='price'], [data-testid*='price']")
        if price_el:
            listing.price_mad = parse_price(price_el.get_text())

    # Attribute aus der Seite
    params = soup.select("li[class*='param'], span[class*='param'], div[class*='info'] li, ol li")
    for p in params:
        pt = p.get_text(strip=True).lower()
        if any(w in pt for w in ["pièce", "piece", "chambre"]):
            n = parse_number(pt)
            if n:
                if "chambre" in pt:
                    listing.bedrooms = n
                else:
                    listing.rooms = n
        elif "m²" in pt or "m2" in pt:
            n = parse_number(pt)
            if n and 20 < n < 1000:
                listing.area_sqm = n
        elif "salle" in pt or "sdb" in pt:
            listing.bathrooms = parse_number(pt)
        elif "étage" in pt or "etage" in pt:
            listing.floor = detect_floor(pt)

    # Beschreibung
    desc_el = soup.select_one("[class*='description'], [class*='body'], [class*='content'] p")
    if desc_el:
        listing.description = desc_el.get_text(" ", strip=True)

    full_text = listing.description + " " + listing.title + " " + text

    # Amenities
    amenities = detect_amenities(full_text)
    for k, v in amenities.items():
        if v:
            setattr(listing, k, v)

    # Floor
    if not listing.floor:
        listing.floor = detect_floor(full_text)
    listing.is_ground_floor = listing.floor == "RDC" if listing.floor else None

    # Ownership
    listing.ownership_type = detect_ownership(full_text)

    # Riad check
    listing.is_riad = any(w in full_text.lower() for w in ["riad", "riyad"])

    # Neighborhood
    if not listing.neighborhood:
        for nb in PREFERRED_NEIGHBORHOODS:
            if nb in full_text.lower():
                listing.neighborhood = nb.title()
                break

    # Post-init
    listing.__post_init__()


# ═══════════════════════════════════════════════════════════
# SCRAPER: MUBAWAB
# ═══════════════════════════════════════════════════════════

def scrape_mubawab(session, max_pages=MAX_PAGES):
    """Scrapt Mubawab.ma."""
    listings = []
    base = "https://www.mubawab.ma/fr/st/marrakech/appartements-a-vendre"

    print(f"\n{'='*50}")
    print(f"🔍 MUBAWAB — Starte Scraping...")
    print(f"{'='*50}")

    for page in range(1, max_pages + 1):
        url = f"{base}:p:{page}" if page > 1 else base
        print(f"\n  📄 Seite {page}/{max_pages}: {url}")

        resp = fetch(url, session)
        if not resp:
            continue

        soup = BeautifulSoup(resp.text, "html.parser")

        # Mubawab Listing-Cards
        cards = soup.select("li.listingBox, div.listingBox, div[class*='adItem'], a[class*='listing']")
        if not cards:
            # Breitere Suche
            cards = soup.select("li[class*='List'], div[class*='result'], div[class*='annonce']")

        for card in cards:
            listing = Listing(source="Mubawab")

            # URL
            link = card.select_one("a[href*='/fr/']") or card if card.name == "a" else None
            if link:
                href = link.get("href", "")
                if not href.startswith("http"):
                    href = "https://www.mubawab.ma" + href
                listing.url = href

            # Titel
            title_el = card.select_one("h2, h3, [class*='title'], [class*='titre']")
            listing.title = title_el.get_text(strip=True) if title_el else ""

            # Preis
            price_el = card.select_one("[class*='price'], [class*='prix']")
            if price_el:
                listing.price_mad = parse_price(price_el.get_text())

            # Quick-Infos auf der Karte
            infos = card.select("span[class*='info'], li[class*='char'], span[class*='feat']")
            for info in infos:
                it = info.get_text(strip=True).lower()
                if "m²" in it or "m2" in it:
                    listing.area_sqm = parse_number(it)
                elif "pièce" in it or "ch" in it:
                    n = parse_number(it)
                    if n:
                        if "ch" in it:
                            listing.bedrooms = n
                        else:
                            listing.rooms = n

            # Location
            loc_el = card.select_one("[class*='location'], [class*='adresse'], [class*='address']")
            if loc_el:
                listing.neighborhood = loc_el.get_text(strip=True)

            if listing.title or listing.url:
                listings.append(listing)

        print(f"  → {len(cards)} Karten gefunden")
        if not cards:
            break
        polite_delay()

    # Details
    print(f"\n  📋 {len(listings)} Listings, lade Details...")
    for i, listing in enumerate(listings):
        if not listing.url:
            continue
        print(f"  🔎 Detail {i+1}/{len(listings)}: {listing.title[:50]}...")
        _mubawab_enrich(listing, session)
        polite_delay()

    print(f"\n  ✅ Mubawab fertig: {len(listings)} Inserate")
    return listings


def _mubawab_enrich(listing, session):
    """Detail-Seite für Mubawab."""
    resp = fetch(listing.url, session)
    if not resp:
        return

    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text(" ", strip=True)

    # Preis
    if not listing.price_mad:
        price_el = soup.select_one("[class*='price'], [class*='prix']")
        if price_el:
            listing.price_mad = parse_price(price_el.get_text())

    # Attribute
    attrs = soup.select("div[class*='info'] li, ul[class*='char'] li, div[class*='detail'] span, table td")
    for attr in attrs:
        at = attr.get_text(strip=True).lower()
        if "pièce" in at:
            listing.rooms = listing.rooms or parse_number(at)
        elif "chambre" in at:
            listing.bedrooms = listing.bedrooms or parse_number(at)
        elif "salle" in at or "sdb" in at:
            listing.bathrooms = listing.bathrooms or parse_number(at)
        elif "m²" in at and not listing.area_sqm:
            n = parse_number(at)
            if n and 20 < n < 1000:
                listing.area_sqm = n
        elif "étage" in at:
            listing.floor = listing.floor or detect_floor(at)

    # Description
    desc = soup.select_one("[class*='description'], [class*='text'], [class*='content'] p")
    if desc:
        listing.description = desc.get_text(" ", strip=True)

    full = listing.description + " " + listing.title + " " + text
    amenities = detect_amenities(full)
    for k, v in amenities.items():
        if v:
            setattr(listing, k, v)

    if not listing.floor:
        listing.floor = detect_floor(full)
    listing.is_ground_floor = listing.floor == "RDC" if listing.floor else None
    listing.ownership_type = detect_ownership(full)
    listing.is_riad = any(w in full.lower() for w in ["riad", "riyad"])

    if not listing.neighborhood:
        for nb in PREFERRED_NEIGHBORHOODS:
            if nb in full.lower():
                listing.neighborhood = nb.title()
                break

    listing.__post_init__()


# ═══════════════════════════════════════════════════════════
# SCRAPER: SAROUTY
# ═══════════════════════════════════════════════════════════

def scrape_sarouty(session, max_pages=MAX_PAGES):
    """Scrapt Sarouty.ma."""
    listings = []
    base = "https://www.sarouty.ma/fr/immobilier/appartements/a-vendre/marrakech"

    print(f"\n{'='*50}")
    print(f"🔍 SAROUTY — Starte Scraping...")
    print(f"{'='*50}")

    for page in range(1, max_pages + 1):
        url = f"{base}?page={page}" if page > 1 else base
        print(f"\n  📄 Seite {page}/{max_pages}: {url}")

        resp = fetch(url, session)
        if not resp:
            continue

        soup = BeautifulSoup(resp.text, "html.parser")

        cards = soup.select("div[class*='listing'], article[class*='listing'], div[class*='property'], div[class*='result']")
        if not cards:
            cards = soup.select("a[href*='appartement'][href*='marrakech']")

        for card in cards:
            listing = Listing(source="Sarouty")

            link = card.select_one("a[href]") or (card if card.name == "a" else None)
            if link:
                href = link.get("href", "")
                if not href.startswith("http"):
                    href = "https://www.sarouty.ma" + href
                listing.url = href

            title_el = card.select_one("h2, h3, [class*='title']")
            listing.title = title_el.get_text(strip=True) if title_el else ""

            price_el = card.select_one("[class*='price'], [class*='prix']")
            if price_el:
                listing.price_mad = parse_price(price_el.get_text())

            infos = card.select("span, li")
            for info in infos:
                it = info.get_text(strip=True).lower()
                if "m²" in it:
                    listing.area_sqm = listing.area_sqm or parse_number(it)
                elif "pièce" in it or "chambre" in it:
                    n = parse_number(it)
                    if n and "chambre" in it:
                        listing.bedrooms = n
                    elif n:
                        listing.rooms = n

            loc_el = card.select_one("[class*='location'], [class*='address']")
            if loc_el:
                listing.neighborhood = loc_el.get_text(strip=True)

            if listing.title or listing.url:
                listings.append(listing)

        print(f"  → {len(cards)} Karten gefunden")
        if not cards:
            break
        polite_delay()

    print(f"\n  📋 {len(listings)} Listings, lade Details...")
    for i, listing in enumerate(listings):
        if not listing.url:
            continue
        print(f"  🔎 Detail {i+1}/{len(listings)}: {listing.title[:50]}...")
        _sarouty_enrich(listing, session)
        polite_delay()

    print(f"\n  ✅ Sarouty fertig: {len(listings)} Inserate")
    return listings


def _sarouty_enrich(listing, session):
    """Detail-Seite für Sarouty."""
    resp = fetch(listing.url, session)
    if not resp:
        return

    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text(" ", strip=True)

    if not listing.price_mad:
        price_el = soup.select_one("[class*='price']")
        if price_el:
            listing.price_mad = parse_price(price_el.get_text())

    attrs = soup.select("li, span[class*='feat'], div[class*='detail'] span")
    for attr in attrs:
        at = attr.get_text(strip=True).lower()
        if "pièce" in at:
            listing.rooms = listing.rooms or parse_number(at)
        elif "chambre" in at:
            listing.bedrooms = listing.bedrooms or parse_number(at)
        elif "salle" in at:
            listing.bathrooms = listing.bathrooms or parse_number(at)
        elif "m²" in at and not listing.area_sqm:
            n = parse_number(at)
            if n and 20 < n < 1000:
                listing.area_sqm = n
        elif "étage" in at:
            listing.floor = listing.floor or detect_floor(at)

    desc = soup.select_one("[class*='description']")
    if desc:
        listing.description = desc.get_text(" ", strip=True)

    full = listing.description + " " + listing.title + " " + text
    for k, v in detect_amenities(full).items():
        if v:
            setattr(listing, k, v)

    if not listing.floor:
        listing.floor = detect_floor(full)
    listing.is_ground_floor = listing.floor == "RDC" if listing.floor else None
    listing.ownership_type = detect_ownership(full)
    listing.is_riad = any(w in full.lower() for w in ["riad", "riyad"])

    if not listing.neighborhood:
        for nb in PREFERRED_NEIGHBORHOODS:
            if nb in full.lower():
                listing.neighborhood = nb.title()
                break

    listing.__post_init__()


# ═══════════════════════════════════════════════════════════
# FILTER-GATES
# ═══════════════════════════════════════════════════════════

def apply_gates(listings):
    """Harte Ausschlusskriterien. Returns (passed, rejected)."""
    passed = []
    rejected = []

    for l in listings:
        reason = None

        # Gate 1: Preis
        if l.price_mad is not None:
            if l.price_mad < BUDGET_MIN_MAD:
                reason = f"Preis zu niedrig: {l.price_mad:,} MAD < {BUDGET_MIN_MAD:,}"
            elif l.price_mad > BUDGET_MAX_MAD:
                reason = f"Preis zu hoch: {l.price_mad:,} MAD > {BUDGET_MAX_MAD:,}"
        else:
            reason = "Kein Preis erkennbar"

        # Gate 2: Zimmer
        if not reason:
            rooms_ok = (l.rooms and l.rooms >= MIN_ROOMS) or (l.bedrooms and l.bedrooms >= MIN_BEDROOMS)
            if l.rooms and l.rooms < MIN_ROOMS and (not l.bedrooms or l.bedrooms < MIN_BEDROOMS):
                reason = f"Zu wenig Zimmer: {l.rooms} Zi / {l.bedrooms or '?'} Schlafzi"

        # Gate 3: Erdgeschoss
        if not reason and l.is_ground_floor:
            reason = "Erdgeschoss (RDC)"

        # Gate 4: Riad
        if not reason and l.is_riad:
            reason = "Riad-Stil"

        # Gate 5: Melkia
        if not reason and l.ownership_type == "Melkia":
            reason = "Melkia (unklare Eigentumslage)"

        # Gate 6: No-Go Keywords
        if not reason:
            full = (l.title + " " + l.description).lower()
            for kw in NO_GO_KEYWORDS:
                if kw in full and not reason:
                    reason = f"No-Go Keyword: {kw}"

        if reason:
            rejected.append({"title": l.title[:60], "url": l.url, "reason": reason})
        else:
            passed.append(l)

    return passed, rejected


# ═══════════════════════════════════════════════════════════
# DEDUP
# ═══════════════════════════════════════════════════════════

def deduplicate(listings):
    """Entfernt Duplikate basierend auf URL und Titel+Preis."""
    seen = set()
    unique = []
    for l in listings:
        # Key: URL oder Kombination aus Titel+Preis
        key = l.url if l.url else f"{l.title}_{l.price_mad}"
        if key not in seen:
            seen.add(key)
            unique.append(l)
    return unique


# ═══════════════════════════════════════════════════════════
# OUTPUT
# ═══════════════════════════════════════════════════════════

def to_json(listings, rejected=None):
    """Konvertiert zu sauberem JSON."""
    data = {
        "meta": {
            "scraped_at": datetime.now().isoformat(),
            "total_found": len(listings) + (len(rejected) if rejected else 0),
            "passed_gates": len(listings),
            "rejected": len(rejected) if rejected else 0,
            "budget_range_mad": f"{BUDGET_MIN_MAD:,}–{BUDGET_MAX_MAD:,}",
            "min_rooms": MIN_ROOMS,
        },
        "listings": [asdict(l) for l in listings],
    }
    if rejected:
        data["rejected_log"] = rejected
    return data


def print_summary(passed, rejected):
    """Gibt eine übersichtliche Zusammenfassung aus."""
    print(f"\n{'='*60}")
    print(f"📊 ERGEBNIS")
    print(f"{'='*60}")
    print(f"  ✅ Qualifiziert:  {len(passed)}")
    print(f"  ⛔ Abgelehnt:     {len(rejected)}")
    print(f"  {'─'*40}")

    if passed:
        print(f"\n  🏡 QUALIFIZIERTE INSERATE:")
        for i, l in enumerate(sorted(passed, key=lambda x: x.price_mad or 0), 1):
            price = f"{l.price_mad:>12,} MAD" if l.price_mad else "   unbekannt"
            area = f"{l.area_sqm}m²" if l.area_sqm else "?m²"
            rooms = f"{l.rooms}Zi" if l.rooms else "?Zi"
            nb = l.neighborhood[:20] if l.neighborhood else "?"
            amenities = []
            if l.has_terrace: amenities.append("☀")
            if l.has_pool: amenities.append("🏊")
            if l.has_parking: amenities.append("🅿")
            if l.has_elevator: amenities.append("🛗")
            am_str = " ".join(amenities) if amenities else ""
            print(f"  {i:>3}. {price} | {area:>6} | {rooms:>3} | {nb:<20} | {am_str}")
            print(f"       {l.title[:65]}")
            print(f"       {l.url}")
            print()

    if rejected:
        print(f"\n  ⛔ ABGELEHNT (Top-Gründe):")
        from collections import Counter
        reasons = Counter(r["reason"].split(":")[0] for r in rejected)
        for reason, count in reasons.most_common(10):
            print(f"      {count:>3}× {reason}")


# ═══════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Marrakech Immobilien-Scraper")
    parser.add_argument("--portal", choices=["avito", "mubawab", "sarouty", "all"], default="all")
    parser.add_argument("--output", "-o", default="marrakech_leads.json")
    parser.add_argument("--pages", type=int, default=MAX_PAGES)
    parser.add_argument("--raw", action="store_true", help="Ohne Filter (alle Ergebnisse)")
    args = parser.parse_args()

    print(f"""
╔══════════════════════════════════════════════════╗
║     🏡  MARRAKECH IMMOBILIEN-SCRAPER  🏡        ║
║                                                  ║
║  Budget:  {BUDGET_MIN_MAD/1e6:.1f}M – {BUDGET_MAX_MAD/1e6:.1f}M MAD              ║
║  Zimmer:  ≥{MIN_ROOMS}                                    ║
║  Portale: {args.portal.upper():<20}                 ║
║  Seiten:  {args.pages} pro Portal                        ║
╚══════════════════════════════════════════════════╝
    """)

    session = requests.Session()
    session.headers.update(HEADERS)
    all_listings = []

    scrapers = {
        "avito": scrape_avito,
        "mubawab": scrape_mubawab,
        "sarouty": scrape_sarouty,
    }

    portals = scrapers.keys() if args.portal == "all" else [args.portal]

    for portal in portals:
        try:
            results = scrapers[portal](session, args.pages)
            all_listings.extend(results)
        except Exception as e:
            print(f"\n  ❌ Fehler bei {portal}: {e}")
            import traceback
            traceback.print_exc()

    # Dedup
    before_dedup = len(all_listings)
    all_listings = deduplicate(all_listings)
    print(f"\n🔄 Deduplizierung: {before_dedup} → {len(all_listings)}")

    # Filter
    if args.raw:
        passed, rejected = all_listings, []
        print("⚠  Raw-Modus: Keine Filter angewendet")
    else:
        passed, rejected = apply_gates(all_listings)

    # Summary
    print_summary(passed, rejected)

    # Save
    output = to_json(passed, rejected)
    out_path = Path(args.output)
    out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2))
    print(f"\n💾 Gespeichert: {out_path.absolute()}")
    print(f"   → {len(passed)} qualifizierte Inserate")
    print(f"   → Datei kann direkt ins Silbertablett geladen werden\n")


if __name__ == "__main__":
    main()
