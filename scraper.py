#!/usr/bin/env python3
"""
MARRAKECH SCRAPER v7 — PLAYWRIGHT
Fixes basierend auf echtem HTML:
- Avito: URLs enthalten Stadtteil nicht 'marrakech', Links enden auf .htm
- Mubawab: Dedup-Fix, korrekte Selektoren
- Sarouty: Kartenextraktion gefixt
"""

import json, time, re, argparse, hashlib
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Optional
from pathlib import Path

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("pip install playwright && playwright install chromium")
    exit(1)

BUDGET_MIN = 1_000_000
BUDGET_MAX = 2_300_000
MAX_PAGES = 3
NEIGHBORHOODS = [
    "targa","palmeraie","agdal","tamansourt","massira","m'hamid","mhamid",
    "izdihar","amerchich","tassoultant","route de l'ourika","route ourika",
    "route de fes","route de casablanca","route casablanca","sidi ghanem",
    "saada","semlalia","camp el ghoul","hay mohammadi","marjane","annakhil",
]
NO_GO = ["riad","riyad","rez-de-chaussee","rez de chaussee"]
IMG_BAD = ["logo","icon","avatar","placeholder","pixel","spacer","banner","ad-",
    "badge","button","arrow","sprite","flag","emoji","social","facebook","twitter",
    "instagram","youtube","google-play","app-store","afdal","widget","star","rating",
    "loader","spinner","1x1","blank","transparent","favicon","marker","pin","svg"]

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
    contact_email: str = ""
    contact_whatsapp: str = ""
    image: str = ""
    images: list = field(default_factory=list)
    scraped_at: str = ""

    def finalize(self):
        if not self.id and self.url:
            self.id = hashlib.md5(self.url.encode()).hexdigest()[:12]
        elif not self.id:
            self.id = hashlib.md5(f"{self.title}_{self.price_mad}".encode()).hexdigest()[:12]
        self.scraped_at = self.scraped_at or datetime.now().isoformat()
        if self.price_mad and self.area_sqm and self.area_sqm > 0:
            self.price_per_sqm_mad = int(self.price_mad / self.area_sqm)
        if self.price_mad and not self.price_eur:
            self.price_eur = int(self.price_mad / 10.8)
        if self.images and not self.image:
            self.image = self.images[0]
        if self.contact_phone and not self.contact_whatsapp:
            p = re.sub(r'[\s.\-()]', '', self.contact_phone)
            if p.startswith('0'): p = '+212' + p[1:]
            if not p.startswith('+'): p = '+212' + p
            self.contact_whatsapp = p
        return self

def parse_price(text):
    if not text: return None
    text = re.sub(r'[MADHsDh€\s\.\,\xa0]', '', text)
    m = re.search(r'(\d{6,8})', text)
    return int(m.group(1)) if m and 100_000 <= int(m.group(1)) <= 50_000_000 else None

def xnum(text, mn=0, mx=9999):
    if not text: return None
    m = re.search(r'(\d+)', str(text))
    return int(m.group(1)) if m and mn <= int(m.group(1)) <= mx else None

def good_img(src):
    if not src or len(src) < 20: return False
    sl = src.lower()
    if any(b in sl for b in IMG_BAD): return False
    if sl.startswith("data:"): return False
    if ".svg" in sl or ".gif" in sl: return False
    return True

def detect(text):
    t = text.lower()
    r = {}
    m = re.search(r'(\d{2,4})\s*m[²2\s]', t)
    if m and 20 < int(m.group(1)) < 1000: r['area_sqm'] = int(m.group(1))
    m = re.search(r'(\d)\s*(?:pièce|piece|pcs)', t)
    if m: r['rooms'] = int(m.group(1))
    m = re.search(r'(\d)\s*(?:chambre|chbr|ch\.)', t)
    if m: r['bedrooms'] = int(m.group(1))
    m = re.search(r'(\d)\s*(?:salle|sdb)', t)
    if m: r['bathrooms'] = int(m.group(1))
    if "rez-de-chauss" in t or "rdc" in t or "étage 0" in t:
        r['is_ground_floor'] = True; r['floor'] = "RDC"
    else:
        m = re.search(r'(?:étage|etage)\s*(\d)', t)
        if m: r['floor'] = m.group(1) + ". Etage"
    r['has_terrace'] = any(w in t for w in ["terrasse","balcon","rooftop"])
    r['has_pool'] = any(w in t for w in ["piscine","pool"])
    r['has_parking'] = any(w in t for w in ["parking","garage","sous-sol"])
    r['has_elevator'] = "ascenseur" in t
    r['is_new_build'] = any(w in t for w in ["neuf","jamais habit","livraison 202"])
    r['is_riad'] = any(w in t for w in ["riad","riyad"])
    if "titre foncier" in t or " tf " in t: r['ownership_type'] = "Titre Foncier"
    elif "melkia" in t or "melk " in t: r['ownership_type'] = "Melkia"
    if any(w in t for w in ["neuf","jamais habit"]): r['condition'] = "Neu"
    elif "rénov" in t or "renov" in t: r['condition'] = "Renoviert"
    for nb in NEIGHBORHOODS:
        if nb in t: r['neighborhood'] = nb.title(); break
    phones = re.findall(r'(?:\+212|0)[5-7][\s.-]?\d{2}[\s.-]?\d{2}[\s.-]?\d{2}[\s.-]?\d{2}', t)
    if phones: r['contact_phone'] = phones[0].strip()
    emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', t)
    good = [e for e in emails if not any(x in e.lower() for x in ['noreply','admin','support','info@sarouty','info@mubawab'])]
    if good: r['contact_email'] = good[0]
    return r

def apply_d(l, d):
    for k, v in d.items():
        cur = getattr(l, k, None)
        if cur is None or cur == "" or cur == "Unbekannt":
            setattr(l, k, v)

def click_phone(page):
    """Klickt Telefon-Button und extrahiert Nummer."""
    for sel in ["button:has-text('Afficher')","button:has-text('numéro')","button:has-text('Appeler')","button:has-text('téléphone')","[class*='phone'] button","button[class*='phone']","a:has-text('Afficher le numéro')"]:
        try:
            btn = page.query_selector(sel)
            if btn and btn.is_visible():
                btn.click()
                page.wait_for_timeout(2000)
                # Check tel: links
                for el in page.query_selector_all("a[href^='tel:']"):
                    p = (el.get_attribute("href") or "").replace("tel:","").strip()
                    if len(p) >= 8: return p
                # Check body text
                body = page.inner_text("body")
                phones = re.findall(r'(?:\+212|0)[5-7][\s.-]?\d{2}[\s.-]?\d{2}[\s.-]?\d{2}[\s.-]?\d{2}', body)
                if phones: return phones[0].strip()
        except: pass
    return ""

def get_contacts(page):
    r = {}
    try:
        for el in page.query_selector_all("a[href^='tel:']"):
            p = (el.get_attribute("href") or "").replace("tel:","").strip()
            if len(p) >= 8 and not r.get('contact_phone'): r['contact_phone'] = p
    except: pass
    try:
        for el in page.query_selector_all("a[href^='mailto:']"):
            em = (el.get_attribute("href") or "").replace("mailto:","").split("?")[0].strip()
            if "@" in em and not any(x in em.lower() for x in ['noreply','admin','support']): r['contact_email'] = em
    except: pass
    try:
        for el in page.query_selector_all("a[href*='wa.me'], a[href*='whatsapp']"):
            m = re.search(r'(?:wa\.me/|phone=)(\+?\d+)', el.get_attribute("href") or "")
            if m: r['contact_whatsapp'] = m.group(1); break
    except: pass
    return r

def get_images(page):
    imgs = []
    try:
        og = page.query_selector("meta[property='og:image']")
        if og:
            src = og.get_attribute("content") or ""
            if good_img(src): imgs.append(src)
    except: pass
    try:
        for el in page.query_selector_all("img"):
            src = el.get_attribute("data-src") or el.get_attribute("src") or ""
            if src.startswith("//"): src = "https:" + src
            if not good_img(src): continue
            try:
                box = el.bounding_box()
                if box and (box["width"] < 50 or box["height"] < 50): continue
            except: pass
            if src not in imgs: imgs.append(src)
    except: pass
    return imgs[:5]

# ═══════════════════════════════════════
# AVITO — URLs sind /fr/{stadtteil}/appartements/{titel}_{id}.htm
# ═══════════════════════════════════════
def scrape_avito(page, max_pages):
    print(f"\n  AVITO")
    listings = []
    for pg in range(1, max_pages + 1):
        url = f"https://www.avito.ma/fr/marrakech/appartements-%C3%A0_vendre?o={pg}"
        try:
            page.goto(url, timeout=30000, wait_until="domcontentloaded")
            page.wait_for_timeout(4000)  # Avito braucht JS-Rendering
        except:
            print(f"    Seite {pg}: Timeout"); continue

        # ALLE Links die auf .htm enden und /appartements/ enthalten
        links = page.query_selector_all("a[href*='/appartements/'][href$='.htm']")
        count = 0
        existing = {l.url for l in listings}

        for link in links:
            try:
                href = link.get_attribute("href") or ""
                if not href.startswith("http"): href = "https://www.avito.ma" + href
                if href in existing: continue
                if "/appartements/" not in href: continue

                l = Listing(source="Avito", url=href)

                # Titel + Info aus dem Link-Text
                text = link.inner_text().strip()
                if len(text) < 5: continue

                # Preis extrahieren
                l.price_mad = parse_price(text)

                # Bild: content.avito.ma
                img = link.query_selector("img")
                if img:
                    src = img.get_attribute("src") or img.get_attribute("data-src") or ""
                    if "content.avito.ma" in src or good_img(src):
                        l.image = src

                # Text-Analyse fuer Zimmer, Flaeche etc.
                apply_d(l, detect(text))

                # Titel: erste Zeile oder alles
                lines = [x.strip() for x in text.split('\n') if x.strip()]
                # Suche nach der Titelzeile (nicht Preis, nicht Metadaten)
                for line in lines:
                    if len(line) > 10 and not re.match(r'^[\d\s.,]+$', line) and "DH" not in line and "chambre" not in line.lower():
                        l.title = line[:150]
                        break
                if not l.title and lines:
                    l.title = lines[0][:150]

                # Neighborhood aus URL extrahieren
                m = re.search(r'avito\.ma/fr/([^/]+)/appartements/', href)
                if m and not l.neighborhood:
                    nb = m.group(1).replace('_', ' ').replace('-', ' ').title()
                    l.neighborhood = nb

                listings.append(l)
                existing.add(href)
                count += 1
            except: pass

        print(f"    Seite {pg}: {count} neu ({len(listings)} gesamt)")
        page.wait_for_timeout(2000)

    print(f"    {len(listings)} Listings, lade Details...")

    for i, l in enumerate(listings):
        if not l.url: continue
        if i % 10 == 0: print(f"    Detail {i+1}/{len(listings)}...")
        try:
            page.goto(l.url, timeout=20000, wait_until="domcontentloaded")
            page.wait_for_timeout(2500)
            l.url = page.url  # echte URL nach Redirect

            # Telefon-Button klicken
            phone = click_phone(page)
            if phone: l.contact_phone = phone

            # Kontakte
            contacts = get_contacts(page)
            for k, v in contacts.items():
                if v and not getattr(l, k, ""): setattr(l, k, v)

            # Bilder
            if not l.images:
                l.images = get_images(page)

            # Preis aus Detail-Seite
            if not l.price_mad:
                for sel in ["[class*='price']", "[data-testid*='price']"]:
                    el = page.query_selector(sel)
                    if el:
                        l.price_mad = parse_price(el.inner_text())
                        if l.price_mad: break

            # Titel von der Detail-Seite (besser)
            for sel in ["h1", "[class*='title'] h1", "[data-testid='title']"]:
                el = page.query_selector(sel)
                if el:
                    t = el.inner_text().strip()
                    if len(t) > 5: l.title = t[:200]; break

            # Beschreibung
            for sel in ["[class*='description']", "[class*='body']", "[data-testid='description']"]:
                el = page.query_selector(sel)
                if el:
                    l.description = el.inner_text().strip()[:500]
                    break

            # Aus Script-Tags
            try:
                content = page.content()
                for m in re.findall(r'\{[^{}]*"(?:surface|rooms|bedrooms)"[^{}]*\}', content):
                    try:
                        d = json.loads(m)
                        l.area_sqm = l.area_sqm or xnum(str(d.get("surface","")), 20, 1000)
                        l.rooms = l.rooms or xnum(str(d.get("rooms","")), 1, 20)
                        l.bedrooms = l.bedrooms or xnum(str(d.get("bedrooms","")), 1, 10)
                    except: pass
            except: pass

            # Body text analyse
            try:
                body = page.inner_text("body")
                apply_d(l, detect(body))
            except: pass

        except Exception as ex:
            if i < 3: print(f"    Fehler: {ex}")
        l.finalize()
        page.wait_for_timeout(1500)

    print(f"    Avito: {len(listings)}")
    return listings


# ═══════════════════════════════════════
# MUBAWAB
# ═══════════════════════════════════════
def scrape_mubawab(page, max_pages):
    print(f"\n  MUBAWAB")
    listings = []

    for pg in range(1, max_pages + 1):
        url = f"https://www.mubawab.ma/fr/st/marrakech/appartements-a-vendre:p:{pg}"
        try:
            page.goto(url, timeout=30000, wait_until="domcontentloaded")
            page.wait_for_timeout(4000)
        except:
            print(f"    Seite {pg}: Timeout"); continue

        # Mubawab: Links die zu Detail-Seiten fuehren
        # Detail-URLs: /fr/marrakech/.../detail-XXX.htm oder aehnlich
        all_links = page.query_selector_all("a[href]")
        existing = {l.url for l in listings}
        count = 0

        for link in all_links:
            try:
                href = link.get_attribute("href") or ""
                if not href.startswith("http"): href = "https://www.mubawab.ma" + href

                # Mubawab Detail-URLs enthalten eine Nummer am Ende
                if "mubawab.ma" not in href: continue
                if not re.search(r'/\d+\.htm|detail-\d+|/\d+$', href): continue
                if href in existing: continue
                if "/st/" in href: continue  # Listing-Seiten, nicht Details

                text = link.inner_text().strip()
                if len(text) < 5 or len(text) > 500: continue

                l = Listing(source="Mubawab", url=href)
                l.price_mad = parse_price(text)

                # Bild
                img = link.query_selector("img")
                if img:
                    src = img.get_attribute("data-src") or img.get_attribute("src") or ""
                    if not src.startswith("http"): src = "https://www.mubawab.ma" + src
                    if good_img(src): l.image = src

                # Titel: erste sinnvolle Zeile
                lines = [x.strip() for x in text.split('\n') if x.strip() and len(x.strip()) > 3]
                for line in lines:
                    if "DH" not in line and not re.match(r'^\d+\s*(chambre|sdb|m²)', line.lower()):
                        l.title = line[:200]; break
                if not l.title and lines: l.title = lines[0][:200]

                apply_d(l, detect(text))

                listings.append(l)
                existing.add(href)
                count += 1
            except: pass

        print(f"    Seite {pg}: {count} neu ({len(listings)} gesamt)")
        page.wait_for_timeout(2000)

    print(f"    {len(listings)} Listings, lade Details...")

    for i, l in enumerate(listings):
        if not l.url: continue
        if i % 10 == 0: print(f"    Detail {i+1}/{len(listings)}...")
        try:
            page.goto(l.url, timeout=20000, wait_until="domcontentloaded")
            page.wait_for_timeout(2500)
            l.url = page.url

            phone = click_phone(page)
            if phone: l.contact_phone = phone

            contacts = get_contacts(page)
            for k, v in contacts.items():
                if v and not getattr(l, k, ""): setattr(l, k, v)

            if not l.images: l.images = get_images(page)

            if not l.price_mad:
                el = page.query_selector("[class*='price'], [class*='prix']")
                if el: l.price_mad = parse_price(el.inner_text())

            # Titel
            el = page.query_selector("h1")
            if el:
                t = el.inner_text().strip()
                if len(t) > 5: l.title = t[:200]

            for sel in ["[class*='description']", "[class*='blockParagraph']"]:
                el = page.query_selector(sel)
                if el: l.description = el.inner_text().strip()[:500]; break

            # Agent
            for sel in ["[class*='agent']", "[class*='seller']"]:
                el = page.query_selector(sel)
                if el and not l.contact_name:
                    name = el.inner_text().strip()
                    if 2 < len(name) < 50 and '@' not in name: l.contact_name = name

            body = page.inner_text("body")
            apply_d(l, detect(body))

        except Exception as ex:
            if i < 3: print(f"    Fehler: {ex}")
        l.finalize()
        page.wait_for_timeout(1500)

    print(f"    Mubawab: {len(listings)}")
    return listings


# ═══════════════════════════════════════
# SAROUTY
# ═══════════════════════════════════════
def scrape_sarouty(page, max_pages):
    print(f"\n  SAROUTY")
    listings = []

    for pg in range(1, max_pages + 1):
        url = "https://www.sarouty.ma/acheter/marrakech/appartements-a-vendre/" if pg == 1 else f"https://www.sarouty.ma/acheter/marrakech/appartements-a-vendre/?page={pg}"
        try:
            page.goto(url, timeout=30000, wait_until="domcontentloaded")
            page.wait_for_timeout(4000)
        except:
            print(f"    Seite {pg}: Timeout"); continue

        all_links = page.query_selector_all("a[href]")
        existing = {l.url for l in listings}
        count = 0

        for link in all_links:
            try:
                href = link.get_attribute("href") or ""
                if not href.startswith("http"): href = "https://www.sarouty.ma" + href

                if "sarouty.ma" not in href: continue
                # Sarouty detail URLs: enthalten eine property-ID oder enden auf .html mit Zahl
                if not re.search(r'/\d+\.html|/property/|/detail/', href.lower()): continue
                if href in existing: continue
                if "/acheter/marrakech/appartements-a-vendre" in href: continue  # listing page

                text = link.inner_text().strip()
                if len(text) < 5 or len(text) > 500: continue

                l = Listing(source="Sarouty", url=href)
                l.price_mad = parse_price(text)

                img = link.query_selector("img")
                if img:
                    src = img.get_attribute("src") or img.get_attribute("data-src") or ""
                    if not src.startswith("http"): src = "https://www.sarouty.ma" + src
                    if good_img(src): l.image = src

                lines = [x.strip() for x in text.split('\n') if x.strip() and len(x.strip()) > 3]
                for line in lines:
                    if "MAD" not in line and "DH" not in line:
                        l.title = line[:200]; break
                if not l.title and lines: l.title = lines[0][:200]

                apply_d(l, detect(text))
                listings.append(l)
                existing.add(href)
                count += 1
            except: pass

        print(f"    Seite {pg}: {count} neu ({len(listings)} gesamt)")
        if count == 0: break
        page.wait_for_timeout(2000)

    print(f"    {len(listings)} Listings, lade Details...")

    for i, l in enumerate(listings):
        if not l.url: continue
        if i % 10 == 0: print(f"    Detail {i+1}/{len(listings)}...")
        try:
            page.goto(l.url, timeout=20000, wait_until="domcontentloaded")
            page.wait_for_timeout(2500)
            l.url = page.url

            phone = click_phone(page)
            if phone: l.contact_phone = phone
            contacts = get_contacts(page)
            for k, v in contacts.items():
                if v and not getattr(l, k, ""): setattr(l, k, v)
            if not l.images: l.images = get_images(page)
            if not l.price_mad:
                el = page.query_selector("[class*='price']")
                if el: l.price_mad = parse_price(el.inner_text())
            el = page.query_selector("h1")
            if el:
                t = el.inner_text().strip()
                if len(t) > 5: l.title = t[:200]
            body = page.inner_text("body")
            apply_d(l, detect(body))
        except: pass
        l.finalize()
        page.wait_for_timeout(1500)

    print(f"    Sarouty: {len(listings)}")
    return listings


# ═══════════════════════════════════════
# FILTER
# ═══════════════════════════════════════
def apply_gates(listings):
    passed, rejected = [], []
    for l in listings:
        reason = None
        if l.price_mad is not None:
            if l.price_mad < BUDGET_MIN: reason = f"Preis: {l.price_mad:,} < Min"
            elif l.price_mad > BUDGET_MAX: reason = f"Preis: {l.price_mad:,} > Max"
        else: reason = "Kein Preis"
        if not reason and l.rooms and l.rooms < 3 and (not l.bedrooms or l.bedrooms < 2):
            reason = "Zu wenig Zimmer"
        if not reason and l.is_ground_floor: reason = "Erdgeschoss"
        if not reason and l.is_riad: reason = "Riad"
        if not reason and l.ownership_type == "Melkia": reason = "Melkia"
        if not reason:
            full = (l.title + " " + l.description).lower()
            for kw in NO_GO:
                if kw in full: reason = f"No-Go: {kw}"; break
        if reason: rejected.append({"title":l.title[:60],"url":l.url,"reason":reason})
        else: passed.append(l)
    return passed, rejected


# ═══════════════════════════════════════
# MAIN
# ═══════════════════════════════════════
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--portal", choices=["avito","mubawab","sarouty","all"], default="all")
    ap.add_argument("--output", "-o", default="data/latest_raw.json")
    ap.add_argument("--pages", type=int, default=MAX_PAGES)
    ap.add_argument("--raw", action="store_true")
    args = ap.parse_args()

    print(f"\n  SCRAPER v7 PLAYWRIGHT | {args.portal.upper()} | {args.pages} Seiten\n")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            locale="fr-FR", viewport={"width":1280,"height":800},
        )
        page = ctx.new_page()
        # Block images/CSS to speed up listing pages (not detail pages)
        all_l = []
        scrapers = {"avito":scrape_avito,"mubawab":scrape_mubawab,"sarouty":scrape_sarouty}
        for p in (scrapers.keys() if args.portal == "all" else [args.portal]):
            try: all_l.extend(scrapers[p](page, args.pages))
            except Exception as ex:
                print(f"  FEHLER {p}: {ex}")
                import traceback; traceback.print_exc()
        browser.close()

    # Dedup
    seen = set()
    unique = []
    for l in all_l:
        k = l.url or f"{l.title}_{l.price_mad}"
        if k and k not in seen: seen.add(k); unique.append(l)

    passed, rejected = (unique, []) if args.raw else apply_gates(unique)

    wu = len([l for l in passed if l.url])
    wi = len([l for l in passed if l.image])
    wp = len([l for l in passed if l.contact_phone])
    we = len([l for l in passed if l.contact_email])
    ww = len([l for l in passed if l.contact_whatsapp])

    print(f"\n  ERGEBNIS: {len(passed)} qualifiziert, {len(rejected)} abgelehnt")
    print(f"  URLs: {wu} | Bilder: {wi} | Tel: {wp} | Email: {we} | WA: {ww}")

    for i, l in enumerate(passed[:5], 1):
        p = f"{l.price_mad:,}" if l.price_mad else "?"
        ph = f" Tel:{l.contact_phone}" if l.contact_phone else ""
        im = " [IMG]" if l.image else ""
        print(f"  {i}. {p} MAD | {l.area_sqm or '?'}m2 | {l.rooms or '?'}Zi | {l.neighborhood or '?'}{ph}{im}")
        print(f"     {l.title[:60]}")
        print(f"     {l.url[:80]}")

    output = {
        "meta":{"scraped_at":datetime.now().isoformat(),"total_found":len(passed)+len(rejected),"passed_gates":len(passed),"rejected":len(rejected)},
        "listings":[asdict(l) for l in passed],
        "rejected_log":rejected,
    }
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(output, ensure_ascii=False, indent=2))
    print(f"  Gespeichert: {out}\n")

if __name__ == "__main__":
    main()
