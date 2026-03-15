#!/usr/bin/env python3
"""
MARRAKECH IMMOBILIEN-SCRAPER v6 — PLAYWRIGHT
=============================================
Echter Browser: klickt Buttons, rendert JavaScript, extrahiert alles.
pip install playwright beautifulsoup4
playwright install chromium
"""

import json, time, re, argparse, hashlib
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Optional
from pathlib import Path
from bs4 import BeautifulSoup

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout
except ImportError:
    print("pip install playwright && playwright install chromium")
    exit(1)

# ═══════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════
BUDGET_MIN_MAD = 1_000_000
BUDGET_MAX_MAD = 2_300_000
MAX_PAGES = 3

PREFERRED_NEIGHBORHOODS = [
    "targa","palmeraie","agdal","tamansourt","massira","m'hamid","mhamid",
    "izdihar","amerchich","tassoultant","route de l'ourika","route ourika",
    "route de fes","route fes","route de casablanca","route casablanca",
    "sidi ghanem","route d'amizmiz","saada","semlalia","camp el ghoul",
    "hay mohammadi","marjane","annakhil","tamesna",
]
NO_GO_KEYWORDS = ["riad","riyad","rez-de-chaussee","rez de chaussee"]
MELKIA_KEYWORDS = ["melkia","melk"]
TITRE_FONCIER_KEYWORDS = ["titre foncier","tf","titre"]

IMG_BLACKLIST = [
    "logo","icon","avatar","placeholder","pixel","spacer","banner","ad-","ads/",
    "badge","button","arrow","sprite","flag","emoji","social","facebook","twitter",
    "instagram","youtube","google-play","app-store","appstore","googleplay",
    "afdal","widget","star","rating","loader","spinner","check","close","menu",
    "search","share","print","mail-icon","phone-icon","whatsapp-icon",
    "1x1","2x2","blank","transparent","favicon","marker","pin"
]

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
        elif not self.id and self.title:
            self.id = hashlib.md5(f"{self.title}_{self.price_mad}".encode()).hexdigest()[:12]
        if not self.scraped_at:
            self.scraped_at = datetime.now().isoformat()
        if self.price_mad and self.area_sqm and self.area_sqm > 0:
            self.price_per_sqm_mad = int(self.price_mad / self.area_sqm)
        if self.price_mad and not self.price_eur:
            self.price_eur = int(self.price_mad / 10.8)
        if self.images and not self.image:
            self.image = self.images[0]
        if self.contact_phone and not self.contact_whatsapp:
            phone = re.sub(r'[\s\.\-\(\)]', '', self.contact_phone)
            if phone.startswith('0'): phone = '+212' + phone[1:]
            if not phone.startswith('+'): phone = '+212' + phone
            self.contact_whatsapp = phone
        return self

# ═══════════════════════════════════════
# UTILS
# ═══════════════════════════════════════
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

def is_real_photo(src):
    if not src or len(src) < 30: return False
    sl = src.lower()
    for b in IMG_BLACKLIST:
        if b in sl: return False
    if not any(x in sl for x in [".jpg",".jpeg",".png",".webp"]): return False
    if sl.startswith("data:"): return False
    return True

def detect_from_text(text):
    t = text.lower()
    r = {}
    m = re.search(r'(\d{2,4})\s*m[^a-z]', t)
    if m:
        v = int(m.group(1))
        if 20 < v < 1000: r['area_sqm'] = v
    m = re.search(r'(\d)\s*(?:pi[eè]ces?|pcs?)\b', t)
    if m: r['rooms'] = int(m.group(1))
    m = re.search(r'(\d)\s*(?:chambres?|chbr?|ch\.)\b', t)
    if m: r['bedrooms'] = int(m.group(1))
    m = re.search(r'(\d)\s*(?:salles?\s*de\s*bain|sdb)', t)
    if m: r['bathrooms'] = int(m.group(1))
    if any(w in t for w in ["rez-de-chauss", "rdc"]):
        r['floor'] = "RDC"; r['is_ground_floor'] = True
    else:
        m = re.search(r'(\d+)\s*(?:[eè]me|er|e)?\s*[eé]tage', t)
        if m: r['floor'] = m.group(1) + ". Etage"; r['is_ground_floor'] = False
    r['has_terrace'] = any(w in t for w in ["terrasse","balcon","rooftop","toit terrasse"])
    r['has_pool'] = any(w in t for w in ["piscine","pool"])
    r['has_parking'] = any(w in t for w in ["parking","garage","sous-sol","stationnement"])
    r['has_elevator'] = any(w in t for w in ["ascenseur"])
    r['is_new_build'] = any(w in t for w in ["neuf","nouvelle construction","livraison 202","jamais habit"])
    if any(w in t for w in TITRE_FONCIER_KEYWORDS): r['ownership_type'] = "Titre Foncier"
    elif any(w in t for w in MELKIA_KEYWORDS): r['ownership_type'] = "Melkia"
    if any(w in t for w in ["neuf","jamais habit"]): r['condition'] = "Neu"
    elif any(w in t for w in ["rnov","renov","refait"]): r['condition'] = "Renoviert"
    elif "bon tat" in t or "bon état" in t: r['condition'] = "Gut"
    r['is_riad'] = any(w in t for w in ["riad","riyad"])
    for nb in PREFERRED_NEIGHBORHOODS:
        if nb in t: r['neighborhood'] = nb.title(); break
    phones = re.findall(r'(?:\+212|00212|0)[\s.-]?[5-7][\s.-]?\d{2}[\s.-]?\d{2}[\s.-]?\d{2}[\s.-]?\d{2}', t)
    if phones: r['contact_phone'] = phones[0].strip()
    emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', t)
    valid = [e for e in emails if not any(x in e.lower() for x in ['noreply','no-reply','admin@','support@','info@sarouty','info@mubawab','contact@sarouty','contact@mubawab','example'])]
    if valid: r['contact_email'] = valid[0]
    return r

def apply_detected(listing, detected):
    for key, val in detected.items():
        current = getattr(listing, key, None)
        if current is None or current == "" or current == "Unbekannt":
            setattr(listing, key, val)

def extract_contacts_from_page(page):
    """Extrahiert Kontakte aus der gerenderten Seite."""
    result = {}

    # Telefon aus tel: links
    try:
        tel_links = page.query_selector_all("a[href^='tel:']")
        for el in tel_links:
            href = el.get_attribute("href") or ""
            phone = href.replace("tel:", "").strip()
            if len(phone) >= 8 and not result.get('contact_phone'):
                result['contact_phone'] = phone
    except: pass

    # Email aus mailto: links
    try:
        mail_links = page.query_selector_all("a[href^='mailto:']")
        for el in mail_links:
            href = el.get_attribute("href") or ""
            email = href.replace("mailto:", "").split("?")[0].strip()
            if "@" in email and not any(x in email.lower() for x in ['noreply','admin','support','info@sarouty','info@mubawab']):
                result['contact_email'] = email
    except: pass

    # WhatsApp links
    try:
        wa_links = page.query_selector_all("a[href*='wa.me'], a[href*='whatsapp']")
        for el in wa_links:
            href = el.get_attribute("href") or ""
            m = re.search(r'(?:wa\.me/|phone=)(\+?\d+)', href)
            if m and not result.get('contact_whatsapp'):
                result['contact_whatsapp'] = m.group(1)
    except: pass

    # Agent/Kontakt-Name
    try:
        for sel in ["[class*='agent'] [class*='name']", "[class*='seller']", "[class*='contact-name']", "[class*='broker']"]:
            el = page.query_selector(sel)
            if el:
                name = el.inner_text().strip()
                if 2 < len(name) < 50 and not re.search(r'\d{5}', name) and '@' not in name:
                    result['contact_name'] = name
                    break
    except: pass

    return result

def extract_images_from_page(page):
    """Extrahiert nur echte Fotos aus gerendertem DOM."""
    imgs = []
    try:
        # OG image
        og = page.query_selector("meta[property='og:image']")
        if og:
            src = og.get_attribute("content") or ""
            if is_real_photo(src): imgs.append(src)

        # Alle img Elemente mit natürlicher Größe > 100px
        all_imgs = page.query_selector_all("img")
        for img in all_imgs:
            src = img.get_attribute("data-src") or img.get_attribute("data-lazy-src") or img.get_attribute("src") or ""
            if src.startswith("//"): src = "https:" + src
            if not is_real_photo(src): continue
            # Prüfe tatsächliche Bildgröße im DOM
            try:
                box = img.bounding_box()
                if box and (box["width"] < 50 or box["height"] < 50):
                    continue  # Zu klein = Icon
            except: pass
            if src not in imgs:
                imgs.append(src)
    except: pass
    return imgs[:5]

def click_phone_button(page):
    """Versucht den 'Afficher le numéro' Button zu klicken."""
    phone = ""
    selectors = [
        "button:has-text('Afficher')",
        "button:has-text('afficher')",
        "button:has-text('numéro')",
        "button:has-text('numero')",
        "button:has-text('Appeler')",
        "button:has-text('téléphone')",
        "button:has-text('telephone')",
        "[class*='phone'] button",
        "[class*='tel'] button",
        "a:has-text('Afficher le numéro')",
        "[data-phone-button]",
        "button[class*='phone']",
        "button[class*='Phone']",
    ]
    for sel in selectors:
        try:
            btn = page.query_selector(sel)
            if btn and btn.is_visible():
                btn.click()
                page.wait_for_timeout(2000)  # Warte auf Antwort

                # Suche nach der Telefonnummer die jetzt sichtbar ist
                body = page.inner_text("body")
                phones = re.findall(r'(?:\+212|0)[\s.-]?[5-7][\s.-]?\d{2}[\s.-]?\d{2}[\s.-]?\d{2}[\s.-]?\d{2}', body)
                if phones:
                    phone = phones[0].strip()
                    break

                # Auch in tel: links suchen
                tel_links = page.query_selector_all("a[href^='tel:']")
                for el in tel_links:
                    href = el.get_attribute("href") or ""
                    p = href.replace("tel:", "").strip()
                    if len(p) >= 8:
                        phone = p
                        break
                if phone: break
        except: pass
    return phone


# ═══════════════════════════════════════
# MUBAWAB
# ═══════════════════════════════════════
def scrape_mubawab(page, max_pages=MAX_PAGES):
    print(f"\n  MUBAWAB")
    listings = []
    for pg in range(1, max_pages + 1):
        url = f"https://www.mubawab.ma/fr/st/marrakech/appartements-a-vendre:p:{pg}"
        print(f"    Seite {pg}...")
        try:
            page.goto(url, timeout=30000, wait_until="domcontentloaded")
            page.wait_for_timeout(3000)
        except:
            print(f"    Timeout")
            continue

        # Cards extrahieren
        cards = page.query_selector_all("li.listingBox, div.listingBox, [class*='adItem']")
        if not cards:
            cards = page.query_selector_all("a[href*='/fr/marrakech/']")
        print(f"    {len(cards)} Karten")

        for card in cards:
            try:
                l = Listing(source="Mubawab")

                # URL
                link = card.query_selector("a[href]") if card.evaluate("el => el.tagName") != "A" else card
                if link:
                    href = link.get_attribute("href") or ""
                    if not href.startswith("http"): href = "https://www.mubawab.ma" + href
                    if "/fr/marrakech/" in href or "/fr/st/" in href:
                        l.url = href

                # Titel
                for sel in ["h2","h3","[class*='title']"]:
                    el = card.query_selector(sel)
                    if el:
                        l.title = el.inner_text().strip()
                        if l.title: break
                if not l.title:
                    l.title = card.inner_text().strip()[:150]

                # Preis
                price_el = card.query_selector("[class*='price'], [class*='prix']")
                if price_el:
                    l.price_mad = parse_price(price_el.inner_text())

                # Thumbnail
                img = card.query_selector("img[src], img[data-src]")
                if img:
                    src = img.get_attribute("data-src") or img.get_attribute("src") or ""
                    if not src.startswith("http") and src.startswith("/"): src = "https://www.mubawab.ma" + src
                    if is_real_photo(src): l.image = src

                # Location
                loc = card.query_selector("[class*='location'], [class*='adresse']")
                if loc: l.neighborhood = loc.inner_text().strip()

                apply_detected(l, detect_from_text(card.inner_text()))

                if l.title and len(l.title) > 3 and l.url:
                    listings.append(l)
            except: pass

        if not cards: break
        page.wait_for_timeout(2000)

    # Dedup
    seen = set()
    unique = []
    for l in listings:
        key = l.url
        if key and key not in seen:
            seen.add(key)
            unique.append(l)
    listings = unique
    print(f"    {len(listings)} unique, lade Details...")

    # Details
    for i, l in enumerate(listings):
        if not l.url: continue
        if i % 10 == 0: print(f"    Detail {i+1}/{len(listings)}...")
        try:
            page.goto(l.url, timeout=20000, wait_until="domcontentloaded")
            page.wait_for_timeout(2000)

            # Aktuelle URL übernehmen (Redirects!)
            l.url = page.url

            # Telefon-Button klicken
            phone = click_phone_button(page)
            if phone: l.contact_phone = phone

            # Kontakte
            contacts = extract_contacts_from_page(page)
            for k, v in contacts.items():
                if v and not getattr(l, k, ""):
                    setattr(l, k, v)

            # Bilder
            if not l.images:
                l.images = extract_images_from_page(page)

            # Preis
            if not l.price_mad:
                price_el = page.query_selector("[class*='price'], [class*='prix']")
                if price_el: l.price_mad = parse_price(price_el.inner_text())

            # Beschreibung
            if not l.description:
                for sel in ["[class*='description']", "[class*='blockParagraph']", "div.detailDesc"]:
                    el = page.query_selector(sel)
                    if el:
                        l.description = el.inner_text().strip()
                        break

            # Text-Analyse
            body = page.inner_text("body")
            apply_detected(l, detect_from_text(body))

            # HTML-Elemente
            for el in page.query_selector_all("li, span"):
                try:
                    t = el.inner_text().strip()
                    if not t or len(t) > 80: continue
                    tl = t.lower()
                    if "pièce" in tl and not l.rooms: l.rooms = extract_number(t, 1, 20)
                    elif "chambre" in tl and not l.bedrooms: l.bedrooms = extract_number(t, 1, 10)
                    elif "sdb" in tl and not l.bathrooms: l.bathrooms = extract_number(t, 1, 10)
                    elif "m²" in tl and not l.area_sqm:
                        v = extract_number(t, 20, 1000)
                        if v: l.area_sqm = v
                except: pass

        except Exception as ex:
            print(f"    Fehler Detail: {ex}")

        l.finalize()
        page.wait_for_timeout(1500)

    print(f"    Mubawab: {len(listings)}")
    return listings


# ═══════════════════════════════════════
# AVITO
# ═══════════════════════════════════════
def scrape_avito(page, max_pages=MAX_PAGES):
    print(f"\n  AVITO")
    listings = []

    for pg in range(1, max_pages + 1):
        url = f"https://www.avito.ma/fr/marrakech/appartements-%C3%A0_vendre?o={pg}"
        print(f"    Seite {pg}...")
        try:
            page.goto(url, timeout=30000, wait_until="domcontentloaded")
            page.wait_for_timeout(3000)
        except:
            print(f"    Timeout")
            continue

        # Avito rendert per JS — versuche JSON aus dem DOM
        try:
            content = page.content()
            soup = BeautifulSoup(content, "html.parser")

            # JSON-LD
            for script in soup.find_all("script", type="application/ld+json"):
                try:
                    data = json.loads(script.string)
                    if isinstance(data, dict) and data.get("@type") == "ItemList":
                        for item in data.get("itemListElement", []):
                            i2 = item.get("item", item)
                            l = Listing(source="Avito", title=i2.get("name",""), url=i2.get("url",""))
                            if i2.get("offers"): l.price_mad = parse_price(str(i2["offers"].get("price","")))
                            img = i2.get("image")
                            if isinstance(img, str) and is_real_photo(img): l.image = img
                            elif isinstance(img, list) and img:
                                for im in img:
                                    src = im if isinstance(im, str) else im.get("url","")
                                    if is_real_photo(src): l.image = src; break
                            if l.title and l.url: listings.append(l)
                except: pass

            # __NEXT_DATA__ oder inline JSON
            for script in soup.find_all("script"):
                text = script.string or ""
                if '"subject"' in text and '"price"' in text:
                    _extract_avito_json(text, listings)
        except: pass

        # Fallback: DOM Cards
        cards = page.query_selector_all("a[href*='/fr/marrakech/']")
        existing_urls = {l.url for l in listings}
        for card in cards:
            try:
                href = card.get_attribute("href") or ""
                if "appartement" not in href.lower(): continue
                if not href.startswith("http"): href = "https://www.avito.ma" + href
                if href in existing_urls: continue

                l = Listing(source="Avito", url=href)
                l.title = card.inner_text().strip()[:200]

                price_el = card.query_selector("[class*='price']")
                if price_el: l.price_mad = parse_price(price_el.inner_text())

                img = card.query_selector("img")
                if img:
                    src = img.get_attribute("src") or img.get_attribute("data-src") or ""
                    if is_real_photo(src): l.image = src

                if l.title and len(l.title) > 5:
                    listings.append(l)
                    existing_urls.add(href)
            except: pass

        print(f"    {len(listings)} bisher")
        page.wait_for_timeout(2000)

    # Dedup
    seen = set()
    unique = []
    for l in listings:
        key = l.url or l.title
        if key and key not in seen:
            seen.add(key)
            unique.append(l)
    listings = unique
    print(f"    {len(listings)} unique, lade Details...")

    # Details
    for i, l in enumerate(listings):
        if not l.url: continue
        if i % 10 == 0: print(f"    Detail {i+1}/{len(listings)}...")
        try:
            page.goto(l.url, timeout=20000, wait_until="domcontentloaded")
            page.wait_for_timeout(2000)

            l.url = page.url  # Echte URL nach Redirect

            # Telefon-Button klicken
            phone = click_phone_button(page)
            if phone: l.contact_phone = phone

            # Kontakte
            contacts = extract_contacts_from_page(page)
            for k, v in contacts.items():
                if v and not getattr(l, k, ""): setattr(l, k, v)

            # Bilder
            if not l.images:
                l.images = extract_images_from_page(page)

            # Preis
            if not l.price_mad:
                price_el = page.query_selector("[class*='price'], [class*='amount']")
                if price_el: l.price_mad = parse_price(price_el.inner_text())

            # Beschreibung
            if not l.description:
                for sel in ["[class*='description']", "[class*='body']"]:
                    el = page.query_selector(sel)
                    if el:
                        l.description = el.inner_text().strip()
                        break

            # Aus Script-Tags
            content = page.content()
            for m in re.findall(r'\{[^{}]*"(?:price|surface|rooms)"[^{}]*\}', content):
                try:
                    d = json.loads(m)
                    l.price_mad = l.price_mad or parse_price(str(d.get("price","")))
                    l.area_sqm = l.area_sqm or extract_number(str(d.get("surface","")), 20, 1000)
                    l.rooms = l.rooms or extract_number(str(d.get("rooms","")), 1, 20)
                except: pass

            body = page.inner_text("body")
            apply_detected(l, detect_from_text(body))

        except Exception as ex:
            print(f"    Fehler Detail: {ex}")

        l.finalize()
        page.wait_for_timeout(1500)

    print(f"    Avito: {len(listings)}")
    return listings


def _extract_avito_json(text, listings):
    """Rekursiv JSON aus Script-Text extrahieren."""
    try:
        # Versuche __NEXT_DATA__
        m = re.search(r'__NEXT_DATA__\s*=\s*(\{.+?\})\s*;', text, re.DOTALL)
        if m:
            data = json.loads(m.group(1))
            _parse_avito_obj(data, listings)
            return
    except: pass
    try:
        data = json.loads(text)
        _parse_avito_obj(data, listings)
    except: pass

def _parse_avito_obj(data, listings):
    if isinstance(data, dict):
        if data.get("subject") or (data.get("title") and data.get("price")):
            l = Listing(source="Avito")
            l.title = data.get("subject", data.get("title",""))
            l.url = data.get("url","")
            l.price_mad = parse_price(str(data.get("price", data.get("priceValue",""))))
            p = data.get("params", data.get("attributes",{}))
            if isinstance(p, dict):
                l.rooms = extract_number(str(p.get("rooms","")), 1, 20)
                l.area_sqm = extract_number(str(p.get("surface",p.get("size",""))), 20, 1000)
                l.bedrooms = extract_number(str(p.get("bedrooms","")), 1, 10)
            loc = data.get("location")
            if isinstance(loc, dict): l.neighborhood = loc.get("name",loc.get("label",""))
            phone = data.get("phone",data.get("phoneNumber",""))
            if phone: l.contact_phone = str(phone)
            imgs = data.get("images",data.get("photos",[]))
            if isinstance(imgs, list):
                for im in imgs[:5]:
                    src = im if isinstance(im, str) else (im.get("url","") if isinstance(im, dict) else "")
                    if is_real_photo(src): l.images.append(src)
            if l.title: listings.append(l)
        for v in data.values():
            if isinstance(v, (dict, list)): _parse_avito_obj(v, listings)
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, (dict, list)): _parse_avito_obj(item, listings)


# ═══════════════════════════════════════
# SAROUTY
# ═══════════════════════════════════════
def scrape_sarouty(page, max_pages=MAX_PAGES):
    print(f"\n  SAROUTY")
    listings = []

    for pg in range(1, max_pages + 1):
        url = "https://www.sarouty.ma/acheter/marrakech/appartements-a-vendre/" if pg == 1 else f"https://www.sarouty.ma/acheter/marrakech/appartements-a-vendre/?page={pg}"
        print(f"    Seite {pg}...")
        try:
            page.goto(url, timeout=30000, wait_until="domcontentloaded")
            page.wait_for_timeout(3000)
        except:
            print(f"    Timeout")
            continue

        # Listing-Links finden
        links = page.query_selector_all("a[href]")
        page_listings = []
        seen_urls = {l.url for l in listings}

        for link in links:
            try:
                href = link.get_attribute("href") or ""
                if not href.startswith("http"): href = "https://www.sarouty.ma" + href

                # Nur Listing-Detail-Seiten (haben eine ID am Ende)
                if "/acheter/" not in href.lower() and "/fr/" not in href.lower(): continue
                if href in seen_urls: continue
                # Sarouty detail URLs enthalten meist eine Nummer
                if not re.search(r'/\d+\.html|/\d+/?$|property|listing', href.lower()): continue

                text = link.inner_text().strip()
                if len(text) < 5 or len(text) > 300: continue

                l = Listing(source="Sarouty", url=href, title=text[:200])

                # Preis aus dem Link-Text
                l.price_mad = parse_price(text)

                # Thumbnail
                img = link.query_selector("img")
                if img:
                    src = img.get_attribute("src") or img.get_attribute("data-src") or ""
                    if not src.startswith("http"): src = "https://www.sarouty.ma" + src
                    if is_real_photo(src): l.image = src

                apply_detected(l, detect_from_text(text))
                page_listings.append(l)
                seen_urls.add(href)
            except: pass

        # Auch Cards versuchen
        cards = page.query_selector_all("[class*='listingCard'], [class*='property-card'], article")
        for card in cards:
            try:
                a = card.query_selector("a[href]")
                if not a: continue
                href = a.get_attribute("href") or ""
                if not href.startswith("http"): href = "https://www.sarouty.ma" + href
                if href in seen_urls: continue

                l = Listing(source="Sarouty", url=href)
                title_el = card.query_selector("h2, h3, [class*='title']")
                l.title = title_el.inner_text().strip() if title_el else card.inner_text().strip()[:200]
                price_el = card.query_selector("[class*='price']")
                if price_el: l.price_mad = parse_price(price_el.inner_text())

                img = card.query_selector("img")
                if img:
                    src = img.get_attribute("src") or img.get_attribute("data-src") or ""
                    if not src.startswith("http"): src = "https://www.sarouty.ma" + src
                    if is_real_photo(src): l.image = src

                apply_detected(l, detect_from_text(card.inner_text()))
                if l.title and len(l.title) > 3:
                    page_listings.append(l)
                    seen_urls.add(href)
            except: pass

        listings.extend(page_listings)
        print(f"    {len(page_listings)} gefunden (gesamt: {len(listings)})")

        if not page_listings: break
        page.wait_for_timeout(2000)

    # Dedup
    seen = set()
    unique = []
    for l in listings:
        key = l.url
        if key and key not in seen:
            seen.add(key)
            unique.append(l)
    listings = unique
    print(f"    {len(listings)} unique, lade Details...")

    for i, l in enumerate(listings):
        if not l.url: continue
        if i % 10 == 0: print(f"    Detail {i+1}/{len(listings)}...")
        try:
            page.goto(l.url, timeout=20000, wait_until="domcontentloaded")
            page.wait_for_timeout(2000)

            l.url = page.url

            phone = click_phone_button(page)
            if phone: l.contact_phone = phone

            contacts = extract_contacts_from_page(page)
            for k, v in contacts.items():
                if v and not getattr(l, k, ""): setattr(l, k, v)

            if not l.images:
                l.images = extract_images_from_page(page)

            if not l.price_mad:
                price_el = page.query_selector("[class*='price']")
                if price_el: l.price_mad = parse_price(price_el.inner_text())

            if not l.description:
                desc = page.query_selector("[class*='description']")
                if desc: l.description = desc.inner_text().strip()

            body = page.inner_text("body")
            apply_detected(l, detect_from_text(body))

        except Exception as ex:
            print(f"    Fehler: {ex}")

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
            if l.price_mad < BUDGET_MIN_MAD: reason = f"Preis: {l.price_mad:,} < Min"
            elif l.price_mad > BUDGET_MAX_MAD: reason = f"Preis: {l.price_mad:,} > Max"
        else: reason = "Kein Preis"
        if not reason and l.rooms and l.rooms < 3 and (not l.bedrooms or l.bedrooms < 2):
            reason = "Zu wenig Zimmer"
        if not reason and l.is_ground_floor: reason = "Erdgeschoss"
        if not reason and l.is_riad: reason = "Riad"
        if not reason and l.ownership_type == "Melkia": reason = "Melkia"
        if not reason:
            full = (l.title + " " + l.description).lower()
            for kw in NO_GO_KEYWORDS:
                if kw in full: reason = f"No-Go: {kw}"; break
        if reason: rejected.append({"title":l.title[:60],"url":l.url,"reason":reason})
        else: passed.append(l)
    return passed, rejected


# ═══════════════════════════════════════
# MAIN
# ═══════════════════════════════════════
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--portal", choices=["avito","mubawab","sarouty","all"], default="all")
    parser.add_argument("--output", "-o", default="data/latest_raw.json")
    parser.add_argument("--pages", type=int, default=MAX_PAGES)
    parser.add_argument("--raw", action="store_true")
    args = parser.parse_args()

    print(f"\n  SCRAPER v6 PLAYWRIGHT | {args.portal.upper()} | {args.pages} Seiten\n")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            locale="fr-FR",
            viewport={"width": 1280, "height": 800},
        )
        page = context.new_page()

        all_listings = []
        scrapers = {"avito": scrape_avito, "mubawab": scrape_mubawab, "sarouty": scrape_sarouty}
        portals = scrapers.keys() if args.portal == "all" else [args.portal]

        for portal in portals:
            try:
                results = scrapers[portal](page, args.pages)
                all_listings.extend(results)
            except Exception as ex:
                print(f"  FEHLER {portal}: {ex}")
                import traceback; traceback.print_exc()

        browser.close()

    # Global dedup
    seen = set()
    unique = []
    for l in all_listings:
        key = l.url or f"{l.title}_{l.price_mad}"
        if key and key not in seen:
            seen.add(key)
            unique.append(l)

    if args.raw:
        passed, rejected = unique, []
    else:
        passed, rejected = apply_gates(unique)

    wu = len([l for l in passed if l.url])
    wi = len([l for l in passed if l.image])
    wp = len([l for l in passed if l.contact_phone])
    we = len([l for l in passed if l.contact_email])
    ww = len([l for l in passed if l.contact_whatsapp])

    print(f"\n  ERGEBNIS: {len(passed)} qualifiziert, {len(rejected)} abgelehnt")
    print(f"  URLs: {wu} | Bilder: {wi} | Tel: {wp} | Email: {we} | WA: {ww}")

    for i, l in enumerate(passed[:10], 1):
        p = f"{l.price_mad:,}" if l.price_mad else "?"
        ph = f" Tel:{l.contact_phone}" if l.contact_phone else ""
        print(f"  {i}. {p} MAD | {l.area_sqm or '?'}m2 | {l.rooms or '?'}Zi | {l.neighborhood or '?'}{ph}")
        print(f"     {l.title[:70]}")
        print(f"     {l.url[:80]}")

    output = {
        "meta": {"scraped_at":datetime.now().isoformat(),"total_found":len(passed)+len(rejected),"passed_gates":len(passed),"rejected":len(rejected)},
        "listings": [asdict(l) for l in passed],
        "rejected_log": rejected,
    }
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(output, ensure_ascii=False, indent=2))
    print(f"  Gespeichert: {out}\n")


if __name__ == "__main__":
    main()
