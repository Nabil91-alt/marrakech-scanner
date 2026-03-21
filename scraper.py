#!/usr/bin/env python3
"""
MARRAKECH SCRAPER v7 — PLAYWRIGHT
Fixes basierend auf echtem HTML:
- Avito: URLs enthalten Stadtteil nicht 'marrakech', Links enden auf .htm
- Mubawab: Dedup-Fix, korrekte Selektoren
- Sarouty: Kartenextraktion gefixt
"""

import json, time, re, argparse, hashlib, random
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
    "saada","semlalia","hay mohammadi","marjane","annakhil",
    "route d'amizmiz","route amizmiz","chrifia","bouaakkaz","mhamid",
    "sidi abbad","hay charaf","mabrouka","najd",
]
NO_GO = ["rez-de-chaussee","rez de chaussee"]

# Zentrum-Lagen die NICHT Speckgürtel sind
NO_GO_NEIGHBORHOODS = [
    "guéliz","gueliz","hivernage","medina","médina","centre ville",
    "centre-ville","bab doukkala","bab doukala","kasbah","mellah",
    "victor hugo","majorelle",
]
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
    # Pattern 1: Zahl vor DH/MAD/Dhs (sicherste)
    m = re.search(r'(\d[\d\s]{4,10})\s*(?:DH|MAD|Dhs|dh|mad)', text)
    if m:
        digits = re.sub(r'\s', '', m.group(1))
        val = int(digits)
        if 100_000 <= val <= 50_000_000: return val
    # Pattern 2: Zahl mit Punkt/Komma-Trenner
    m = re.search(r'(\d{1,3}[\.\,]\d{3}[\.\,]\d{3})', text)
    if m:
        digits = re.sub(r'[\.\,]', '', m.group(1))
        val = int(digits)
        if 100_000 <= val <= 50_000_000: return val
    # Pattern 3: Zahl mit Leerzeichen OHNE DH/MAD — "1 800 000" oder "876 000"
    m = re.search(r'(?<!\d)(\d{1,2})\s(\d{3})\s(\d{3})(?!\d)', text)
    if m:
        val = int(m.group(1) + m.group(2) + m.group(3))
        if 100_000 <= val <= 50_000_000: return val
    m = re.search(r'(?<!\d)(\d{3})\s(\d{3})(?!\d)', text)
    if m:
        val = int(m.group(1) + m.group(2))
        if 100_000 <= val <= 50_000_000: return val
    # Pattern 4: Durchgehende Zahl 6-8 Stellen
    m = re.search(r'(?<!\d)(\d{6,8})(?!\d)', text)
    if m:
        val = int(m.group(1))
        if 100_000 <= val <= 50_000_000: return val
    return None

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
    if any(w in t for w in ["terrasse","balcon","rooftop"]): r['has_terrace'] = True
    if any(w in t for w in ["piscine","pool"]): r['has_pool'] = True
    if any(w in t for w in ["parking","garage","sous-sol"]): r['has_parking'] = True
    if "ascenseur" in t: r['has_elevator'] = True
    if any(w in t for w in ["neuf","jamais habit","livraison 202"]): r['is_new_build'] = True
    # Riad-Erkennung DEAKTIVIERT: Wir suchen Appartements, "riad" im Text ist fast immer Adresse
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
    """Aggressiv Telefonnummer extrahieren — mehrere Strategien."""
    
    # Strategy 1: Schon sichtbare tel: links
    try:
        for el in page.query_selector_all("a[href^='tel:']"):
            p = (el.get_attribute("href") or "").replace("tel:","").strip()
            if len(p) >= 8: return p
    except: pass

    # Strategy 2: Telefon aus __NEXT_DATA__ oder Script-Tags (Avito)
    try:
        content = page.content()
        # Avito speichert Daten in __NEXT_DATA__
        for pattern in [r'"phone"\s*:\s*"(\+?[\d\s.-]{8,})"', r'"phoneNumber"\s*:\s*"(\+?[\d\s.-]{8,})"', r'"seller_phone"\s*:\s*"(\+?[\d\s.-]{8,})"', r'"mobile"\s*:\s*"(\+?[\d\s.-]{8,})"']:
            m = re.search(pattern, content)
            if m:
                phone = m.group(1).strip()
                if len(phone) >= 8: return phone
    except: pass
    
    # Strategy 3: Alle moeglichen Buttons klicken
    button_texts = [
        "Afficher le numéro", "Afficher le numero",
        "Contacter le Vendeur", "Contacter le vendeur", "Contacter",
        "Voir le téléphone", "Voir le telephone", "Voir le numéro",
        "Appeler", "Téléphone", "Telephone",
        "Show phone", "Call",
        "numéro", "numero",
    ]
    
    for txt in button_texts:
        try:
            # Versuche button und a tags
            for tag in ["button", "a", "span", "div"]:
                sel = tag + ":has-text('" + txt + "')"
                btn = page.query_selector(sel)
                if btn and btn.is_visible():
                    btn.click()
                    page.wait_for_timeout(2500)
                    
                    # Check tel: links nach dem Klick
                    for el in page.query_selector_all("a[href^='tel:']"):
                        p = (el.get_attribute("href") or "").replace("tel:","").strip()
                        if len(p) >= 8: return p
                    
                    # Check ob Telefonnummer im sichtbaren Text erschienen ist
                    body = page.inner_text("body")
                    phones = re.findall(r'(?:\+212|0)[5-7][\s.-]?\d{2}[\s.-]?\d{2}[\s.-]?\d{2}[\s.-]?\d{2}', body)
                    if phones: return phones[0].strip()
                    
                    # Check Modals/Popups
                    for modal_sel in ["[class*='modal']", "[class*='popup']", "[class*='dialog']", "[role='dialog']"]:
                        modal = page.query_selector(modal_sel)
                        if modal and modal.is_visible():
                            mt = modal.inner_text()
                            mphones = re.findall(r'(?:\+212|0)[5-7][\s.-]?\d{2}[\s.-]?\d{2}[\s.-]?\d{2}[\s.-]?\d{2}', mt)
                            if mphones: return mphones[0].strip()
        except: pass
    
    # Strategy 4: Nochmal den ganzen Seitentext durchsuchen
    try:
        body = page.inner_text("body")
        phones = re.findall(r'(?:\+212|0)[5-7][\s.-]?\d{2}[\s.-]?\d{2}[\s.-]?\d{2}[\s.-]?\d{2}', body)
        if phones: return phones[0].strip()
    except: pass
    
    # Strategy 5: Avito Boutique-Link folgen fuer Telefon
    try:
        boutique = page.query_selector("a[href*='/boutique']")
        if boutique:
            href = boutique.get_attribute("href") or ""
            if href:
                if not href.startswith("http"): href = "https://www.avito.ma" + href
                page.goto(href, timeout=15000, wait_until="domcontentloaded")
                page.wait_for_timeout(2000)
                body = page.inner_text("body")
                phones = re.findall(r'(?:\+212|0)[5-7][\s.-]?\d{2}[\s.-]?\d{2}[\s.-]?\d{2}[\s.-]?\d{2}', body)
                if phones: return phones[0].strip()
                for el in page.query_selector_all("a[href^='tel:']"):
                    p = (el.get_attribute("href") or "").replace("tel:","").strip()
                    if len(p) >= 8: return p
                page.go_back()
                page.wait_for_timeout(1000)
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
# UNIVERSELLE DETAIL-SEITEN-EXTRAKTION
# Oeffnet die Seite, liest ALLES, fertig.
# ═══════════════════════════════════════
def scrape_detail_page(page, listing, verbose=False):
    """Oeffnet Detail-Seite, liest den KOMPLETTEN Seiteninhalt, extrahiert alles."""
    try:
        page.goto(listing.url, timeout=25000, wait_until="domcontentloaded")
        page.wait_for_timeout(2000)

        # CAPTCHA/Cloudflare Detection — warte bis zu 15s wenn Challenge erkannt
        for _ in range(3):
            body_text = page.inner_text("body").lower()
            if any(x in body_text for x in ["checking your browser", "just a moment", "captcha", "ray id", "cf-browser-verification", "please wait", "vérification"]):
                if verbose: print(f"      Cloudflare/CAPTCHA erkannt, warte...")
                page.wait_for_timeout(5000)
            else:
                break

        listing.url = page.url  # Echte URL nach Redirect

        # 1. TELEFON — Buttons klicken
        phone = click_phone(page)
        if phone:
            listing.contact_phone = phone
            if verbose: print(f"      Tel: {phone}")

        # 2. KONTAKTE aus Links
        contacts = get_contacts(page)
        for k, v in contacts.items():
            if v and not getattr(listing, k, ""): setattr(listing, k, v)

        # 3. BILDER — alle echten Fotos von der gerenderten Seite
        listing.images = get_images(page)

        # 4. DEN KOMPLETTEN SEITENTEXT LESEN
        full_text = ""
        try:
            full_text = page.inner_text("body")
        except: pass

        # 5. TITEL — h1 ist fast immer der Titel
        try:
            h1 = page.query_selector("h1")
            if h1:
                t = h1.inner_text().strip()
                if len(t) > 5: listing.title = t[:200]
        except: pass

        # 6. BESCHREIBUNG — den laengsten Textblock finden
        if not listing.description:
            listing.description = full_text[:1000]  # Erstmal alles, KI filtert

        # 7. PREIS aus dem vollen Text
        if not listing.price_mad:
            listing.price_mad = parse_price(full_text)

        # 8. ALLE strukturierten Daten aus dem Text extrahieren
        apply_d(listing, detect(full_text))

        # 9. VERKAEUFERNAME
        if not listing.contact_name:
            for sel in ["a[href*='/boutique']", "[class*='seller']", "[class*='agent']", "[class*='store']", "[class*='author']"]:
                try:
                    el = page.query_selector(sel)
                    if el:
                        name = el.inner_text().strip()
                        if 2 < len(name) < 60 and '@' not in name and 'http' not in name and 'DH' not in name:
                            listing.contact_name = name
                            break
                except: pass

        # 10. JSON-LD und Script-Daten (strukturierte Daten der Portale)
        try:
            html = page.content()
            # JSON-LD
            from bs4 import BeautifulSoup as BS
            soup = BS(html, "html.parser")
            for script in soup.find_all("script", type="application/ld+json"):
                try:
                    data = json.loads(script.string)
                    if isinstance(data, dict):
                        listing.title = listing.title or data.get("name","")
                        if data.get("offers"):
                            listing.price_mad = listing.price_mad or parse_price(str(data["offers"].get("price","")))
                        if data.get("floorSize") and isinstance(data["floorSize"], dict):
                            listing.area_sqm = listing.area_sqm or xnum(str(data["floorSize"].get("value","")), 20, 1000)
                        listing.rooms = listing.rooms or xnum(str(data.get("numberOfRooms","")), 1, 20)
                except: pass

            # Inline JSON mit Immobilien-Daten
            for m in re.findall(r'\{[^{}]*"(?:price|surface|rooms|bedrooms|bathrooms)"[^{}]*\}', html):
                try:
                    d = json.loads(m)
                    listing.price_mad = listing.price_mad or parse_price(str(d.get("price","")))
                    listing.area_sqm = listing.area_sqm or xnum(str(d.get("surface",d.get("size",""))), 20, 1000)
                    listing.rooms = listing.rooms or xnum(str(d.get("rooms","")), 1, 20)
                    listing.bedrooms = listing.bedrooms or xnum(str(d.get("bedrooms","")), 1, 10)
                    listing.bathrooms = listing.bathrooms or xnum(str(d.get("bathrooms","")), 1, 10)
                    p = d.get("phone", d.get("phoneNumber",""))
                    if p and not listing.contact_phone and len(str(p)) >= 8:
                        listing.contact_phone = str(p)
                except: pass
        except: pass

        if verbose and listing.price_mad:
            print(f"      Preis: {listing.price_mad:,} | {listing.area_sqm or '?'}m2 | {listing.rooms or '?'}Zi")

    except Exception as ex:
        if verbose: print(f"      Fehler: {ex}")

    listing.finalize()
    # Menschliche Pause: 1-3 Sekunden randomisiert
    time.sleep(random.uniform(1.0, 3.0))

# ═══════════════════════════════════════
# FILTER AUF DER SEITE SETZEN (Playwright)
# ═══════════════════════════════════════
def apply_portal_filters(page, portal):
    """Setzt Preis- und Ausstattungsfilter direkt auf der Portal-Seite."""
    print(f"    Filter setzen...")
    
    # Versuche Preis-Filter
    try:
        # Preis Min
        for sel in ["input[name*='price_min']", "input[placeholder*='Min']", "input[data-testid*='min']", "input[id*='priceMin']", "input[name*='min_price']"]:
            el = page.query_selector(sel)
            if el and el.is_visible():
                el.fill(str(BUDGET_MIN))
                break
        # Preis Max
        for sel in ["input[name*='price_max']", "input[placeholder*='Max']", "input[data-testid*='max']", "input[id*='priceMax']", "input[name*='max_price']"]:
            el = page.query_selector(sel)
            if el and el.is_visible():
                el.fill(str(BUDGET_MAX))
                break
    except: pass
    
    # Versuche Balkon/Terrasse Filter anzuklicken
    try:
        for txt in ["Balcon", "Terrasse", "balcon", "terrasse"]:
            for tag in ["label", "span", "div", "button", "input"]:
                sel = tag + ":has-text('" + txt + "')"
                el = page.query_selector(sel)
                if el and el.is_visible():
                    el.click()
                    print(f"      Filter geklickt: {txt}")
                    page.wait_for_timeout(1000)
                    break
    except: pass
    
    # Versuche "Appliquer" / "Rechercher" Button
    try:
        for txt in ["Appliquer", "Rechercher", "Filtrer", "Valider", "Afficher"]:
            btn = page.query_selector("button:has-text('" + txt + "')")
            if btn and btn.is_visible():
                btn.click()
                page.wait_for_timeout(3000)
                print(f"      Filter angewendet")
                break
    except: pass

# ═══════════════════════════════════════
# AVITO — URLs sind /fr/{stadtteil}/appartements/{titel}_{id}.htm
# ═══════════════════════════════════════
def scrape_avito(page, max_pages):
    print(f"\n  AVITO")
    listings = []
    
    # Erste Seite mit Preis-Filter in URL (Avito unterstuetzt das)
    base_url = "https://www.avito.ma/fr/marrakech/appartements-%C3%A0_vendre"
    first_url = base_url + "?price_min=" + str(BUDGET_MIN) + "&price_max=" + str(BUDGET_MAX)
    
    for pg in range(1, max_pages + 1):
        url = first_url + "&o=" + str(pg) if pg > 1 else first_url
        try:
            page.goto(url, timeout=30000, wait_until="domcontentloaded")
            page.wait_for_timeout(4000)
            
            # Auf erster Seite: zusaetzliche Filter per Klick setzen
            if pg == 1:
                apply_portal_filters(page, "avito")
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
        scrape_detail_page(page, l, verbose=(i < 5))
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
            if pg == 1:
                apply_portal_filters(page, "mubawab")
        except:
            print(f"    Seite {pg}: Timeout"); continue

        # Mubawab: Links die zu Detail-Seiten fuehren
        all_links = page.query_selector_all("a[href]")
        existing = {l.url for l in listings}
        count = 0

        for link in all_links:
            try:
                href = link.get_attribute("href") or ""
                if not href.startswith("http"): href = "https://www.mubawab.ma" + href

                # Mubawab Detail-URLs: /fr/marrakech/...-detail-XXXX oder /fr/XXXX
                if "mubawab.ma" not in href: continue
                if href in existing: continue
                # Skip listing pages and non-detail pages
                if "/st/" in href or "/ct/" in href or "/sd/" in href or "/is/" in href: continue
                if "/appartements-a-vendre" in href: continue
                # Skip navigation/system pages
                if any(x in href for x in ["/login","/app-mobile","/about","/cms","/study-guide","/contact","/terms","/privacy","/faq","/help","/blog","/sitemap","/register","/forgot"]): continue
                # Must be a detail page - contains /fr/ and has some path
                if "/fr/" not in href: continue
                # Must contain a number (listing ID) or /pa/ or /b/ (property paths)
                if not re.search(r'/pa/\d|/b/\d|/\d+[/-]', href): continue
                parts = href.replace("https://www.mubawab.ma","").split("/")
                if len(parts) < 3: continue

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
        scrape_detail_page(page, l, verbose=(i < 3))
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
            if pg == 1:
                apply_portal_filters(page, "sarouty")
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
                if href in existing: continue
                if "/acheter/marrakech/appartements-a-vendre" in href and "?" not in href.split("/")[-1]: continue
                # Must look like a detail page (not a category/filter page)
                if href.rstrip("/") == "https://www.sarouty.ma": continue
                path = href.replace("https://www.sarouty.ma","")
                # Detail pages have deeper paths or contain numbers
                if len(path) < 10: continue
                if path.count("/") < 2 and not re.search(r'\d', path): continue

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
        scrape_detail_page(page, l, verbose=(i < 3))
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
        full = (l.title + " " + l.description + " " + l.neighborhood).lower()

        # Preis
        if l.price_mad is not None:
            if l.price_mad < BUDGET_MIN: reason = f"Preis: {l.price_mad:,} < Min"
            elif l.price_mad > BUDGET_MAX: reason = f"Preis: {l.price_mad:,} > Max"
        # Kein Preis? Durchlassen — KI bewertet es, Portal-Filter hat vorselektiert

        # Zimmer
        if not reason and l.rooms and l.rooms < 3 and (not l.bedrooms or l.bedrooms < 2):
            reason = "Zu wenig Zimmer"

        # Erdgeschoss
        if not reason and l.is_ground_floor: reason = "Erdgeschoss"

        # Melkia
        if not reason and l.ownership_type == "Melkia": reason = "Melkia"

        # No-Go Keywords
        if not reason:
            for kw in NO_GO:
                if kw in full: reason = f"No-Go: {kw}"; break

        # TERRASSE/BALKON: Portal-Filter uebernimmt das vorab
        # Kein harter Gate hier noetig — KI bewertet es im Score

        # SPECKGÜRTEL PFLICHT — nur bei BEKANNTER Zentrum-Lage ablehnen
        if not reason:
            nb = l.neighborhood.lower() if l.neighborhood else ""
            for nogo_nb in NO_GO_NEIGHBORHOODS:
                # Nur auf Neighborhood-Feld pruefen, NICHT auf URL
                # (URLs enthalten oft Stadtteil-Namen die nicht die Lage sind)
                if nogo_nb in nb or nogo_nb in (l.title or "").lower():
                    reason = f"Zentrum-Lage: {l.neighborhood or nogo_nb}"
                    break

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
        browser = pw.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ]
        )
        ctx = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.94 Safari/537.36",
            locale="fr-FR",
            viewport={"width":1366,"height":768},
            java_script_enabled=True,
            extra_http_headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7,ar;q=0.6",
                "Accept-Encoding": "gzip, deflate, br",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Upgrade-Insecure-Requests": "1",
            }
        )
        page = ctx.new_page()

        # Stealth: WebDriver-Flag entfernen + Navigator-Properties faelschen
        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3]});
            Object.defineProperty(navigator, 'languages', {get: () => ['fr-FR', 'fr', 'en']});
            window.chrome = {runtime: {}};
        """)

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

    # Ablehnungsgruende aufschluesseln
    reasons = {}
    for r in rejected:
        reason = r.get("reason","?")
        # Gruppieren
        key = reason.split(":")[0] if ":" in reason else reason
        reasons[key] = reasons.get(key, 0) + 1
    if reasons:
        print(f"\n  Ablehnungsgruende:")
        for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
            print(f"    {count:3d}x {reason}")

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
