#!/usr/bin/env python3
"""
MARRAKECH KI-ANALYZER v4
- Kumulative Leads (alte bleiben, neue kommen dazu)
- Bulletproof URL/Bild/Kontakt-Bewahrung
- Robustes Error-Handling
"""

import json, os, sys, time, argparse
from datetime import datetime
from pathlib import Path

try:
    import requests
except ImportError:
    print("pip install requests"); sys.exit(1)

API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MODEL = "claude-sonnet-4-20250514"
API_URL = "https://api.anthropic.com/v1/messages"
MAX_TOKENS = 8000
CHUNK_SIZE = 8
MIN_SCORE = 60

DEEP_PROMPT = """Du bist ein erfahrener Immobilien-Analyst fuer Marrakesch.

KAEUFER-PROFIL:
- Budget sweet spot: 1.200.000-1.800.000 MAD
- Eigennutzung + Investment + Hauptwohnsitz
- Min. 3 Zimmer, ideal >= 90m2
- Lagen: Targa, Palmeraie, Route de l'Ourika, Agdal, Tamansourt, M'Hamid, Massira, Izdihar, Amerchich, Tassoultant, Route de Casablanca, Sidi Ghanem
- Must-haves: Terrasse, Pool, Parkplatz, Aufzug/Neubau

SCORING:
BUDGET(25%): 100=1.2-1.6M, 80=1.6-1.9M/1.0-1.2M, 60=1.9-2.1M, 40=Rest
LAGE(25%): 100=Targa/Agdal, 90=Palmeraie/Ourika, 80=Izdihar/Massira/M'Hamid, 70=Tamansourt/Route Casa, 60=Amerchich, 50=Gueliz/Hivernage
GROESSE(15%): 100=3Zi+100m2, 80=3Zi+80m2, 60=3Zi+60m2
AUSSTATTUNG(20%): je 25 fuer Terrasse, Pool, Parkplatz, Aufzug/Neubau
INVESTMENT(15%): m2-Preis, Entwicklung, Neubau
ABZUEGE: Eigentum unklar -10, Zustand unklar -5, wenig Infos -10

WICHTIG: Kopiere id, url, source, contact_phone, contact_name, image EXAKT aus dem Input.

Antworte NUR mit JSON-Array:
[{"id":"WIE-INPUT","title":"Deutsch","source":"WIE-INPUT","url":"WIE-INPUT","contact_phone":"WIE-INPUT","contact_name":"WIE-INPUT","image":"WIE-INPUT","price_mad":N,"price_eur":N,"area_sqm":N,"rooms":N,"bedrooms":N,"bathrooms":N,"floor":"X","neighborhood":"X","property_type":"X","price_per_sqm_mad":N,"has_terrace":B,"has_pool":B,"has_parking":B,"has_elevator":B,"is_new_build":B,"ownership_type":"X","condition":"X","highlights":["4x"],"concerns":["4x"],"neighborhood_outlook":"2 Saetze","investment_potential":1-5,"livability_score":1-5,"market_price_assessment":"Unter Markt/Marktgerecht/Ueber Markt/Nicht beurteilbar","scores":{"budget_fit":N,"location_fit":N,"size_fit":N,"amenities_fit":N,"investment_fit":N,"info_penalty":N,"overall":N},"verdict":"TOP-KANDIDAT/INTERESSANT/BEDINGT GEEIGNET/NICHT GEEIGNET","verdict_reason":"2-3 Saetze","besichtigung_fragen":["5x"],"verhandlung_tipps":"1-2 Saetze"}]"""


def call_claude(chunk, num, total):
    if not API_KEY: return []
    headers = {"Content-Type":"application/json","x-api-key":API_KEY,"anthropic-version":"2023-06-01"}
    payload = {"model":MODEL,"max_tokens":MAX_TOKENS,"system":DEEP_PROMPT,
        "messages":[{"role":"user","content":"Analysiere "+str(len(chunk))+" Inserate ("+str(num)+"/"+str(total)+"):\n\n"+json.dumps(chunk,ensure_ascii=False)}]}
    for attempt in range(5):
        try:
            print(f"    API {attempt+1}/5...")
            resp = requests.post(API_URL, headers=headers, json=payload, timeout=120)
            if resp.status_code == 200:
                text = "".join(b.get("text","") for b in resp.json().get("content",[]) if b.get("type")=="text")
                clean = text.strip()
                if clean.startswith("```"): clean = clean.split("\n",1)[1] if "\n" in clean else clean[3:]
                if clean.endswith("```"): clean = clean[:-3]
                clean = clean.strip()
                if not clean: time.sleep(5); continue
                result = json.loads(clean)
                r = result if isinstance(result, list) else [result]
                print(f"    OK: {len(r)} analysiert")
                return r
            elif resp.status_code == 429: time.sleep(min(60,10*(attempt+1)))
            elif resp.status_code == 529: time.sleep(20)
            else: print(f"    Fehler {resp.status_code}: {resp.text[:150]}"); time.sleep(10)
        except requests.RequestException as ex: print(f"    {ex}"); time.sleep(10)
        except json.JSONDecodeError: time.sleep(5)
    print("    FEHLGESCHLAGEN")
    return []


def restore_from_raw(analyzed, raw_listings):
    """URLs, Bilder, Kontakte IMMER aus Scraper-Daten nehmen."""
    by_id = {l.get("id"): l for l in raw_listings if l.get("id")}
    for a in analyzed:
        raw = by_id.get(a.get("id"))
        if raw:
            # Diese Felder IMMER aus Rohdaten, nie der KI vertrauen
            for field in ["url","source","contact_phone","contact_name","image","images"]:
                raw_val = raw.get(field)
                if raw_val:
                    a[field] = raw_val


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input","-i",default="data/latest_raw.json")
    parser.add_argument("--output","-o",default="data/latest.json")
    args = parser.parse_args()
    if not API_KEY: print("KEIN API KEY!"); sys.exit(1)

    print(f"\n  KI-ANALYZER v4\n")

    with open(args.input) as f:
        data = json.load(f)
    listings = data.get("listings",[])
    meta = data.get("meta",{})
    rejected_log = data.get("rejected_log",[])

    # Bestehende Leads laden
    existing = []
    existing_ids = set()
    try:
        with open(args.output) as f:
            old = json.load(f)
            existing = old.get("listings",[])
            existing_ids = {l.get("id") for l in existing if l.get("id")}
            print(f"  {len(existing)} bestehende Leads")
    except:
        print("  Keine bestehenden Leads")

    # Nur neue analysieren
    new_listings = [l for l in listings if l.get("id") and l["id"] not in existing_ids]
    print(f"  {len(new_listings)} neue Inserate (von {len(listings)} gescrapt)")

    if not new_listings:
        print("  Nichts Neues zu analysieren.")
        result = {"meta":{**meta,"analyzed_at":datetime.now().isoformat(),"analyzed_count":0,"qualified_count":len(existing),
            "top_count":len([a for a in existing if a.get("verdict")=="TOP-KANDIDAT"]),
            "interesting_count":len([a for a in existing if a.get("verdict")=="INTERESSANT"])},
            "listings":existing,"rejected_log":rejected_log}
        Path(args.output).parent.mkdir(parents=True,exist_ok=True)
        Path(args.output).write_text(json.dumps(result,ensure_ascii=False,indent=2))
        return

    new_analyzed = []
    total = (len(new_listings)+CHUNK_SIZE-1)//CHUNK_SIZE
    for i in range(0,len(new_listings),CHUNK_SIZE):
        chunk = new_listings[i:i+CHUNK_SIZE]
        num = i//CHUNK_SIZE+1
        print(f"\n  Chunk {num}/{total}: {len(chunk)} Inserate")
        analyzed = call_claude(chunk, num, total)
        if analyzed:
            q = [a for a in analyzed if a.get("scores",{}).get("overall",0) >= MIN_SCORE]
            new_analyzed.extend(q)
            print(f"    {len(q)} qualifiziert")
        if i+CHUNK_SIZE < len(new_listings): time.sleep(3)

    # URLs/Bilder aus Rohdaten zurueckfuehren
    restore_from_raw(new_analyzed, new_listings)

    # Alte + neue zusammenfuehren
    all_leads = existing + new_analyzed
    all_leads.sort(key=lambda x: x.get("scores",{}).get("overall",0), reverse=True)

    result = {"meta":{**meta,"analyzed_at":datetime.now().isoformat(),
        "analyzed_count":len(new_listings),
        "qualified_count":len(all_leads),
        "new_count":len(new_analyzed),
        "top_count":len([a for a in all_leads if a.get("verdict")=="TOP-KANDIDAT"]),
        "interesting_count":len([a for a in all_leads if a.get("verdict")=="INTERESSANT"])},
        "listings":all_leads,"rejected_log":rejected_log}

    Path(args.output).parent.mkdir(parents=True,exist_ok=True)
    Path(args.output).write_text(json.dumps(result,ensure_ascii=False,indent=2))

    with_url = len([a for a in all_leads if a.get("url")])
    with_img = len([a for a in all_leads if a.get("image")])

    print(f"\n  ERGEBNIS:")
    print(f"  Gesamt:  {len(all_leads)} Leads ({len(new_analyzed)} neu)")
    print(f"  URLs:    {with_url}/{len(all_leads)}")
    print(f"  Bilder:  {with_img}/{len(all_leads)}")
    print(f"  Top:     {result['meta']['top_count']}")
    print(f"  Gespeichert: {args.output}\n")

if __name__ == "__main__":
    main()
