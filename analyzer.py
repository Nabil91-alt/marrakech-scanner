#!/usr/bin/env python3
"""MARRAKECH KI-ANALYZER v5 - Bewahrt alle Kontaktdaten + kumulative Leads"""

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

DEEP_PROMPT = """Du bist Immobilien-Analyst fuer Marrakesch.

KAEUFER: Budget 1.2-1.8M MAD, Eigennutzung+Investment, >=3Zi >=90m2, Speckguertel (Targa/Agdal/Palmeraie/Ourika/Tamansourt/M'Hamid/Massira/Izdihar/Amerchich), Terrasse+Pool+Parkplatz+Aufzug.

SCORING: BUDGET(25%):100=1.2-1.6M,80=1.6-1.9M. LAGE(25%):100=Targa/Agdal,90=Palmeraie,80=Izdihar/Massira,70=Tamansourt. GROESSE(15%):100=3Zi+100m2. AUSSTATTUNG(20%):je25. INVESTMENT(15%). ABZUEGE: Eigentum unklar -10, wenig Infos -10.

WICHTIG: Felder id, url, source, contact_phone, contact_email, contact_whatsapp, contact_name, image, images EXAKT aus Input kopieren! NICHT aendern!

Antworte NUR mit JSON-Array:
[{"id":"WIE-INPUT","title":"Deutsch","source":"WIE-INPUT","url":"WIE-INPUT","contact_phone":"WIE-INPUT","contact_email":"WIE-INPUT","contact_whatsapp":"WIE-INPUT","contact_name":"WIE-INPUT","image":"WIE-INPUT","images":"WIE-INPUT","price_mad":N,"price_eur":N,"area_sqm":N,"rooms":N,"bedrooms":N,"bathrooms":N,"floor":"X","neighborhood":"X","property_type":"X","price_per_sqm_mad":N,"has_terrace":B,"has_pool":B,"has_parking":B,"has_elevator":B,"is_new_build":B,"ownership_type":"X","condition":"X","highlights":["4x"],"concerns":["4x"],"neighborhood_outlook":"2S","investment_potential":1-5,"livability_score":1-5,"market_price_assessment":"Unter Markt/Marktgerecht/Ueber Markt/Nicht beurteilbar","scores":{"budget_fit":N,"location_fit":N,"size_fit":N,"amenities_fit":N,"investment_fit":N,"info_penalty":N,"overall":N},"verdict":"TOP-KANDIDAT/INTERESSANT/BEDINGT GEEIGNET/NICHT GEEIGNET","verdict_reason":"2-3S","besichtigung_fragen":["5x"],"verhandlung_tipps":"1-2S"}]"""

# Felder die IMMER aus Scraper-Daten kommen muessen
PRESERVE_FIELDS = ["url","source","contact_phone","contact_email","contact_whatsapp","contact_name","image","images"]

def call_claude(chunk, num, total):
    if not API_KEY: return []
    headers = {"Content-Type":"application/json","x-api-key":API_KEY,"anthropic-version":"2023-06-01"}
    payload = {"model":MODEL,"max_tokens":MAX_TOKENS,"system":DEEP_PROMPT,
        "messages":[{"role":"user","content":"Analysiere "+str(len(chunk))+" Inserate ("+str(num)+"/"+str(total)+"):\n\n"+json.dumps(chunk,ensure_ascii=False)}]}
    for attempt in range(5):
        try:
            resp = requests.post(API_URL, headers=headers, json=payload, timeout=120)
            if resp.status_code == 200:
                text = "".join(b.get("text","") for b in resp.json().get("content",[]) if b.get("type")=="text")
                clean = text.strip()
                if clean.startswith("```"): clean = clean.split("\n",1)[1] if "\n" in clean else clean[3:]
                if clean.endswith("```"): clean = clean[:-3]
                clean = clean.strip()
                if not clean: time.sleep(5); continue
                r = json.loads(clean)
                result = r if isinstance(r, list) else [r]
                print(f"    OK: {len(result)} analysiert")
                return result
            elif resp.status_code == 429: time.sleep(min(60,10*(attempt+1)))
            elif resp.status_code == 529: time.sleep(20)
            else: print(f"    Fehler {resp.status_code}"); time.sleep(10)
        except requests.RequestException as ex: print(f"    {ex}"); time.sleep(10)
        except json.JSONDecodeError: time.sleep(5)
    print("    FEHLGESCHLAGEN"); return []

def restore(analyzed, raw):
    by_id = {l.get("id"): l for l in raw if l.get("id")}
    for a in analyzed:
        r = by_id.get(a.get("id"))
        if r:
            for f in PRESERVE_FIELDS:
                rv = r.get(f)
                if rv: a[f] = rv

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input","-i",default="data/latest_raw.json")
    parser.add_argument("--output","-o",default="data/latest.json")
    args = parser.parse_args()
    if not API_KEY: print("KEIN API KEY!"); sys.exit(1)
    print(f"\n  ANALYZER v5\n")
    with open(args.input) as f: data = json.load(f)
    listings = data.get("listings",[])
    meta = data.get("meta",{})
    rejected_log = data.get("rejected_log",[])

    existing = []; existing_ids = set()
    try:
        with open(args.output) as f:
            old = json.load(f)
            existing = old.get("listings",[])
            existing_ids = {l.get("id") for l in existing if l.get("id")}
            print(f"  {len(existing)} bestehende Leads")
    except: print("  Keine bestehenden Leads")

    new_l = [l for l in listings if l.get("id") and l["id"] not in existing_ids]
    print(f"  {len(new_l)} neue (von {len(listings)})")

    if not new_l:
        print("  Nichts Neues.")
        result = {"meta":{**meta,"analyzed_at":datetime.now().isoformat(),"analyzed_count":0,
            "qualified_count":len(existing),"new_count":0,
            "top_count":len([a for a in existing if a.get("verdict")=="TOP-KANDIDAT"]),
            "interesting_count":len([a for a in existing if a.get("verdict")=="INTERESSANT"])},
            "listings":existing,"rejected_log":rejected_log}
        Path(args.output).parent.mkdir(parents=True,exist_ok=True)
        Path(args.output).write_text(json.dumps(result,ensure_ascii=False,indent=2))
        return

    new_a = []
    total = (len(new_l)+CHUNK_SIZE-1)//CHUNK_SIZE
    for i in range(0,len(new_l),CHUNK_SIZE):
        chunk = new_l[i:i+CHUNK_SIZE]
        num = i//CHUNK_SIZE+1
        print(f"\n  Chunk {num}/{total}: {len(chunk)}")
        analyzed = call_claude(chunk, num, total)
        if analyzed:
            q = [a for a in analyzed if a.get("scores",{}).get("overall",0) >= MIN_SCORE]
            new_a.extend(q)
            print(f"    {len(q)} qualifiziert")
        if i+CHUNK_SIZE < len(new_l): time.sleep(3)

    restore(new_a, new_l)
    all_leads = existing + new_a
    all_leads.sort(key=lambda x: x.get("scores",{}).get("overall",0), reverse=True)

    result = {"meta":{**meta,"analyzed_at":datetime.now().isoformat(),
        "analyzed_count":len(new_l),"qualified_count":len(all_leads),"new_count":len(new_a),
        "top_count":len([a for a in all_leads if a.get("verdict")=="TOP-KANDIDAT"]),
        "interesting_count":len([a for a in all_leads if a.get("verdict")=="INTERESSANT"])},
        "listings":all_leads,"rejected_log":rejected_log}
    Path(args.output).parent.mkdir(parents=True,exist_ok=True)
    Path(args.output).write_text(json.dumps(result,ensure_ascii=False,indent=2))

    wu=len([a for a in all_leads if a.get("url")])
    wi=len([a for a in all_leads if a.get("image")])
    wp=len([a for a in all_leads if a.get("contact_phone")])
    we=len([a for a in all_leads if a.get("contact_email")])
    ww=len([a for a in all_leads if a.get("contact_whatsapp")])
    print(f"\n  Gesamt: {len(all_leads)} ({len(new_a)} neu)")
    print(f"  URLs:{wu} Bilder:{wi} Tel:{wp} Email:{we} WA:{ww}")
    print(f"  Gespeichert: {args.output}\n")

if __name__ == "__main__": main()
