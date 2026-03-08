#!/usr/bin/env python3
"""
MARRAKECH KI-ANALYZER v2
Robuster: Mehr Retries, größere Chunks, besseres Error-Handling.
"""

import json, os, sys, time, argparse
from datetime import datetime
from pathlib import Path

try:
    import requests
except ImportError:
    print("pip install requests")
    sys.exit(1)

API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MODEL = "claude-sonnet-4-20250514"
API_URL = "https://api.anthropic.com/v1/messages"
MAX_TOKENS = 8000
CHUNK_SIZE = 8
MIN_SCORE = 60

DEEP_PROMPT = """Du bist ein erfahrener Immobilien-Analyst für Marrakesch. Tiefenanalyse für vorgeprüfte Angebote.

KÄUFER-PROFIL:
- Budget sweet spot: 1.200.000-1.800.000 MAD (100.000-200.000 EUR)
- Nutzung: Eigennutzung/Feriendomizil + Langzeit-Investment + Hauptwohnsitz
- Min. 3 Zimmer, ideal >= 90m2
- Lagen: Speckgürtel - Targa, Palmeraie, Route de l'Ourika, Agdal, Tamansourt, M'Hamid, Massira, Izdihar, Amerchich, Tassoultant, Route de Casablanca, Nähe Flughafen, Sidi Ghanem
- Must-haves: Terrasse, Pool, Parkplatz, Aufzug/Neubau
- No-Gos: Erdgeschoss, Riad-Stil, Melkia

BEWERTUNG:
BUDGET(25%): 100=1.2-1.6M, 80=1.6-1.9M/1.0-1.2M, 60=1.9-2.1M, 40=Rest
LAGE(25%): 100=Targa/Agdal, 90=Palmeraie/Ourika, 80=Izdihar/Massira/M'Hamid, 70=Tamansourt/Route Casa, 60=Amerchich, 50=Gueliz/Hivernage, 40=Medina
GROESSE(15%): 100=>=3Zi+>=100m2, 80=>=3Zi+>=80m2, 60=3Zi+60-80m2, 40=kleiner
AUSSTATTUNG(20%): je 25 fuer Terrasse, Pool, Parkplatz, Aufzug/Neubau
INVESTMENT(15%): m2-Preis, Entwicklung, Neubau, Qualitaet
ABZUEGE: Eigentum "Unbekannt" -10, Zustand "Unbekannt" -5, wenig Infos -10

Wenn ein Inserat wenig Details hat (kein Zimmer, keine Flaeche), schaetze konservativ basierend auf Preis und Lage. Gib trotzdem eine Bewertung ab.

Antworte NUR mit JSON-Array (keine Backticks, kein Markdown):
[{"id":"gleiche-id","title":"Titel Deutsch","source":"Portal","url":"URL","price_mad":Zahl,"price_eur":Zahl,"area_sqm":Zahl,"rooms":Zahl,"bedrooms":Zahl,"bathrooms":Zahl,"floor":"Etage","neighborhood":"Viertel","property_type":"Typ","price_per_sqm_mad":Zahl,"has_terrace":bool,"has_pool":bool,"has_parking":bool,"has_elevator":bool,"is_new_build":bool,"ownership_type":"String","condition":"String","highlights":["max 4"],"concerns":["max 4"],"neighborhood_outlook":"2 Saetze","investment_potential":1-5,"livability_score":1-5,"market_price_assessment":"Unter Markt/Marktgerecht/Ueber Markt/Nicht beurteilbar","scores":{"budget_fit":0-100,"location_fit":0-100,"size_fit":0-100,"amenities_fit":0-100,"investment_fit":0-100,"info_penalty":0,"overall":0-100},"verdict":"TOP-KANDIDAT/INTERESSANT/BEDINGT GEEIGNET/NICHT GEEIGNET","verdict_reason":"2-3 Saetze","besichtigung_fragen":["5 Fragen"],"verhandlung_tipps":"1-2 Saetze"}]

Sei STRENG aber FAIR. Wenig Infos = konservativ bewerten, aber nicht automatisch 0."""


def call_claude(listings_chunk, chunk_num, total_chunks):
    if not API_KEY:
        print("  KEIN API KEY!")
        return []

    headers = {
        "Content-Type": "application/json",
        "x-api-key": API_KEY,
        "anthropic-version": "2023-06-01",
    }

    payload = {
        "model": MODEL,
        "max_tokens": MAX_TOKENS,
        "system": DEEP_PROMPT,
        "messages": [{
            "role": "user",
            "content": f"Analysiere diese {len(listings_chunk)} Inserate (Chunk {chunk_num}/{total_chunks}):\n\n{json.dumps(listings_chunk, ensure_ascii=False)}"
        }]
    }

    for attempt in range(5):
        try:
            print(f"    API-Call (Versuch {attempt+1})...")
            resp = requests.post(API_URL, headers=headers, json=payload, timeout=120)

            if resp.status_code == 200:
                data = resp.json()
                text = "".join(b.get("text", "") for b in data.get("content", []) if b.get("type") == "text")
                clean = text.strip()
                if clean.startswith("```"): clean = clean.split("\n", 1)[1] if "\n" in clean else clean[3:]
                if clean.endswith("```"): clean = clean[:-3]
                clean = clean.strip()
                if not clean:
                    print("    Leere Antwort, retry...")
                    time.sleep(5)
                    continue
                parsed = json.loads(clean)
                result = parsed if isinstance(parsed, list) else [parsed]
                print(f"    OK: {len(result)} Inserate analysiert")
                return result

            elif resp.status_code == 429:
                wait = min(60, 10 * (attempt + 1))
                print(f"    Rate limit, warte {wait}s...")
                time.sleep(wait)
            elif resp.status_code == 529:
                print(f"    API ueberlastet, warte 20s...")
                time.sleep(20)
            else:
                print(f"    API Fehler {resp.status_code}: {resp.text[:200]}")
                time.sleep(10)

        except requests.RequestException as e:
            print(f"    Request-Fehler: {e}")
            time.sleep(10)
        except json.JSONDecodeError as e:
            print(f"    JSON-Fehler: {e}")
            print(f"    Response-Start: {text[:200] if 'text' in dir() else 'N/A'}...")
            time.sleep(5)

    print(f"    FEHLGESCHLAGEN nach 5 Versuchen")
    return []


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", "-i", default="data/latest_raw.json")
    parser.add_argument("--output", "-o", default="data/latest.json")
    args = parser.parse_args()

    if not API_KEY:
        print("KEIN ANTHROPIC_API_KEY!")
        sys.exit(1)

    print(f"\n{'='*55}")
    print(f"  KI-ANALYZER v2")
    print(f"{'='*55}")

    with open(args.input) as f:
        data = json.load(f)

    listings = data.get("listings", [])
    meta = data.get("meta", {})
    rejected_log = data.get("rejected_log", [])

    print(f"  {len(listings)} Inserate zu analysieren")

    if not listings:
        result = {"meta": {**meta, "analyzed_at": datetime.now().isoformat(), "analyzed_count": 0, "qualified_count": 0, "top_count": 0, "interesting_count": 0}, "listings": [], "rejected_log": rejected_log}
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(json.dumps(result, ensure_ascii=False, indent=2))
        print("  Keine Inserate.")
        return

    all_analyzed = []
    total_chunks = (len(listings) + CHUNK_SIZE - 1) // CHUNK_SIZE

    for i in range(0, len(listings), CHUNK_SIZE):
        chunk = listings[i:i + CHUNK_SIZE]
        chunk_num = i // CHUNK_SIZE + 1
        print(f"\n  Chunk {chunk_num}/{total_chunks}: {len(chunk)} Inserate")

        analyzed = call_claude(chunk, chunk_num, total_chunks)
        if analyzed:
            qualified = [a for a in analyzed if a.get("scores", {}).get("overall", 0) >= MIN_SCORE]
            all_analyzed.extend(qualified)
            print(f"    {len(qualified)} qualifiziert (>={MIN_SCORE})")
        else:
            print(f"    Keine Ergebnisse fuer diesen Chunk")

        if i + CHUNK_SIZE < len(listings):
            time.sleep(3)

    all_analyzed.sort(key=lambda x: x.get("scores", {}).get("overall", 0), reverse=True)

    result = {
        "meta": {
            **meta,
            "analyzed_at": datetime.now().isoformat(),
            "analyzed_count": len(listings),
            "qualified_count": len(all_analyzed),
            "top_count": len([a for a in all_analyzed if a.get("verdict") == "TOP-KANDIDAT"]),
            "interesting_count": len([a for a in all_analyzed if a.get("verdict") == "INTERESSANT"]),
        },
        "listings": all_analyzed,
        "rejected_log": rejected_log,
    }

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(json.dumps(result, ensure_ascii=False, indent=2))

    print(f"\n{'='*55}")
    print(f"  ERGEBNIS")
    print(f"{'='*55}")
    print(f"  Input:        {len(listings)}")
    print(f"  Qualifiziert: {len(all_analyzed)}")
    print(f"  Top:          {result['meta']['top_count']}")
    print(f"  Interessant:  {result['meta']['interesting_count']}")
    print(f"  Gespeichert:  {args.output}\n")

    for i, l in enumerate(all_analyzed[:10], 1):
        s = l.get("scores", {}).get("overall", "?")
        v = l.get("verdict", "?")
        p = l.get("price_mad")
        print(f"  {i}. [{s}] {v} | {f'{p:,} MAD' if p else '?'} | {l.get('neighborhood','?')}")
        print(f"     {l.get('title','?')[:70]}")


if __name__ == "__main__":
    main()
