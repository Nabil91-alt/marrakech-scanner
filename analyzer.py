#!/usr/bin/env python3
"""
MARRAKECH KI-ANALYZER
=====================
Nimmt die Scraper-Ergebnisse und schickt sie an Claude zur Tiefenanalyse.
Nur qualifizierte Leads (Score ≥60) kommen ins finale JSON.

Nutzung:
    python analyzer.py --input data/latest_raw.json --output data/latest.json
"""

import json, os, sys, time, argparse
from pathlib import Path

try:
    import requests
except ImportError:
    print("❌ requests nicht installiert. Bitte: pip install requests")
    sys.exit(1)

API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MODEL = "claude-sonnet-4-20250514"
API_URL = "https://api.anthropic.com/v1/messages"
MAX_TOKENS = 4096
CHUNK_SIZE = 5  # Inserate pro API-Call
MIN_SCORE = 60

DEEP_PROMPT = """Du bist ein erfahrener Immobilien-Analyst für Marrakesch. Du erhältst vorgeprüfte Angebote die bereits harte Ausschlusskriterien bestanden haben (Budget, Zimmer, Lage, No-Gos).

KÄUFER-PROFIL:
- Budget sweet spot: 1.200.000–1.800.000 MAD (100.000–200.000€)
- Nutzung: Eigennutzung/Feriendomizil + Langzeit-Investment + potentieller Hauptwohnsitz
- Min. 3 Zimmer, idealerweise ≥90m²
- Bevorzugte Lagen im Speckgürtel mit Wachstumspotenzial
- Must-haves: Terrasse, Pool, Parkplatz, Aufzug/Neubau (je mehr desto besser)

BEWERTUNGSLOGIK:

BUDGET (25%):
- 100 = 1.200.000–1.600.000 MAD (sweet spot)
- 80 = 1.600.000–1.900.000 oder 1.000.000–1.200.000
- 60 = 1.900.000–2.100.000
- 40 = 2.100.000–2.300.000 oder unter 1.000.000

LAGE (25%):
- 100 = Targa, Agdal (etabliert + wachsend)
- 90 = Palmeraie, Route de l'Ourika (Premium-Potenzial)
- 80 = Izdihar, Massira, M'Hamid (solide Infrastruktur)
- 70 = Tamansourt, Route de Casablanca, Route de Fès
- 60 = Amerchich, Tassoultant (aufstrebend)
- 40 = Medina, Guéliz (nicht Zielgebiet)

GRÖSSE (15%):
- 100 = ≥3 Zimmer UND ≥100m²
- 80 = ≥3 Zimmer UND ≥80m²
- 60 = 3 Zimmer UND 60-80m²
- 40 = kleiner

AUSSTATTUNG (20%) — je 25 Punkte für:
- Terrasse/Balkon/Dachterrasse vorhanden
- Pool (privat oder Anlage)
- Parkplatz/Garage
- Aufzug ODER bestätigter Neubau

INVESTMENT (15%):
- Basierend auf: m²-Preis vs. Durchschnitt, Lage-Entwicklung, Neubau-Status, Bauqualität

OVERALL = gewichteter Durchschnitt, MINUS Risiko-Abzüge:
- Eigentum "Unbekannt" (nicht bestätigt Titre Foncier): -10
- Zustand "Unbekannt": -5
- Wenig Infos verfügbar: -10

Antworte NUR mit einem JSON-Array (keine Backticks, kein Markdown). Pro Angebot:
[{
  "id": "gleiche id wie im Input",
  "title": "Titel auf Deutsch",
  "source": "Portal-Name",
  "url": "URL oder null",
  "price_mad": Zahl oder null,
  "price_eur": Zahl oder null,
  "area_sqm": Zahl oder null,
  "rooms": Zahl oder null,
  "bedrooms": Zahl oder null,
  "bathrooms": Zahl oder null,
  "floor": "Etage oder null",
  "neighborhood": "Viertel",
  "property_type": "Apartment/Villa",
  "price_per_sqm_mad": Zahl oder null,
  "has_terrace": true/false/null,
  "has_pool": true/false/null,
  "has_parking": true/false/null,
  "has_elevator": true/false/null,
  "is_new_build": true/false/null,
  "ownership_type": "Titre Foncier/Melkia/Unbekannt",
  "condition": "Neu/Renoviert/Gut/Unbekannt",
  "highlights": ["max 4 echte Stärken"],
  "concerns": ["max 4 echte Risiken — sei ehrlich!"],
  "neighborhood_outlook": "2 Sätze zur Lage-Entwicklung und Perspektive",
  "investment_potential": 1-5,
  "livability_score": 1-5,
  "market_price_assessment": "Unter Markt/Marktgerecht/Über Markt/Nicht beurteilbar",
  "scores": {
    "budget_fit": 0-100,
    "location_fit": 0-100,
    "size_fit": 0-100,
    "amenities_fit": 0-100,
    "investment_fit": 0-100,
    "info_penalty": 0 oder negativer Wert,
    "overall": 0-100
  },
  "verdict": "TOP-KANDIDAT/INTERESSANT/BEDINGT GEEIGNET/NICHT GEEIGNET",
  "verdict_reason": "2-3 Sätze ehrliche Einschätzung auf Deutsch",
  "besichtigung_fragen": ["5 wichtige Fragen bei Besichtigung"],
  "verhandlung_tipps": "1-2 Sätze Verhandlungsstrategie"
}]

QUALITÄTSREGELN:
- Vergib NICHT leichtfertig hohe Scores. Ein "TOP-KANDIDAT" muss wirklich überdurchschnittlich sein.
- Sei ehrlich bei Concerns — jedes Angebot hat Schwächen.
- Wenn zu wenig Infos da sind, bewerte konservativ nach unten.
- ≥80 = TOP-KANDIDAT, ≥60 = INTERESSANT, ≥40 = BEDINGT GEEIGNET, <40 = NICHT GEEIGNET
"""


def call_claude(listings_chunk):
    """Sendet eine Gruppe von Inseraten an Claude zur Analyse."""
    if not API_KEY:
        print("  ❌ Kein ANTHROPIC_API_KEY gesetzt!")
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
        "messages": [
            {
                "role": "user",
                "content": f"Tiefenanalyse für diese vom Scraper extrahierten Inserate:\n\n{json.dumps(listings_chunk, ensure_ascii=False)}"
            }
        ]
    }

    for attempt in range(3):
        try:
            resp = requests.post(API_URL, headers=headers, json=payload, timeout=60)

            if resp.status_code == 200:
                data = resp.json()
                text = ""
                for block in data.get("content", []):
                    if block.get("type") == "text":
                        text += block.get("text", "")

                # Parse JSON
                clean = text.strip()
                if clean.startswith("```"):
                    clean = clean.split("\n", 1)[1] if "\n" in clean else clean[3:]
                if clean.endswith("```"):
                    clean = clean[:-3]
                clean = clean.strip()

                parsed = json.loads(clean)
                return parsed if isinstance(parsed, list) else [parsed]

            elif resp.status_code == 429:
                print(f"  ⏳ Rate limit, warte 30s... (Versuch {attempt + 1})")
                time.sleep(30)
            elif resp.status_code == 529:
                print(f"  ⏳ API überlastet, warte 15s... (Versuch {attempt + 1})")
                time.sleep(15)
            else:
                print(f"  ⚠ API Fehler {resp.status_code}: {resp.text[:200]}")
                return []

        except requests.RequestException as e:
            print(f"  ⚠ Request-Fehler (Versuch {attempt + 1}): {e}")
            time.sleep(5)
        except json.JSONDecodeError as e:
            print(f"  ⚠ JSON-Parse-Fehler: {e}")
            print(f"    Response: {text[:300]}...")
            return []

    return []


def analyze(input_path, output_path):
    """Hauptfunktion: Lädt Scraper-Daten, analysiert, speichert."""

    print(f"\n{'='*55}")
    print(f"  🔬 MARRAKECH KI-ANALYZER")
    print(f"{'='*55}")

    # Laden
    print(f"\n  📂 Lade: {input_path}")
    with open(input_path) as f:
        data = json.load(f)

    listings = data.get("listings", [])
    meta = data.get("meta", {})
    rejected_log = data.get("rejected_log", [])

    if not listings:
        print("  ⚠ Keine Inserate zum Analysieren.")
        # Leeres Ergebnis speichern
        result = {
            "meta": {**meta, "analyzed_at": datetime.now().isoformat() if 'datetime' in dir() else "", "analyzed_count": 0, "qualified_count": 0},
            "listings": [],
            "rejected_log": rejected_log,
        }
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        Path(output_path).write_text(json.dumps(result, ensure_ascii=False, indent=2))
        return

    print(f"  📋 {len(listings)} Inserate gefunden")
    print(f"  🔬 Starte KI-Tiefenanalyse in Chunks von {CHUNK_SIZE}...\n")

    all_analyzed = []
    total_chunks = (len(listings) + CHUNK_SIZE - 1) // CHUNK_SIZE

    for i in range(0, len(listings), CHUNK_SIZE):
        chunk = listings[i:i + CHUNK_SIZE]
        chunk_num = i // CHUNK_SIZE + 1
        print(f"  📊 Chunk {chunk_num}/{total_chunks}: {len(chunk)} Inserate analysieren...")

        analyzed = call_claude(chunk)

        if analyzed:
            qualified = [a for a in analyzed if a.get("scores", {}).get("overall", 0) >= MIN_SCORE]
            all_analyzed.extend(qualified)
            print(f"     → {len(analyzed)} analysiert, {len(qualified)} qualifiziert (Score ≥{MIN_SCORE})")
        else:
            print(f"     → ⚠ Keine Ergebnisse für diesen Chunk")

        # Pause zwischen Chunks
        if i + CHUNK_SIZE < len(listings):
            time.sleep(2)

    # Sortieren nach Score
    all_analyzed.sort(key=lambda x: x.get("scores", {}).get("overall", 0), reverse=True)

    # Ergebnis zusammenstellen
    from datetime import datetime
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

    # Speichern
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_text(json.dumps(result, ensure_ascii=False, indent=2))

    # Summary
    print(f"\n{'='*55}")
    print(f"  📊 ERGEBNIS")
    print(f"{'='*55}")
    print(f"  📋 Vom Scraper:    {len(listings)} Inserate")
    print(f"  🔬 Analysiert:     {len(listings)}")
    print(f"  ✅ Qualifiziert:   {len(all_analyzed)} (Score ≥{MIN_SCORE})")
    print(f"  ★  Top-Kandidaten: {result['meta']['top_count']}")
    print(f"  ◆  Interessant:    {result['meta']['interesting_count']}")
    print(f"\n  💾 Gespeichert: {output_path}")

    if all_analyzed:
        print(f"\n  🏡 TOP LEADS:")
        for i, lead in enumerate(all_analyzed[:10], 1):
            score = lead.get("scores", {}).get("overall", "?")
            verdict = lead.get("verdict", "?")
            price = lead.get("price_mad")
            price_str = f"{price:>12,} MAD" if price else "   unbekannt"
            nb = lead.get("neighborhood", "?")[:20]
            print(f"  {i:>3}. [{score:>3}] {verdict:<18} | {price_str} | {nb}")
            print(f"       {lead.get('title', '?')[:65]}")


def main():
    parser = argparse.ArgumentParser(description="Marrakech KI-Analyzer")
    parser.add_argument("--input", "-i", default="data/latest_raw.json")
    parser.add_argument("--output", "-o", default="data/latest.json")
    args = parser.parse_args()

    if not API_KEY:
        print("❌ ANTHROPIC_API_KEY Umgebungsvariable nicht gesetzt!")
        print("   Setze sie mit: export ANTHROPIC_API_KEY=sk-ant-...")
        sys.exit(1)

    analyze(args.input, args.output)


if __name__ == "__main__":
    main()
