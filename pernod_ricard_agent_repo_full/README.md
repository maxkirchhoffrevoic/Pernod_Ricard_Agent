# Pernod Ricard Agent Repo (MVP)

Dieses Repo enthält ein kleines, lauffähiges Starter-MVP für einen KI-Agenten, der öffentlich verfügbare Informationen zu Pernod Ricard sammelt, extrahiert und in eine Postgres-Datenbank schreibt. Außerdem gibt es eine einfache Streamlit-UI, die die gesammelten Quellen anzeigt.

## Quickstart
1. Kopiere das Repo lokal.
2. Erstelle `.env` aus `.env.example` und fülle `DATABASE_URL` + `OPENAI_API_KEY`.
3. `pip install -r requirements.txt`
4. Stelle sicher, dass Postgres läuft und erreichbar ist (z. B. Supabase / ElephantSQL / local Postgres).
5. `python db.py` -> initialisiert DB (Postgres muss laufen).
6. `python scripts/run_agent.py` -> crawlt Seed-URLs und befüllt DB.
7. `streamlit run app.py` -> öffne http://localhost:8501

## Hinweis
Dieses Projekt ist ein MVP. Für Produktion: robots.txt-Respect, Rate-Limiting, bessere Published-Date-Erkennung, robuste Fehlerbehandlung, Testabdeckung, Secrets-Management.
