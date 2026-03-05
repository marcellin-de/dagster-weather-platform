# LinkedIn Post — Retour d'expérience Dagster

> **Image à joindre au post** : [`docs/Global_Asset_Lineage.svg`](Global_Asset_Lineage.svg)

---

🌦️ **Retour d'expérience : orchestrer un pipeline weather end-to-end avec Dagster**

J'ai récemment construit une plateforme de données météo complète — de l'ingestion API jusqu'aux prévisions ML et aux dashboards — le tout orchestré par Dagster.

Voici ce que j'en retiens 👇

**🔧 La stack**

• Dagster — orchestration, scheduling, sensors, quality checks
• dlt — ingestion de l'API Open-Meteo (aucune clé API requise)
• DuckDB — stockage analytique local ultra-rapide
• dbt — transformations SQL, data contracts, tests
• Great Expectations — quality gates sur les données brutes et enrichies
• Hugging Face — enrichissement IA (labellisation météo automatique)
• scikit-learn (Ridge) — modèle de prévision de température à 7 jours
• MLflow — tracking des expérimentations ML
• Evidence — dashboards analytiques
• Docker Compose — stack complète reproductible en un seul `make compose-up`

**🏗️ L'architecture du pipeline**

```
Open-Meteo API
  → dlt (raw_weather.open_meteo_hourly)
  → dbt staging (analytics.stg_open_meteo_hourly)
  → dbt mart (analytics.mart_weather_daily)
  → Enrichissement IA (analytics.weather_daily_enriched)
  → ML training (Ridge, MAE gate) + forecast 7j (analytics.weather_forecast_7d)
  → Evidence dashboards
```

Le lineage complet des assets est visible sur l'image ci-dessous ⬇️

**💡 Ce que Dagster m'a apporté**

1. **Asset-centric thinking** — Modéliser le pipeline comme un graphe d'assets plutôt qu'un DAG de tâches change profondément la façon de raisonner sur les données. Chaque table, chaque modèle ML est un asset observable et traçable.

2. **Orchestration déclarative** — Un schedule horaire pour l'ingestion, un schedule quotidien pour le training ML, et un sensor qui déclenche automatiquement le re-training après chaque ingestion réussie. Tout ça en quelques lignes de Python.

3. **Qualité intégrée** — Les asset checks (Great Expectations + seuil MAE du modèle) vivent dans le même graphe que les assets. Pas de pipeline parallèle de validation : la qualité fait partie du flux.

4. **dbt natif** — L'intégration `dagster-dbt` permet de voir les modèles dbt comme des assets Dagster à part entière, avec leurs data contracts et leurs tests.

5. **Observabilité unifiée** — Dagster UI + MLflow + Evidence dans le même stack Docker Compose. Data quality, model quality et experiment tracking au même endroit.

6. **Itération rapide en local** — `uv sync && make up` et tout tourne. Pas besoin de cloud, pas besoin de conteneur pour développer. Le passage en Docker Compose ne demande qu'un `make compose-up`.

**⚠️ Points d'attention**

• La courbe d'apprentissage des concepts Dagster (Definitions, resources, sensors) demande un investissement initial, mais le retour est rapide.
• DuckDB en local est parfait pour le prototypage, mais il faut planifier la migration vers un warehouse pour la production multi-utilisateurs.
• L'enrichissement IA via Hugging Face nécessite un token et peut varier selon le modèle — prévoir des fallbacks robustes.

**🎯 En résumé**

Dagster est devenu mon orchestrateur de référence pour les projets data+ML. La philosophie asset-first, l'intégration native de dbt et dlt, et l'observabilité intégrée en font un outil redoutablement efficace pour construire des pipelines de bout en bout.

Le code est open source : 🔗 [dagster-weather-intelligence-platform](https://github.com/marcellin-de/dagster-weather-intelligence-platform)

---

#Dagster #DataEngineering #MLOps #dbt #DuckDB #MLflow #Python #WeatherData #OpenSource #RetourDExperience
