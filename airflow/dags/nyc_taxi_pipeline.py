"""Pipeline NYC Yellow Taxi : chargement mensuel dans la couche RAW.

Une exécution traite UN mois : celui de sa date logique.
"""
from datetime import datetime, timedelta
from pathlib import Path

import requests
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, task

CONN_ID = "snowflake_nyc_taxi"
URL_TRAJETS = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{mois}.parquet"
STAGE = "@NYC_TAXI.RAW.TLC_STAGE"
DOSSIER_TEMP = Path("/tmp/nyc_taxi")


@dag(
    dag_id="nyc_taxi_pipeline",
    schedule="@monthly",                 # une exécution par mois
    start_date=datetime(2025, 1, 1),     # premier mois traité
    end_date=datetime(2025, 3, 31),      # dernier mois traité
    catchup=True,                        # rejouer les mois passés
    max_active_runs=1,                   # un mois à la fois, dans l'ordre
    default_args={
        "retries": 2,                          # relances automatiques
        "retry_delay": timedelta(minutes=5),
    },
    tags=["nyc_taxi"],
)
def nyc_taxi_pipeline():

    @task
    def calculer_mois(**context):
        """Déduit le mois traité de la date logique (et non de la date du jour)."""
        return context["logical_date"].strftime("%Y-%m")

    @task
    def verifier_publication(mois):
        """Échoue si le fichier du mois n'est pas encore publié par la TLC."""
        url = URL_TRAJETS.format(mois=mois)
        reponse = requests.head(url, timeout=30)
        if reponse.status_code != 200:
            raise ValueError(f"Fichier non publié (HTTP {reponse.status_code}) : {url}")
        return url

    @task
    def telecharger(url):
        """Télécharge le fichier par blocs dans un dossier temporaire du conteneur."""
        DOSSIER_TEMP.mkdir(parents=True, exist_ok=True)
        chemin = DOSSIER_TEMP / url.rsplit("/", 1)[-1]
        with requests.get(url, stream=True, timeout=300) as reponse:
            reponse.raise_for_status()
            with open(chemin, "wb") as fichier:
                for bloc in reponse.iter_content(chunk_size=1024 * 1024):
                    fichier.write(bloc)
        return str(chemin)

    @task
    def deposer_sur_stage(chemin):
        """PUT à la racine du stage, sans compression (nom exact) ni écrasement."""
        hook = SnowflakeHook(snowflake_conn_id=CONN_ID)
        resultat = hook.run(
            f"PUT 'file://{chemin}' {STAGE} AUTO_COMPRESS = FALSE OVERWRITE = FALSE",
            handler=lambda curseur: curseur.fetchall(),
        )
        print(resultat)
        return Path(chemin).name

    @task
    def copier_dans_raw(nom_fichier):
        """COPY INTO avec les mêmes options qu'au jour 2 : rejouable, sans doublon."""
        hook = SnowflakeHook(snowflake_conn_id=CONN_ID)
        resultat = hook.run(
            f"""
            COPY INTO NYC_TAXI.RAW.YELLOW_TRIPDATA
            FROM {STAGE}
            FILES = ('{nom_fichier}')
            FILE_FORMAT = (FORMAT_NAME = 'NYC_TAXI.RAW.FF_PARQUET')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            INCLUDE_METADATA = (
                _source_file = METADATA$FILENAME,
                _loaded_at = METADATA$START_SCAN_TIME
            )
            ON_ERROR = ABORT_STATEMENT
            """,
            handler=lambda curseur: curseur.fetchall(),
        )
        print(resultat)

    # Ordre des tâches : chaque tâche reçoit le résultat de la précédente
    mois = calculer_mois()
    url = verifier_publication(mois)
    chemin = telecharger(url)
    nom_fichier = deposer_sur_stage(chemin)
    copier_dans_raw(nom_fichier)


nyc_taxi_pipeline()