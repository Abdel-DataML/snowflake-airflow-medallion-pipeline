"""Pipeline NYC Yellow Taxi : chargement mensuel, transformations et contrôles.

Une exécution traite UN mois : celui de sa date logique.
"""
from datetime import datetime, timedelta
from pathlib import Path

import requests
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLExecuteQueryOperator,
)
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import TaskGroup, dag, task

CONN_ID = "snowflake_nyc_taxi"
URL_TRAJETS = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{mois}.parquet"
STAGE = "@NYC_TAXI.RAW.TLC_STAGE"
DOSSIER_TEMP = Path("/tmp/nyc_taxi")


def sql(task_id, fichier):
    """Une tâche = un fichier SQL fourni, exécuté instruction par instruction."""
    return SQLExecuteQueryOperator(
        task_id=task_id,
        conn_id=CONN_ID,
        sql=fichier,
        split_statements=True,
    )


def controle(task_id, fichier):
    """Un contrôle : échoue si une valeur de la ligne renvoyée est fausse."""
    return SQLCheckOperator(
        task_id=task_id,
        conn_id=CONN_ID,
        sql=fichier,
        retries=0,
    )


@dag(
    dag_id="nyc_taxi_pipeline",
    schedule="@monthly",
    start_date=datetime(2025, 1, 1),
    end_date=datetime(2025, 3, 31),
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    template_searchpath=["/usr/local/airflow/include/sql"],
    params={
        "max_trip_distance_miles": 100,
        "max_trip_duration_min": 180,
        "start_month": "2025-01-01",
        "end_month": "2025-04-01",
        "max_taux_ecartes": 0.15,  # au-delà de 15 % de trajets écartés, on s'arrête
    },
    tags=["nyc_taxi"],
)
def nyc_taxi_pipeline():

    # Chargement (jour 3)
    with TaskGroup("chargement") as chargement:

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
            print(hook.run(
                f"PUT 'file://{chemin}' {STAGE} AUTO_COMPRESS = FALSE OVERWRITE = FALSE",
                handler=lambda curseur: curseur.fetchall(),
            ))
            return Path(chemin).name

        @task
        def copier_dans_raw(nom_fichier):
            """COPY INTO avec les mêmes options qu'au jour 2 : rejouable, sans doublon."""
            hook = SnowflakeHook(snowflake_conn_id=CONN_ID)
            print(hook.run(
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
            ))

        copier_dans_raw(deposer_sur_stage(telecharger(verifier_publication(calculer_mois()))))

    # Contrôle
    controle_raw = controle("controle_raw_mois_charge", "controles/raw_mois_charge.sql")

    # Tables alimentées mois par mois
    creer_tables = sql("creer_tables", "00_tables.sql")

    # STAGING
    with TaskGroup("staging") as staging:
        sql("codes_tlc", "staging/codes_tlc.sql")
        sql("stg_tlc__yellow_trips", "staging/stg_tlc__yellow_trips.sql")
        sql("stg_tlc__taxi_zones", "staging/stg_tlc__taxi_zones.sql")

    # INTERMEDIATE
    with TaskGroup("intermediate") as intermediate:
        flagged = sql("int_trips__flagged", "intermediate/int_trips__flagged.sql")
        enriched = sql("int_trips__enriched", "intermediate/int_trips__enriched.sql")
        controle_ecartes = controle(
            "controle_taux_ecartes", "controles/intermediate_taux_ecartes.sql"
        )
        flagged >> controle_ecartes >> enriched

    # MARTS
    with TaskGroup("marts") as marts:
        dims = [
            sql("dim_date", "marts/dim_date.sql"),
            sql("dim_zone", "marts/dim_zone.sql"),
            sql("dim_payment_type", "marts/dim_payment_type.sql"),
            sql("dim_rate_code", "marts/dim_rate_code.sql"),
            sql("dim_vendor", "marts/dim_vendor.sql"),
        ]
        fct = sql("fct_trips", "marts/fct_trips.sql")
        sql("mart_data_quality", "marts/mart_data_quality.sql")
        agregats = [
            sql("mart_daily_revenue", "marts/mart_daily_revenue.sql"),
            sql("mart_zone_hourly_demand", "marts/mart_zone_hourly_demand.sql"),
        ]
        for dim in dims:
            dim >> agregats
        controle_doublons = controle(
            "controle_fct_sans_doublon", "controles/marts_fct_sans_doublon.sql"
        )
        fct >> controle_doublons >> agregats

    # Ordre des couches
    chargement >> controle_raw >> creer_tables >> staging >> intermediate >> marts


nyc_taxi_pipeline()