"""Prouve qu'Airflow se connecte à Snowflake avec l'utilisateur de service."""
from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag


@dag(
    dag_id="test_connexion_snowflake",
    schedule=None,                 # aucune planification : lancé à la main
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["test"],
)
def test_connexion_snowflake():
    SQLExecuteQueryOperator(
        task_id="qui_suis_je",
        conn_id="snowflake_nyc_taxi",   # la connexion définie dans .env
        sql="SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()",
        show_return_value_in_logs=True,  # affiche le résultat dans le journal
    )


test_connexion_snowflake()