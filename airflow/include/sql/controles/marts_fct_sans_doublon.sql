-- Aucun trajet en double dans la table de faits pour le mois traité.
SELECT
    COUNT(*) > 0,
    COUNT(*) = COUNT(DISTINCT trip_sk)
FROM NYC_TAXI.MARTS.FCT_TRIPS
WHERE source_file_month = '{{ ds }}'::date;