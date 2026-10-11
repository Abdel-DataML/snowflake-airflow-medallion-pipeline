-- La part de trajets écartés du mois traité reste sous le seuil toléré.
-- Une hausse brutale signale un fichier anormal ou une règle cassée.
SELECT
    COUNT(*) > 0,
    COUNT_IF(rejection_reason IS NOT NULL) / NULLIF(COUNT(*), 0)
        <= {{ params.max_taux_ecartes }}
FROM NYC_TAXI.INTERMEDIATE.INT_TRIPS__FLAGGED
WHERE source_file_month = '{{ ds }}'::date;