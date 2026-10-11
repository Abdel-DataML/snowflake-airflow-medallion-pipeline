# Réponse à la direction d'Hudson Cab Partners

## La question

Où et quand la demande de taxis jaunes est-elle la plus forte à New York, et combien rapporte un trajet selon la zone, l'heure et le mode de paiement ?

## La requête

La demande est mesurée en **trajets par jour** pour chaque créneau (zone de prise en charge × heure × type de jour), et non en trajets au total : la période compte environ 64 jours de semaine pour 26 jours de week-end, et un total avantagerait mécaniquement la semaine.

```sql
SELECT
    pickup_zone_name                         AS zone,
    pickup_borough                           AS arrondissement,
    pickup_hour                              AS heure,
    IFF(is_weekend, 'week-end', 'semaine')   AS type_de_jour,
    avg_trips_per_day                        AS trajets_par_jour,
    avg_revenue_per_trip                     AS revenu_moyen_par_trajet,
    revenue_per_hour_driven                  AS revenu_par_heure_roulee
FROM NYC_TAXI.MARTS.MART_ZONE_HOURLY_DEMAND
ORDER BY avg_trips_per_day DESC
LIMIT 10;
```

## Le résultat : les 10 premières lignes

| Zone | Arrondissement | Heure | Type de jour | Trajets par jour | Revenu moyen par trajet ($) | Revenu par heure roulée ($) |
|---|---|---|---|---|---|---|
| East Village | Manhattan | 0 h | week-end | 738,0 | 22,84 | 113,75 |
| East Village | Manhattan | 1 h | week-end | 725,7 | 22,11 | 109,36 |
| Midtown Center | Manhattan | 18 h | semaine | 596,4 | 24,98 | 106,77 |
| Midtown Center | Manhattan | 17 h | semaine | 571,9 | 30,06 | 115,24 |
| West Village | Manhattan | 0 h | week-end | 559,1 | 23,14 | 108,21 |
| Midtown Center | Manhattan | 20 h | semaine | 534,5 | 22,81 | 107,93 |
| West Village | Manhattan | 1 h | week-end | 519,4 | 22,57 | 105,77 |
| East Village | Manhattan | 2 h | week-end | 519,0 | 22,00 | 117,25 |
| Midtown Center | Manhattan | 19 h | semaine | 512,1 | 24,14 | 113,12 |
| Midtown Center | Manhattan | 21 h | semaine | 494,3 | 23,01 | 108,84 |

### Revenu par mode de paiement

```sql
SELECT
    p.payment_type_label                 AS mode_de_paiement,
    COUNT(*)                             AS nb_trajets,
    ROUND(AVG(f.total_amount), 2)        AS revenu_moyen_par_trajet
FROM NYC_TAXI.MARTS.FCT_TRIPS f
LEFT JOIN NYC_TAXI.MARTS.DIM_PAYMENT_TYPE p
       ON p.payment_type_key = f.payment_type_key
GROUP BY 1
ORDER BY 2 DESC;
```

| Mode de paiement | Trajets | Revenu moyen par trajet ($) |
| Carte bancaire (Credit card) | 7 397 954 | 28,32 |
| Tarif flexible (Flex Fare trip) | 1 767 528 | 24,35 |
| Espèces (Cash) | 1 063 448 | 23,70 |
| Litige (Dispute) | 115 468 | 35,21 |
| Gratuit (No charge) | 37 980 | 27,35 |

## Ce qu'il faut en retenir

1. La demande la plus forte se concentre à Manhattan, et suit deux rythmes : les **nuits de week-end** dans les quartiers de sortie (East Village et West Village, de minuit à 3 h), avec jusqu'à 738 prises en charge par jour sur un seul créneau horaire, et la **sortie des bureaux en semaine** à Midtown Center (de 17 h à 22 h).
2. Dans ces créneaux, un trajet rapporte en moyenne entre 22 et 30 $, et une heure de conduite avec client entre 105 et 117 $ ; le créneau de 17 h en semaine à Midtown offre le trajet le mieux payé du top 10 (30 $).
3. Plus de sept trajets sur dix (71 %) sont payés par carte, et rapportent en moyenne 28,32 $, contre 23,70 $ en espèces : environ 4,60 $ de plus par trajet, en bonne partie parce que les pourboires ne sont enregistrés que pour les paiements par carte.

## Les limites

- **Période** : janvier à mars 2025 seulement, soit l'hiver. La saisonnalité (été, fêtes) n'est pas couverte.
- **Trajets écartés** : 10 382 378 trajets valides sur 11 198 026 lignes chargées. Les autres ont été écartés par les règles de qualité (durée nulle ou supérieure à 3 h, distance nulle ou supérieure à 100 miles, montant négatif, date hors du mois, zone inconnue) ou parce qu'ils étaient en double.
- **Montants** : le revenu correspond au montant total payé par le client, qui inclut taxes, péages, suppléments et pourboires ; ce n'est pas la recette nette du chauffeur. Les pourboires en espèces ne sont pas enregistrés.
- **Revenu par heure roulée** : calculé sur le temps passé avec un client ; il ne tient pas compte du temps passé à vide entre deux courses.
- **Heure** : il s'agit de l'heure de prise en charge du client.