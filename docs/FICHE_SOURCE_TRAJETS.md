# Fiche source — Trajets des taxis jaunes (NYC TLC)

## Identité

| Rubrique | Réponse |
|---|---|
| Nom de la source | Yellow Taxi Trip Records |
| Producteur des données | NYC Taxi and Limousine Commission (TLC) |
| Adresse (URL) | `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_AAAA-MM.parquet` (page : https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) |
| Accès (public, authentifié) | Public, en HTTPS, sans authentification |
| Format du fichier | Parquet (format en colonnes, qui contient le nom et le type de chaque colonne) |
| Fréquence de publication | Un fichier par mois |
| Délai entre la période couverte et la publication | Environ deux mois, d'après la page de la TLC |

## Volume mesuré

| Fichier | Taille | Nombre de lignes | Nombre de colonnes | Outil et commande utilisés |
|---|---|---|---|---|
| `yellow_tripdata_2025-01.parquet` | 59 158 238 octets (59,2 Mo) | 3 475 226 | 20 | taille : `ls -l data/` ; lignes : `COPY INTO` puis `COUNT(*)` dans Snowflake ; colonnes : `DESCRIBE` avec DuckDB |
| `yellow_tripdata_2025-02.parquet` | 60 343 088 octets (60,3 Mo) | 3 577 543 | 20 | taille : `LIST @NYC_TAXI.RAW.TLC_STAGE` ; lignes : `COUNT(*)` dans Snowflake |
| `yellow_tripdata_2025-03.parquet` | 69 964 752 octets (70,0 Mo) | 4 145 257 | 20 | taille : `LIST @NYC_TAXI.RAW.TLC_STAGE` ; lignes : `COUNT(*)` dans Snowflake |
| **Total** | **189,5 Mo** | **11 198 026** | | |

Commandes utilisées :

```bash
ls -l data/
python -c "import duckdb; print(duckdb.sql(\"DESCRIBE SELECT * FROM 'data/yellow_tripdata_2025-01.parquet'\"))"
```

```sql
LIST @NYC_TAXI.RAW.TLC_STAGE;
SELECT _source_file, COUNT(*) FROM NYC_TAXI.RAW.YELLOW_TRIPDATA GROUP BY 1 ORDER BY 1;
```

## Colonnes

Types lus dans le fichier de janvier avec `DESCRIBE` (DuckDB).

| Colonne | Type dans le fichier | Signification | Exemple de valeur |
|---|---|---|---|
| `VendorID` | INTEGER | Fournisseur du système d'enregistrement | 2 |
| `tpep_pickup_datetime` | TIMESTAMP | Date et heure de prise en charge | 2025-01-01 00:18:38 |
| `tpep_dropoff_datetime` | TIMESTAMP | Date et heure de dépose | 2025-01-01 00:26:59 |
| `passenger_count` | BIGINT | Nombre de passagers, saisi par le chauffeur | 1 |
| `trip_distance` | DOUBLE | Distance parcourue, en miles | 1.6 |
| `RatecodeID` | BIGINT | Type de tarif appliqué en fin de course | 1 |
| `store_and_fwd_flag` | VARCHAR | Course enregistrée dans le véhicule puis transmise plus tard (Y/N) | N |
| `PULocationID` | INTEGER | Zone de prise en charge (code de la liste des 265 zones) | 229 |
| `DOLocationID` | INTEGER | Zone de dépose | 237 |
| `payment_type` | BIGINT | Mode de paiement | 1 |
| `fare_amount` | DOUBLE | Prix au compteur (temps et distance), en dollars | 10.0 |
| `extra` | DOUBLE | Suppléments divers (heures de pointe, nuit) | 3.5 |
| `mta_tax` | DOUBLE | Taxe MTA | 0.5 |
| `tip_amount` | DOUBLE | Pourboire, enregistré seulement pour les paiements par carte | 3.0 |
| `tolls_amount` | DOUBLE | Péages | 0.0 |
| `improvement_surcharge` | DOUBLE | Surtaxe d'amélioration | 1.0 |
| `total_amount` | DOUBLE | Montant total payé (hors pourboire en espèces) | 18.0 |
| `congestion_surcharge` | DOUBLE | Surtaxe de congestion de l'État de New York | 2.5 |
| `Airport_fee` | DOUBLE | Frais de prise en charge aux aéroports | 0.0 |
| `cbd_congestion_fee` | DOUBLE | Péage de congestion du centre de Manhattan, en vigueur depuis janvier 2025 | 0.75 |

## Codes

D'après le dictionnaire de données de la TLC ; ce sont aussi les valeurs des tables de codes de la couche STAGING (`PAYMENT_TYPE_CODES`, `RATE_CODE_CODES`, `VENDOR_CODES`).

| Colonne | Valeur | Signification |
|---|---|---|
| `VendorID` | 1 | Creative Mobile Technologies |
| | 2 | Curb Mobility |
| | 6 | Myle Technologies |
| | 7 | Helix |
| `RatecodeID` | 1 | Tarif standard |
| | 2 | Aéroport JFK |
| | 3 | Aéroport de Newark |
| | 4 | Nassau ou Westchester |
| | 5 | Tarif négocié |
| | 6 | Course partagée |
| | 99 | Inconnu |
| `payment_type` | 0 | Tarif flexible (Flex Fare trip) |
| | 1 | Carte bancaire (Credit card) |
| | 2 | Espèces (Cash) |
| | 3 | Gratuit (No charge) |
| | 4 | Litige (Dispute) |
| | 5 | Inconnu (Unknown) |
| | 6 | Course annulée (Voided trip) |
| `store_and_fwd_flag` | Y | Course stockée dans le véhicule puis transmise plus tard |
| | N | Course transmise directement |

## Ce qui a surpris

Mesuré sur les trois mois avec `MART_DATA_QUALITY`, et recoupé pour janvier par un comptage direct sur `INT_TRIPS__FLAGGED` (mêmes chiffres, ligne par ligne).

| Raison de rejet | Janvier | Février | Mars |
|---|---|---|---|
| Valide | 3 251 337 (93,6 %) | 3 305 246 (92,4 %) | 3 825 795 (92,3 %) |
| Montant négatif ou nul | 130 112 (3,7 %) | 166 569 (4,7 %) | 192 454 (4,6 %) |
| Distance nulle ou supérieure à 100 miles | 90 327 (2,6 %) | 99 232 (2,8 %) | 103 121 (2,5 %) |
| Durée nulle ou négative | 2 051 | 5 164 | 22 280 |
| Durée supérieure à 3 heures | 1 377 | 1 301 | 1 574 |
| Prise en charge hors du mois du fichier | 22 | 31 | 33 |

- Entre 6,4 % et 7,7 % des trajets de chaque mois sont écartés par les règles de qualité. Les **montants négatifs ou nuls** et les **distances aberrantes** en représentent la quasi-totalité.
- Les **durées nulles ou négatives** (dépose enregistrée avant ou au moment de la prise en charge) sont dix fois plus nombreuses en mars (22 280) qu'en janvier (2 051) : un point à surveiller sur les prochains mois.
- Chaque fichier mensuel contient quelques dizaines de courses **d'un autre mois**, ce qui confirme qu'il ne faut pas se fier au nom du fichier pour dater un trajet.
- La somme des trajets valides des trois mois (10 382 378) est égale au nombre de lignes de la table de faits `FCT_TRIPS`.