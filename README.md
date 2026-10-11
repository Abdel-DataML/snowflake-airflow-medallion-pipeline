# Pipeline médaillon NYC Yellow Taxi : Snowflake et Airflow

Hudson Cab Partners exploite 180 taxis jaunes à New York. Son analyste préparait chaque mois ses chiffres à la main, dans un notebook qui plantait faute de mémoire, sans traçabilité ni possibilité de rejouer le calcul.

Ce projet automatise tout le parcours de la donnée : un entrepôt **Snowflake** organisé en couches (RAW, STAGING, INTERMEDIATE, MARTS), et un pipeline **Airflow** qui, chaque mois, charge le fichier public de la TLC, exécute les transformations SQL dans le bon ordre et contrôle la qualité des données.

Il répond de façon fiable et rejouable à la question : **où et quand la demande de taxis jaunes est-elle la plus forte, et combien rapporte un trajet ?** La réponse est dans [`docs/REPONSE.md`](docs/REPONSE.md).

Périmètre : taxis jaunes, janvier à mars 2025 (11 198 026 trajets chargés, 10 382 378 trajets valides).

## Architecture

![Schéma du pipeline](docs/architecture.png)

| Partie | Rôle |
|---|---|
| **Sources TLC** | Un fichier Parquet de trajets par mois (environ 3,5 millions de lignes, 60 Mo) et un CSV de 265 zones, publics, en HTTPS |
| **Script Python** (`ingestion/`) | Charge un mois ou les zones : téléchargement, dépôt sur le stage (`PUT`), copie dans RAW (`COPY INTO`). Sert aussi à charger les zones, une seule fois, hors Airflow |
| **Airflow** (`airflow/`) | Le chef d'orchestre : une exécution par mois, qui charge le fichier du mois, puis enchaîne les transformations et les contrôles |
| **Stage** `RAW.TLC_STAGE` | Zone de dépôt des fichiers dans Snowflake, avant leur copie dans une table |
| **RAW** | Copie fidèle des fichiers, plus deux colonnes techniques : le fichier d'origine (`_source_file`) et la date de chargement (`_loaded_at`) |
| **STAGING** | Colonnes renommées et typées (vues), tables de codes de la TLC |
| **INTERMEDIATE** | Chaque trajet est étiqueté avec une éventuelle raison de rejet, puis les trajets valides sont dédoublonnés et enrichis |
| **MARTS** | Table de faits des trajets, 5 dimensions et 3 tables d'analyse, prêtes à interroger |

## Structure du dépôt

```
.
├── snowflake/
│   ├── 01_infrastructure.sql    rôle, warehouse, base, schémas, droits, utilisateur de service
│   └── 02_raw.sql               formats de fichier, stage et tables RAW
├── ingestion/
│   ├── charger.py               charge un mois (AAAA-MM) ou les zones
│   ├── test_connexion.py        teste la connexion par paire de clés
│   └── requirements.txt
├── airflow/                     projet Astro
│   ├── dags/
│   │   ├── nyc_taxi_pipeline.py chargement, transformations et contrôles, mois par mois
│   │   └── test_connexion.py    prouve la connexion d'Airflow à Snowflake
│   ├── include/sql/             fichiers SQL fournis (non modifiés) et contrôles
│   ├── requirements.txt
│   └── .env.example             format de la connexion Snowflake (sans secret)
├── docs/
│   ├── REPONSE.md               la réponse à la direction
│   ├── FICHE_SOURCE_TRAJETS.md  la fiche de la source des trajets
│   └── captures/                captures d'écran des preuves
├── CONTRAT_RAW.md               noms et colonnes imposés à la couche RAW
└── ETAPES.md                    le déroulé du brief
```

## Prérequis

- Python 3.10 ou plus récent, Git, OpenSSL
- Docker (sous Windows : Docker Desktop, avec l'intégration WSL activée pour Ubuntu)
- Astro CLI
- Un compte Snowflake (essai gratuit, édition Enterprise, région européenne)
- Sous Windows, tout se fait dans un terminal **WSL (Ubuntu)**, avec le projet dans le dossier personnel d'Ubuntu (`~`)

`bash verifier_poste.sh` vérifie les outils : tout doit afficher `OK`.

## Installation et lancement

### 1. Récupérer le projet

```bash
git clone https://github.com/AbdelDpro/snowflake-airflow-medallion-pipeline.git
cd snowflake-airflow-medallion-pipeline
```

### 2. Créer l'entrepôt Snowflake

Dans Snowsight, avec un rôle administrateur, exécuter **tout** le script `snowflake/01_infrastructure.sql` (bouton « Tout exécuter »). Il crée le rôle `TRANSFORMER`, le warehouse `NYC_TAXI_WH`, la base `NYC_TAXI`, les quatre schémas, les droits et l'utilisateur de service `AIRFLOW_SVC`. Il est rejouable sans erreur.

### 3. Générer la paire de clés et l'attacher à l'utilisateur

Les clés sont rangées **hors du dépôt** :

```bash
mkdir -p ~/.snowflake/keys && cd ~/.snowflake/keys
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
chmod 600 rsa_key.p8
grep -v "PUBLIC KEY" rsa_key.pub | tr -d '\n'; echo     # clé publique sur une ligne
```

Puis, dans Snowsight :

```sql
USE ROLE USERADMIN;
ALTER USER AIRFLOW_SVC SET RSA_PUBLIC_KEY = '<clé publique sur une ligne>';
```

### 4. Créer la couche RAW

Dans Snowsight, exécuter tout le script `snowflake/02_raw.sql`. Il s'exécute avec le rôle `TRANSFORMER`, qui devient propriétaire du stage, des formats et des tables.

### 5. Charger les zones avec le script Python

Créer un fichier de variables **hors du dépôt**, `~/.snowflake/env.sh` :

```bash
export SNOWFLAKE_ACCOUNT="<ORGANISATION-COMPTE>"
export SNOWFLAKE_USER="AIRFLOW_SVC"
export SNOWFLAKE_PRIVATE_KEY_PATH="~/.snowflake/keys/rsa_key.p8"
```

L'identifiant de compte s'obtient dans Snowsight avec `SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME();`.

Puis, à la racine du projet :

```bash
uv venv .venv                      # ou : python3 -m venv .venv
source .venv/bin/activate
uv pip install -r ingestion/requirements.txt
source ~/.snowflake/env.sh

python ingestion/test_connexion.py    # ('AIRFLOW_SVC', 'TRANSFORMER', 'NYC_TAXI_WH', 'NYC_TAXI', 'RAW')
python ingestion/charger.py zones     # 265 zones
```

Le script charge aussi un mois à la demande, par exemple `python ingestion/charger.py 2025-01`. En temps normal, c'est Airflow qui s'en charge.

### 6. Lancer Airflow

```bash
cd airflow
cp .env.example .env
awk 'NF {printf "%s\\n", $0}' ~/.snowflake/keys/rsa_key.p8   # clé privée sur une ligne
```

Dans `.env`, remplacer `ORGANISATION-COMPTE` par l'identifiant de compte et la valeur de `private_key_content` par la clé obtenue. Ce fichier est exclu de Git (`.gitignore`) et de l'image Docker (`.dockerignore`). Puis :

```bash
astro dev start
```

Dans l'interface (`http://localhost:8080`), **activer** le DAG `nyc_taxi_pipeline` avec son interrupteur, sans utiliser le bouton Trigger. Le rattrapage (catchup) crée une exécution pour janvier, février et mars 2025, traitées l'une après l'autre.

### 7. Vérifier

| Table | Lignes attendues après les trois mois |
|---|---|
| `RAW.YELLOW_TRIPDATA` | 11 198 026 (3 475 226 en janvier) |
| `RAW.TAXI_ZONE_LOOKUP` | 265 |
| `INTERMEDIATE.INT_TRIPS__FLAGGED` | 11 198 026 |
| `MARTS.FCT_TRIPS` | 10 382 378 |
| `MARTS.MART_ZONE_HOURLY_DEMAND` | 11 524 |
| `MARTS.MART_DATA_QUALITY` | 18 |

## Choix techniques

Chaque choix découle d'une contrainte du brief.

### Sécurité et droits

- **Un rôle dédié aux outils, `TRANSFORMER`, avec les droits minimaux** : `USAGE` sur le warehouse et la base ; `USAGE`, `CREATE TABLE`, `CREATE STAGE` et `CREATE FILE FORMAT` sur `RAW` ; `USAGE`, `CREATE TABLE` et `CREATE VIEW` sur les trois autres schémas. Aucun `SELECT` ni `INSERT` n'est nécessaire, car le rôle est propriétaire de tout ce qu'il crée. Il ne peut ni créer de base, ni toucher à autre chose : un accès hors périmètre est refusé (capture `06_acces_refuse.png`). Les outils n'utilisent jamais `ACCOUNTADMIN`.
- **`TRANSFORMER` rattaché sous `SYSADMIN`**, comme le recommande Snowflake : l'administrateur garde la main sur tout ce que le rôle crée.
- **Chaque rôle d'administration dans son métier** : `USERADMIN` crée le rôle et l'utilisateur, `SYSADMIN` crée les objets et distribue les droits dessus.
- **Un utilisateur de type `SERVICE`** : il ne peut pas avoir de mot de passe, ni ouvrir l'interface ; il s'authentifie uniquement par paire de clés.
- **Aucun secret dans Git ni dans l'image** : la clé privée et les variables de connexion sont rangées hors du dépôt (`~/.snowflake/`) ; le `.env` d'Airflow est exclu par `.gitignore` et `.dockerignore`. La clé publique n'est pas écrite dans le script versionné, qui reste ainsi valable pour un autre compte ou une autre clé.
- **Clé privée sans phrase de passe**, protégée par ses droits de fichier (`chmod 600`) : un programme qui se connecte seul devrait de toute façon stocker la phrase de passe à côté de la clé. En entreprise, on chiffrerait la clé et on rangerait la phrase de passe dans un coffre à secrets (Key Vault, Secrets Manager, Vault).

### Coûts

- **Un seul warehouse XS**, suspendu après 60 secondes d'inactivité (`AUTO_SUSPEND = 60`), rallumé automatiquement (`AUTO_RESUME`), créé éteint (`INITIALLY_SUSPENDED`). La taille XS suffit pour 3,5 millions de lignes par mois.
- **Crédits consommés** : 0.415 crédits sur l'ensemble du projet (capture `07_credits.png`). La vue `WAREHOUSE_METERING_HISTORY` a jusqu'à trois heures de retard.

### Chargement

- **Un stage interne** : `COPY INTO` ne lit que des fichiers déjà déposés dans Snowflake. Le stage interne ne demande aucun compte de stockage cloud à configurer.
- **Des types larges en RAW** (`FLOAT` pour les montants et la distance, `NUMBER` pour les codes, `TIMESTAMP_NTZ` pour les dates) : une même colonne peut être entière un mois et décimale le suivant, et un nombre sans décimales arrondirait les montants. RAW reste une copie fidèle ; les types sont resserrés dans les couches suivantes.
- **`MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE`** : chaque colonne du fichier va dans la colonne de même nom. La couche RAW accepte ainsi les variations de colonnes entre mois (colonne absente, en trop, ou en majuscules).
- **`INCLUDE_METADATA`** remplit les colonnes techniques : nom exact du fichier (déposé à la racine du stage, sans compression, pour garder le motif `AAAA-MM` attendu par les fichiers SQL) et heure de chargement. `_loaded_at` est enregistrée dans le fuseau de la session Snowflake ; elle ne sert qu'à départager deux versions d'un même trajet.
- **`USE_LOGICAL_TYPE = TRUE`** pour le Parquet, afin que les dates soient lues comme des dates ; **`ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE`** pour le CSV, qui a 4 colonnes quand la table en a 6 avec les colonnes techniques.

### Rejouabilité

- Tous les objets sont créés avec `IF NOT EXISTS` : les scripts se relancent sans erreur.
- `COPY INTO` garde l'historique des fichiers chargés : relancer un mois déjà chargé ne copie rien (`LOAD_SKIPPED`). Pour recharger une table, il faut `TRUNCATE`, qui efface aussi cet historique, et non `DELETE`.
- Les transformations alimentées mois par mois effacent le mois traité avant de le réinsérer (`DELETE` puis `INSERT`) : relancer février ne change le nombre de lignes d'aucune table.

### Orchestration

- **Une exécution par mois, déduite de la date logique** : le DAG calcule le nom du fichier à partir du mois traité par l'exécution, et non de la date du jour. C'est ce qui permet de rejouer un ancien mois. Le bouton Trigger, qui crée une exécution datée d'aujourd'hui, n'est donc pas utilisé.
- **Rattrapage (`catchup=True`)** entre le 1er janvier et le 31 mars 2025, un mois à la fois (`max_active_runs=1`).
- **Relances automatiques** (`retries=2`, toutes les 5 minutes) : si le fichier du mois n'est pas encore publié, la vérification échoue et Airflow réessaie. Les contrôles n'ont pas de relance (`retries=0`) : ils doivent échouer tout de suite.
- **Une tâche par fichier SQL, regroupées par couche** (`TaskGroup`). L'ordre découle des dépendances : un fichier s'exécute après ceux qui créent les tables qu'il lit. D'où l'enchaînement chargement → contrôle RAW → tables → STAGING → INTERMEDIATE → MARTS.
- Le fichier téléchargé passe d'une tâche à l'autre par son chemin dans `/tmp`, ce qui fonctionne parce que toutes les tâches tournent dans le même conteneur. Sur un Airflow réparti sur plusieurs machines, il faudrait un stockage partagé.

### Contrôles qualité

Chaque contrôle renvoie une ligne de valeurs vraies ou fausses ; une valeur fausse fait échouer la tâche, et la suite ne s'exécute pas. Chacun est placé juste après la table qu'il vérifie, pour qu'une donnée fausse ne se propage pas.

| Contrôle | Placé après | Vérifie |
|---|---|---|
| `raw_mois_charge` (fourni) | le chargement | le mois traité est présent dans RAW |
| `intermediate_taux_ecartes` | `int_trips__flagged` | la part de trajets écartés du mois reste sous 15 % |
| `marts_fct_sans_doublon` | `fct_trips` | le mois n'est pas vide, et aucun trajet n'apparaît deux fois |

Un contrôle mis en échec exprès, en abaissant son seuil, bloque bien la suite du pipeline (capture `04_controle_en_echec.png`).

### Et avec un quatrième mois ?

Il suffit de repousser la fin du DAG (`end_date` au 30 avril 2025) et le paramètre `end_month` (au 1er mai 2025, pour la dimension des dates). Le rattrapage crée alors l'exécution d'avril, qui charge son fichier et n'ajoute que ce mois aux tables, sans toucher aux trois premiers.

## Captures

Dans [`docs/captures/`](docs/captures/) : droits du rôle des outils, trois exécutions réussies, graphe du DAG, contrôle en échec, historique de chargement, accès refusé, crédits consommés.

## Auteur

Abdel DAADI dans le cadre de la formation Data Engineer de Simplon.