# Prendre en main le projet

Projet final du module Data Engineer, Spark et Scala. Groupe 9 : ABOUTA Eudoxie, BAMBA Issouf, AHOGA Josias.

Ce document s'adresse à toute personne qui ouvre le projet pour la première fois et veut en voir le résultat, puis le rejouer.

## En cinq minutes, sans rien installer

1. Ouvrir `output/dashboard.html` par double clic. C'est le tableau de bord produit par le pipeline lui même, à partir des sorties de la dernière exécution. Le bouton en haut à droite bascule entre thème clair et thème sombre.
2. Ouvrir `docs/architecture-medaillon.svg` pour visualiser le trajet des données, de la lecture des quatre fichiers bruts jusqu'aux quatorze rapports métier.
3. Lire la section « Ce qui distingue cette livraison » du `README.md`.

## Rejouer le pipeline

L'exécution complète dure environ deux minutes et demie sur un poste local. Elle réécrit `output/csv`, `output/parquet` et `output/dashboard.html`.

Sous Windows, avec l'environnement portable préparé par `scripts/setup-windows.ps1` :

```powershell
.\make.ps1 test
.\make.ps1 run
```

Sous Linux ou macOS, avec SBT et Spark installés :

```sh
make test
make run
```

Sans installer ni JDK, ni SBT, ni Spark, avec Docker :

```sh
docker compose run --rm pipeline all
```

Le pipeline accepte une étape en argument : `all`, `ingestion`, `transformation`, `analytics` ou `benchmark`.

```powershell
.\make.ps1 run -Stage benchmark
```

## Où regarder les résultats

| Fichier | Contenu |
| :-- | :-- |
| `output/dashboard.html` | Tableau de bord décisionnel, thème clair et thème sombre |
| `output/csv/quality_report` | Lignes lues, valides, rejetées, taux de rejet et références orphelines |
| `output/csv/rejected_transactions` | Les lignes écartées, chacune avec son motif |
| `output/csv/merchant_kpis` | Chiffre d'affaires, commission et classements par marchand |
| `output/csv/cohort_matrix` | Matrice de rétention par cohorte |
| `output/csv/rfm_customers` | Segmentation RFM client par client |
| `output/csv/execution_timings` | Durée de chaque étape du pipeline |
| `samples/` | Extraits de mille lignes, pour consulter sans ouvrir les fichiers complets |

## Documents du projet

| Document | Objet |
| :-- | :-- |
| `README.md` | Prérequis, compilation, exécution locale, déploiement sur cluster, configuration |
| `EQUIPE.md` | Membres, rôles et périmètre de chacun |
| `CONTRIBUTIONS.md` | Journal technique par module, décisions justifiées, relectures croisées |
| `COUVERTURE_SUJET.md` | Correspondance question par question avec l'énoncé |
| `VERIFICATION.md` | Vérifications réalisées et résultats observés |
| `GUIDE_SOUTENANCE.md` | Déroulé de la soutenance et aide mémoire technique |
| `soutenance/` | Support de présentation |

## Points de vigilance

Le JAR `dist/ecommerce-analytics.jar` est prévu pour Spark 3.5.6 et Scala 2.12. Il se lance avec `spark-submit`, jamais par double clic ni avec `java -jar` seul.

Les données fournies sont synthétiques et contiennent volontairement des anomalies. Un rapport de qualité affichant zéro pour cent de rejet signalerait donc une erreur d'implémentation.
