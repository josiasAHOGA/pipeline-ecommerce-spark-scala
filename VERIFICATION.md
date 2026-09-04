# Vérification du projet livré

Vérifications réalisées le 4 septembre 2026, heure locale Africa/Lagos. Les journaux applicatifs horodatent en UTC, le fuseau étant fixé dans la configuration pour que le résultat soit identique sur les trois postes.

## Environnement de vérification

| Élément | Version |
| :-- | :-- |
| Apache Spark | 3.5.6 |
| Scala | 2.12.18 |
| Java | Microsoft OpenJDK 17.0.20.1 |
| SBT | 1.10.7 |
| Système | Windows x64 |

## Compilation et suite de tests

* Compilation Scala réussie sur les douze fichiers source du projet.
* Suite de régression : **20 cas exécutés, 20 réussis, 0 échec**. Le rapport JUnit correspondant est écrit dans `target/test-reports/regression.xml`.
* Le JAR livré embarque le code du projet et Typesafe Config. Il n'embarque ni Spark ni Scala, qui sont fournis par `spark-submit`.
* Empreinte SHA256 de `dist/ecommerce-analytics.jar` : `aba1c26a968bb4757c2c34937ec4c8406c966d36bea2dc22806f42403162d5d6`

Les vingt cas couvrent le parsing strict des dates, les frontières de `day_period` à 21h59, 22h00 et minuit, les bornes de validation, le comptage des valeurs nulles du rapport de qualité, la fenêtre glissante de sept jours, l'absence d'anticipation dans la moyenne historique, le seuil de deux signaux pour `is_suspicious`, les cohortes, les scores RFM, les classements marchands, la conservation des lignes aux jointures, les valeurs par défaut de configuration et la génération du tableau de bord.

## Exécution du pipeline

Lancement via `spark-submit`, `local[2]`, mémoire du driver 3 Go. Code de sortie : 0.

| Étape | Durée |
| :-- | --: |
| ingestion | 12,26 s |
| validation | 0,62 s |
| qualité | 12,05 s |
| transformation | 16,38 s |
| analytique | 27,35 s |
| écriture | 27,02 s |
| **total** | **95,93 s** |

Ces durées sont celles du fichier `output/csv/execution_timings` livré avec le projet. Une exécution concurrente avec d'autres travaux sur le même poste allonge sensiblement ce total : la mesure ci dessus a été prise sur une machine au repos.

La diffusion par `broadcast` ayant été étendue aux trois référentiels, et non plus aux seuls marchands, le total est passé de 122,94 s à 95,93 s sur la même machine et dans les mêmes conditions, soit une réduction de 22 %. Le gain porte sur l'ingestion et la transformation, les deux étapes où les jointures interviennent.

## Contrôle indépendant des résultats

`scripts/verify_outputs.py` relit les sources avec pandas et PyArrow, indépendamment de Spark. Les contrôles confirment les volumes, les rejets, les valeurs nulles, les transactions enrichissables, le chiffre d'affaires et toutes les cellules de la matrice de rétention. Les exports CSV et Parquet concordent en volumes et en colonnes ; les identifiants, les scores RFM et les totaux de synthèse sont cohérents entre eux.

### Réconciliation des volumes

| Grandeur | Valeur |
| :-- | --: |
| Transactions lues | 138 047 |
| Rejets de validation | 1 890 |
| Transactions valides | 136 157 |
| Rejets de jointure | 11 406 |
| **Transactions analysées** | **124 751** |

L'égalité 138 047 = 1 890 + 11 406 + 124 751 est vérifiée.

### Indicateurs métier

| Indicateur | Valeur |
| :-- | --: |
| Chiffre d'affaires analysé | 49 897 506,62 EUR |
| Acheteurs distincts | 10 193 |
| Marchands actifs | 586 |
| Transactions signalées | 3 760, soit 3,01 % |
| Meilleure cohorte à M+3 | juin 2025 : 118 acheteurs actifs sur 310, soit 38,06 % |

## Tableau de bord

`output/dashboard.html` est écrit par le pipeline à la fin de chaque exécution, à partir des seuls DataFrame Gold. La page a été ouverte et inspectée dans ses deux thèmes, clair et sombre. Elle ne référence aucune URL externe, ce qu'un cas de la suite de régression vérifie automatiquement. Sur un cluster, l'écriture passant par l'API Hadoop FileSystem, la page est produite dans HDFS.

## Performance

Le mode `benchmark` a terminé ses deux passages. Les résultats sont cohérents entre les deux modes pour la synthèse, les KPI marchands, les cohortes et les clients RFM. Le comparatif détaillé figure dans `README.md` et dans `output/csv/benchmark_comparison`.

L'essai reste exploratoire : un seul passage sans puis avec optimisations, sur un poste non dédié. L'écart total de 84,84 % n'est donc pas une estimation isolée du gain apporté par le cache. Une étape est même plus lente avec les optimisations activées, et ce chiffre est conservé tel quel. Une conclusion robuste demanderait de répéter les essais en alternant l'ordre et de comparer les médianes.

## Support de soutenance et scripts

* Support de 18 diapositives avec notes de présentateur : toutes les pages ont été rendues en image et inspectées visuellement, une par une.
* Contrôle des cadres : aucun élément hors diapositive, aucun texte débordant de son bloc, aucun recouvrement.
* Syntaxe PowerShell vérifiée sur les six scripts. Le lanceur Windows a été exécuté avec les dépendances préparées.
* Le script de compilation portable, qui compile sans SBT à partir des bibliothèques Spark locales, a été exécuté avec succès.

## Limites concrètes

Le cluster n'a pas été testé. La procédure de déploiement, les commandes HDFS et les options `spark-submit` sont documentées dans le README, mais tous les résultats joints proviennent d'exécutions locales.

L'image Docker n'a pas été construite : aucun démon Docker n'était démarré au moment de la vérification. Le `Dockerfile` s'appuie sur les images officielles Apache Spark et SBT.

L'installation complète depuis un Windows vierge n'a pas été rejouée de bout en bout ; le lanceur a été validé avec un environnement déjà préparé.

Les bibliothèques Hadoop pour Windows proviennent du dépôt communautaire `cdarlint/winutils`. Les sources et les empreintes SHA256 utilisées sont enregistrées dans `scripts/windows-runtime-sources.json`.

Après le succès du pipeline, Spark sous Windows journalise parfois un échec de suppression d'une copie temporaire du JAR encore verrouillée par le système. Le processus sort tout de même avec le code 0, et les sorties finales passent le contrôle indépendant. Cet avertissement concerne le nettoyage des fichiers temporaires, pas le calcul des rapports.

## Preuves incluses

`verification/sbt_compile_assembly.log`, `verification/tests.log`, `verification/spark_submit.log`, `verification/independent_checks.log`, `verification/benchmark.log`, `verification/benchmark_consistency.log`, `verification/metrics.json`.

Les doubles exports complets des deux passages du benchmark ne sont pas inclus dans l'archive, pour éviter d'y placer trois copies du même jeu de résultats. Le comparatif, les journaux et le contrôle de cohérence y figurent, et la commande `benchmark` permet de les régénérer.

## Taille de l'archive de remise

L'archive omet uniquement le CSV complet des transactions enrichies, régénérable avec l'étape `all`, ainsi que les doubles sorties de benchmark. Les 124 751 transactions enrichies sont toutes conservées au format Parquet, et un extrait de 1 000 lignes est fourni dans `samples`. Les autres rapports analytiques restent complets en CSV et en Parquet.
