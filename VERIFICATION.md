# Vérification du projet livré

Vérifications réalisées le 3 septembre 2026, heure locale Africa/Lagos (les journaux applicatifs indiquent aussi UTC).

## Compilation et exécution
- SBT 1.10.7 : compilation Scala réussie et assembly final généré.
- Spark 3.5.6, Scala 2.12.18, Microsoft OpenJDK 17.0.20.1, Windows x64.
- Dix tests métier exécutés par sbt test : dix réussis.
- Le JAR final a été lancé via scripts/run-windows.ps1 all, qui appelle le spark-submit officiel. Code de sortie : 0.
- Pipeline final complet : 94.64 secondes, local[2], mémoire du driver 3 Go.
- Le JAR embarque le projet et Typesafe Config ; il n'embarque pas Spark.
- Empreinte SHA256 du JAR : c5bf995d64f5f6c9f25a56b400c92674bb1cc43d2857c4f59ae2543f4363102a.

## Contrôle indépendant des résultats finaux
scripts/verify_outputs.py a relu les sources avec pandas et PyArrow. Les contrôles ont confirmé les volumes, rejets, nulls, transactions enrichissables, chiffre d'affaires et toutes les cellules de rétention. Les exports CSV et Parquet concordent en volumes et colonnes ; les identifiants, scores RFM et totaux de synthèse sont cohérents.

Résultats : 138 047 transactions lues ; 1 890 rejets de validation ; 136 157 valides ; 11 406 rejets de jointure ; 124 751 analysées. Montant analytique total : 49 897 506,62 €. Acheteurs distincts : 10 193. Marchands : 586. Transactions signalées : 3 760.

Meilleure cohorte observable à M+3 : juin 2025, 118 acheteurs actifs sur 310 initiaux, soit 38,06 %.

## Performance
Le mode benchmark a terminé les deux passages. Résultats cohérents entre les modes pour la synthèse, tous les KPI marchands, les cohortes et les clients RFM. Les temps détaillés figurent dans README.md et output/csv/benchmark_comparison.
L'essai est exploratoire : un seul passage sans puis avec optimisations, et une charge partiellement concurrente de compilation et de contrôle. Les 84,84 % d'écart total ne sont pas une estimation isolée du gain du cache. La dernière modification du code concerne l'échappement CSV standard ; le benchmark précède uniquement cette modification de format.

## Support et scripts
- PowerPoint de 18 diapositives avec notes : toutes les pages ont été rendues et inspectées visuellement.
- Contrôle des cadres : aucun élément hors diapositive ; recouvrements involontaires corrigés.
- Syntaxe PowerShell vérifiée ; lanceur Windows exécuté avec les dépendances préparées.
- Setup complet depuis un Windows vierge et script de compilation portable supplémentaire : fournis, mais non exécutés de bout en bout. La chaîne SBT a bien été utilisée.

## Limites concrètes
Le cluster n'a pas été testé. Les bibliothèques Hadoop Windows viennent du dépôt communautaire cdarlint/winutils ; les sources et SHA256 utilisés sont enregistrés dans scripts/windows-runtime-sources.json.
Après le succès du pipeline, Spark sous Windows a journalisé un échec de suppression d'une copie temporaire du JAR encore verrouillée. Le processus est sorti avec le code 0 ; les sorties finales ont ensuite passé le contrôle indépendant. Cet avertissement concerne le nettoyage temporaire, pas le calcul des rapports.
Les noms, heures personnelles, contributions et relectures étudiantes restent à renseigner avec les faits réels. Le dépôt initial décrit la préparation assistée et ne constitue pas quatre commits par étudiant. La présentation nécessite une répétition par les trois membres.

## Preuves incluses
verification/sbt_compile_assembly.log ; verification/tests.log ; verification/spark_submit.log ; verification/independent_checks.log ; verification/benchmark.log ; verification/benchmark_consistency.log ; verification/metrics.json.
Les doubles exports complets des deux passages du benchmark ne sont pas inclus dans le ZIP pour éviter trois copies du même jeu de résultats ; le comparatif, les logs et le contrôle de cohérence sont inclus. La commande benchmark permet de les régénérer.

## Taille de l’archive de remise

Pour rester sous 25 Mo, le ZIP omet uniquement le CSV complet des transactions enrichies (régénérable avec all) et les doubles sorties de benchmark. Les 124 751 transactions enrichies sont toutes conservées en Parquet ; un CSV de 1 000 lignes est fourni dans samples. Les autres rapports analytiques restent complets en CSV et Parquet. Le dossier de travail local conserve aussi le CSV complet.
