# Correspondance avec le sujet, Groupe 9

ABOUTA Eudoxie (Membre A), BAMBA Issouf (Membre B), AHOGA Josias (Membre C).

## Organisation et livrables humains
0.1 : EQUIPE.md nomme les trois membres, leurs rôles exclusifs, les questions traitées et les fichiers dont chacun est propriétaire. Les adresses e mail d'Eudoxie et d'Issouf restent à renseigner par les intéressés.
0.2 : dépôt Git local à la racine, .gitignore conforme. L'historique en place couvre l'import de la base commune et l'industrialisation. Les quatre commits personnels d'Eudoxie et d'Issouf sont à créer depuis leurs postes ; scripts/commits-membres.ps1 les guide sans fabriquer d'historique.
0.3 : CONTRIBUTIONS.md porte la répartition nominative, la grille de relecture croisée, vingt décisions techniques justifiées et les règles RFM. Les heures et les comptes rendus de relecture restent à compléter honnêtement.
8.1 : source, configuration, JAR, sorties, tableau de bord et support inclus. L'envoi et le nom nominatif final de l'archive restent à la charge du groupe.
8.2 : support de 18 diapositives avec notes de présentateur, guide oral, questions probables et scénario de démonstration fournis ; répétition collective à effectuer.

## Tronc commun technique
1.1 : projet SBT et classes séparées par module. Les données sont externes au JAR dans data/ pour être remplaçables.
1.2 : build.sbt, Spark Core, Spark SQL, Typesafe Config et assembly léger.
1.3 : README.md, commandes locales et exemple de déploiement cluster.
2.1 : DataIngestion, modèles Transaction/User/Product/Merchant, schéma explicite des transactions et inférence des marchands.
2.2 : DataValidation, deux DataFrames par source et rejection_reason.
2.3 : erreurs de lecture contextualisées, actions count et affichage des volumes.
2.4 : quality_report affiché et écrit en CSV et Parquet.
3.1 : TimeFeatures, UDF structuré et parsing strict tolérant aux entrées invalides.
3.2 : DataTransformation, jointures renommées, rangs, nombre de transactions et tranches d'âge.
3.3 : somme sur sept jours, jours distincts, is_active et délai avec lag.
4.1 : merchant_kpis, merchant_age_sales, commissions et rangs par catégorie et région.
4.2 : cohort_retention, cohort_matrix et best_cohort_m3 ; horizons non observables exclus.
5.1 : SparkOptimizations, cache, persist MEMORY_AND_DISK_SER et unpersist.
5.2 : broadcast explicite des marchands et partitions configurables.
6.1 : MainApp, orchestration, affichage, double export, arrêt dans finally.
7.1 : application.conf et reference.conf, surcharge externe et valeurs par défaut.

## Bonus implémentés
2.5 : compteurs de références orphelines dans quality_report.
3.4 : historique strictement antérieur, quatre critères, is_suspicious et top 20.
4.3 : scores RFM, règles de segmentation justifiées et croisement avec customer_segment.
4.4 : produits, catégories/régions et paiement/période.
5.3 : mode benchmark, timings par étape et comparaison publiée dans README ; essai exploratoire et limites documentés.
6.2 : modes ingestion, transformation, analytics, all et benchmark ; aide si argument inconnu.
Bonus internes : taux suspect par marchand, chiffre d'affaires et revenus moyens par cohorte/période.

## Au delà du sujet

Ces éléments ne sont demandés par aucune question. Ils servent la lisibilité du travail et sa reproductibilité.

1. Tableau de bord décisionnel : DashboardReport.scala écrit output/dashboard.html à partir des seules sorties Gold, avec thème clair et thème sombre commutables, sans aucune dépendance réseau.
2. Architecture en médaillon : docs/architecture-medaillon.svg documente le lignage Bronze, Silver, Gold avec les classes responsables et les volumes réels.
3. Conteneurisation : Dockerfile et docker-compose.yml figent Spark 3.5.6, Scala 2.12 et Java 17.
4. Intégration continue : .github/workflows/ci.yml compile, teste, empaquette et compte les commits par auteur à chaque push.
5. Interface de commandes unique : Makefile sous Linux et macOS, make.ps1 sous Windows.
6. Suite de régression étendue à seize cas isolés, avec rapport JUnit XML dans target/test-reports/regression.xml.

## Limites déclarées
Exécution validée localement sous Windows ; le déploiement cluster est documenté, pas exécuté. La comparaison de performance n'est pas une expérience isolée à répétitions multiples. Le setup Windows complet sur un poste vierge reste à éprouver ; le lanceur a été testé avec l'environnement préparé. La voie Docker est écrite à partir des images officielles Apache Spark et SBT, mais l'image n'a pas encore été construite : aucun démon Docker n'était démarré au moment de la vérification. Les identités et contributions étudiantes ne sont pas inventées.
