# Journal de contribution, Groupe 9

Membres : ABOUTA Eudoxie (Membre A), BAMBA Issouf (Membre B), AHOGA Josias (Membre C).

## Origine du code et transparence

La première version de la base de code, des tests et des supports a été préparée avec l'assistance d'un agent IA, à partir de l'archive et du sujet fournis par l'enseignant. Le groupe l'a ensuite reprise, comprise, corrigée et étendue. Les résultats joints proviennent d'exécutions réelles, tracées dans `VERIFICATION.md`.

Ce fichier ne contient aucune heure, aucun commit et aucun avis de relecture inventé. Les cases marquées « À COMPLÉTER » doivent être renseignées par la personne concernée avant l'envoi.

## Répartition des questions et relecture croisée

| Questions | Responsable | Relecteur |
| :-- | :-- | :-- |
| 0.1 à 0.3 (organisation) | Collectif | Collectif |
| 1.1 à 1.3 (structure SBT, build, README) | Eudoxie ABOUTA | Josias AHOGA |
| 2.1 à 2.5 (ingestion, validation, qualité) | Eudoxie ABOUTA | Issouf BAMBA |
| 3.1 à 3.4 (UDF, enrichissement, fenêtrage) | Issouf BAMBA | Eudoxie ABOUTA |
| 4.1 à 4.4 (KPI, cohortes, RFM, produits) | Josias AHOGA | Issouf BAMBA |
| 5.1 à 5.3 (cache, broadcast, benchmark) | Josias AHOGA | Eudoxie ABOUTA et Issouf BAMBA |
| 6.1 à 6.2 (MainApp, exécution modulaire) | Josias AHOGA | Eudoxie ABOUTA et Issouf BAMBA |
| 7.1 (configuration externalisée) | Eudoxie ABOUTA | Josias AHOGA |
| 8.1 à 8.2 (archive, soutenance) | Collectif | Collectif |

La règle du sujet est respectée : aucun module n'est relu par son propre auteur.

## Charge de travail et difficultés

### ABOUTA Eudoxie, Membre A
Charge estimée : À COMPLÉTER heures.
Travaux effectués : À COMPLÉTER.
Difficultés rencontrées : À COMPLÉTER. Pistes attendues : typage `Option` pour distinguer une valeur absente d'un zéro, lecture `FAILFAST` du JSON avec un champ tableau, différence entre un orphelin brut et une référence rejetée à la jointure.

### BAMBA Issouf, Membre B
Charge estimée : À COMPLÉTER heures.
Travaux effectués : À COMPLÉTER.
Difficultés rencontrées : À COMPLÉTER. Pistes attendues : robustesse de l'UDF face à un horodatage nul ou impossible, `rangeBetween` exprimé en secondes plutôt qu'en nombre de lignes, calcul de la moyenne historique strictement antérieure sans anticipation.

### AHOGA Josias, Membre C
Charge estimée : À COMPLÉTER heures.
Travaux effectués : À COMPLÉTER.
Difficultés rencontrées : À COMPLÉTER. Pistes attendues : exclusion des horizons non observables dans la matrice de rétention, effet du `broadcast` explicite quand le seuil automatique est désactivé, libération du cache dans un bloc `finally`.

## Journal des relectures croisées

Chaque entrée est datée et signée par le relecteur, qui n'est jamais l'auteur du module.

| Date | Module et fichiers relus | Auteur | Relecteur | Remarques et suites données | Commande de vérification |
| :-- | :-- | :-- | :-- | :-- | :-- |
| À COMPLÉTER | Partie 2, `DataIngestion.scala`, `DataValidation.scala` | Eudoxie ABOUTA | Issouf BAMBA | À COMPLÉTER | `sbt test` puis lecture de `output/csv/quality_report` |
| À COMPLÉTER | Parties 1 et 7, `build.sbt`, `application.conf`, `reference.conf` | Eudoxie ABOUTA | Josias AHOGA | À COMPLÉTER | `sbt compile` puis `sbt assembly` |
| À COMPLÉTER | Partie 3, `TimeFeatures.scala`, `DataTransformation.scala` | Issouf BAMBA | Eudoxie ABOUTA | À COMPLÉTER | `sbt test` puis lecture de `output/csv/enriched_transactions` |
| À COMPLÉTER | Partie 4, `Analytics.scala` | Josias AHOGA | Issouf BAMBA | À COMPLÉTER | lecture de `output/csv/cohort_matrix` et `output/csv/rfm_customers` |
| À COMPLÉTER | Parties 5 et 6, `SparkOptimizations.scala`, `MainApp.scala` | Josias AHOGA | Eudoxie ABOUTA et Issouf BAMBA | À COMPLÉTER | `sbt "run benchmark"` puis lecture de `output/csv/benchmark_comparison` |

## Historique Git

Le dépôt local se trouve à la racine du projet. Chaque membre a configuré son nom et son adresse avant de committer, de sorte que `git log` relie chaque commit à son auteur.

```sh
git shortlog -sne
git log --oneline --pretty="%h %an %s"
```

Le sujet demande au minimum quatre commits par membre. Le décompte à jour est vérifiable avec la première commande.

## Décisions techniques
1. Spark 3.5.6, Scala 2.12.18 et Java 17 : combinaison compatible, avec versions fixées pour reproduire la compilation. Les options de modules Java nécessaires à une exécution SBT sont explicites.
2. Assembly léger : le JAR embarque notre code et Typesafe Config, tandis que Spark et Scala sont fournis par spark-submit. On évite un JAR contenant toutes les bibliothèques Spark et les conflits de versions sur un cluster.
3. Modèles typés avec Option : les valeurs numériques absentes restent nulles jusqu'à la validation. Une valeur absente n'est pas remplacée par zéro, ce qui permet d'en expliquer précisément le rejet.
4. Validation stricte des dates : en plus des 14 caractères, on vérifie le calendrier. Une date impossible comme le 29 février 2025 est rejetée sans faire échouer l'UDF.
5. Jointures gauches, puis séparation : les transactions sont la table de départ ; users, products et merchants sont joints en left join. Les références manquantes ou invalidées vont dans join_rejections ; les analyses utilisent seulement les lignes enrichissables. Les noms homonymes sont renommés.
6. Deux notions d'orphelin : quality_report compte les références absentes des fichiers bruts ; join_rejections inclut aussi les références présentes mais invalidées. Ces compteurs ne s'additionnent pas directement, car une même ligne peut violer plusieurs règles.
7. Temps et frontières : fuseau UTC, sept fois 24 heures jusqu'à la transaction courante, borne inférieure exclue. Les achats au même instant appartiennent à la fenêtre ; les jours actifs sont des dates distinctes, pas un nombre d'achats.
8. Ambiguïtés du sujet : 25 ans appartient à Adulte (25 à 44). Night inclut 22 h à 5 h 59 ; les heures travaillées vont de 9 h à 17 h 59, suivant le test de l'heure entière inclusif. Ces conventions sont annoncées et testées.
9. Historique sans anticipation : la moyenne historique exclut la transaction courante et toutes les transactions au même timestamp. Sans achat antérieur, la moyenne et l'écart restent nulls. La détection utilise le délai exact en secondes, pas une différence de dates arrondie.
10. Rétention : cohorte = mois du premier achat retenu ; M+0 vaut 100 %. Les mois observables sans activité valent zéro, les mois futurs restent absents. La meilleure cohorte à M+3 est recherchée uniquement parmi les cohortes observables à cet horizon.
11. RFM : référence = dernière date du jeu analytique. Les scores ntile(5) augmentent avec la valeur souhaitée ; les égalités sont ordonnées par user_id pour reproduire les résultats. Les segments sont des règles pédagogiques, pas une vérité commerciale universelle.
12. Cache et broadcast : petits résultats réutilisés en cache, transactions enrichies en MEMORY_AND_DISK_SER, marchands diffusés explicitement. Le broadcast automatique est désactivé pour isoler l'option expérimentale ; unpersist est exécuté en finally.
13. Montants et commission : après validation, amount est converti en decimal(20,2). La commission est calculée par transaction puis agrégée et arrondie au centime ; elle représente la commission théorique du taux marchand fourni.
14. Cohérence catalogue : merchant_id de la transaction reste la référence pour le chiffre d'affaires marchand. Une divergence avec le marchand du catalogue est signalée par catalog_merchant_mismatch et n'entraîne pas une correction silencieuse.
15. Export : Parquet conserve les types ; CSV rend les résultats consultables. Les tableaux et structures sont convertis en JSON uniquement pour l'export CSV. Une partition convient au jeu fourni ; ce paramètre doit être augmenté sur un grand volume.

## Règles RFM, dans l'ordre d'application
Champions : R, F et M supérieurs ou égaux à 4.
Nouveaux : R supérieur ou égal à 4 et F inférieur ou égal à 2.
À risque : R inférieur ou égal à 2 et F supérieur ou égal à 3.
Perdus : R inférieur ou égal à 2 après application de la règle précédente.
Clients fidèles : autres acheteurs. Cette classe résiduelle est une convention simplifiée.
Les utilisateurs sans achat retenu n'ont pas de score RFM.

## Décisions techniques complémentaires : industrialisation et restitution

16. Architecture en médaillon (Bronze, Silver, Gold) : le pipeline est décrit et documenté selon les trois couches usuelles de la profession. Bronze correspond aux quatre fichiers bruts lus tels quels, Silver aux jeux validés puis enrichis, Gold aux quatorze rapports métier. Ce vocabulaire rend le lignage lisible par un jury comme par une équipe métier, sans changer une ligne de logique Spark.

17. Conteneurisation avec Docker : un `Dockerfile` et un `docker-compose.yml` figent la version exacte de Spark, de Java et de Scala. N'importe quel poste, y compris sans JDK ni SBT installés, peut rejouer le pipeline avec une seule commande. C'est la réponse la plus directe à l'exigence « rendre le projet exécutable par un tiers » de la Question 1.3.

18. Intégration continue avec GitHub Actions : le workflow `.github/workflows/ci.yml` compile, exécute les tests et produit le JAR à chaque `push`. Un défaut de compilation devient visible immédiatement au lieu d'être découvert la veille de la remise. Le service est gratuit pour un dépôt public.

19. `Makefile` comme interface unique : les commandes longues de `sbt` et de `spark-submit` sont regroupées derrière des cibles courtes (`make test`, `make run`, `make dashboard`). Les trois membres exécutent exactement les mêmes commandes, ce qui supprime une source classique d'écarts entre postes.

20. Tableau de bord HTML autonome : les résultats Gold sont restitués dans une page unique, sans dépendance réseau et sans serveur. Le jury voit les indicateurs métier au lieu de lire des fichiers CSV en console. La page est régénérée à partir des sorties réelles du pipeline, jamais saisie à la main.
