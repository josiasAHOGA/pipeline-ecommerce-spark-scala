# Journal de contribution, Groupe 9

Module Data Engineer, Spark et Scala. Projet final « Système d'analyse de données e commerce distribué ».

| Membre | Rôle | Parties du sujet |
| :-- | :-- | :-- |
| ABOUTA Eudoxie | Membre A, Data Ingestion & Platform Engineer | 1, 2 et 7 |
| BAMBA Issouf | Membre B, Data Transformation Engineer | 3 |
| AHOGA Josias | Membre C, Analytics & Performance Engineer | 4, 5 et 6 |

## Objet de ce document

Ce journal retrace la répartition du travail, les choix techniques retenus et les vérifications effectuées. Le projet part de l'archive et du sujet fournis par l'enseignant.

Tous les chiffres cités proviennent d'exécutions réelles du pipeline, tracées dans `VERIFICATION.md`, et se reproduisent avec les commandes indiquées en fin de document.

## Répartition des questions et relecture croisée

| Questions | Responsable | Relecteur | Livrables concernés |
| :-- | :-- | :-- | :-- |
| 0.1 à 0.3 | Collectif | Collectif | `EQUIPE.md`, `CONTRIBUTIONS.md`, dépôt Git |
| 1.1 à 1.3 | Eudoxie ABOUTA | Josias AHOGA | `build.sbt`, `project/`, `README.md` |
| 2.1 à 2.5 | Eudoxie ABOUTA | Issouf BAMBA | `Models.scala`, `DataIngestion.scala`, `DataValidation.scala` |
| 3.1 à 3.4 | Issouf BAMBA | Eudoxie ABOUTA | `TimeFeatures.scala`, `DataTransformation.scala` |
| 4.1 à 4.4 | Josias AHOGA | Issouf BAMBA | `Analytics.scala` |
| 5.1 à 5.3 | Josias AHOGA | Eudoxie et Issouf | `SparkOptimizations.scala`, mode `benchmark` |
| 6.1 à 6.2 | Josias AHOGA | Eudoxie et Issouf | `MainApp.scala`, `ResultWriter.scala` |
| 7.1 | Eudoxie ABOUTA | Josias AHOGA | `application.conf`, `reference.conf` |
| 8.1 à 8.2 | Collectif | Collectif | Archive, `soutenance/`, `GUIDE_SOUTENANCE.md` |

La règle du sujet est respectée : aucun module n'est relu par son propre auteur.

## Journal technique détaillé, module par module

Cette section décrit ce que fait réellement chaque fichier, les points délicats rencontrés et la manière dont ils ont été traités. Elle sert de mémoire technique au groupe et de support aux questions du jury.

### Membre A : ingestion, validation et configuration

**`models/Models.scala`.** Quatre `case class` (`Transaction`, `User`, `Product`, `Merchant`) plus trois types de sortie (`TimeInfo`, `QualityRow`, `Timing`). Les champs numériques susceptibles d'être absents sont déclarés en `Option` : `amount: Option[Double]`, `age: Option[Int]`, `price`, `rating`, `stock`, `commission_rate`. Point délicat : sans `Option`, Spark remplacerait une valeur absente par zéro dans un type primitif, ce qui rendrait impossible de distinguer « montant nul » de « montant manquant » et fausserait le motif de rejet.

**`analytics/DataIngestion.scala`.** Une méthode par source, avec quatre stratégies de lecture différentes imposées par le sujet.

| Source | Stratégie | Raison |
| :-- | :-- | :-- |
| `transactions.csv` | Schéma explicite dérivé de la `case class`, mode `FAILFAST` | Le sujet l'exige, et cela empêche une inférence de type instable selon l'échantillon lu |
| `users.json` | Schéma typé, `preferred_categories` en `Seq[String]` | Le champ imbriqué serait mal typé par inférence sur un fichier partiel |
| `products.parquet` | Lecture directe | Le Parquet porte son propre schéma ; c'est un répertoire de douze fichiers, pas un fichier unique |
| `merchants.csv` | `inferSchema`, puis cast explicite | Inférence demandée par le sujet, mais `establishment_date` serait lue en entier et `commission_rate` mal typée |

La méthode générique `read` enveloppe chaque lecture dans un `try` / `catch` et déclenche volontairement un `count()`. Point délicat : Spark étant paresseux, sans cette action une erreur de fichier ne remonterait qu'au milieu du pipeline, avec une trace incompréhensible. Le message d'erreur produit nomme la source et son chemin.

**`analytics/DataValidation.scala`.** La fonction `split` est le cœur du module : elle prend une liste de couples (condition, motif) et construit une colonne `rejection_reason` par `concat_ws`, puis sépare le DataFrame en deux. Points délicats :

* Une condition Spark sur une colonne nulle renvoie `null`, pas `false`. Le `coalesce(ok, lit(false))` force donc une ligne à valeur nulle à être rejetée au lieu d'être silencieusement conservée.
* Une ligne peut violer plusieurs règles à la fois. `concat_ws` les concatène toutes, ce qui donne un motif complet plutôt que le premier rencontré.
* La validation du timestamp ne se contente pas des quatorze caractères demandés : un parsing strict vérifie le calendrier, ce qui rejette le 29 février 2025.
* `orphanCount` utilise une jointure `left_anti`, la forme la plus économique pour compter les références absentes sans matérialiser la jointure complète.

**`utils/ConfigLoader.scala` et `utils/SparkSessionBuilder.scala`.** `reference.conf` contient toutes les valeurs par défaut, `application.conf` seulement ce qui change sur le poste. Point délicat : `SparkSessionBuilder` ne force le `master` que si l'opérateur ne l'a pas déjà imposé en ligne de commande, sinon un `spark-submit --master yarn` serait écrasé par la valeur du fichier.

### Membre B : transformations temporelles et fenêtrage

**`analytics/TimeFeatures.scala`.** L'UDF `extractTimeFeatures` transforme une chaîne `yyyyMMddHHmmss` en une structure de six champs. Points délicats :

* Le parsing utilise `ResolverStyle.STRICT`. En mode indulgent, le 29 février 2025 serait silencieusement décalé au 1er mars.
* La fonction renvoie `null` et non une exception sur une entrée nulle, vide ou mal formée. Une UDF qui lève une exception fait tomber la tâche Spark entière.
* Le sujet ne dit rien de la plage entre minuit et six heures. Nous la classons en `Night`, et cette convention est testée explicitement aux frontières 21h59, 22h00 et minuit.

**`analytics/DataTransformation.scala`.** Deux responsabilités : l'enrichissement par jointures, puis les fenêtres de comportement.

Pour les jointures, chaque table de référence reçoit un drapeau `_user_found`, `_product_found`, `_merchant_found` avant la jointure `left`. Après la jointure, un drapeau resté nul signale une référence absente et alimente `rejection_reason`. Point délicat : une jointure `inner` aurait donné le même périmètre final, mais aurait supprimé les lignes sans laisser de trace. Ici les rejets partent dans `join_rejections` avec leur motif.

Les colonnes homonymes (`name`, `category`) sont renommées avant la jointure, sinon Spark produit des colonnes ambiguës impossibles à sélectionner.

Pour le fenêtrage, trois fenêtres distinctes coexistent :

| Fenêtre | Définition | Usage |
| :-- | :-- | :-- |
| `ordered` | `partitionBy(user_id).orderBy(epoch_seconds, transaction_id)` | `row_number` et `lag` |
| `rolling` | `range.rangeBetween(-7*86400+1, 0)` | Montant cumulé et jours actifs sur sept jours |
| `historical` | `range.rangeBetween(unboundedPreceding, -1)` | Moyenne historique strictement antérieure |

Points délicats :

* `rangeBetween` porte sur les secondes écoulées, pas sur un nombre de lignes. Trois achats dans la même journée ne font pas sortir les autres de la fenêtre, ce qu'un `rowsBetween(-6, 0)` aurait fait.
* `active_days_7d` compte des dates distinctes via `collect_set`, pas des transactions. Deux achats le même jour comptent pour un seul jour actif.
* La borne haute de `historical` est `-1` et non `0` : la transaction courante est exclue de sa propre moyenne, sinon le calcul de l'écart au panier moyen serait circulaire. Les transactions au même timestamp sont exclues elles aussi, ce qui évite toute fuite d'information future.
* `is_suspicious` exige au moins deux signaux sur quatre. Chaque signal est encadré par un `coalesce(..., lit(0))` : sans lui, un premier achat sans historique rendrait la somme nulle et non nulle au sens booléen.

### Membre C : analytique, optimisations et orchestration

**`analytics/Analytics.scala`.** Six familles de rapports.

* `merchants` : chiffre d'affaires, transactions, clients uniques, panier moyen, commission et taux suspect, avec deux `dense_rank` indépendants, un par catégorie et un par région.
* `merchantAges` : répartition par tranche d'âge, exprimée en part du chiffre d'affaires du marchand grâce à une fenêtre `partitionBy(merchant_id)`.
* `cohorts` : c'est le calcul le plus délicat du projet. Une grille des couples (cohorte, mois observable) est générée explicitement avec `sequence` et `explode`, puis l'activité réelle est jointe en `left` et les trous remplis à zéro. Point délicat : sans cette grille, un mois sans achat disparaîtrait au lieu de valoir zéro, et la courbe de rétention serait fausse. La borne haute est le dernier mois observé, donc une cohorte récente n'est jamais pénalisée pour un horizon qu'elle n'a pas encore atteint.
* `rfm` : trois `ntile(5)` sur récence, fréquence et montant. Point délicat : la récence est ordonnée en décroissant pour que le score 5 corresponde à l'achat le plus récent. Les égalités sont départagées par `user_id`, sans quoi deux exécutions donneraient des scores différents.
* `top_products`, `category_region`, `payments_day_period` : agrégations simples avec parts calculées par fenêtre.
* `summary` : la ligne unique qui sert de contrôle global.

**`analytics/SparkOptimizations.scala`.** Une classe qui mémorise les DataFrame mis en cache pour les libérer dans l'ordre inverse. Point délicat : `unpersist` est appelé dans un `finally`, donc même un échec du pipeline ne laisse pas de blocs en mémoire.

**`utils/ResultWriter.scala`.** Écriture double, CSV et Parquet. Point délicat : un CSV ne sait pas représenter un tableau ni une structure. Les colonnes de type `ArrayType`, `MapType` et `StructType` sont converties en JSON pour le seul export CSV, tandis que le Parquet conserve les types d'origine.

**`analytics/MainApp.scala`.** Orchestration et chronométrage. Le dispatch de l'étape utilise un `match` sur `args.toList` : un argument inconnu ou surnuméraire tombe dans le cas par défaut, affiche l'aide et n'ouvre même pas de session Spark. La fonction `timed` prend un bloc de code en paramètre et enregistre sa durée, ce qui alimente `execution_timings` et le mode `benchmark`. Une assertion vérifie après l'enrichissement que retenues plus rejetées égale exactement les transactions valides : c'est le garde fou contre une jointure qui dupliquerait des lignes.

**`report/DashboardReport.scala`.** Génère la page HTML autonome à partir des seuls DataFrame Gold. Points délicats : l'écriture passe par l'API Hadoop FileSystem, ce qui fait fonctionner le même code en local et sur HDFS. Les valeurs destinées au CSS et au SVG sont formatées en locale neutre, car la locale française produit une virgule décimale qui rend une largeur CSS invalide. Un échec de génération est capturé et n'interrompt jamais le pipeline.

## Charge de travail et difficultés

### ABOUTA Eudoxie, Membre A
Charge estimée : À COMPLÉTER heures.
Travaux effectués : À COMPLÉTER.
Difficultés rencontrées : À COMPLÉTER. Pistes vécues sur ce module : le typage `Option` pour distinguer une valeur absente d'un zéro, la lecture `FAILFAST` d'un JSON contenant un champ tableau, et la différence entre un orphelin du fichier brut et une référence rejetée à la jointure.

### BAMBA Issouf, Membre B
Charge estimée : À COMPLÉTER heures.
Travaux effectués : À COMPLÉTER.
Difficultés rencontrées : À COMPLÉTER. Pistes vécues sur ce module : rendre l'UDF insensible aux horodatages invalides, comprendre pourquoi `rangeBetween` doit porter sur des secondes et non sur des lignes, et exclure la transaction courante de sa propre moyenne historique.

### AHOGA Josias, Membre C
Charge estimée : À COMPLÉTER heures.
Travaux effectués : À COMPLÉTER.
Difficultés rencontrées : À COMPLÉTER. Pistes vécues sur ce module : générer la grille des mois observables pour la rétention, constater l'effet du `broadcast` explicite une fois le seuil automatique désactivé, et libérer le cache dans un bloc `finally`.

## Journal des relectures croisées

Chaque entrée est datée et signée par le relecteur, qui n'est jamais l'auteur du module. La colonne « points contrôlés » indique ce que le relecteur doit vérifier concrètement.

| Date | Module | Auteur | Relecteur | Points contrôlés | Remarques et suites | Commande de vérification |
| :-- | :-- | :-- | :-- | :-- | :-- | :-- |
| À COMPLÉTER | Parties 1 et 7 | Eudoxie | Josias | Aucun chemin ni seuil codé en dur, `reference.conf` couvre toutes les clés, l'assembly n'embarque pas Spark | À COMPLÉTER | `sbt assembly` puis `unzip -l` sur le JAR |
| À COMPLÉTER | Partie 2, ingestion | Eudoxie | Issouf | Les quatre stratégies de lecture, le message d'erreur nomme la source, les volumes lus correspondent au sujet | À COMPLÉTER | `.\make.ps1 run -Stage ingestion` |
| À COMPLÉTER | Partie 2, validation | Eudoxie | Issouf | Une ligne nulle est rejetée, un motif multiple est concaténé, le taux de rejet n'est pas nul | À COMPLÉTER | Lecture de `output/csv/rejected_transactions` |
| À COMPLÉTER | Partie 3, UDF | Issouf | Eudoxie | Entrée nulle, vide, date impossible, frontières 21h59 et 22h00 | À COMPLÉTER | `sbt test`, cas « frontieres exactes de day_period » |
| À COMPLÉTER | Partie 3, fenêtrage | Issouf | Eudoxie | La fenêtre porte sur des secondes, les jours actifs sont distincts, la moyenne n'anticipe pas | À COMPLÉTER | `sbt test`, cas « fenetre de sept jours » |
| À COMPLÉTER | Partie 4 | Josias | Issouf | M+0 vaut cent pour cent, un mois creux vaut zéro, les horizons non observables sont exclus, les scores RFM sont reproductibles | À COMPLÉTER | Lecture de `output/csv/cohort_matrix` |
| À COMPLÉTER | Parties 5 et 6 | Josias | Eudoxie et Issouf | `unpersist` dans un `finally`, argument inconnu géré, durées enregistrées par étape | À COMPLÉTER | `.\make.ps1 run -Stage benchmark` |

## Traçabilité des vérifications

Les chiffres ci dessous proviennent d'exécutions réelles et se reproduisent avec les commandes indiquées. `VERIFICATION.md` en conserve le détail.

| Contrôle | Résultat observé | Commande |
| :-- | :-- | :-- |
| Suite de régression | 16 cas, 0 échec | `sbt test` |
| Lignes lues | 138 047 transactions, 12 000 users, 6 000 products, 600 merchants | `.\make.ps1 run -Stage ingestion` |
| Rejets de validation | 1 890 transactions, 345 users, 179 products, 14 merchants | `output/csv/quality_report` |
| Rejets de jointure | 11 406 | trace `ENRICHISSEMENT` du pipeline |
| Réconciliation | 138 047 = 1 890 + 11 406 + 124 751 | somme des trois compteurs |
| Périmètre analysé | 124 751 transactions, 49 897 506,62 EUR, 10 193 clients, 586 marchands | `output/csv/summary` |
| Durée du pipeline complet | environ 123 secondes en `local[2]` avec 3 Go | `output/csv/execution_timings` |
| Écart avant et après optimisations | 84,84 % sur le total, une étape plus lente | `output/csv/benchmark_comparison` |

## Historique Git

Le dépôt local se trouve à la racine du projet et il est publié sur GitHub. Chaque membre configure son nom et son adresse avant de committer, de sorte que `git log` relie chaque commit à son auteur.

```sh
git config user.name  "Prénom NOM"
git config user.email "adresse@exemple.com"
git shortlog -sne HEAD
```

Le sujet demande au minimum quatre commits par membre. Le script `scripts/commits-membres.ps1` affiche, pour chaque membre, les fichiers dont il est propriétaire et les messages de commit correspondant à ses questions.

Les commits portent la date réelle à laquelle ils ont été faits. Aucune date n'a été antidatée.

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
