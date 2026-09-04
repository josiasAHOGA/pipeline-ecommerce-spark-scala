# Guide de soutenance, Groupe 9

Support : `soutenance/Presentation_Groupe9.pptx`, 18 diapositives. Chaque diapositive porte ses notes de présentateur : qui parle, combien de temps, quoi dire. Ce guide les complète et prépare les questions du jury.

## Déroulé, 30 minutes

| Séquence | Diapositives | Durée | Qui parle |
| :-- | :-- | :-- | :-- |
| Présentation collective | 1 à 5 | 5 min | Les trois, à tour de rôle |
| Exposé Membre A | 6 et 7 | 5 min | ABOUTA Eudoxie |
| Exposé Membre B | 8, 9 et 10 | 5 min | BAMBA Issouf |
| Exposé Membre C | 11 à 14 | 5 min | AHOGA Josias |
| Démonstration | 15 et 16 | 5 min | Les trois |
| Limites et conclusion | 17 et 18 | 2 min | Collectif |
| Questions du jury | | 10 min | Chacun, sur n'importe quelle partie |

## Avant d'entrer dans la salle

1. Lancer une fois le pipeline pour que `output/dashboard.html` soit à jour.
2. Ouvrir dans le navigateur, en onglets et dans cet ordre : `output/dashboard.html`, puis `output/csv/quality_report`, puis `output/csv/rejected_transactions`.
3. Ouvrir dans l'éditeur : `DataValidation.scala`, `DataTransformation.scala`, `Analytics.scala`.
4. Vérifier le vidéoprojecteur avec le thème clair du tableau de bord. Le bouton en haut à droite bascule en thème sombre si la salle est éteinte.
5. Avoir la commande `spark-submit` déjà saisie dans un terminal, prête à être lancée.

## Texte d'ouverture proposé

« Notre projet analyse des données e commerce avec Spark et Scala. Nous partons de quatre sources : les transactions, les clients, les produits et les marchands. Notre première priorité n'est pas le volume, c'est la confiance : les fichiers contiennent volontairement des anomalies. Nous contrôlons donc les données, nous expliquons chaque rejet, puis nous enrichissons les achats pour produire des indicateurs de vente, de rétention et de comportement. La démonstration se termine sur un tableau de bord que le pipeline publie lui même. Les données sont synthétiques : elles permettent de démontrer notre méthode et ses limites. »

## Ce que chaque membre doit savoir dire de son module

**ABOUTA Eudoxie, Membre A.** « Mon module transforme quatre fichiers de formats différents en données typées. Les contrôles séparent les lignes utilisables des lignes rejetées, et chaque rejet garde son motif dans une colonne dédiée. C'est ce qui permet d'expliquer l'écart entre les 138 047 lignes lues et les 124 751 analysées, au lieu de le subir. »

**BAMBA Issouf, Membre B.** « Mon module relie les achats aux clients, aux produits et aux marchands, puis ajoute les informations temporelles et l'historique de chaque client. Pour la fenêtre de sept jours, j'utilise une borne sur le temps écoulé et non sur un nombre de lignes : sept lignes ne représentent pas nécessairement sept jours. »

**AHOGA Josias, Membre C.** « Mon module transforme les transactions enrichies en indicateurs. Je calcule les ventes, les commissions et les classements, puis les cohortes pour mesurer le retour des clients. J'organise aussi la réutilisation des données en cache, l'orchestration du pipeline et la publication du tableau de bord. »

Ces textes doivent être adaptés à ce que chaque membre a effectivement étudié, modifié et compris.

## Les cinq questions les plus probables, et la réponse courte

**Pourquoi 124 751 et pas 138 047 ?** Deux étapes distinctes. La validation juge chaque fichier isolément et écarte 1 890 transactions. La jointure écarte ensuite 11 406 transactions dont l'utilisateur, le produit ou le marchand est absent du référentiel, ou a lui même été rejeté. Les deux notions ne s'additionnent pas naïvement : une même ligne peut cumuler plusieurs anomalies.

**Pourquoi des jointures à gauche et non des jointures internes ?** Parce qu'une référence manquante doit être expliquée avant d'être exclue. Une jointure interne supprimerait les lignes en silence. Nous les conservons dans `join_rejections` avec leur motif, et une assertion vérifie que retenues plus rejetées égale exactement les transactions valides.

**Que fait votre UDF si l'horodatage est nul ou impossible ?** Elle renvoie `null`, jamais une exception. Le parsing est strict : le 29 février 2025 est rejeté au lieu d'être décalé silencieusement au 1er mars. Un test couvre ce cas précis.

**Le gain de 84,84 % prouve t il l'effet du cache ?** Non, et nous le disons. Un seul passage par mode ne sépare pas l'effet du cache de l'échauffement de la JVM, du cache disque et de l'ordre d'exécution. Une étape est même plus lente avec optimisations, et nous conservons ce chiffre. Pour une conclusion robuste il faudrait répéter les essais en alternant l'ordre et comparer les médianes.

**Comment sait on que le tableau de bord n'est pas écrit à la main ?** Il est produit par `DashboardReport.scala`, à partir des DataFrame Gold de l'exécution en cours. Un test de la suite de régression vérifie qu'il est généré, qu'il contient les indicateurs de tête et qu'il ne référence aucune URL externe. Il suffit de relancer le pipeline pour le voir se réécrire.

## Préparer les questions croisées

Le jury peut interroger chaque étudiant sur le code écrit par un autre. Ces trois points valent d'être révisés par les trois membres.

1. **Les deux notions de rejet** (module d'Eudoxie) : validation par fichier, puis exclusion à la jointure.
2. **`rangeBetween` exprimé en secondes** (module d'Issouf) : la fenêtre porte sur le temps écoulé, `collect_set` compte des jours distincts et non des transactions.
3. **Les horizons non observables en rétention** (module de Josias) : une cohorte de novembre 2025 n'a pas de M+3 ; la compter comme un échec fausserait le classement, elle est donc exclue.

## Autres conventions à savoir défendre

Vingt cinq ans appartient à la tranche Adulte. La période Night couvre 22h00 à 05h59. L'heure 17 fait partie des heures ouvrées. Les horodatages sont interprétés en UTC. La moyenne historique exclut la transaction courante et toutes celles du même instant. M+0 vaut toujours cent pour cent par construction. Ces conventions sont détaillées dans `CONTRIBUTIONS.md`, section Décisions techniques, et couvertes par la suite de régression.

## Aide mémoire technique : où se trouve chaque notion

Chaque membre peut être interrogé sur une partie écrite par un autre. Ce tableau donne, pour chaque notion du projet, le fichier et la fonction où la trouver.

### Socle Big Data et exécution distribuée

| Notion | Où elle se trouve dans le projet |
| :-- | :-- |
| Les 5V, en particulier Véracité | Toute la Partie 2 : la validation et le rapport de qualité traitent la véracité de la donnée |
| HDFS, nœud Edge, commandes `hdfs dfs` | Section « Déploiement sur un cluster » du README |
| YARN comme gestionnaire de ressources | `--master yarn` dans la commande de déploiement, suivi via le ResourceManager |
| Cluster Master, Worker, Edge | Le JAR se dépose sur le nœud Edge, les exécuteurs tournent sur les Workers |
| Data pipeline : ingestion, traitement, visualisation | Bronze, Silver, Gold, puis le tableau de bord |

### Langage Scala

| Notion | Où elle se trouve dans le projet |
| :-- | :-- |
| `val` immuable | Utilisé partout ; aucun `var` dans la logique métier |
| Collections `Seq`, `Map`, `Set` | `Sources.frames` renvoie une `Seq`, `validateAll` une `Map`, `collect_set` côté Spark |
| Tuples | Les compteurs d'orphelins dans `DataValidation.quality` |
| `Option` | `Transaction.amount: Option[Double]` : une valeur absente reste absente |
| **Pattern matching** | `MainApp.main` : `args.toList match` dispatche l'étape et gère l'argument inconnu |
| Fonctions et fonctions passées en argument | L'UDF `extractTimeFeatures`, et `timed` qui prend un bloc de code en paramètre |
| `try` / `catch` | `DataIngestion.read` et la gestion d'erreur globale de `MainApp` |

### API Spark

| Notion | Où elle se trouve dans le projet |
| :-- | :-- |
| SparkSession comme point d'entrée | `SparkSessionBuilder.build`, créée une seule fois |
| Driver, Cluster Manager, Executor | `spark-submit --master yarn`, mémoire et cœurs paramétrés |
| RDD, DataFrame, Dataset | Nous utilisons `Dataset[T]` typé : voir la justification ci dessous |
| Lazy evaluation et DAG | `DataIngestion.read` déclenche volontairement un `count()` pour forcer la lecture |
| `groupBy` et `agg` : `sum`, `count`, `countDistinct`, `min`, `max`, `avg` | `Analytics.merchants` et `Analytics.cohorts` |
| Jointures `left` | `DataTransformation.enrichTransactionData` |
| Jointure **`left_anti`** | `DataValidation.orphanCount` : compte les références absentes |
| `when` / `otherwise` | Tranches d'âge, drapeaux de suspicion |
| `withColumn`, `.as()`, `drop`, `distinct` | Partout dans les transformations |
| `filter` / `where` | Séparation des lignes valides et rejetées |
| Lecture CSV avec `header`, JSON, Parquet | `DataIngestion` |
| Écriture CSV et Parquet en mode `overwrite` | `ResultWriter` |
| `persist` et cache des exécuteurs | `SparkOptimizations` : `cache`, `MEMORY_AND_DISK_SER`, `unpersist` |
| Structure SBT et `spark-submit` | `build.sbt`, `dist/ecommerce-analytics.jar` |

### Les deux points les plus techniques du projet

L'**UDF temporelle** et les **fonctions de fenêtrage** sont les mécanismes les moins courants que nous ayons mis en œuvre, et ceux sur lesquels une question est la plus probable. Ils sont traités aux diapositives 7, 8 et 9, et couverts par cinq cas de la suite de régression.

### Pourquoi Dataset et non RDD

Le `Dataset` cumule trois avantages que le RDD n'a pas : le typage fort par les objets `case class`, la vérification des colonnes à la compilation et l'optimiseur Catalyst. Le RDD reste utile pour des transformations non structurées, ce qui n'est le cas d'aucune de nos opérations. Descendre à ce niveau nous aurait coûté l'optimiseur sans rien apporter.

## Scénario de démonstration, 5 minutes

1. Lancer `spark-submit ... all` et commenter les traces pendant l'exécution : lecture, validation, enrichissement, analytique, écriture.
2. Ouvrir `output/dashboard.html`. Montrer l'entonnoir de qualité, puis la matrice de rétention.
3. Commenter deux résultats métier : la concentration du chiffre d'affaires sur les marchands Electronics, et la cohérence entre le segment RFM calculé et le segment déclaré, avec ses exceptions.
4. Ouvrir `output/csv/rejected_transactions` et lire une ligne rejetée avec son motif.
5. Conclure sur les limites déclarées, sans attendre que le jury les soulève.

## Répétition

Chronométrer au moins une fois en conditions réelles. Les notes de présentateur de chaque diapositive indiquent la durée cible. Les dépassements viennent presque toujours des diapositives 5, 9 et 14 : préparer une version courte de chacune.
