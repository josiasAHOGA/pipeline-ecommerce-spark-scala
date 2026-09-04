# Équipe : Groupe 9

Module Data Engineer, Spark et Scala. Projet final « Système d'analyse de données e commerce distribué ».

Trois membres, trois rôles distincts, conformément à la Question 0.1 du sujet. Un rôle n'est occupé que par une seule personne.

## Membre A : Data Ingestion & Platform Engineer

| Champ | Valeur |
| :-- | :-- |
| Nom et prénom | ABOUTA Eudoxie |
| Adresse e mail | À RENSEIGNER PAR EUDOXIE |
| Nom configuré dans Git (`git config user.name`) | Eudoxie ABOUTA |
| Parties du sujet | Parties 1, 2 et 7 |
| Questions traitées | 1.1, 1.2, 1.3, 2.1, 2.2, 2.3, 2.4, 2.5 (bonus), 7.1 |

Fichiers dont Eudoxie est propriétaire :
`build.sbt`, `src/main/scala/com/ecommerce/models/Models.scala`,
`src/main/scala/com/ecommerce/analytics/DataIngestion.scala`,
`src/main/scala/com/ecommerce/analytics/DataValidation.scala`,
`src/main/scala/com/ecommerce/utils/ConfigLoader.scala`,
`src/main/scala/com/ecommerce/utils/SparkSessionBuilder.scala`,
`src/main/resources/application.conf`, `src/main/resources/reference.conf`,
`README.md`.

## Membre B : Data Transformation Engineer

| Champ | Valeur |
| :-- | :-- |
| Nom et prénom | BAMBA Issouf |
| Adresse e mail | À RENSEIGNER PAR ISSOUF |
| Nom configuré dans Git (`git config user.name`) | Issouf BAMBA |
| Parties du sujet | Partie 3 |
| Questions traitées | 3.1, 3.2, 3.3, 3.4 (bonus) |

Fichiers dont Issouf est propriétaire :
`src/main/scala/com/ecommerce/analytics/TimeFeatures.scala`,
`src/main/scala/com/ecommerce/analytics/DataTransformation.scala`.

## Membre C : Analytics & Performance Engineer

| Champ | Valeur |
| :-- | :-- |
| Nom et prénom | AHOGA Josias |
| Adresse e mail | josiasahoga25@gmail.com |
| Nom configuré dans Git (`git config user.name`) | Josias AHOGA |
| Parties du sujet | Parties 4, 5 et 6 |
| Questions traitées | 4.1, 4.2, 4.3 (bonus), 4.4 (bonus), 5.1, 5.2, 5.3 (bonus), 6.1, 6.2 (bonus) |

Fichiers dont Josias est propriétaire :
`src/main/scala/com/ecommerce/analytics/Analytics.scala`,
`src/main/scala/com/ecommerce/analytics/SparkOptimizations.scala`,
`src/main/scala/com/ecommerce/analytics/MainApp.scala`,
`src/main/scala/com/ecommerce/utils/ResultWriter.scala`.

## Travail collectif

Parties 0 et 8 : organisation, journal de contribution, relectures croisées, tests, documentation, archive de remise et soutenance. Chaque membre y contribue pour la portion de code dont il est propriétaire.

Le sujet mentionne une « Partie 9 » dans son introduction mais n'en décrit aucune. Le périmètre livré suit donc les Parties 0 à 8 et la liste officielle du tronc commun.

## Commandes Git utilisées par chaque membre

```sh
git config user.name  "Eudoxie ABOUTA"
git config user.email "adresse@exemple.com"
```

Le nom ci dessus doit correspondre exactement à la colonne « Nom configuré dans Git » pour que `git log` permette de relier chaque commit à son auteur.

## Vérification rapide de l'historique

```sh
git shortlog -sne
```

Cette commande affiche le nombre de commits par auteur. Le sujet exige au minimum quatre commits par membre.
