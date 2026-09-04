# Prendre en main le projet, Groupe 9

ABOUTA Eudoxie, BAMBA Issouf, AHOGA Josias.

## En cinq minutes

1. Ouvrir `output/dashboard.html` par double clic. C'est le résultat du pipeline, tel qu'il sera montré en démonstration. Le bouton en haut à droite bascule entre thème clair et thème sombre.
2. Ouvrir `docs/architecture-medaillon.svg` pour comprendre le trajet des données.
3. Lire `README.md`, section « Ce qui distingue cette livraison ».

## Exécuter le pipeline

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

Sans rien installer du tout, avec Docker :

```sh
docker compose run --rm pipeline all
```

L'exécution complète dure environ deux minutes et demie sur un poste local. Elle réécrit `output/csv`, `output/parquet` et `output/dashboard.html`.

## Ce qu'il reste à faire par le groupe

1. Eudoxie et Issouf renseignent leur adresse e mail dans `EQUIPE.md`.
2. Chaque membre relit le module dont il est relecteur, puis consigne la date et ses remarques dans `CONTRIBUTIONS.md`.
3. Chaque membre complète sa charge de travail réelle et ses difficultés dans `CONTRIBUTIONS.md`.
4. Eudoxie et Issouf créent leurs commits depuis leur propre poste. `scripts/commits-membres.ps1 -Liste` affiche le plan, fichier par fichier et étape par étape.
5. Répéter la soutenance avec `GUIDE_SOUTENANCE.md` et `soutenance/Presentation_Groupe9.pptx`.
6. Renommer l'archive `GROUPE_ABOUTA_BAMBA_AHOGA.zip` avant l'envoi.

## Points de vigilance

Le JAR `dist/ecommerce-analytics.jar` est prévu pour Spark 3.5.6 et Scala 2.12. Il se lance avec `spark-submit`, jamais par double clic ni avec `java -jar` seul.

Le code livré est une base complète à comprendre et à s'approprier, pas un livrable à recopier tel quel. Les champs d'identité et de contribution ne sont pas remplis avec des informations inventées : ils attendent des faits réels.
