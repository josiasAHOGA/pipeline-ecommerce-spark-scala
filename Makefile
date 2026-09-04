# EcommerceAnalytics, Groupe 9
# Interface unique de commandes. Les trois membres lancent exactement les
# mêmes cibles, ce qui supprime les écarts entre postes.
# Sous Windows sans make, utiliser .\make.ps1 <cible> (mêmes noms de cibles).

SPARK_SUBMIT ?= spark-submit
MASTER       ?= local[2]
DRIVER_MEM   ?= 3g
JAR          ?= dist/ecommerce-analytics.jar
MAIN         ?= com.ecommerce.analytics.MainApp
STAGE        ?= all

.DEFAULT_GOAL := help
.PHONY: help compile test assembly run ingestion transformation analytics benchmark dashboard docker-build docker-run clean verify

help: ## Affiche la liste des cibles disponibles
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	 | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-16s %s\n", $$1, $$2}'

compile: ## Compile les sources Scala
	sbt compile

test: ## Exécute la suite de régression
	sbt test

assembly: ## Produit le JAR exécutable dans target/scala-2.12
	sbt assembly
	@mkdir -p dist
	@cp target/scala-2.12/ecommerce-analytics.jar dist/ecommerce-analytics.jar

run: ## Exécute le pipeline complet avec spark-submit (STAGE=all par défaut)
	$(SPARK_SUBMIT) --master '$(MASTER)' --driver-memory $(DRIVER_MEM) \
	  --class $(MAIN) $(JAR) $(STAGE)

ingestion: ## Exécute uniquement l'ingestion et le rapport de qualité
	$(MAKE) run STAGE=ingestion

transformation: ## Exécute l'ingestion puis l'enrichissement
	$(MAKE) run STAGE=transformation

analytics: ## Exécute le pipeline jusqu'aux rapports analytiques
	$(MAKE) run STAGE=analytics

benchmark: ## Compare l'exécution sans puis avec cache et broadcast
	$(MAKE) run STAGE=benchmark

dashboard: ## Régénère le tableau de bord HTML (le pipeline le produit lui même)
	$(MAKE) run STAGE=all
	@echo "Tableau de bord : output/dashboard.html"

verify: ## Contrôle de cohérence des sorties produites (nécessite Python 3)
	python3 scripts/verify_outputs.py

docker-build: ## Construit l'image Docker reproductible
	docker compose build

docker-run: ## Exécute le pipeline dans le conteneur
	docker compose run --rm pipeline $(STAGE)

clean: ## Supprime les artefacts de compilation
	sbt clean
	rm -rf target project/target project/project
