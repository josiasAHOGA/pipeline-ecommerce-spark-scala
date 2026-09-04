#!/usr/bin/env bash
# Lancement du pipeline sous macOS, Groupe 9.
#
#     bash scripts/run-macos.sh [compile|test|assembly|all|ingestion|transformation|analytics|benchmark]
#
# Les étapes fonctionnelles passent par spark-submit et le JAR assemblé, donc
# exactement la commande de déploiement documentée dans le README. Le JAR est
# reconstruit automatiquement s'il est absent ou plus ancien que les sources.
set -euo pipefail

cd "$(dirname "$0")/.."
# shellcheck source=/dev/null
source scripts/env-macos.sh >/dev/null

CIBLE="${1:-all}"
JAR="dist/ecommerce-analytics.jar"
MASTER="${MASTER:-local[2]}"
DRIVER_MEM="${DRIVER_MEM:-3g}"

assembler() {
  sbt assembly
  mkdir -p dist
  cp target/scala-2.12/ecommerce-analytics.jar "$JAR"
  echo "JAR : $JAR"
}

# Reconstruit si le JAR est absent ou si une source est plus récente que lui.
assembler_si_necessaire() {
  if [ ! -f "$JAR" ] || [ -n "$(find src build.sbt -newer "$JAR" -type f -print -quit)" ]; then
    assembler
  else
    echo "JAR à jour : $JAR"
  fi
}

case "$CIBLE" in
  compile)  sbt compile ;;
  test)     sbt test ;;
  assembly) assembler ;;
  all|ingestion|transformation|analytics|benchmark)
    if command -v spark-submit >/dev/null 2>&1; then
      # Chemin nominal : exactement la commande de déploiement du README.
      assembler_si_necessaire
      spark-submit --master "$MASTER" --driver-memory "$DRIVER_MEM" \
        --class com.ecommerce.analytics.MainApp "$JAR" "$CIBLE"
    else
      # Repli si la distribution Spark n'est pas encore installée. build.sbt
      # place volontairement les dépendances « provided » sur le classpath de
      # run, donc le pipeline s'exécute à l'identique, Spark venant des JAR
      # résolus par sbt au lieu de la distribution.
      echo "spark-submit introuvable, exécution via sbt (résultats identiques)."
      sbt "runMain com.ecommerce.analytics.MainApp $CIBLE"
    fi
    echo ""
    echo "Sorties CSV et Parquet : output/"
    [ -f output/dashboard.html ] && echo "Tableau de bord : open output/dashboard.html"
    ;;
  *)
    echo "Cible inconnue : $CIBLE"
    echo "Cibles acceptées : compile, test, assembly, all, ingestion, transformation, analytics, benchmark"
    exit 1 ;;
esac
