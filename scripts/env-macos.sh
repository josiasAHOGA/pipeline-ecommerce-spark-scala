#!/usr/bin/env bash
# Environnement de démonstration macOS, Groupe 9.
#
# Ce fichier ne s'exécute pas : il se source.
#     source scripts/env-macos.sh
#
# Il ne modifie aucun réglage système et n'écrit rien en dehors de
# ~/.ecommerce-demo. Tout le toolchain (JDK, sbt, Spark) et tous les caches de
# téléchargement vivent dans ce seul répertoire, ce qui rend la désinstallation
# aussi simple qu'un rm -rf (voir scripts/uninstall-macos.sh).

ECOM_HOME="${ECOM_HOME:-$HOME/.ecommerce-demo}"

export JAVA_HOME="$ECOM_HOME/jdk/Contents/Home"
export SPARK_HOME="$ECOM_HOME/spark"

# Caches de dépendances confinés : rien ne va dans ~/.sbt, ~/.ivy2 ni ~/.cache.
export COURSIER_CACHE="$ECOM_HOME/cache/coursier"
export SBT_OPTS="-Dsbt.global.base=$ECOM_HOME/cache/sbt-global -Dsbt.boot.directory=$ECOM_HOME/cache/sbt-boot -Dsbt.ivy.home=$ECOM_HOME/cache/ivy2 -Divy.home=$ECOM_HOME/cache/ivy2 -Xmx3g"

export PATH="$JAVA_HOME/bin:$ECOM_HOME/sbt/bin:$SPARK_HOME/bin:$PATH"

# Spark écrit ses fichiers temporaires de shuffle dans ce répertoire.
export SPARK_LOCAL_DIRS="$ECOM_HOME/tmp"

echo "Environnement chargé depuis $ECOM_HOME"
echo "  java  : $(command -v java || echo absent)"
echo "  sbt   : $(command -v sbt || echo absent)"
echo "  spark : $(command -v spark-submit || echo absent)"
