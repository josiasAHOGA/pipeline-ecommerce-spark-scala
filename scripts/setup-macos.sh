#!/usr/bin/env bash
# Installation du toolchain de démonstration sous macOS, Groupe 9.
#
#     bash scripts/setup-macos.sh
#
# Installe, dans le seul répertoire ~/.ecommerce-demo :
#   - un JDK 17 Temurin (Spark 3.5 ne supporte pas encore Java 21 partout) ;
#   - sbt 1.10.7, la version déclarée dans project/build.properties ;
#   - Apache Spark 3.5.6 pour disposer de spark-submit, comme sur un cluster.
#
# Aucune installation système, aucun sudo, aucun Homebrew : le script est le
# pendant macOS de scripts/setup-windows.ps1. La désinstallation complète se
# fait avec scripts/uninstall-macos.sh.
set -euo pipefail

ECOM_HOME="${ECOM_HOME:-$HOME/.ecommerce-demo}"
SBT_VERSION="1.10.7"
SPARK_VERSION="3.5.6"
JDK_MAJOR="17"

case "$(uname -m)" in
  arm64)  JDK_ARCH="aarch64" ;;
  x86_64) JDK_ARCH="x64" ;;
  *) echo "Architecture non gérée : $(uname -m)" >&2; exit 1 ;;
esac

JDK_URL="https://api.adoptium.net/v3/binary/latest/${JDK_MAJOR}/ga/mac/${JDK_ARCH}/jdk/hotspot/normal/eclipse"
SBT_URL="https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz"
# Les miroirs actifs ne servent que les versions courantes ; archive.apache.org
# conserve toutes les versions mais débite lentement. On tente donc le miroir
# rapide, puis l'archive.
SPARK_URLS=(
  "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz"
  "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz"
)

mkdir -p "$ECOM_HOME/telechargements" "$ECOM_HOME/cache" "$ECOM_HOME/tmp"

# Télécharge une seule fois : -C - reprend un transfert interrompu.
recuperer() {
  local url="$1" cible="$2"
  if [ -s "$cible" ]; then
    echo "  déjà téléchargé : $(basename "$cible")"
  else
    echo "  téléchargement : $(basename "$cible")"
    curl -fL --retry 3 --retry-delay 2 -C - -o "$cible" "$url"
  fi
}

echo "1/4 JDK ${JDK_MAJOR} (${JDK_ARCH})"
if [ -x "$ECOM_HOME/jdk/Contents/Home/bin/java" ]; then
  echo "  déjà installé"
else
  recuperer "$JDK_URL" "$ECOM_HOME/telechargements/jdk.tar.gz"
  rm -rf "$ECOM_HOME/jdk" && mkdir -p "$ECOM_HOME/jdk"
  # --strip-components=1 supprime le niveau jdk-17.x.y+z/ de l'archive.
  tar -xzf "$ECOM_HOME/telechargements/jdk.tar.gz" -C "$ECOM_HOME/jdk" --strip-components=1
fi

echo "2/4 sbt ${SBT_VERSION}"
if [ -x "$ECOM_HOME/sbt/bin/sbt" ]; then
  echo "  déjà installé"
else
  recuperer "$SBT_URL" "$ECOM_HOME/telechargements/sbt.tgz"
  rm -rf "$ECOM_HOME/sbt"
  tar -xzf "$ECOM_HOME/telechargements/sbt.tgz" -C "$ECOM_HOME"
fi

echo "3/4 Apache Spark ${SPARK_VERSION} (environ 380 Mo)"
if [ -x "$ECOM_HOME/spark/bin/spark-submit" ]; then
  echo "  déjà installé"
else
  for url in "${SPARK_URLS[@]}"; do
    if recuperer "$url" "$ECOM_HOME/telechargements/spark.tgz"; then break; fi
    echo "  miroir indisponible, essai suivant"
  done
  rm -rf "$ECOM_HOME/spark" && mkdir -p "$ECOM_HOME/spark"
  tar -xzf "$ECOM_HOME/telechargements/spark.tgz" -C "$ECOM_HOME/spark" --strip-components=1
fi

echo "4/4 vérification"
# shellcheck source=/dev/null
source "$(dirname "$0")/env-macos.sh"
java -version
sbt --script-version
spark-submit --version 2>&1 | head -3

cat <<MSG

Toolchain installé dans $ECOM_HOME
Taille : $(du -sh "$ECOM_HOME" | cut -f1)

Prochaine étape, depuis la racine du projet :
    source scripts/env-macos.sh
    bash scripts/run-macos.sh test        # suite de régression
    bash scripts/run-macos.sh all         # pipeline complet

Pour tout supprimer :
    bash scripts/uninstall-macos.sh
MSG
