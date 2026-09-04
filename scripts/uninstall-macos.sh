#!/usr/bin/env bash
# Désinstallation complète de la démonstration sous macOS, Groupe 9.
#
#     bash scripts/uninstall-macos.sh            # demande confirmation
#     bash scripts/uninstall-macos.sh --oui      # sans confirmation
#     bash scripts/uninstall-macos.sh --oui --tout  # supprime aussi le dépôt cloné
#
# Rien n'a été installé au niveau système : la suppression se limite donc à
# ~/.ecommerce-demo (JDK, sbt, Spark, caches de dépendances) et aux artefacts de
# compilation du projet. Les fichiers versionnés dans Git ne sont pas touchés,
# sauf avec --tout qui supprime le répertoire de travail lui même.
set -euo pipefail

ECOM_HOME="${ECOM_HOME:-$HOME/.ecommerce-demo}"
PROJET="$(cd "$(dirname "$0")/.." && pwd)"
SANS_QUESTION=0
TOUT=0
for arg in "$@"; do
  case "$arg" in
    --oui|-y) SANS_QUESTION=1 ;;
    --tout)   TOUT=1 ;;
    *) echo "Argument inconnu : $arg" >&2; exit 1 ;;
  esac
done

echo "Éléments qui vont être supprimés :"
[ -d "$ECOM_HOME" ] && echo "  $ECOM_HOME  ($(du -sh "$ECOM_HOME" 2>/dev/null | cut -f1))" || echo "  $ECOM_HOME  (absent)"
for d in target project/target project/project output .bsp .metals .bloop; do
  [ -e "$PROJET/$d" ] && echo "  $PROJET/$d"
done
[ "$TOUT" -eq 1 ] && echo "  $PROJET  (le dépôt cloné lui même, y compris .git)"

if [ "$SANS_QUESTION" -eq 0 ]; then
  printf "Confirmer la suppression ? [oui/non] "
  read -r reponse
  case "$reponse" in
    oui|o|y|yes) ;;
    *) echo "Annulé, rien n'a été supprimé."; exit 0 ;;
  esac
fi

rm -rf "$ECOM_HOME"
for d in target project/target project/project output .bsp .metals .bloop; do
  rm -rf "$PROJET/$d"
done
echo "Toolchain et artefacts supprimés."

if [ "$TOUT" -eq 1 ]; then
  # Se placer hors du répertoire avant de le supprimer.
  cd "$HOME"
  rm -rf "$PROJET"
  echo "Dépôt supprimé : $PROJET"
fi

echo "Vérification : aucune trace résiduelle attendue."
for chemin in "$HOME/.sbt" "$HOME/.ivy2" "$HOME/.cache/coursier"; do
  [ -e "$chemin" ] && echo "  présent mais non créé par cette démonstration : $chemin"
done
echo "Terminé."
