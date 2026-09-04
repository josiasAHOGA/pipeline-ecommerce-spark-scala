# Historique Git par membre, Groupe 9
#
# Le sujet (Question 0.2) demande que chaque membre committe depuis son propre
# poste, avec son propre nom configuré, et au minimum quatre commits chacun.
# Ce script ne fabrique aucun historique : il configure l'identité du membre,
# lui rappelle les fichiers dont il est propriétaire et propose un message de
# commit prêt à l'emploi pour chaque étape. Le membre committe son propre
# travail, après l'avoir réellement effectué.
#
# Exemples :
#   .\scripts\commits-membres.ps1 -Liste
#   .\scripts\commits-membres.ps1 -Membre A -Nom "Eudoxie ABOUTA" -Email "eudoxie@exemple.com" -Etape 1
#   .\scripts\commits-membres.ps1 -Membre B -Nom "Issouf BAMBA"  -Email "issouf@exemple.com"  -Etape 3

[CmdletBinding(DefaultParameterSetName = 'Commit')]
param(
    [Parameter(ParameterSetName = 'Liste')]
    [switch]$Liste,

    [Parameter(ParameterSetName = 'Commit', Mandatory = $true)]
    [ValidateSet('A', 'B', 'C')]
    [string]$Membre,

    [Parameter(ParameterSetName = 'Commit', Mandatory = $true)]
    [string]$Nom,

    [Parameter(ParameterSetName = 'Commit', Mandatory = $true)]
    [string]$Email,

    [Parameter(ParameterSetName = 'Commit', Mandatory = $true)]
    [ValidateRange(1, 4)]
    [int]$Etape
)

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path $PSScriptRoot -Parent

$plan = @{
    'A' = @{
        Role    = 'Data Ingestion & Platform Engineer, Parties 1, 2 et 7'
        Fichiers = @(
            'build.sbt',
            'src/main/scala/com/ecommerce/models/Models.scala',
            'src/main/scala/com/ecommerce/analytics/DataIngestion.scala',
            'src/main/scala/com/ecommerce/analytics/DataValidation.scala',
            'src/main/scala/com/ecommerce/utils/ConfigLoader.scala',
            'src/main/scala/com/ecommerce/utils/SparkSessionBuilder.scala',
            'src/main/resources/application.conf',
            'src/main/resources/reference.conf',
            'README.md',
            'CONTRIBUTIONS.md'
        )
        Etapes = @(
            'Questions 1.1 et 1.2 : structure SBT, dépendances Spark et Typesafe Config',
            'Questions 2.1 et 2.2 : ingestion multi format typée et validation avec motif de rejet',
            'Questions 2.3 à 2.5 : gestion des erreurs, rapport de qualité et intégrité référentielle',
            'Questions 7.1 et 1.3 : configuration externalisée, valeurs par défaut et README'
        )
    }
    'B' = @{
        Role    = 'Data Transformation Engineer, Partie 3'
        Fichiers = @(
            'src/main/scala/com/ecommerce/analytics/TimeFeatures.scala',
            'src/main/scala/com/ecommerce/analytics/DataTransformation.scala',
            'CONTRIBUTIONS.md'
        )
        Etapes = @(
            'Question 3.1 : UDF extractTimeFeatures robuste aux horodatages invalides',
            'Question 3.2 : enrichissement par jointures, rang par utilisateur et tranche d''âge',
            'Question 3.3 : fenêtres glissantes de sept jours, utilisateur actif et délai entre achats',
            'Question 3.4 : écart au panier moyen historique et détection des transactions suspectes'
        )
    }
    'C' = @{
        Role    = 'Analytics & Performance Engineer, Parties 4, 5 et 6'
        Fichiers = @(
            'src/main/scala/com/ecommerce/analytics/Analytics.scala',
            'src/main/scala/com/ecommerce/analytics/SparkOptimizations.scala',
            'src/main/scala/com/ecommerce/analytics/MainApp.scala',
            'src/main/scala/com/ecommerce/utils/ResultWriter.scala',
            'src/main/scala/com/ecommerce/report/DashboardReport.scala',
            'CONTRIBUTIONS.md'
        )
        Etapes = @(
            'Question 4.1 : KPI par marchand, commissions et classements par catégorie et région',
            'Question 4.2 : cohortes, matrice de rétention et meilleure cohorte à trois mois',
            'Questions 4.3 et 4.4 : segmentation RFM, produits, catégories et moyens de paiement',
            'Questions 5 et 6 : cache, broadcast, orchestration et exécution modulaire par étape'
        )
    }
}

if ($Liste -or $PSCmdlet.ParameterSetName -eq 'Liste') {
    foreach ($cle in 'A', 'B', 'C') {
        $p = $plan[$cle]
        Write-Host ""
        Write-Host ("Membre {0} : {1}" -f $cle, $p.Role) -ForegroundColor Cyan
        Write-Host "  Fichiers dont ce membre est propriétaire :"
        $p.Fichiers | ForEach-Object { Write-Host "    $_" }
        Write-Host "  Commits prévus :"
        for ($i = 0; $i -lt $p.Etapes.Count; $i++) {
            Write-Host ("    Étape {0} : {1}" -f ($i + 1), $p.Etapes[$i])
        }
    }
    Write-Host ""
    Write-Host "État actuel de l'historique :" -ForegroundColor Cyan
    Push-Location $projectRoot
    try { & git shortlog -sne HEAD } finally { Pop-Location }
    return
}

$p = $plan[$Membre]
$message = $p.Etapes[$Etape - 1]

Push-Location $projectRoot
try {
    & git config user.name  $Nom
    & git config user.email $Email
    Write-Host ("Identité Git configurée : {0} <{1}>" -f $Nom, $Email) -ForegroundColor Green

    $existants = $p.Fichiers | Where-Object { Test-Path (Join-Path $projectRoot $_) }
    & git add -- $existants

    $indexe = & git diff --cached --name-only
    if (-not $indexe) {
        Write-Host ""
        Write-Host "Aucune modification à committer sur vos fichiers." -ForegroundColor Yellow
        Write-Host "Modifiez d'abord votre module, puis relancez cette commande." -ForegroundColor Yellow
        Write-Host "Fichiers concernés :" -ForegroundColor Yellow
        $p.Fichiers | ForEach-Object { Write-Host "  $_" }
        return
    }

    Write-Host ""
    Write-Host "Fichiers qui vont être committés :"
    $indexe | ForEach-Object { Write-Host "  $_" }
    Write-Host ""
    Write-Host ("Message : {0}" -f $message)

    & git commit -m $message
    if ($LASTEXITCODE -ne 0) { throw "Échec du commit." }

    Write-Host ""
    & git shortlog -sne HEAD
} finally { Pop-Location }
