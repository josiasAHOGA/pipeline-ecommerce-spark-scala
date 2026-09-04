# EcommerceAnalytics, Groupe 9
# Équivalent Windows du Makefile. Mêmes noms de cibles.
#   .\make.ps1 help
#   .\make.ps1 run
#   .\make.ps1 run -Stage benchmark
#   .\make.ps1 dashboard
#
# La cible run s'appuie sur l'environnement portable préparé par
# scripts\setup-windows.ps1 (dossier .runtime), ou sur JAVA_HOME et
# SPARK_HOME s'ils sont déjà définis dans la session.

param(
    [Parameter(Position = 0)]
    [ValidateSet('help', 'compile', 'test', 'assembly', 'run', 'ingestion', 'transformation',
                 'analytics', 'benchmark', 'dashboard', 'verify', 'docker-build', 'docker-run', 'clean')]
    [string]$Target = 'help',

    [ValidateSet('all', 'ingestion', 'transformation', 'analytics', 'benchmark')]
    [string]$Stage = 'all'
)

$ErrorActionPreference = 'Stop'
$projectRoot = $PSScriptRoot

function Show-Help {
    @'
Cibles disponibles :
  help            Affiche cette aide
  compile         Compile les sources Scala (sbt compile)
  test            Exécute la suite de régression (sbt test)
  assembly        Produit le JAR exécutable dans dist
  run             Exécute le pipeline (-Stage all par défaut)
  ingestion       Exécute uniquement l'ingestion et le rapport de qualité
  transformation  Exécute l'ingestion puis l'enrichissement
  analytics       Exécute le pipeline jusqu'aux rapports analytiques
  benchmark       Compare l'exécution sans puis avec cache et broadcast
  dashboard       Régénère le tableau de bord HTML (le pipeline le produit)
  verify          Contrôle de cohérence des sorties produites (Python 3)
  docker-build    Construit l'image Docker reproductible
  docker-run      Exécute le pipeline dans le conteneur
  clean           Supprime les artefacts de compilation
'@ | Write-Host
}

function Invoke-Spark([string]$SparkStage) {
    . (Join-Path $projectRoot 'scripts\env-windows.ps1')
    Push-Location $projectRoot
    try {
        & "$env:SPARK_HOME\bin\spark-submit.cmd" --master 'local[2]' --driver-memory 3g `
            --class com.ecommerce.analytics.MainApp 'dist\ecommerce-analytics.jar' $SparkStage
        if ($LASTEXITCODE -ne 0) { throw "Échec Spark : code $LASTEXITCODE" }
    } finally { Pop-Location }
}

function Invoke-Sbt([string[]]$SbtArgs) {
    Push-Location $projectRoot
    try {
        & sbt @SbtArgs
        if ($LASTEXITCODE -ne 0) { throw "Échec SBT : code $LASTEXITCODE" }
    } finally { Pop-Location }
}

switch ($Target) {
    'help'           { Show-Help }
    'compile'        { Invoke-Sbt @('compile') }
    'test'           { Invoke-Sbt @('test') }
    'assembly'       {
        Invoke-Sbt @('assembly')
        $dist = Join-Path $projectRoot 'dist'
        if (-not (Test-Path $dist)) { New-Item -ItemType Directory -Path $dist | Out-Null }
        Copy-Item (Join-Path $projectRoot 'target\scala-2.12\ecommerce-analytics.jar') `
                  (Join-Path $dist 'ecommerce-analytics.jar') -Force
    }
    'run'            { Invoke-Spark $Stage }
    'ingestion'      { Invoke-Spark 'ingestion' }
    'transformation' { Invoke-Spark 'transformation' }
    'analytics'      { Invoke-Spark 'analytics' }
    'benchmark'      { Invoke-Spark 'benchmark' }
    'dashboard'      {
        # Le tableau de bord est écrit par le pipeline lui même, à la fin de l'exécution.
        Invoke-Spark 'all'
        Write-Host ("Tableau de bord : " + (Join-Path $projectRoot 'output\dashboard.html')) -ForegroundColor Green
    }
    'verify'         { & python (Join-Path $projectRoot 'scripts\verify_outputs.py') }
    'docker-build'   { Push-Location $projectRoot; try { & docker compose build } finally { Pop-Location } }
    'docker-run'     { Push-Location $projectRoot; try { & docker compose run --rm pipeline $Stage } finally { Pop-Location } }
    'clean'          {
        Invoke-Sbt @('clean')
        foreach ($d in 'target', 'project\target', 'project\project') {
            $p = Join-Path $projectRoot $d
            if (Test-Path $p) { Remove-Item -Recurse -Force $p }
        }
    }
}
