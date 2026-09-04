# Archive de remise, Groupe 9
#
# Produit GROUPE_ABOUTA_BAMBA_AHOGA.zip à côté du projet, avec le contenu
# exigé par la Question 8.1 : code source complet, répertoire .git, JAR
# exécutable, README, EQUIPE, CONTRIBUTIONS, fichiers de configuration,
# échantillon des résultats et support de présentation.
#
# Les artefacts de compilation et l'environnement portable sont exclus :
# ils se régénèrent et alourdiraient l'archive inutilement.
#
#   .\scripts\creer-archive.ps1
#   .\scripts\creer-archive.ps1 -SansDonnees   (si l'archive dépasse la taille acceptée)

param(
    [switch]$SansDonnees,
    [string]$Destination
)

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path $PSScriptRoot -Parent
$nom = 'GROUPE_ABOUTA_BAMBA_AHOGA'
if (-not $Destination) { $Destination = Join-Path (Split-Path $projectRoot -Parent) "$nom.zip" }

# Artefacts de compilation, environnement portable et sorties volumineuses
# régénérables. Le CSV complet des transactions enrichies pèse à lui seul
# 48 Mo : il est reconstruit par `make run` et un échantillon de mille lignes
# figure dans samples. Sa version Parquet, elle, reste dans l'archive.
# Les doubles sorties de benchmark sont reconstruites par `make benchmark`.
$exclus = @(
    'target', '.runtime', 'output_verif', '.idea', '.bsp', '.metals',
    'spark-warehouse', 'project\target', 'project\project',
    'output\benchmark', 'output\csv\enriched_transactions'
)
if ($SansDonnees) { $exclus += 'data' }

$sas = Join-Path ([System.IO.Path]::GetTempPath()) ("archive-" + [guid]::NewGuid().ToString('N'))
$racine = Join-Path $sas 'EcommerceAnalytics'
New-Item -ItemType Directory -Force -Path $racine | Out-Null

try {
    Write-Host "Copie du projet..." -ForegroundColor Cyan
    $arguments = @($projectRoot, $racine, '/E', '/NFL', '/NDL', '/NJH', '/NJS')
    if ($exclus.Count -gt 0) {
        $arguments += '/XD'
        foreach ($d in $exclus) { $arguments += (Join-Path $projectRoot $d) }
    }
    & robocopy @arguments | Out-Null
    if ($LASTEXITCODE -ge 8) { throw "Échec de la copie, code robocopy $LASTEXITCODE" }

    if (Test-Path $Destination) { Remove-Item $Destination -Force }
    Write-Host "Compression..." -ForegroundColor Cyan
    # Compress-Archive ignore les éléments cachés : le répertoire .git, exigé
    # par la Question 8.1, en serait absent. On passe donc par .NET.
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    [System.IO.Compression.ZipFile]::CreateFromDirectory(
        $sas, $Destination, [System.IO.Compression.CompressionLevel]::Optimal, $false)

    $mo = [math]::Round((Get-Item $Destination).Length / 1MB, 1)
    Write-Host ""
    Write-Host ("Archive : {0}" -f $Destination) -ForegroundColor Green
    Write-Host ("Taille  : {0} Mo" -f $mo) -ForegroundColor Green
    Write-Host ""
    Write-Host "Vérifier avant l'envoi :"
    Write-Host "  1. Les adresses e mail sont renseignées dans EQUIPE.md"
    Write-Host "  2. Les relectures et les heures sont renseignées dans CONTRIBUTIONS.md"
    Write-Host "  3. git shortlog -sne HEAD montre au moins quatre commits par membre"
    Write-Host "  4. Le courriel part d'un membre, les deux autres en copie"
} finally {
    if (Test-Path $sas) { Remove-Item -Recurse -Force $sas }
    # robocopy signale par un code non nul le simple fait d'avoir copié des
    # fichiers : sans cette remise à zéro, le script paraît échouer.
    $global:LASTEXITCODE = 0
}
