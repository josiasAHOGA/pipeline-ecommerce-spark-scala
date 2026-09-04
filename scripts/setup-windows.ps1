param()
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path $PSScriptRoot -Parent
$runtimeRoot = Join-Path $projectRoot '.runtime'
New-Item -ItemType Directory -Force -Path $runtimeRoot | Out-Null

function Get-Download([string]$Url, [string]$Destination) {
    if (-not (Test-Path -LiteralPath $Destination)) {
        Write-Host "Téléchargement : $Url"
        Invoke-WebRequest -Uri $Url -OutFile $Destination
    }
}

if (-not (Get-ChildItem -LiteralPath $runtimeRoot -Directory -Filter 'jdk-*')) {
    $jdkZip = Join-Path $runtimeRoot 'jdk17.zip'
    Get-Download 'https://aka.ms/download-jdk/microsoft-jdk-17-windows-x64.zip' $jdkZip
    Expand-Archive -LiteralPath $jdkZip -DestinationPath $runtimeRoot -Force
}
$sparkRoot = Join-Path $runtimeRoot 'spark-3.5.6-bin-hadoop3'
if (-not (Test-Path -LiteralPath (Join-Path $sparkRoot 'jars'))) {
    $sparkArchive = Join-Path $runtimeRoot 'spark-3.5.6-bin-hadoop3.tgz'
    $sparkUrl = 'https://archive.apache.org/dist/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz'
    Get-Download $sparkUrl $sparkArchive
    $checksumFile = Join-Path $runtimeRoot 'spark.sha512'
    Get-Download "$sparkUrl.sha512" $checksumFile
    $expected = [regex]::Match((Get-Content -LiteralPath $checksumFile -Raw), '(?i)\b[0-9a-f]{128}\b').Value
    $actual = (Get-FileHash -LiteralPath $sparkArchive -Algorithm SHA512).Hash
    if (-not $expected -or $actual -ne $expected) { throw 'Checksum SHA512 Spark incorrect.' }
    tar -xf $sparkArchive -C $runtimeRoot
    if ($LASTEXITCODE -ne 0) { throw 'Extraction de Spark impossible.' }
}

# Apache ne distribue pas ces binaires Windows. Source communautaire explicitée.
# Les empreintes fixées ci-dessous correspondent aux fichiers utilisés pour la vérification.
$hadoopBin = Join-Path $runtimeRoot 'hadoop\bin'
New-Item -ItemType Directory -Force -Path $hadoopBin | Out-Null
$provenance = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'windows-runtime-sources.json') -Raw | ConvertFrom-Json
foreach ($entry in $provenance) {
    $target = Join-Path $hadoopBin $entry.file
    Get-Download $entry.url $target
    if ((Get-FileHash -LiteralPath $target -Algorithm SHA256).Hash -ne $entry.sha256) {
        throw "Empreinte SHA256 incorrecte : $($entry.file)"
    }
}
Get-Download 'https://repo.maven.apache.org/maven2/com/typesafe/config/1.4.3/config-1.4.3.jar' (Join-Path $runtimeRoot 'config-1.4.3.jar')
Write-Host 'Environnement local prêt. Lancez .\scripts\run-windows.ps1 all depuis le dossier du projet.'
