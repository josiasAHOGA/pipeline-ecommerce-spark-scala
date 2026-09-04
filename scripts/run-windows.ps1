param([ValidateSet('all','ingestion','transformation','analytics','benchmark')][string]$Stage = 'all')
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'env-windows.ps1')
Push-Location $projectRoot
try {
    & "$env:SPARK_HOME\bin\spark-submit.cmd" --master 'local[2]' --driver-memory 3g --class com.ecommerce.analytics.MainApp 'dist\ecommerce-analytics.jar' $Stage
    if ($LASTEXITCODE -ne 0) { throw "Échec Spark : code $LASTEXITCODE" }
} finally { Pop-Location }
