param()
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'env-windows.ps1')
$buildRoot = Join-Path $projectRoot ('target\portable-' + [guid]::NewGuid().ToString('N'))
$classes = Join-Path $buildRoot 'classes'
New-Item -ItemType Directory -Force -Path $classes | Out-Null
$configJar = Join-Path $runtimeRoot 'config-1.4.3.jar'
if (-not (Test-Path -LiteralPath $configJar)) { throw 'Typesafe Config absent : lancer setup-windows.ps1.' }
$classpath = "$env:SPARK_HOME\jars\*;$configJar"
$sources = @(Get-ChildItem -LiteralPath (Join-Path $projectRoot 'src\main\scala') -Recurse -Filter '*.scala' | ForEach-Object { $_.FullName })
& "$env:JAVA_HOME\bin\java.exe" -cp $classpath scala.tools.nsc.Main -usejavacp -encoding UTF-8 -d $classes @sources
if ($LASTEXITCODE -ne 0) { throw 'Compilation Scala échouée.' }
Copy-Item -Path (Join-Path $projectRoot 'src\main\resources\*') -Destination $classes -Recurse -Force
Push-Location $classes
try {
    & "$env:JAVA_HOME\bin\jar.exe" xf $configJar
    if ($LASTEXITCODE -ne 0) { throw 'Extraction de Typesafe Config échouée.' }
} finally { Pop-Location }
$dist = Join-Path $projectRoot 'dist'
New-Item -ItemType Directory -Force -Path $dist | Out-Null
& "$env:JAVA_HOME\bin\jar.exe" --create --file (Join-Path $dist 'ecommerce-analytics.jar') --main-class com.ecommerce.analytics.MainApp -C $classes .
if ($LASTEXITCODE -ne 0) { throw 'Création du JAR échouée.' }
Write-Host 'JAR créé dans dist\ecommerce-analytics.jar'
