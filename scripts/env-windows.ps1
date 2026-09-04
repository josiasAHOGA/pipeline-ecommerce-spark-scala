$projectRoot = Split-Path $PSScriptRoot -Parent
$runtimeRoot = Join-Path $projectRoot '.runtime'
if (-not $env:JAVA_HOME) {
    $jdk = Get-ChildItem -LiteralPath $runtimeRoot -Directory -Filter 'jdk-*' -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($jdk) { $env:JAVA_HOME = $jdk.FullName }
}
if (-not $env:SPARK_HOME) { $env:SPARK_HOME = Join-Path $runtimeRoot 'spark-3.5.6-bin-hadoop3' }
if (-not $env:HADOOP_HOME) { $env:HADOOP_HOME = Join-Path $runtimeRoot 'hadoop' }
if (-not (Test-Path -LiteralPath "$env:JAVA_HOME\bin\java.exe")) { throw 'JDK 17 absent : lancez setup-windows.ps1.' }
if (-not (Test-Path -LiteralPath "$env:SPARK_HOME\jars")) { throw 'Spark absent : lancez setup-windows.ps1.' }
$env:PATH = "$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;$env:PATH"
$env:SPARK_LOCAL_IP = '127.0.0.1'
