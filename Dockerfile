# EcommerceAnalytics, Groupe 9
# Image reproductible : Spark 3.5.6, Scala 2.12.18, Java 17.
# Objectif : permettre à un tiers d'exécuter le pipeline sans installer
# ni JDK, ni SBT, ni Spark sur son poste.

# ---------------------------------------------------------------------------
# Étape 1 : compilation du JAR avec SBT
# ---------------------------------------------------------------------------
FROM sbtscala/scala-sbt:eclipse-temurin-17.0.10_7_1.9.9_2.12.18 AS build

WORKDIR /build

# Les descripteurs de build sont copiés en premier : tant qu'ils ne changent
# pas, Docker réutilise la couche de dépendances déjà téléchargée.
COPY build.sbt ./
COPY project/build.properties project/plugins.sbt ./project/

RUN sbt update

COPY src ./src

RUN sbt assembly

# ---------------------------------------------------------------------------
# Étape 2 : image d'exécution, Spark officiel
# ---------------------------------------------------------------------------
FROM apache/spark:3.5.6-scala2.12-java17-python3-ubuntu

USER root
WORKDIR /opt/ecommerce

COPY --from=build /build/target/scala-2.12/ecommerce-analytics.jar ./dist/ecommerce-analytics.jar
COPY data ./data
COPY src/main/resources/application.conf ./conf/application.conf

RUN mkdir -p /opt/ecommerce/output && chmod -R 777 /opt/ecommerce/output

USER spark

ENV SPARK_LOCAL_IP=127.0.0.1

# L'étape du pipeline se passe en argument : all, ingestion, transformation,
# analytics ou benchmark. Valeur par défaut : all.
ENTRYPOINT ["/opt/spark/bin/spark-submit", \
            "--master", "local[*]", \
            "--driver-memory", "3g", \
            "--class", "com.ecommerce.analytics.MainApp", \
            "/opt/ecommerce/dist/ecommerce-analytics.jar"]
CMD ["all"]
