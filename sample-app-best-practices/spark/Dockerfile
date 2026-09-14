FROM docker.io/maven:3.9.11-eclipse-temurin-17 AS dependencies
WORKDIR /build
COPY spark/pom.xml .
RUN mvn --batch-mode dependency:copy-dependencies -DoutputDirectory=/jars

FROM docker.io/apache/spark:4.0.1-scala2.13-java17-python3-ubuntu
USER 0
COPY --from=dependencies /jars/ /opt/spark/jars/
COPY spark/transform.py /opt/spark/work-dir/transform.py
RUN chgrp -R 0 /opt/spark/work-dir /opt/spark/jars \
    && chmod -R g=u /opt/spark/work-dir /opt/spark/jars
USER 185

