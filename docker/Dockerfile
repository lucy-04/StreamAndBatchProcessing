FROM eclipse-temurin:17-jre-jammy

# Install dependencies needed for Spark
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl procps tini && \
    rm -rf /var/lib/apt/lists/*

# Download and install Spark 3.5.1 with Scala 2.13 (matches build.sbt)
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
ENV SCALA_VERSION=2.13
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

RUN curl -fSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala${SCALA_VERSION}.tgz" \
    | tar -xz -C /opt && \
    mv "/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala${SCALA_VERSION}" "${SPARK_HOME}"

WORKDIR ${SPARK_HOME}

# Copy the fat jar and configuration
COPY target/scala-2.13/UnifiedTransactionPipeline-assembly-1.0.0.jar ${SPARK_HOME}/jars/app.jar
COPY conf/ ${SPARK_HOME}/conf/

# Create data directory for Delta Lake
RUN mkdir -p ${SPARK_HOME}/data

EXPOSE 8080 4040

ENTRYPOINT ["tini", "--", "/opt/spark/bin/spark-submit", \
  "--class", "com.pipeline.Main", \
  "--master", "local[*]", \
  "--driver-memory", "4g", \
  "--conf", "spark.driver.extraJavaOptions=-Dconfig.file=/opt/spark/conf/application.conf", \
  "/opt/spark/jars/app.jar"]
