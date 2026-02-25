FROM apache/spark:3.5.1-scala2.13-java17-ubuntu

USER root
COPY target/scala-2.13/UnifiedTransactionPipeline-assembly-1.0.0.jar /opt/spark/jars/app.jar
COPY conf/ /opt/spark/conf/

ENV SPARK_DRIVER_MEMORY=4g
ENV SPARK_EXECUTOR_MEMORY=8g

EXPOSE 8080 4040

ENTRYPOINT ["/opt/spark/bin/spark-submit", \
    "--class", "com.pipeline.Main", \
    "--master", "local[*]", \
    "--driver-memory", "4g", \
    "/opt/spark/jars/app.jar"]
