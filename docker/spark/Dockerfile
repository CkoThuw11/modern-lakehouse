FROM apache/spark:3.5.3

USER root

COPY downloads/iceberg-spark-runtime-3.5_2.12-1.9.1.jar /opt/spark/jars/
COPY downloads/iceberg-aws-bundle-1.9.1.jar /opt/spark/jars/
COPY downloads/hadoop-aws-3.3.4.jar /opt/spark/jars/
COPY downloads/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/

COPY spark/configs/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER spark
