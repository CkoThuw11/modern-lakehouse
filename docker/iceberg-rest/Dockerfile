FROM apache/iceberg-rest-fixture:1.9.1
COPY downloads/postgresql.jar /usr/lib/iceberg-rest/postgresql.jar
CMD ["java", "-cp", "iceberg-rest-adapter.jar:postgresql.jar", "org.apache.iceberg.rest.RESTCatalogServer"]
