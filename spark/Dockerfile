FROM bitnamilegacy/spark:3.5

USER root

# Trust the corporate SSL-inspecting proxy's CA so pip/maven/ivy can reach the internet.
COPY certs/fortinet-ca-1.pem /usr/local/share/ca-certificates/fortinet-ca-1.crt
COPY certs/fortinet-ca-2.pem /usr/local/share/ca-certificates/fortinet-ca-2.crt
RUN update-ca-certificates \
    && keytool -importcert -noprompt -trustcacerts \
        -alias fortinet-proxy-ca-1 \
        -file /usr/local/share/ca-certificates/fortinet-ca-1.crt \
        -keystore /opt/bitnami/java/lib/security/cacerts \
        -storepass changeit \
    && keytool -importcert -noprompt -trustcacerts \
        -alias fortinet-proxy-ca-2 \
        -file /usr/local/share/ca-certificates/fortinet-ca-2.crt \
        -keystore /opt/bitnami/java/lib/security/cacerts \
        -storepass changeit

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --retries 10 --timeout 120 --resume-retries 10 \
        delta-spark==3.2.0 boto3 pyarrow pyyaml

RUN apt-get update \
    && apt-get install -y --no-install-recommends git curl \
    && rm -rf /var/lib/apt/lists/*

# Delta/S3A jars as real files under $SPARK_HOME/jars instead of resolved via
# spark.jars.packages (Ivy) at every spark-submit. Two reasons: (1) it was
# adding ~15-20s of Ivy resolution to every job, and (2) Spark Thrift Server
# spawns each JDBC session on its own thread pool whose context classloader
# doesn't see jars Ivy added dynamically at startup -- DeltaCatalog resolved
# fine for normal spark-submit jobs but threw ClassNotFoundException the
# moment a JDBC client opened a session. Baking them into the base classpath
# fixes both. Versions match what spark-defaults.conf used to pull via
# --packages (including transitive deps Ivy was silently resolving for us:
# delta-storage, antlr4-runtime, wildfly-openssl).
RUN SPARK_JARS=/opt/bitnami/spark/jars \
    && curl -fsSL -o $SPARK_JARS/delta-spark_2.12-3.2.0.jar https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar \
    && curl -fsSL -o $SPARK_JARS/delta-storage-3.2.0.jar https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar \
    && curl -fsSL -o $SPARK_JARS/antlr4-runtime-4.9.3.jar https://repo1.maven.org/maven2/org/antlr/antlr4-runtime/4.9.3/antlr4-runtime-4.9.3.jar \
    && curl -fsSL -o $SPARK_JARS/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -fsSL -o $SPARK_JARS/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    && curl -fsSL -o $SPARK_JARS/wildfly-openssl-1.0.7.Final.jar https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.0.7.Final/wildfly-openssl-1.0.7.Final.jar

# bsis_extractor is a private repo — installed via a Docker build secret so the
# token never ends up in the Dockerfile, compose file, or image layer history.
RUN --mount=type=secret,id=github_token \
    --mount=type=cache,target=/root/.cache/pip \
    pip install --retries 10 --timeout 120 --resume-retries 10 \
        "git+https://$(cat /run/secrets/github_token)@github.com/ashuu944/bsis_extractor.git"

COPY conf/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf

WORKDIR /opt/spark-jobs
