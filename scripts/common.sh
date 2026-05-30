ADD_OPENS="--add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/java.util.zip=ALL-UNNAMED"
AERON_LIB_JAR="/Users/jamesyeap/.gradle/caches/modules-2/files-2.1/io.aeron/aeron-all/1.51.0/4d17308cba9d4ff3ff97833d6dac52e3289f81e1/aeron-all-1.51.0.jar"
VM_OPTIONS="-XX:+UnlockExperimentalVMOptions -XX:+TrustFinalNonStaticFields -XX:+UnlockDiagnosticVMOptions -XX:GuaranteedSafepointInterval=300000 -XX:+UseParallelGC"

GRADLE_CACHE="$HOME/.gradle/caches/modules-2/files-2.1"
LOG4J_API_JAR="$(find "$GRADLE_CACHE/org.apache.logging.log4j/log4j-api/2.23.1" -name 'log4j-api-2.23.1.jar' -print -quit 2>/dev/null)"
LOG4J_CORE_JAR="$(find "$GRADLE_CACHE/org.apache.logging.log4j/log4j-core/2.23.1" -name 'log4j-core-2.23.1.jar' -print -quit 2>/dev/null)"
LOG4J_SLF4J_JAR="$(find "$GRADLE_CACHE/org.apache.logging.log4j/log4j-slf4j2-impl/2.23.1" -name 'log4j-slf4j2-impl-2.23.1.jar' -print -quit 2>/dev/null)"
SLF4J_API_JAR="$(find "$GRADLE_CACHE/org.slf4j/slf4j-api/2.0.13" -name 'slf4j-api-2.0.13.jar' -print -quit 2>/dev/null)"

LOGGING_JARS="${SLF4J_API_JAR}:${LOG4J_API_JAR}:${LOG4J_CORE_JAR}:${LOG4J_SLF4J_JAR}"
