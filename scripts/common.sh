ADD_OPENS="--add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/java.util.zip=ALL-UNNAMED"
VM_OPTIONS="-XX:+UnlockExperimentalVMOptions -XX:+TrustFinalNonStaticFields -XX:+UnlockDiagnosticVMOptions -XX:GuaranteedSafepointInterval=300000 -XX:+UseParallelGC"

# The application fat jars bundle Aeron and the SLF4J/Log4j2 stack, so only the
# Aeron agent jar (loaded via -javaagent, not on the classpath) is needed here.
AERON_AGENT_JAR="/Users/jamesyeap/.gradle/caches/modules-2/files-2.1/io.aeron/aeron-agent/1.51.0/17baa9c839ce6c4b35eaa40d63188b41383ced80/aeron-agent-1.51.0.jar"
