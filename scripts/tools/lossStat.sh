# === JVM STUFF ===
AERON_JAR="/Users/jamesyeap/Developer/java/aeron/aeron-all/build/libs/aeron-all-1.51.0.jar"
ADD_OPENS="--add-opens java.base/jdk.internal.misc=ALL-UNNAMED"

java \
  -cp ${AERON_JAR} \
  ${ADD_OPENS} \
  io.aeron.samples.LossStat
