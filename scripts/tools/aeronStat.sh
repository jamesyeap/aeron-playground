# === JVM STUFF ===
AERON_JAR="/Users/jamesyeap/Developer/java/aeron/aeron-all/build/libs/aeron-all-1.51.0.jar"
ADD_OPENS="--add-opens java.base/jdk.internal.misc=ALL-UNNAMED"

# === CONFIG ===
AERON_DIR=$1; shift

java \
  -cp ${AERON_JAR} \
  ${ADD_OPENS} \
  -Daeron.dir=${AERON_DIR} \
  io.aeron.samples.AeronStat \
  "$@"
