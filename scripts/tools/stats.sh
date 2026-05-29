# === OPTIONS ===
AERON_STAT="io.aeron.samples.AeronStat"
ERROR_STAT=io.aeron.samples.ErrorStat
STREAM_STAT=io.aeron.samples.StreamStat
BACKLOG_STAT=io.aeron.samples.BacklogStat
LOSS_STAT=io.aeron.samples.LossStat

# === JVM STUFF ===
AERON_JAR="/Users/jamesyeap/Developer/java/aeron/aeron-all/build/libs/aeron-all-1.51.0.jar"
ADD_OPENS="--add-opens java.base/jdk.internal.misc=ALL-UNNAMED"

# === CONFIG ===
AERON_DIR="/tmp/media-driver-1"
PROGRAM=${AERON_STAT}

java \
  -cp ${AERON_JAR} \
  ${ADD_OPENS} \
  -Daeron.dir=${AERON_DIR} \
  ${PROGRAM}
