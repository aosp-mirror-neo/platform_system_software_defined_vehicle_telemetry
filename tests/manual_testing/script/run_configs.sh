#!/bin/bash
set -eu -o pipefail
source tests/manual_testing/script/.env
OUT_DIR=$(realpath $OUT_DIR)
CF_REPORTS_PATH="/data/local/tmp/binary_reports"

log() {
    echo -e "\n$1\n"
}

if [[ -z "$ANDROID_BUILD_TOP" ]]
then
  log "[ERROR] ANDROID_BUILD_TOP is missing. Please, lunch the Android target."
  exit -1
fi

cvd clear

log "Starting VM"
if [[ $RUN_CVD_WITH_SECURITY_FLAG ]]; then
    cvd start -guest-enforce-security false &
else
    cvd start &
fi
CVD_START_PID=$!
# Stop vm when the script is done or cancelled
trap "cvd stop" 0 2 3 15 # Runs 'cvd stop' when one of these signals is received: 0=exit shell, 2=interrupt, 3=quit, 15=terminate

# Create temp directory to hold binary proto files
TEMP_DIR=$(mktemp -d)
# Remove temp dir when the script is done or cancelled
trap "rm -rf $TEMP_DIR" 0 2 3 15 # Run the command when one of these signals is received: 0=exit shell, 2=interrupt, 3=quit, 15=terminate

# These are necessary for gqui to pick up the right protos.
ln -s $ANDROID_BUILD_TOP/external/protobuf/src/google $TEMP_DIR/google
ln -s $ANDROID_BUILD_TOP/system                       $TEMP_DIR/system

convert_proto() {
  log "Converting $1 to $2 as $3"

  cd $TEMP_DIR
  gqui from "$1" \
      proto $3 \
      --outfile="$2"
  cd -
}

# The vm seems to take a bit of time to be ready after the command finishes
wait $CVD_START_PID
sleep 10
adb root

if [ "$TARGET_PRODUCT" = "sdv_core_cf" ]; then
  log "Running in SDV Core"
  PUBLISHERS=$CORE_PUBLISHERS

  if [[ $# -eq 0 ]]; then
      FILES=$CORE_DEFAULT_CONFIGS
  else
      FILES="$@"
  fi
elif [ "$TARGET_PRODUCT" = "sdv_ivi_cf" ]; then
  log "Running in SDV IVI"
  PUBLISHERS=$IVI_PUBLISHERS

  if [[ $# -eq 0 ]]; then
      FILES=$IVI_DEFAULT_CONFIGS
  else
      FILES="$@"
  fi

  log "Starting Telemetry Service"
  adb shell sdv_ivi_telemetry_service_agent &
  trap "pkill -f sdv_ivi_telemetry_service_agent" 0 2 3 15
else
  log "[ERROR] \$TARGET_PRODUCT is missing or unsupported. Please \`lunch\` one of the supported Android targets (\`sdv_core_cf-*\` or \`sdv_ivi_cf-*\`)."
  exit 1
fi


log "Pushing configs to vm"

num_configs=0
CONFIG_FILE_NAMES=""
for FILE in $FILES; do
    NAME=${FILE##*/}
    adb push "$FILE" "/data/local/tmp/$NAME"
    CONFIG_FILE_NAMES+="/data/local/tmp/$NAME "
    ((num_configs += 1))
done

log "Pushed $num_configs configs"

log "Starting publishers: $PUBLISHERS"
for PUBLISHER in ${PUBLISHERS//,/ }; do
    adb shell "$PUBLISHER" &
    trap "pkill -f $PUBLISHER" 0 2 3 15
done

adb shell mkdir "$CF_REPORTS_PATH"
log "Starting telemetry client"
adb shell telemetry_rust_c_client --write-reports-to="$CF_REPORTS_PATH" "$CONFIG_FILE_NAMES" &
TELEMETRY_CLIENT_PID=$!

mkdir -p "$OUT_DIR"
if [ "$LOG_TO_FILE" ]; then
    NOW=$(date '+%F_%H-%M-%S')
    adb logcat | stdbuf -o0 grep -iE 'telemetry|telemetry_rust_c_client' >"$OUT_DIR/$NOW.log" &
else
    adb logcat | stdbuf -o0 grep -iE 'telemetry|telemetry_rust_c_client' &
fi
LOGCAT_PID=$!

# Wait until the telemetry client has finished running
wait $TELEMETRY_CLIENT_PID
sleep 2
log "Telemetry client finished"

log "Retrieving reports into $TEMP_DIR/binary_reports"
adb pull "$CF_REPORTS_PATH" "$TEMP_DIR"

mkdir -p "$OUT_DIR/reports"
num_reports=0
for FILE in $TEMP_DIR/binary_reports/*.pb; do
    NAME=${FILE##*/}
    BASE=${NAME%.pb}
    # Convert report from binary 'pb' to human-readable 'textproto' format
    convert_proto \
      "rawproto:$TEMP_DIR/binary_reports/$BASE.pb" \
      "textproto:$OUT_DIR/reports/$BASE.textproto" \
      system/software_defined_vehicle/telemetry/proto/metrics_configuration.proto:MetricsReport
    ((num_reports += 1))
done

log "Got $num_reports reports (from $num_configs configs)."

adb shell rm -rf "$CF_REPORTS_PATH"
kill -9 "$LOGCAT_PID"
