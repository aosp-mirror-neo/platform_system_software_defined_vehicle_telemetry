#!/bin/bash
# Copyright 2024 Google LLC

# This script automates the process of running the SDV Telemetry Simulator on a
# Cuttlefish VM. It handles VM startup, configuration, running the simulator in
# various modes, and collecting output.

# #############################################################################
# Script Configuration and Setup
# #############################################################################

# Exit immediately if a command exits with a non-zero status.
# Treat unset variables as an error.
# The return value of a pipeline is the status of the last command to exit
# with a non-zero status, or zero if no command exited with a non-zero status.
set -eu -o pipefail

# Global variables that will be set by command-line arguments or
# interactive prompts.
use_defaults=false
log_mode=""
sim_mode=""
max_report_count=""
write_binary_reports=false
max_simulation_time=""
base_output_dir=""
sim_actions_path=""

# Output directory for this run.
output_dir=""

# Directory on cuttlefish VM that simulator writes its output to.
readonly SIMULATOR_VM_OUTPUT_DIR="/data/local/tmp/telemetry_simulator_out"
# Get the directory of this script.
SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# ANSI color and style codes for better output formatting.
BOLD=""
BLUE=""
GREEN=""
RED=""
NORMAL=""
# Check if stdout is a terminal before using tput, to avoid garbage in logs.
if [ -t 1 ]; then
    BOLD=$(tput bold)
    BLUE=$(tput setaf 4)
    GREEN=$(tput setaf 2)
    RED=$(tput setaf 1)
    NORMAL=$(tput sgr0)
fi


# Parse command-line arguments to configure the script's behavior.
while [[ $# -gt 0 ]]; do
  case "$1" in
    -d|--use-defaults)
      # -d, --use-defaults: Skips interactive prompts and uses default paths.
      use_defaults=true
      shift # past argument
      ;;
    -l|--log-mode)
      # -l, --log-mode: Sets the log handling mode (console, file, both, none).
      if [[ -n "$2" ]] && [[ "$2" != -* ]]; then
        case "$2" in
          console|file|both|none)
            log_mode="$2"
            shift 2 # past argument and value
            ;;
          *)
            echo "Error: Invalid value for --log-mode: '$2'. Must be one of 'console', 'file', 'both', 'none'." >&2
            exit 1
            ;;
        esac
      else
        echo "Error: --log-mode requires an argument." >&2
        exit 1
      fi
      ;;
    -r|--max-reports)
      # -r, --max-reports: Sets the number of reports to wait for.
      if [[ -z "$2" ]] || [[ "$2" == -* ]]; then
        echo "Error: --max-reports requires an argument." >&2
        exit 1
      fi
      if ! [[ "$2" =~ ^[1-9][0-9]*$ ]]; then
        echo "Error: --max-reports requires a positive integer argument, but got '$2'." >&2
        exit 1
      fi
      max_report_count="$2"
      shift 2
      ;;
    -c|--client-only)
      # -c, --client-only: Runs as Telemetry Client only.
      if [ -n "$sim_mode" ]; then echo "Error: Only one simulation mode (-c|--client-only, -p|--publisher-only, -b|--both) can be specified." >&2; exit 1; fi
      sim_mode="client"
      shift # past argument
      ;;
    -p|--publisher-only)
      # -p, --publisher-only: Runs Simulation Publishers only.
      if [ -n "$sim_mode" ]; then echo "Error: Only one simulation mode (-c|--client-only, --p|--publisher-only, -b|--both) can be specified." >&2; exit 1; fi
      sim_mode="publisher"
      shift # past argument
      ;;
    -b|--both)
      # -b, --both: Runs both Client and Publishers.
      if [ -n "$sim_mode" ]; then echo "Error: Only one simulation mode (-c|--client-only, -p|--publisher-only, -b|--both) can be specified." >&2; exit 1; fi
      sim_mode="both"
      shift # past argument
      ;;
    -o|--output-dir)
      # -o, --output-dir: Sets the base directory for output files.
      if [[ -n "$2" ]] && [[ "$2" != -* ]]; then
        base_output_dir="$2"
        shift 2 # past argument and value
      else
        echo "Error: --output-dir requires an argument." >&2
        exit 1
      fi
      ;;
    -t|--max-time)
      # -t, --max-time: Sets the max simulation time, e.g., "minutes:2".
      if [[ -n "$2" ]] && [[ "$2" != -* ]]; then
        max_simulation_time="$2"
        shift 2 # past argument and value
      else
        echo "Error: --max-time requires an argument." >&2
        exit 1
      fi
      ;;
    --binary-reports)
      # --binary-reports: Enables writing binary report files.
      write_binary_reports=true
      shift # past argument
      ;;
    --sim-actions)
      # --sim-actions: Sets the path to the simulation actions file.
      if [[ -n "$2" ]] && [[ "$2" != -* ]]; then
        sim_actions_path="$2"
        shift 2 # past argument and value
      else
        echo "Error: --sim-actions requires an argument." >&2
        exit 1
      fi
      ;;
    *)
      shift # unknown option
      ;;
  esac
done

# #############################################################################
# Function Definitions
# #############################################################################

# Prints a formatted status message.
# Arguments:
#   $1: The message to print.
status_update() {
    echo -e "\n${BOLD}${GREEN}==>${NORMAL} ${BOLD}$1${NORMAL}"
}

# Prints a formatted error message to stderr.
# Arguments:
#   $1: The message to print.
error_update() {
    echo -e "\n${BOLD}${RED}==>${NORMAL} ${BOLD}$1${NORMAL}" >&2
}

# Clears any previous Cuttlefish instances and starts a new one.
# `cvd` commands are run in the background so the script can continue
# with other tasks while the VM boots.
start_vm() {
    status_update "Starting Cuttlefish VM..."

    # Clear previous Cuttlefish state, redirecting output to /dev/null.
    cvd clear >/dev/null 2>&1
    # Start the VM, redirecting stdin from /dev/null to prevent it from
    # consuming subsequent user input. Runs as a background job (&).
    cvd create < /dev/null >/dev/null 2>&1 &
}

# Waits for the Cuttlefish VM to be fully booted and accessible via ADB.
wait_for_vm() {
    status_update "Waiting for VM to come online..."

    # A simple spinner to provide visual feedback during the wait.
    sp='/-\|'
    printf ' '

    # Loop until `adb shell` can successfully execute a command, which indicates
    # the VM is ready.
    until adb shell echo 'VM running' &>/dev/null; do
        printf '\b%.1s' "$sp"
        # Rotate the spinner character.
        sp=${sp#?}${sp%???}
        sleep .5
    done
    adb root >/dev/null 2>&1

    if [ "$TARGET_PRODUCT" = "sdv_ivi_cf" ]; then
        # Special handling for the IVI target: start the telemetry service agent.
        adb shell sdv_ivi_telemetry_service_agent >/dev/null 2>&1 &
    fi
}

# Asks the user for a config directory, validates it, and returns the path.
#
# Arguments:
#   $1: The name of the configuration type (e.g., "Metrics configs").
#   $2: The default directory path relative to the script's location.
# Outputs:
#   Writes the validated directory path to stdout.
# Exits:
#   If the directory is invalid or empty.
get_config_dir() {
    local config_type="$1"
    local default_dir_relative="$2"
    local default_dir="$SCRIPT_DIR/$default_dir_relative"
    local selected_dir
    local display_path

    # Replace the user's home directory with ~ for a shorter display path.
    display_path="${default_dir/#$HOME/\~}"

    if [ "$use_defaults" = true ]; then
        selected_dir="$default_dir"
        # Redirect informational messages to stderr to avoid being captured by command substitution.
        echo "Using default directory for $config_type: $display_path" >&2
    else
        printf "\n%s dir [or ENTER to use %s]\n>" "$config_type" "${BOLD}${BLUE}$display_path${NORMAL}" >&2
        read -r selected_dir </dev/tty

        # Handle tilde expansion for user input.
        if [[ "$selected_dir" == "~" ]]; then
            selected_dir="$HOME"
        elif [[ "$selected_dir" == "~/"* ]]; then
            selected_dir="$HOME/${selected_dir:2}"
        fi

        # Use default if user input is empty
        : "${selected_dir:=$default_dir}"
    fi

    if [[ ! -d "$selected_dir" ]]; then
        echo "$selected_dir does not seem to exist or is not a directory, aborting" >&2
        exit 1
    fi

    # Check if there are any textproto files in the selected directory.
    # We use find with -quit to efficiently check for the first matching file.
    if [ -z "$(find "$selected_dir" -maxdepth 1 -type f \( -name "*.textproto" -o -name "*.txtpb" \) -print -quit)" ]; then
        echo "$selected_dir does not contain any .textproto or .txtpb files, aborting" >&2
        exit 1
    fi

    # The final echo to stdout is the "return value" of the function.
    echo "$selected_dir"
}

# Prompts the user for the path to a simulation actions file.
# This is only called if the --sim-actions command-line flag is not used.
# The simulator will use its default behavior if not specified.
get_sim_actions_file() {
    # If path is already set by command-line arg, do nothing.
    if [ -n "$sim_actions_path" ]; then
        return
    fi

    # Don't prompt if using defaults, as simulator actions are not used in the default scenario.
    if [ "$use_defaults" = true ]; then
        return
    fi

    local selected_file

    printf "\nSimulation actions file (optional) [or ENTER to skip]\n>" >&2
    read -r selected_file </dev/tty

    # No simulation actions configuration specified.
    if [ -z "$selected_file" ]; then
        return
    fi

    if [[ ! -f "$selected_file" ]]; then
        echo "File '$selected_file' does not seem to exist or is not a regular file, aborting" >&2
        exit 1
    fi

    sim_actions_path="$selected_file"
}
# Prompts the user for the maximum number of reports to wait for.
# Only called if there is no `--max-reports` command-line flag.
get_max_reports() {
    if [ -z "$max_report_count" ]; then
        printf "\nHow many reports to wait for? %s(default: 1)%s\n> " "${BOLD}" "${NORMAL}"
        read -r answer </dev/tty
        : "${answer:=1}"
        if ! [[ "$answer" =~ ^[1-9][0-9]*$ ]]; then
            echo "Error: Invalid input. Please enter a positive integer." >&2
            exit 1
        fi
        max_report_count="$answer"
    fi
}

# Prompts the user for the max simulation time.
# Only called if there is no `--max-time` command-line flag.
get_max_simulation_time() {
    if [ -z "$max_simulation_time" ]; then
        printf "\nMax simulation time? (e.g., seconds:30, minutes:2) %s(default: minutes:1)%s\n> " "${BOLD}" "${NORMAL}"
        read -r answer </dev/tty
        : "${answer:=minutes:1}"
        # Basic validation to check for the colon separator. The simulator binary
        # will perform more thorough validation.
        if ! [[ "$answer" =~ .+:.+ ]]; then
            echo "Error: Invalid format. Expected format is unit:value (e.g., minutes:1)." >&2
            exit 1
        fi
        max_simulation_time="$answer"
    fi
}

# Pushes all metric configuration files from the specified directory to the VM.
# It builds a space-separated string of the destination paths on the VM for
# use as a command-line argument to the simulator.
push_metrics_configs() {
    METRICS_CONFIGS=""
    # Use a glob to find all .textproto and .txtpb files.
    for METRICS_CONFIG in "$METRICS_DIR"/*.textproto "$METRICS_DIR"/*.txtpb; do
        if [ ! -f "$METRICS_CONFIG" ]; then
            continue
        fi
        FILE_NAME=${METRICS_CONFIG##*/}
        adb push "$METRICS_CONFIG" "/data/local/tmp/$FILE_NAME"  >/dev/null 2>&1
        METRICS_CONFIGS+="/data/local/tmp/$FILE_NAME "
    done
}

# Pushes all publisher configuration and data files to the VM.
# It builds a space-separated string of the destination paths for config files
# to be used as a command-line argument to the simulator.
push_publisher_configs() {
    PUBLISHER_CONFIGS=""
    # Loop through all files in the publisher directory.
    for PUBLISHER_CONFIG in "$PUBLISHERS_DIR"/*; do
        if [ ! -f "$PUBLISHER_CONFIG" ]; then
            continue
        fi
        FILE_NAME=${PUBLISHER_CONFIG##*/}
        adb push "$PUBLISHER_CONFIG" "/data/local/tmp/$FILE_NAME"  >/dev/null 2>&1
        if [[ $FILE_NAME == *.textproto || $FILE_NAME == *.txtpb ]]; then
            PUBLISHER_CONFIGS+="/data/local/tmp/$FILE_NAME "
        fi
    done
}

# Pushes the simulation actions file to the VM if specified.
# It returns the destination path on the VM for use as a command-line argument.
push_sim_actions() {
    SIM_ACTIONS_ARG=""
    if [ -n "$sim_actions_path" ]; then
        if [ ! -f "$sim_actions_path" ]; then
            echo "Error: Simulation actions file not found at '$sim_actions_path'" >&2
            exit 1
        fi
        FILE_NAME=${sim_actions_path##*/}
        adb push "$sim_actions_path" "/data/local/tmp/$FILE_NAME" >/dev/null 2>&1
        # This will be expanded into --simulation-actions /path/to/file
        SIM_ACTIONS_ARG="--simulation-actions \"/data/local/tmp/$FILE_NAME\""
    fi
    echo "$SIM_ACTIONS_ARG"
}

# Manages logging and waits for the main simulator process to complete.
wait_for_simulator_finish() {
    local CONSOLE_LOGCAT_PID=""
    local FILE_LOGCAT_PID=""

    # If log mode is 'console' or 'both', start streaming filtered logs.
    if [[ "$log_mode" == "console" || "$log_mode" == "both" ]]; then
        status_update "Streaming filtered simulator logs to console..."
        # Run logcat in the background and save its PID.
        adb logcat -v raw -v color -s SdvTelemetrySimulator &
        CONSOLE_LOGCAT_PID=$!
    fi

    # If log mode is 'file' or 'both', start saving all logs to a file.
    if [[ "$log_mode" == "file" || "$log_mode" == "both" ]]; then
        # Save log to a file inside the unique output directory for this run.
        local log_file="$output_dir/logcat.log"
        status_update "Saving all logs to $log_file..."
        adb logcat -v raw > "$log_file" &
        FILE_LOGCAT_PID=$!
    fi

    if [[ "$log_mode" == "none" ]]; then
        status_update "Waiting for simulator to finish..."
    fi

    # Wait for simulator to finish running and capture return code of the process.
    local sim_succeeded=false
    if wait "$SIMULATOR_PID"; then
        sim_succeeded=true
    fi

    # Clean up any background logcat processes.
    if [ -n "$CONSOLE_LOGCAT_PID" ]; then
        kill -9 "$CONSOLE_LOGCAT_PID"
        wait "$CONSOLE_LOGCAT_PID" 2>/dev/null || true
    fi
    if [ -n "$FILE_LOGCAT_PID" ]; then
        kill -9 "$FILE_LOGCAT_PID"
        wait "$FILE_LOGCAT_PID" 2>/dev/null || true
    fi

    if $sim_succeeded; then
        status_update "Simulator finished successfully."
    else
        error_update "Simulator failed."
    fi
}

# Pulls the output directory generated by the simulator from the VM to the host
# and lists the generated files.
collect_output_files() {
    # After the simulator runs, pull the entire output directory it generated on
    # the VM to the host's run-specific output directory.
    adb pull "$SIMULATOR_VM_OUTPUT_DIR/" "$output_dir/" >/dev/null 2>&1

    status_update "Output files pulled from VM"
    echo "  Location: $output_dir"

    # To provide a summary, find all files within the pulled directory.
    # Process substitution (<(command)) and mapfile are used to read the
    # newline-separated output of `find` directly into a bash array named `files`.
    # This is a robust way to handle filenames that might contain spaces.
    mapfile -t files < <(find "$output_dir" -type f)

    local file_count=${#files[@]}
    local limit=5

    # Check if any files were actually generated and pulled.
    if [ "$file_count" -eq 0 ]; then
        echo "  (No output files were generated)" >&2
        return
    fi

    # To avoid overwhelming the user with a long list of files, only list the
    # first few.
    for (( i=0; i<file_count && i<limit; i++ )); do
        # `basename` is used to strip the directory path, showing only the filename.
        echo "  ➜ $(basename "${files[i]}")"
    done

    # If the number of files exceeds the display limit, print a summary message
    # indicating how many other files were also pulled.
    if [ "$file_count" -gt "$limit" ]; then
        local remaining=$((file_count - limit))
        echo "  ... and ${remaining} other file(s)."
    fi
}

cleanup() {
    echo -e "\n${BOLD}Performing cleanup...${NORMAL}"
    # Kill the simulator if its PID is set
    if [[ -n "${SIMULATOR_PID-}" ]]; then
        kill -9 "$SIMULATOR_PID" 2>/dev/null || true
    fi
    # Always try to kill the agent for the IVI target
    if [ "$TARGET_PRODUCT" = "sdv_ivi_cf" ]; then
        pkill -f sdv_ivi_telemetry_service_agent 2>/dev/null || true
    fi
    echo "Cleanup complete."
}

# #############################################################################
# Main Script Execution
# #############################################################################
#
# The script logic starts here, after all functions have been defined.

# Ensure the Android build environment has been set up.
if [[ -z "${ANDROID_BUILD_TOP+x}" ]]; then
    echo "[ERROR] ANDROID_BUILD_TOP is missing. Please, lunch the Android target." >&2
    exit 1
fi

# Begin the process by starting the Cuttlefish VM in the background.
start_vm

# If the simulation mode was not set via a command-line flag, prompt the user.
if [ -z "$sim_mode" ]; then
    printf "\nSelect simulation mode:\n"
    printf " [c] Run as Telemetry Client only\n"
    printf " [p] Run as Simulation Publisher only\n"
    printf " [b] Run both Client and Publisher %s(default)%s\n> " "${BOLD}" "${NORMAL}"
    # Read a single character for the choice.
    read -r -n1 answer </dev/tty
    echo # Add a newline after user input

    case "$answer" in
        c|C) sim_mode="client" ;;
        p|P) sim_mode="publisher" ;;
        b|B|"") sim_mode="both" ;; # Default case
        *)
            echo -e "\nOops that's not a valid response :(" >&2
            exit 1
            ;;
    esac
fi

# Based on the selected simulation mode, get the necessary configuration
# directories and other parameters from the user.
case "$sim_mode" in
    "client")
        METRICS_DIR=$(get_config_dir "Metrics configs" "average_speed/metrics_configs")
        get_sim_actions_file
        get_max_reports
        ;;
    "publisher")
        PUBLISHERS_DIR=$(get_config_dir "Publisher configs and data" "average_speed/publisher_configs")
        ;;
    "both")
        METRICS_DIR=$(get_config_dir "Metrics configs" "average_speed/metrics_configs")
        PUBLISHERS_DIR=$(get_config_dir "Publisher configs and data" "average_speed/publisher_configs")
        get_sim_actions_file
        get_max_reports
        ;;
esac

get_max_simulation_time

# If the log mode was not set via a command-line flag, prompt the user.
if [ -z "$log_mode" ]; then
    printf "\nHow to handle logs?\n"
    printf " [c] Print filtered simulator logs to console %s(default)%s\n" "${BOLD}" "${NORMAL}"
    printf " [f] Save all logs to a file\n"
    printf " [b] Do both: filtered to console and all to file\n"
    printf " [n] Do nothing\n> "
    read -r -n1 answer </dev/tty
    echo # Add a newline after user input

    case "$answer" in
        f|F) log_mode="file" ;;
        b|B) log_mode="both" ;;
        n|N) log_mode="none" ;;
        c|C|"") log_mode="console" ;; # Default case
        *)
            echo -e "\nOops that's not a valid response :(" >&2
            exit 1
            ;;
    esac
fi

# Determine the base output directory. This is the last prompt before execution.
if [ -z "$base_output_dir" ]; then
    # Define and resolve the default directory path to its canonical form to avoid '..' in prompts.
    # The directory is created with `mkdir -p` if it doesn't exist to allow `cd` to resolve the path.
    # This is safe as this is the script's default output location.
    mkdir -p "$SCRIPT_DIR/../output"
    default_dir="$(cd "$SCRIPT_DIR/../output" && pwd)"
    display_path="${default_dir/#$HOME/\~}"

    if [ "$use_defaults" = true ]; then
        base_output_dir="$default_dir"
        echo "Using default base output directory: $display_path" >&2
    else
        printf "\nBase output directory [or ENTER to use %s]\n>" "${BOLD}${BLUE}$display_path${NORMAL}" >&2
        read -r answer </dev/tty

        if [[ "$answer" == "~" ]]; then
            answer="$HOME"
        elif [[ "$answer" == "~/"* ]]; then
            answer="$HOME/${answer:2}"
        fi

        : "${answer:=$default_dir}"
        base_output_dir="$answer"
    fi

    # Before creating the run-specific subdirectory, ensure the base output
    # directory is valid. We create it if it doesn't exist.
    mkdir -p "$base_output_dir"
    if [[ ! -d "$base_output_dir" || ! -w "$base_output_dir" ]]; then
        echo "Error: Base output directory '$base_output_dir' is not a writable directory." >&2
        exit 1
    fi
fi

# Create a unique, timestamped output directory for this run's logs and reports.
output_dir="$base_output_dir/run_$(date +%Y%m%d-%H%M%S)"
mkdir -p "$output_dir"
# Resolve the path to be absolute and free of any '..' components for cleaner display.
output_dir="$(cd "$output_dir" && pwd)"
if [ ! -w "$output_dir" ]; then
    echo "Error: Output directory '$output_dir' is not writable." >&2
    exit 1
fi
status_update "Output will be saved to: $output_dir"

# Wait for the VM to finish booting before proceeding.
wait_for_vm

trap cleanup EXIT # The EXIT trap runs on any script exit, including signals.


# Execute the main logic based on the simulation mode.
case "$sim_mode" in
    "client")
        # For client-only mode, push metrics configs and run the client.
        push_metrics_configs
        SIM_ACTIONS_ARG=$(push_sim_actions)

        # Ensure the simulator's output directory on the VM is clean before starting.
        # This prevents artifacts from previous runs from being included in the results.
        adb shell "rm -rf $SIMULATOR_VM_OUTPUT_DIR"

        # Construct and run the simulator command via adb shell.
        status_update "Starting simulator..."
        no_binary_reports_flag="--no-binary-reports"
        if [[ "$write_binary_reports" == true ]]; then
            no_binary_reports_flag=""
        fi
        adb shell sdv_telemetry_simulator \
            --max-simulation-time "$max_simulation_time" \
            telemetry-client \
            --output-directory "$SIMULATOR_VM_OUTPUT_DIR" \
            --metrics-configs "$METRICS_CONFIGS" \
            $SIM_ACTIONS_ARG \
            $no_binary_reports_flag \
            --max-report-count "$max_report_count" &
        # Capture the PID of the backgrounded simulator process.
        SIMULATOR_PID=$!

        # Wait for the simulator to finish and then collect its output.
        wait_for_simulator_finish
        collect_output_files
        ;;
    "publisher")
        # For publisher-only mode, push publisher configs and run the publishers.
        push_publisher_configs

        # Construct and run the simulator command.
        status_update "Starting simulator..."
        adb shell sdv_telemetry_simulator \
            --max-simulation-time "$max_simulation_time" \
            simulation-publishers \
            --publisher-configs "$PUBLISHER_CONFIGS" &
        SIMULATOR_PID=$!

        # Wait for the simulator to finish. No output files to collect.
        wait_for_simulator_finish
        ;;
    "both")
        # For full simulation, push both sets of configs.
        push_metrics_configs
        push_publisher_configs
        SIM_ACTIONS_ARG=$(push_sim_actions)

        # Ensure the simulator's output directory on the VM is clean before starting.
        adb shell "rm -rf $SIMULATOR_VM_OUTPUT_DIR"

        # Construct and run the simulator command for a full simulation.
        status_update "Starting simulator..."
        no_binary_reports_flag="--no-binary-reports"
        if [[ "$write_binary_reports" == true ]]; then
            no_binary_reports_flag=""
        fi
        adb shell sdv_telemetry_simulator \
            --max-simulation-time "$max_simulation_time" \
            full-simulation \
            --output-directory "$SIMULATOR_VM_OUTPUT_DIR" \
            --metrics-configs "$METRICS_CONFIGS" \
            $SIM_ACTIONS_ARG \
            --publisher-configs "$PUBLISHER_CONFIGS" \
            $no_binary_reports_flag \
            --max-report-count "$max_report_count" &
        SIMULATOR_PID=$!

        # Wait for the simulator to finish and then collect its output.
        wait_for_simulator_finish
        collect_output_files
        ;;
    *)
        echo -e "\nInvalid simulation mode specified." >&2
        exit 1
        ;;
esac
