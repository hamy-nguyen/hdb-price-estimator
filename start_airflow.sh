#!/bin/bash
# Start Apache Airflow with AIRFLOW_HOME pointing to repo's airflow/ folder

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
export AIRFLOW_HOME="$REPO_DIR/airflow"

source "$REPO_DIR/.venv/bin/activate"

echo "AIRFLOW_HOME=$AIRFLOW_HOME"
echo ""
echo "On first run, the admin password is auto-generated."
echo "Find it in: $AIRFLOW_HOME/simple_auth_manager_passwords.json.generated"
echo "Or look for 'Login with username: admin  password: ...' in the api-server output."
echo ""

# Scheduler and dag-processor run in background
airflow scheduler &
SCHEDULER_PID=$!

airflow dag-processor &
DAG_PROC_PID=$!

echo "Scheduler PID: $SCHEDULER_PID"
echo "Dag-processor PID: $DAG_PROC_PID"
echo "Starting api-server at http://localhost:8081 ..."
echo ""

# api-server runs in foreground (Ctrl+C stops it)
airflow api-server

# Cleanup background processes on exit
kill $SCHEDULER_PID $DAG_PROC_PID 2>/dev/null
