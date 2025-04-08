#!/bin/bash

# ========== Configuration ==========
APP_NAME="My_ETL_Pipeline"
BASE_PATH="/home/hemanthkr004/my_project/my_etl_pipeline"
SCRIPT_DIR="${BASE_PATH}/Scripts"
LOG_PATH="${BASE_PATH}/logs"
MAIL_TO="venkateshreddy0086@gmail.com"
SPARK_SUBMIT="spark-submit"

mkdir -p "$LOG_PATH"

# Timestamp for logging
timestamp=$(date +"%Y%m%d_%H%M%S")

# ========== Spark Configuration ==========
SPARK_CONF="
--master local[*] \
--deploy-mode client \
--conf spark.sql.shuffle.partitions=2 \
--conf spark.sql.execution.arrow.enabled=true \
--conf spark.sql.adaptive.enabled=true \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.rdd.compress=true \
--conf spark.sql.inMemoryColumnarStorage.compressed=true \
--conf spark.sql.parquet.compression.codec=snappy \
--conf spark.ui.enabled=false \
--conf spark.executor.heartbeatInterval=60s \
--conf spark.network.timeout=600s \
--driver-memory 2G \
--executor-memory 2G \
--executor-cores 2 \
--num-executors 1
"

# ========== Function to Run Each Step ==========
run_step() {
  STEP_NAME="$1"
  SCRIPT_NAME="$2"

  echo "Running $STEP_NAME..."

  $SPARK_SUBMIT $SPARK_CONF "${SCRIPT_DIR}/${SCRIPT_NAME}" > "${LOG_PATH}/${STEP_NAME}_${timestamp}.log" 2>&1

  if [ $? -ne 0 ]; then
    echo "$STEP_NAME step failed. Sending alert email..."

    echo -e "Subject: ${APP_NAME} - ${STEP_NAME} Failed\n\nCheck logs at ${LOG_PATH}/${STEP_NAME}_${timestamp}.log" \
      | sendmail "$MAIL_TO"

    echo "$STEP_NAME failed. Aborting pipeline."
    exit 1
  else
    echo "$STEP_NAME completed successfully."
  fi
}

# ========== Run All Steps ==========
run_step "Bronze Layer" "main_bronze.py"
run_step "Silver Layer" "main_silver.py"
run_step "Gold Layer" "main_gold.py"

echo "ETL Pipeline execution completed successfully."
