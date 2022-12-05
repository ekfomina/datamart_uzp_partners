echo "start bash"

# Extremely useful option allowing access to Hive Metastore from Spark 2.2
#export HADOOP_CONF_DIR=$HADOOP_CONF_DIR  #
# Fully qualified principal name e.g. USERNAME@REALM
PRINCIPAL=$USER_NAME"@"$REALM
KEYTAB=$USER_NAME".keytab"


set -e # если дочерний процесс упадет, то и родительский (данный) тоже упадет

echo "start get files from hdfs"
hdfs dfs -get /keytab/$KEYTAB
hdfs dfs -get ${PATH_TO_PROJ_HDFS}/scripts
hdfs dfs -get ${PATH_TO_PROJ_HDFS}/create_sql

set +e # если дочерний процесс (spark-submit) упадет, родительский НЕ упадет

# Set additional configs for launch of spark-submit
if [[ $SPARK_CONF == "-" ]]; then
SPARK_CONF=
fi
echo SPARK_CONF = $SPARK_CONF

echo "start spark submit"
spark-submit \
--master yarn \
--keytab $KEYTAB \
--queue $Y_QUEUE \
--principal $PRINCIPAL \
--conf spark.yarn.queue=$Y_QUEUE \
--executor-memory $Y_EXECUTOR_MEM \
--driver-memory $Y_DRIVER_MEM \
--executor-cores $Y_EXECUTOR_CORES \
--conf spark.pyspark.driver.python=$PATH_TO_PYTHON \
--conf spark.pyspark.python=$PATH_TO_PYTHON \
--conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.minExecutors=$Y_MIN_EXECUTORS \
--conf spark.dynamicAllocation.maxExecutors=$Y_MAX_EXECUTORS \
--conf spark.sql.catalogImplementation=hive \
scripts/main.py --recovery_mode=$RECOVERY_MODE \
                --force_load=$FORCE_LOAD \
                --etl_vars="$ETL_VARS"



spark_submit_exit_code=$?
echo "spark-submit exit code: ${spark_submit_exit_code}"

# если была ошибка то возвращяем не 0 код возврата
if [[ $spark_submit_exit_code != 0 ]]
then
  exit $spark_submit_exit_code
fi

echo "end_bash"