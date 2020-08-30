import airflow
from datetime import timedelta, datetime
import datetime as dt
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator


SLACK_CONN_ID = 'slack'

def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :red_circle: Task Failed.
            *Task*: {task}
            *Dag*: {dag}
            *Execution Time*: {exec_date}
	    *Log URL*: {log_url}
            """.format(
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        ti=context.get('task_instance'),
        exec_date=context.get('execution_date'),
        log_url=context.get('task_instance').log_url,
    )
    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        channel='#ml_alerting',
        username='airflow')
    return failed_alert.execute(context=context)


args = {
    'owner': 'user',
    'start_date': datetime(2020, 04, 8, 13, 30),
    'depends_on_past': False,
    'email': ['user@compnay.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries':3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_fail_slack_alert
}

year = '{{ (execution_date - macros.timedelta(days=1)).strftime("%Y") }}'
month = '{{ (execution_date - macros.timedelta(days=1)).strftime("%m") }}'
day = '{{ (execution_date - macros.timedelta(days=1)).strftime("%d") }}'


input1 = '/ml/agg_recon/' + year + '_' + month + '_' + day
inputTLD = '/ml/agg_recon_tld/'+ year + '_' + month + '_' + day
output1 = '/ml/agg_recon_dailyAgg/'+ year + '_' + month + '_' + day
outputTLD = '/ml/agg_recon_dailyAgg_tld/'+ year + '_' + month + '_' + day
configFilePath = '/user/ml.user/ml-agg/CONFIG/configFile.properties'

daily_args= 'input:' + input1 + ' ' + 'inputTLD:' + inputTLD + ' ' +'output:' + output1 + ' ' +'outputTLD:' + outputTLD + ' ' + 'configFile:' + configFilePath
app_name = 'app_name-' + year + '/' + month + '/' + day

sparkCommand = '/usr/hdp/2.6.5.0-292/spark2/bin/spark-submit --master yarn-cluster --conf spark.io.compression.codec=snappy --conf spark.scheduler.listenerbus.eventqueue.size=30000 --conf spark.yarn.queue=ML --conf spark.driver.cores=5 --conf spark.dynamicAllocation.minExecutors=250 --conf spark.dynamicAllocation.maxExecutors=350 --conf spark.shuffle.compress=false --conf spark.sql.tungsten.enabled=true --conf spark.shuffle.spill=true  --conf spark.sql.parquet.compression.codec=snappy --conf spark.speculation=true --conf spark.kryo.referenceTracking=false --conf spark.hadoop.parquet.block.size=268435456 --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 --conf spark.executor.memory=25g --conf spark.hadoop.dfs.blocksize=268435456 --conf spark.shuffle.manager=sort --conf spark.driver.memory=35g --conf spark.hadoop.mapreduce.input.fileinputformat.split.minsize=134217728 --conf spark.akka.frameSize=1024 --conf spark.yarn.executor.memoryOverhead=3120 --conf spark.sql.parquet.filterPushdown=true --conf spark.sql.inMemoryColumnarStorage.compressed=true --conf spark.hadoop.parquet.enable.summary-metadata=false --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.rdd.compress=true --conf spark.task.maxFailures=50 --conf spark.yarn.max.executor.failures=30 --conf spark.yarn.maxAppAttempts=1  --conf spark.default.parallelism=2001 --conf spark.network.timeout=1200s --conf spark.hadoop.dfs.client.read.shortcircuit=true --conf spark.dynamicAllocation.enabled=true --conf spark.executor.cores=5 --conf spark.yarn.driver.memoryOverhead=5025 --conf spark.shuffle.consolidateFiles=true --conf spark.sql.parquet.mergeSchema=false --conf spark.sql.avro.compression.codec=snappy --conf spark.hadoop.dfs.domain.socket.path=/var/lib/hadoop-hdfs/dn_socket --conf spark.shuffle.spill.compress=false --conf spark.sql.caseSensitive=true --conf spark.hadoop.mapreduce.use.directfileoutputcommitter=true --conf spark.shuffle.service.enabled=true --conf spark.driver.maxResultSize=0 --conf spark.executor.heartbeatInterval=60s --conf spark.sql.shuffle.partitions=2001 --conf spark.sql.files.maxPartitionBytes=268435456 --files /home/ml.user/airflow/dags/ML_Agg/configFile.properties --packages com.databricks:spark-csv_2.10:1.3.0  --class package.class-name'


jarPath = '/home/ml.user/airflow/dags/ML_Agg/path_to_jar.jar'


my_dag = DAG(
    dag_id='dag_id',
    default_args=args,
    schedule_interval='30 13 * * *')


daily_aggregator = BashOperator(
    task_id='task_id',
    bash_command=sparkCommand + ' ' + '--conf spark.app.name=' + app_name + ' ' + jarPath + ' ' + daily_args,
    dag = my_dag)

daily_aggregator

if __name__ == "__main__":
    dag.cli()
