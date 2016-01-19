set -0 nounset

if [ $# -eq 2 ]; then
        ODATE=$(date -d"$1" +"%Y-%m-%d")
        v_ret=$?
        if [ $v_ret -ne 0 ]; then
                echo "Bad ODATE argument at position 1."
                exit 1
        fi
else
        echo "2 arguments required: ODATE and JOBNAME"
        exit 2
fi


SCRIPT_NAME=$2
CURRENT_TIMESTAMP=`date +"%Y-%m-%d_%H%M%S"`
SERVICE_ACCT=`whoami`
check_file="CheckFile.csv"
FILE_EXTN="bz2"

#Environment Variable
. /opt/projects/ent/iot/Env/SmartHome.env

# Set the parameters for hadoop streaming jar
v_streaming_jar="$HADOOP_HOME/hadoop-streaming.jar"

v_jobname=$SCRIPT_NAME

v_stream_tmpdir=$SCRIPT_FOLDER"/.staging"

v_map_write_dir=${MAPPER_OUTPUT_FOLDER}

v_mapper_script="JSDSH10001_irisV1_run_control.py"

v_map_input_file=${INPUT_FILES}
  

# Creating temp files for mapper input
echo `date +"%x %T"` "[main] $SCRIPT_NAME - Creating temp directory and files"

hadoop dfs -mkdir ${MAPPER_TEMP_IN_PATH}/${MAPPER_TEMP_IN_DIR}

for input_file in `hadoop dfs -ls ${INPUT_FILES} | sed '1d;s/  */ /g' | cut -d\  -f8`
 do
   MAPPER_TEMP_IN_FILE=`basename $input_file | sed 's/.bz2//g'`
   echo $input_file | hadoop dfs -put - ${MAPPER_TEMP_IN_PATH}/${MAPPER_TEMP_IN_DIR}/${MAPPER_TEMP_IN_FILE}
done

v_map_input_file=${MAPPER_TEMP_IN_PATH}/${MAPPER_TEMP_IN_DIR}


#Removing mapper output directory
echo `date +"%x %T"` "[main] $SCRIPT_NAME - Removing map output directory"
hadoop dfs -rm -r ${MAPPER_OUTPUT_FOLDER}

echo `date +"%x %T"` "[main] $SCRIPT_NAME INFO - Mapper class - ${SCRIPT_FOLDER}/$v_mapper_script"

echo `date +"%x %T"` "[main] $SCRIPT_NAME INFO - Starting Hadoop streaming jar"


#invcke hadoop streaming jar to calculate and validate the rowcount and checksum of files received
hadoop jar ${v_streaming_jar} -files ${SCRIPT_FOLDER}/${v_mapper_script} \
                              -D mapred.job.name=$v_jobname \
                              -D mapred.reduce.tasks=0 \
                              -D mapred.task.timeout=${MAPRED_TIMEOUT} \
                              -D stream.tmpdir=$v_stream_tmpdir \
                              -mapper ${v_mapper_script} \
                              -input ${v_map_input_file} \
                              -output ${v_map_write_dir} \
                              -cmdenv program_name=${PROGRAM_NAME} \
                              -cmdenv vendor_name=${VENDOR_NAME} \
                              -cmdenv assetname_prefix=${ASSETNAME_PREFIX} \
                              -cmdenv odate=${ODATE} \
                              -cmdenv output_path=${v_map_write_dir} \
                              -cmdenv metadata_path=${metadata_path} \
                              -cmdenv check_file=${check_file} \
                              -cmdenv chunk_size=${chunk_size} \
                             
 
if [ $? -ne 0 ] ; then
   echo `date +"%x %T"` "[main] $SCRIPT_NAME ERROR - Hadoop streaming jar execution failed"i
   hadoop dfs -rm -r ${MAPPER_TEMP_IN_PATH}/${MAPPER_TEMP_IN_DIR}
   exit 1
fi

hadoop dfs -rm -r ${MAPPER_TEMP_IN_PATH}/${MAPPER_TEMP_IN_DIR}

echo `date +"%x %T"` "[main] $SCRIPT_NAME INFO -Starting hbase table load pig script"

#invoke hbase table load pig script
$PIG_HOME/bin/pig -l ${PIG_LOG_DIR}/${SCRIPT_NAME} -param SCRIPT_NAME=${SCRIPT_NAME} -param MAP_OUTPUT=${v_map_write_dir} -param FILE_EXTN=${FILE_EXTN} -param RUNCONTROL_HBASE_TABLE=${RUN_CONTROL_TABLE} ${SCRIPT_FOLDER}/JSDSH10001_irisV1_load_run_control_tbl.pig


if [ $? -ne 0 ] ; then
  echo `date +"%x %T"` "[main] $SCRIPT_NAME ERROR - Hbase table load failed"
  exit 1
else
  echo `date +"%x %T"` "[main] $SCRIPT_NAME INFO - Hbase table -$RUN_CONTROL_TABLE loaded sucessfully"
fi
