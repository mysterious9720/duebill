. ./common.sh
set -x


echo "-------------流水清洗------------------"
input=$1
output=$2
month=$3

$HADOOP_BIN fs -rm -r  $output
$HADOOP_BIN  jar $HADOOP_STREAMING \
    -input $input \
    -output $output  \
    -file  "trans_map_r2.py"\
    -file "trans_red_r2.py" \
    -mapper "python trans_map_r2.py $month" \
    -reducer "python trans_red_r2.py $month" \
    -jobconf mapred.reduce.tasks=200 \
    -jobconf mapreduce.reduce.memory.mb=16000 \
    -jobconf mapred.job.name=$output2 \
    -jobconf mapreduce.job.queuename=$ROOT \
    -jobconf stream.map.input.ignoreKey=true \
    -inputformat com.hadoop.mapred.DeprecatedLzoTextInputFormat \
    -jobconf mapred.output.compress=true \
    -jobconf mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec \
    -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
    -jobconf map.output.key.field.separator="#"\
    -jobconf num.key.fields.for.partition=1\


