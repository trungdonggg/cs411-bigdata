import subprocess
import os

def start_submitting_at_worker():
    spark_home = '/home/acma2k3/spark-3.5.1-bin-hadoop3-scala2.13'
    os.chdir(spark_home)
    cmd = [
        'bin/spark-submit',
        '--packages', 'org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1',
        '/home/acma2k3/cs411/worker/prediction.py'
    ]
    cmd_str = ' '.join(cmd)
    subprocess.Popen(['gnome-terminal', '--', 'bash', '-c', cmd_str])
    # process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # stdout, stderr = process.communicate()
    # print(stdout.decode())
    # print(stderr.decode())
    
# bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,io.delta:delta-spark_2.13:3.2.0 /home/acma2k3/cs411/worker/prediction.py
    
def start_training_at_worker():
    spark_home = '/home/acma2k3/spark-3.5.1-bin-hadoop3-scala2.13'
    os.chdir(spark_home)
    command = [
        'bin/spark-submit',
        '/home/acma2k3/cs411/worker/train_model.py'
    ]
    subprocess.run(command)