import path
import sys
directory = path.Path(__file__).absolute()
sys.path.append(directory.parent)

from job_at_kafka_edge import start_kafka_streaming