from redis import Redis
from rq import Queue
import path
import sys
directory = path.Path(__file__).absolute()
sys.path.append(directory.parent.parent)
from worker import start_submitting_at_worker

redis = Redis(host='192.168.80.78', port=5000)
q = Queue(name='worker',connection=redis)

q.enqueue(start_submitting_at_worker)





