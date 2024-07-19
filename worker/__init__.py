import path
import sys
directory = path.Path(__file__).absolute()
sys.path.append(directory.parent)

from jobs_at_worker import start_submitting_at_worker, start_training_at_worker