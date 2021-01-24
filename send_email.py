from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime, date
import data_management

sched = BlockingScheduler()

def data():
    now = datetime.now()
    if now.hour == 0 and now.minute == 0:
        today = date.today()
        data_management.make_csv(today)
        data_management.delete_data(today)

sched.add_job(data, 'cron', minute='*')
sched.start()