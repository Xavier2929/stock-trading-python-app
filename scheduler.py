import schedule
import time
from script import run_stock_job
from datetime import datetime


def basic_job():
    print(f"Job start at: {datetime.now()}")


# run every minut
schedule.every().minute.do(basic_job)
schedule.every().minute.do(run_stock_job)

while True:
    schedule.run_pending()
    time.sleep(1)
