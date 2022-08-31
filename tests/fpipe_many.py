import pinc
import random
import time

wrk = pinc.workflow(child_address="*:*")

time.sleep(10)
lvl1_job = [wrk.job(cmd="python pinc/sleeper.py %d"%random.randint(10,60),label="slp",jobclass=pinc.fakeqsubjob).get_label() for i in range(10)]
wrk.submit(*lvl1_job)
lvl2_job = [wrk.job(cmd="python pinc/sleeper.py %s"%random.randint(10,60),label="slp",jobclass=pinc.fakeqsubjob,after=random.sample(lvl1_job,random.randint(0,3))).get_label() for i in range(10)]
wrk.submit(*lvl2_job)

wrk.print_status()

#assert 0==1,"Yeah"
wany = random.sample(lvl1_job+lvl2_job,5)
msg = wrk.wait_any(*wany)

time.sleep(5)
lvl3_job = [wrk.job(cmd="python pinc/sleeper.py %d"%random.randint(10,60),label="slp",jobclass=pinc.fakeqsubjob).get_label() for i in range(5)]
wrk.submit(*lvl3_job)
wrk.print_status()


wall = random.sample(lvl1_job+lvl2_job,6)
msg = wrk.wait_all(*wall)
time.sleep(5)

lvl4_job = [wrk.job(cmd="python pinc/sleeper.py %s"%random.randint(10,60),label="slp",jobclass=pinc.fakeqsubjob,after=random.sample(lvl1_job+lvl3_job,random.randint(0,3))).get_label() for i in range(10)]
wrk.submit(*lvl4_job)



wrk.print_status()


wrk.close()
