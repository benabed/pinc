#fake pipe

import pinc

wrk = pinc.workflow(child_address="194.57.221.77:*")

wrk.job(cmd="python /home/benabed/pinc/pinc/pinc/sleeper.py 10",label="slp_1",jobclass=pinc.qsub)
#wrk.job(cmd="python pinc/sleeper.py 10",label="slp_1")
jb = wrk.job(cmd="python /home/benabed/pinc/pinc/pinc/sleeper.py 10 @par",label="slp_2",after=["slp_1"])
pf = jb.new_parfile("par")
pf.set(toto="tata",titi=[1,2,3])

wrk.submit("slp_1","slp_2")
print wrk.job_status()

wrk.close()
