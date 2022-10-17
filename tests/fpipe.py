#fake pipe

import pinc

wrk = pinc.workflow()

#wrk.job(cmd="python pinc/sleeper.py 10",label="slp_1",jobclass=pinc.qsub)
wrk.job(cmd="python3 pinc/sleeper.py 10",label="slp_1")
jb = wrk.job(cmd=["python3 pinc/sleeper.py 10 @par","python3 pinc/sleeper.py 20"],label="slp_2",after=["slp_1"])
pf = jb.new_parfile("par")
pf.set(toto="tata",titi=[1,2,3])

def test_func(a):
  import time
  print ("wait %s"%a)
  time.sleep(a)


print ("before submit")
wrk.submit("slp_1","slp_2")
#wrk.submit("slp_1")
print ("After submit")

jb = wrk.job(cmd = pinc.python_cmd(test_func,(40,)),label="pythoncmd")
wrk.submit(jb)
wrk.print_status()
for j in wrk.wait_iter():
  print (j)
wrk.print_status()

wrk.close()
