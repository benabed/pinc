#fake pipe

import pinc

wrk = pinc.workflow()

def wow(xx,yy):
  import time
  import numpy as nm
  time.sleep(xx)
  return nm.arange(xx)+yy**2

jb = wrk.job(cmd=[wow,(10,1000),{}],label="python_wow",jobclass=pinc.python_job)

wrk.submit("python_wow")
print wrk.job_status()
wrk.wait()

print jb.get_res()
wrk.close()
