#! {pythonpath}
from __future__ import print_function

import sys
sys.path+={syspath}
import pinc.server as perver
import os.path as osp
import json
import os 

usage = """usage: %s option
with option being one of
  status [job1 job2 ...]         : print the status of job1, job2, ... (or all  
                                   the jobs monitored by the pipeline if names 
                                   absent)
  list                           : list the jobs monitored by the pipeline
  kill|terminate|end|quit        : terminate the pipeline
  wait [all|any] [job1 job2 ...] : wait for all (or any) of the job1, job2, ...
                                   to terminate. 
                                   If the first option is absent wait on all 
                                   jobs in the joblist. 
                                   If the joblist is absent wait on all the jobs 
                                   monitored by the pipeline.
  log [server|master|job] [-f]   : print the log of either the server, 
                                   the master, or the monitored job "job".
                                   if the first option is absent, display the 
                                   server log by default.
                                   If the -f option is present, work as the -f 
                                   option of the "tail" unix tool.
"""

def main(argv):

  startime = open(osp.join(osp.dirname(__file__),"start_server")).read().strip()
  print("server started at time %s"%startime)

  f=open(osp.join(osp.dirname(__file__),"pinc_options.json"))
  options = json.load(f)
  f.close()

  end = False
  if osp.exists(osp.join(osp.dirname(__file__),"end_server")):
    print("server ended at time",open(osp.join(osp.dirname(__file__),"end_server")).read().strip())
    end = True

  if len(argv)==1:
    print(usage%argv[0])
    sys.exit(0)
  
  if argv[1]=="log":
    f = False
    fi = "server"
    if len(argv)==3:
      if argv[2]=="-f":
        f = True
      else:
        fi = argv[2]
    if len(argv)==4:
      fi = argv[2]
      f = True

    if fi == "server":
      filog = "server.log"
    elif fi == "master":
      filog = osp.basename(osp.dirname(__file__))
    else:
      filog = fi+"/"+fi+".log"
    ars = ("tail","tail","-c","+0")+("-f",)*f+(osp.join(osp.dirname(__file__),filog),)
    os.execlp(*ars)
    sys.exit(0)

  if end:
    status = open(osp.join(osp.dirname(__file__),options["status_log"])).readlines()
    if argv[1] == list:
      print("\n".join([t.split[0] for t in status[1:]]))
    if argv[1] == status:
      print(status[0].rstrip())
      if len(argv)==2:
        print("\n".join(status[1:].rstrip()))
      else:
        for l in status[1:]:
          if l.split()[0] in argv[2:]:
            print(l.rstrip())
    sys.exit(0) 

  maitre = perver.ertiam('{subaddress}','{pushaddress}',{timeout})
  
  if argv[1] == "status":
    status = maitre.get_status(*argv[2:])
    perver.print_job_status(status)
    return 0

  if argv[1].lower() in ("kill","terminate","end","quit"):
    maitre.terminate()
    return 0


  if argv[1] == "list":
    lst = maitre.get_list()
    for ch in lst:
      print(ch)
    return 0
  if argv[1] == "wait":
    lst = maitre.get_list()
    typ = "all"
    ia = 2
    if len(argv)>2 and (argv[2].lower() in ("all","any")):
      typ = argv[2].lower()
      ia = 3
    status = "SE"
    if len(argv)>ia:
      kst = [a for a in argv[ia:] if a in lst]
      lst = kst
    print("Wait for status %s for %s in (%s)"%(status,typ,", ".join(lst)),end="") 
    sys.stdout.flush()
    msg = maitre.wait(typ,status,*lst)
    print(" :: %s"%msg)
    return 0

if __name__=="__main__":
  main(sys.argv)