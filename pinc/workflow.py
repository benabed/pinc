# define here a class that handles the environment on the pipeline
from __future__ import print_function
import server
import os.path as osp
import os
import re
import sys
import stat
import os
#import numpy as nm
import subprocess as sbp
import shlex
import time 
import datetime
import pinc_defaults
from tools import get_uniq, minitee, first_exists,mkdir_safe
import job
import atexit
import signal
import zmq

class workflow(object):

  def __init__(self,**options):
    atexit.register(self.exit)
    defaults = pinc_defaults.defaults.copy()
    p = first_exists(["./pinc_defaults","./pinc_defaults.py","%s/.pinc_defaults"%os.environ["HOME"],"%s/.pinc_defaults.py"%os.environ["HOME"],"%s/.pinc/pinc_defaults"%os.environ["HOME"],"%s/.pinc/pinc_defaults.py"%os.environ["HOME"]])
    if p:
      rop = {}
      execfile(p,rop)
      defalts.update(rop["defaults"])

    self._start = False
    defaults.update(options)
    self.options = defaults
    path = self.options["path"]
    label = self.options["label"]

    self.dirpath = osp.realpath(osp.join(path,label))
    self.dirpath = get_uniq(self.dirpath,osp.exists)
    self.options["dirpath"] = self.dirpath
    self.label = osp.basename(self.dirpath)
    mkdir_safe(self.dirpath)
    mkdir_safe(osp.join(self.dirpath,self.options["tmpdirname"]))
    self.logfi = file(osp.join(self.dirpath,self.label+self.options["logsuffix"]),"w")
    self.stdout = sys.stdout
    self.stderr = sys.stderr
    sys.stdout = minitee(self.logfi,sys.stdout)
    sys.stderr = minitee(self.logfi,sys.stderr)
    self.server = server.server(self.options)
    self.jobs = {}
    self._start = True
    
    print("start workflow %s (in %s)"%(self.label,self.dirpath))
    print("time is %s"%datetime.datetime.today().isoformat())

  def jobfromany(self,*lbs):
    res = []
    for lb in lbs:
      if isinstance(lb,str):
        res += [self.jobs[lb]]
      else:
        res += [lb]
    return tuple(res)
  def labelfromany(self,*lbs):
    res = []
    for lb in lbs:
      if isinstance(lb,str):
        res += [lb]
        assert lb in self.jobs.keys()
      else:
        res += [lb.get_label()]
    return tuple(res)

  def job(self,**kargs):
    """ initialize new job """
    assert self._start,"workflow stopped"
    if "label" in kargs:
      label = kargs["label"]
    else:
      label = kargs["cmd"].split()[0]
    label = get_uniq(label,lambda lb:lb in self.jobs)
    kargs["label"] = label
    
    kargs["logfile"] = kargs.get("logfile",osp.join(self.dirpath,label,label+".log"))

    kargs["dirpath"] = kargs.get("dirpath",self.dirpath)

    kargs["options"] = self.options.copy()
    jb = kargs.get("jobclass",job.job)
    self.jobs[label] = jb(self,**kargs)
    if kargs.get("run",False):
      self.submit(label)
    return self.jobs[label]

  def submit(self,*labelorjobs):
    assert self._start,"workflow stopped"
    msgs = ""
    for job in self.jobfromany(*labelorjobs):
      try:
        msg = job.register(self.server)
      except server.pincError,e:
        self.failure(e)

      print("submit job %s"%msg,end="")
      dp = job.get_dependency()
      if dp:
        print(" (after %s"%",".join(dp),end=")")
      print("")
      msgs += msg+"\n"
    return msgs.strip()
    
  def failure(self,e):
    text = e.text
    cause = str(e.cause if e.cause else "")
    extended = ""
    if osp.exists(osp.join(self.options["dirpath"],self.options["emergency_log"])):
      extended = open(osp.join(self.options["dirpath"],self.options["emergency_log"])).read()
    extra = "\n".join([t.strip() for t in [text,cause,extended] if t.strip()])
    print("\n\n-----------------------------------\nFailure detected\n%s\n-----------------------------------"%extra)
    self._start = False
    sys.exit()

  def get_jobs(self):
    assert self._start,"workflow stopped"
    return tuple([job for job in self.jobs.keys() if self.jobs[job].sbm])

  def wait(self,*labels):
    if not labels:
      labels = self.get_jobs()
    return self.wait_all(*labels)

  def wait_any(self,*labels):
    if not labels:
      labels = self.get_jobs()
    return self.wait_on("any","SE",*labels)

  def wait_all(self,*labels):
    if not labels:
      labels = self.get_jobs()
    r = self.wait_on("all","SE",*labels)
    return tuple(r)+tuple([l for l in labels if l!=r[0]])

  def wait_iter(self,*labels):
    if not labels:
      labels = self.get_jobs()
    lbls = set(labels)
    while(lbls):
      r = self.wait_any(*tuple(lbls))
      lbls.remove(r[0])
      yield r[0]

  def alllabs(self,*labels):
    labs = self.labelfromany(*labels)
    if not labs:
      labs = self.get_jobs()
    return labs

  def wait_on(self,typ,status,*labels):
    assert self._start,"workflow stopped"
    labs = self.alllabs(*labels)
    #self.job_status(*labs)
    print("wait on status %s for %s jobs in %s"%(status,typ,labs,),end="")
    try:
      msg =  self.server.wait(typ,status,*labs)
    except server.pincError,e:
      self.failure(e)
    print (" : %s"%msg)
    #self.job_status(*labs)
    return msg
      
  def job_status(self,*labels):
    assert self._start,"workflow stopped"
    labs = self.alllabs(*labels)
    try:
      msg =  self.server.get_status(*labels)
    except server.pincError,e:
      self.failure(e)

    #print("MSG:::",msg)
    #print("LABS:::",labs)
    for lb in labs:
      #print(lb)
      self.jobs[lb].last_status = msg[lb][0]
    return msg
  def print_status(self,*labels,**kargs):
    status = self.job_status(*labels)
    server.print_job_status(status,file=kargs.get("file",None))

  def closing_remarks(self):
    print("end workflow %s (in %s)"%(self.label,self.dirpath))
    print("time is %s"%datetime.datetime.today().isoformat())
    self.logfi.close()
    sys.stdout = self.stdout
    sys.stderr = self.stderr
    self._start=False

  def close(self):
    assert self._start,"workflow stopped"
    #self.wait_all(*[jb for jb in self.jobs.keys() if self.jobs[jb].last_status not in "ESKF"])
    try:
      self.server.end_server()
    except server.pincError,e:
      self.failure(e)
    self.closing_remarks()

  
  def exit(self):
    if self._start:
      print("send kill all to server")
      try:
        self.server.kill_server()
      except server.pincError,e:
        pass
      self.closing_remarks()
      os._exit(-1)
  

  
            
