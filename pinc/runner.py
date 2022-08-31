# this defines a runner. This is the piece of code that monitor a single module execution and report to the server


from __future__ import print_function
import zmq
import sys
import subprocess as sbp
import shlex
import time,datetime
import sys
from tools import logger

def noprint(*args,**kargs):
  return

class runner:

  def __init__(self,argv):
    self.label,self.pubaddress,self.pulladdress,self.logfile = argv[1:-1]
    
    self.logger = logger(self.logfile)
    self.logger("------------ Starting %s ----------------"%(self.label))
    self.options={"child_loop_timeout":6,"child_ping_delay":.5}
    import os
    self.logger("host : %s"%os.environ["HOSTNAME"])
    self.rchild = None
    # connect to the server
    self.connect()

    self.loop()

  def connect(self):
    self.ctx=zmq.Context()
    self.subso = self.ctx.socket(zmq.SUB)
    self.subso.setsockopt(zmq.SUBSCRIBE,"")
    self.pushso = self.ctx.socket(zmq.PUSH)

    self.logger("connecting on %s|%s : "%(self.pubaddress,self.pulladdress))
    self.subso.connect(self.pubaddress)
    self.pushso.connect(self.pulladdress)
    self.logger("ok")

    # tell parent I am ready
    self.pushso.send_pyobj((self.label,"ready"))
    self.readyness = True
    self.logger("READY")
    self.finish_status = -1
    self.readyness = False

  def run_child(self,i):
    self.logger("%s"%" ".join(self.cmd[i]))
    self.rchild = sbp.Popen(self.cmd[i],stdout=self.logger.files[0],stderr=sbp.STDOUT)

  def loop(self):
    self.alive = 10
    tms = time.time()
    while(self.alive>0):
      self.alive -=1
      ev = self.subso.poll(self.options["child_loop_timeout"]*1000)
      if ev:
        msg = self.subso.recv_pyobj()
        self.alive += 1
        if msg[0]!=self.label:
          #self.logger("rcv %s"%str(msg))
          pass
        else:
          self.readyness = True
          self.logger("got %s"%str(msg))
          if msg[1] == "run":
            if not self.rchild:
              self.cmd = msg[2]
              self.i_cmd = 0
              self.m_cmd = len(self.cmd)
              self.options = msg[3]
              self.run_child(self.i_cmd)
              self.pushso.send_pyobj((self.label,"run"))

          elif msg[1] == "kill": #kill job
            if self.rchild:
              self.rchild.kill()
            self.pushso.send_pyobj((self.label,"kill"))
            self.cleanup()
          elif msg[1] == "release":
            self.finish(self.finish_status)

      if time.time()-tms>self.options["child_ping_delay"]:
        if self.readyness:
          self.pushso.send_pyobj((self.label,"ping"))
          self.logger("pong")
        else:
          self.pushso.send_pyobj((self.label,"ready"))
          self.logger("READY")
        tms = time.time()
      if self.rchild:
        #self.logger("poll child")
        poll = self.rchild.poll()
        if poll!=None:
          if self.rchild.returncode==0:
            self.i_cmd += 1
            if self.i_cmd<self.m_cmd:
              self.run_child(self.i_cmd)
            else:
              self.logger("success")
              self.pushso.send_pyobj((self.label,"success"))
              self.finish_status = 0
          else:
            self.pushso.send_pyobj((self.label,"fail"))
            self.logger("FAIL")
            self.finish_status = -1
    self.cleanup()

  def finish(self,vl=0):
    self.logger("------------ Ending %s ----------------"%datetime.datetime.today().isoformat())
    del(self.logger)
    self.subso.close(0)
    self.pushso.close(0)
    sys.exit(vl)
  def cleanup(self):
    self.logger("CLEANUP")
    if self.rchild!=None:
      try:
        self.rchild.kill()
      except OSError,e:
        self.logger("cannot kill",e)
    self.finish(-1)








def main(argv):
  #init and connect om argv[1]
  if argv[-1] in globals():
    runme = globals()[argv[-1]](argv)
  else:
    runme = runner(argv)

if __name__=="__main__":
  main(sys.argv)
