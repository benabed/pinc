# this is the server that run on the main node and check the different nodes
from __future__ import print_function

import zmq
import sys
import time,datetime
import subprocess as sbp
import os.path as osp
import pinc.tools as tools
import pinc.runner as runner
import json
import pinc.action as action

class pincError(Exception):
  def __init__(self,text,cause=None):
    self.text = text
    self.cause = cause
    Exception.__init__(self,text)

child_status = "0LPRFKSE"
sts_idx = dict([(v,i) for i,v in enumerate(child_status)])

def print_job_status(status,file=None):
  if file==None:
    file = sys.stdout
  #def order(lb1,lb2):
  #  return cmp(status[lb1][1][0],status[lb2][1][0])
  #lbs = list(status.keys())
  #lbs.sort(order)
  lbs = sorted(status.keys(),key=lambda ss:status[ss][1][0])
  if lbs:
    col1 = max(8,max([len(lb) for lb in lbs]))+1
    cols = [max(8,max([len(status[lb][1][i]) for lb in lbs]))+1 for i in range(8)]
  else:
    col1 = 9
    cols = [9]*8
  print(" "*(col1+2)+"".join([ss.ljust(col) for ss,col in zip(child_status,cols)]),file=file)
  for lb in lbs:
    print(lb.ljust(col1)+status[lb][0].ljust(2)+"".join([status[lb][1][sts_idx[ss]].ljust(col) for ss,col in zip(child_status,cols)] ),file=file)

class client:
  def __init__(self,label,queue,log):
    self.label = label
    self.actionqueue = queue
    self.log = log
    self.resp={}
  
  def register_response(self,key,func):
    self.resp[key]=func

  def __call__(self,msg):
    self.log("%s says %s"%(msg[0],msg[1:]))
    if msg[1] in self.resp:
      self.resp[msg[1]](msg)
    else:
      getattr(self,msg[1]+"_response",lambda x:None)(msg)
    self.timestamp()

  def act(self,msg=(),callback=None,keep=False):
    return self.actionqueue.add(label=self.label,msg=msg,callback=callback,keep=keep)

  def timestamp(self):
    self.tim = time.time()

class chld(client):
  def set_status(self,st,trg=True):
    self.status = st
    if not self.status_db[sts_idx[st]]:
      self.status_db[sts_idx[st]] = datetime.datetime.today().isoformat()
    if trg:
      self.trig()

  def __init__(self,pubaddress,pulladdress,jbi,depend,queue,log):
    
    client.__init__(self,jbi.get_label(),queue,log)
    self.options = jbi.kargs["options"].copy()
    self.cmd = jbi.get_cmd()
    self.rchild = None
    self.status_db = [""]*8
    self.triggers = tools.bag()
    self.logfile = jbi.get_logfile()
    self.actionqueue = queue
    self.log = log
    
    self.runnercmd = " ".join([sys.executable,runner.__file__,self.label,pubaddress,pulladdress,self.logfile,"runner"])

    self.set_status("0")
    
    self.deal_with_jbi(jbi,depend)

    self.register_response("fail",self.end_response)
    self.register_response("kill",self.end_response)
    self.register_response("success",self.end_response)
    
    
  def ready_response(self,msg):
    if self.status=="L":
      act=self.act(msg=("run",self.cmd,self.options),keep=True)
      action.trgr("all","R",[self],act.unkeep)
      self.set_status("P")

  def run_response(self,msg):
    self.set_status("R")

  def end_response(self,msg):
    self.set_status(msg[1][0].upper())
    self.cleanup()
  
  def cleanup(self):
    self.act(msg="release",callback=self.finalize)
    
  def finalize(self):
    st = self.status
    self.set_status("E",False)
    self.set_status(st,False)
    if self.submitchild:
      self.submitchild.wait()
    if self.rchild:
      self.rchild.wait()
    self.trig()
    
  def ping(self):
    if self.status not in "RP":
      return
    if time.time() -  self.tim>self.options["child_ping_timeout"]:
      self.log("child %s timeout"%self.label)
      self.set_status("F",False)
      self.cleanup()
            
  def kill(self,ifneeded=True):
    if self.status=="0":
      self.jbi.kill(self.submitid)
      self.set_status("K")
      return    
    if self.status in "RP":
      self.act(msg="kill")
      self.set_status("K")
      return    
  
  def deal_with_jbi(self,jbi,depend):
    #self.log(self.runnercmd)
    self.submitchild,self.submitid = jbi.submit(self.runnercmd)
    self.jbi = jbi

    if depend:
      action.trgr("all","S",depend,self.run,self.log,"%s dependency"%self.label)
      action.trgr("any","FK",depend,self.kill,self.log,"%s dependency kill"%self.label)
    else:
      self.run()

  def register_trigger(self,trg):
    self.triggers.add(trg)

  def unregister_trigger(self,trg):
    #print("TRG remove",self.label,trg,[str(s) for s in self.triggers])
    self.triggers.remove(trg)
    
  def trig(self):
    lit = list(self.triggers)
    for trg in lit:
      if trg in self.triggers:
        trg(self)

  def run(self,no=None):
    try:
      self.rchild,self.submitid = self.jbi.release(self.runnercmd,self.submitid)
    except Exception as e:
      self.set_status("F")
      return 
    self.timestamp()
    self.set_status("L")
    
class brrr(chld):
  def __init__(self,pubaddress,pulladdress,jbi,depend,queue,log):
    client.__init__(self,jbi.get_label(),queue,log)
    self.options = jbi.kargs["options"].copy()
    #self.cmd = jbi.get_cmd()
    self.rchild = None
    self.status_db = [""]*8
    self.triggers = tools.bag()
    self.logfile = jbi.get_logfile()
    self.actionqueue = queue
    self.log = log
    
    #self.runnercmd = " ".join([sys.executable,runner.__file__,self.label,pubaddress,pulladdress,self.logfile,"runner"])
    self.runnercmd=""
    self.set_status("0")
    
    self.deal_with_jbi(jbi,depend)

  def run(self,no=None):
    self.timestamp()
    self.set_status("L")
    self.set_status("P")
    self.set_status("R")

  def kill(self,ifneeded=True):
      self.set_status("K")
      return    
  
  def ping(self):
      return

  def cleanup(self):
    self.finalize()

  def lower(self):
    self.set_status("S")
    self.cleanup()

class master(client):
  def __init__(self,serv,label,queue,log):
    client.__init__(self,label,queue,log)
    
    self.serv = serv
    label = tools.get_uniq("master",lambda x:x in self.serv.master)
    self.act(msg=("new master",label))
    self.label = label
    
    self.register_response("close pair",self.close_pair_response)
    self.register_response("add job",self.add_job_response)
    
  def disconnect_response(self,msg):
    self.act(msg="disconnect",callback=lambda :self.serv.master.pop(self.label))

  def status_response(self,msg):
    status = self.serv.get_status(*msg[2:])
    self.act(msg=("status",status,))
    
  def list_response(self,msg):
    self.act(msg=("list",)+tuple(self.serv.child.keys()))
  
  def wait_response(self,msg):
    typ,status = msg[2:4]
    labels = msg[4:]
    depend = [self.serv.child[tlabel] for tlabel in labels]
    action.trgr(typ,status,depend,lambda label:self.act(msg=("wait",)+(label,)),self.log,"wait order from master")
    self.serv.delay_dep.update(self.serv.current_master_name,labels)
  
  def eject_response(self,msg):
    self.act(msg=("eject",),callback = self.serv.killall)

  def end_response(self,msg):
    self.act(msg=("end",),callback = self.serv.end)

  def close_pair_response(self,msg):
    self.act(msg=("close pair",),callback = self.serv.pair_close)

  def add_job_response(self,msg):
    jbi = msg[2]
    ch = self.serv.new_child(jbi)
    self.act(msg=("add job",ch.label))

  def lower_response(self,msg):
    labels=msg[1:]
    lbs = self.serv.lower(*labels)
    self.act(msg=("lower",lbs))

class ertiam:
  def __init__(self,subaddress,pushaddress,timeout,ctx=None):
    import uuid
    self.alive = 5
    if ctx==None:
      self.ctx=zmq.Context()
    self.subso = self.ctx.socket(zmq.SUB)
    self.subso.setsockopt_string(zmq.SUBSCRIBE,"")
    self.pushso = self.ctx.socket(zmq.PUSH)
    self.subso.connect(subaddress)
    self.pushso.connect(pushaddress)
    label = str(uuid.uuid4())
    self.label = label
    self.timeout = timeout
    msg = self.communicate("new master")
    self.label = msg[2]

  def communicate(self,*msg):
    self.pushso.send_pyobj((self.label,)+tuple(msg))
    alive = self.alive
    while(alive>0):
      alive -=1
      ev = self.subso.poll(self.timeout*100)
      if ev:
        rsg = self.subso.recv_pyobj()
        alive += 1
        if rsg[0]!=self.label:
          pass          
        else:
          if rsg[1]==msg[0]:
            return rsg
          else:
            raise pincError("server returned error",rsg)

    raise pincError("server timeout")

  def get_status(self,*labels):
    msg = self.communicate("status",*labels)
    return msg[2]

  def wait(self,typ,status,*labels):
    msg = self.communicate("wait",typ,status,*tuple(labels))
    return msg[2:]

  def get_list(self):
    msg = self.communicate("list")
    return msg[2:]

  def terminate(self):
    msg = self.communicate("eject")
    return "done"
  def kill_server(self):
    return self.terminate()

  def end_server(self):
    msg = self.communicate("end")
    return "done"

  def final_handshake(self):
    msg = self.communicate("close pair")
    return "done"

  def add_job(self,jbi):
    msg = self.communicate("add job",jbi)
    return msg[2]

  def lower_barrier(self,*labels):
    msg = self.communicate("lower",*labels)
    return msg 

  def __del__(self):
    try:
      self.timeout=1
      self.communicate("disconnect")
    except Exception as e:
      pass
    self.subso.close()
    self.pushso.close()

    self.ctx.destroy()
    
class server(ertiam):
  def __init__(self,options):
    self.ctx = zmq.Context()
    self.slave = self.ctx.socket(zmq.PAIR)
    self.slave.bind("%s://%s"%(options["server_protocol"],options["server_address"]))
    spath = self.slave.getsockopt_string(zmq.LAST_ENDPOINT)
    self.serverpid = sbp.Popen([sys.executable,__file__.replace("workflow","server"),spath])
    self.slave.recv_pyobj()
    self.slave.send_pyobj(options)
    subadd,pushadd = self.slave.recv_pyobj()
    self.slave.send_pyobj("ok")
    ertiam.__init__(self,subadd,pushadd,options["child_loop_timeout"],self.ctx)
    self.final_handshake()
    self.slave.close(0)



class server_exec:
  
  def __init__(self,adress):
    startime = datetime.datetime.today().isoformat()
    sys.path+=[osp.realpath(osp.join(osp.dirname(__file__),".."))]
    self._backlog = ""
    self.logfi = None
    self.log("starting")
    self.address = adress
    self.context = zmq.Context()
    self.poller = zmq.Poller()
    self.child = {}
    self.label_alias = {}
    self.master = {}
    self._running = tools.bag()
    self.dep = {}
    self.current_master_name = "master_init"
    self._fin = False
    
    # first thing is to connect to master using the pair socket
    self.pair = None
    self.log("connect to master through the init pipe")
    self.pair = self.context.socket(zmq.PAIR)
    self.pair.connect(self.address)
    self.pair_send(("ready",))
    # now get the option dictionnary
    self.options = self.pair.recv_pyobj()
    self.log("received options dictionnary from master")

    json.dump(self.options,tools.open_in_dir(self.options,"pinc_options.json","w"))
    
    print("%s"%startime,file=tools.open_in_dir(self.options,"start_server","w"))
    
    #gotcha, start the log
    self.logfi = tools.open_in_dir(self.options,self.options["server_log"],"w")
    self.log("start log file")
    
    #init functions with a delay
    self.delay_status = tools.delay(self.options["status_log_delay"],self.save_status)
    self.delay_dep = tools.delay(self.options["status_log_delay"],self.save_dep,self.add_dep)

    
    #and open the pub and pull socket 
    self.pubso = self.context.socket(zmq.PUB)
    self.pubso.bind("%s://%s"%(self.options["child_protocol"],self.options["child_address"]))
    self.log("open the pub/sub socket at '%s'"%self.pubso.getsockopt_string(zmq.LAST_ENDPOINT).strip())
    
    self.pullso = self.context.socket(zmq.PULL)
    self.pullso.bind("%s://%s"%(self.options["child_protocol"],self.options["child_address"]))
    self.log("open the pull/push socket at '%s'"%self.pullso.getsockopt_string(zmq.LAST_ENDPOINT).strip())
    
    # register the pull socket in the poller
    self.poller.register(self.pullso,zmq.POLLIN)
    
    #tell master about the pub and pull
    self.pair_send((self.pubso.getsockopt_string(zmq.LAST_ENDPOINT).strip(),self.pullso.getsockopt_string(zmq.LAST_ENDPOINT).strip()))
    self.pair.recv_pyobj()
    
    # create the standalone master to query the server
    code = open(osp.realpath(osp.join(osp.dirname(__file__),"__pinc_template.py"))).read()
    code = code.format(pythonpath=sys.executable,subaddress=self.pubso.getsockopt_string(zmq.LAST_ENDPOINT).strip(),pushaddress=self.pullso.getsockopt_string(zmq.LAST_ENDPOINT).strip(),timeout=self.options["child_loop_timeout"],syspath=sys.path)
    print(code,file=tools.open_in_dir(self.options,"pinc","w",True))
    self.log("create pinc tool at %s"%osp.join(self.options["dirpath"],"pinc"))

    # now launch the event loop
    self.actionqueue = action.actionloop(self.pubso.send_pyobj,self.log,{"msg":"ping","silent":"true"})
    self.loop()

    # out of the loop, do some cleanup
    self.log("event loop closed")
    self.save_status()
    
    for k in list(self.dep.keys()).copy():
      self.dep[k] = ["master_end" if v==self.current_master_name else v for v in self.dep[k]]
      if k==self.current_master_name:
        self.dep["master_end"] = self.dep[k]
        del(self.dep[k])
    self.save_dep()

    # close all sockets 
    self.pullso.close()
    self.pubso.close()

    self.context.destroy()
    self.log("ending")    
    print("%s"%datetime.datetime.today().isoformat(),file=tools.open_in_dir(self.options,"end_server","w"))

    sys.exit(0)

  def loop(self):
    while(self.keep_running()):

      
      # do all the actions
      self.actionqueue.loop()
      
      # poll the pull/push socket for new message
      # wait for one sec
      evs = self.poller.poll(self.options["server_loop_timeout"]*100)
      
      for ev in evs:
        # we have at least one pending message
        msg = ev[0].recv_pyobj()
        #self.log("rcv %s"%str(msg))
        # message is a tuple of at least one element
        # first element is the name of the sender

        if msg[0] in self.child: 
          # sender is one of the children
          # get it from the least and act !
          ch = self.child[msg[0]]
          ch(msg)

        elif msg[0] in self.master:
          # sender is one of the masters
          # get it from the least and act !
          ma = self.master[msg[0]]
          ma(msg)

        elif len(msg)==2 and msg[1]=="new master":
          # particular case, a new master is registering
          nmaitre = master(self,msg[0],self.actionqueue,self.log)
          self.master[nmaitre.label] = nmaitre
      
      for ch in self.child.keys():
        # test the heartbeat of all the monitored children
        self.child[ch].ping()
      
      # run delayed functions
      self.delay_dep()
      self.delay_status()

  def keep_running(self):
      return not (self._fin and self.running()==0)
        
  def emergency_maker(self,cause):
    def emergency(label):
      for c in self.child:
        self.child[c].kill(False)
      print("failure",cause,file=tools.open_in_dir(self.options,self.options["emergency_log"],"a"))
      #self.master_send(("failure",cause))
      self.end()
    return emergency

  def log(self,*txt,**kargs):
    end = kargs.get("end","\n")
    self._backlog+="%s: "%datetime.datetime.today().isoformat()+" ".join(str(t) for t in txt)+end
    if self.logfi:
      print(self._backlog,end="",file=self.logfi)
      self.logfi.flush()
      self._backlog = ""


  def add_dep(self,node,after):
    if node==self.current_master_name:
      if self.current_master_name[-1]=="t":
        self.current_master_name = "master_step_1"
      else:
        self.current_master_name = "master_step_%d"%(int(self.current_master_name.split("_")[-1])+1)
      node = self.current_master_name
    if not after:
      after = [self.current_master_name]
    self.dep[node] = after

  def save_dep(self):
    f=tools.open_in_dir(self.options,self.options["dep_log"],"w")
    print("""digraph %s {
      rankdir=LR;
      """%self.options["label"],file=f)
    print("node [shape = doublecircle];"," ".join(["master_init"]+[m for m in self.dep if "master" in m]),";",file=f)
    print("node [shape = circle];",file=f)
    for k in self.dep:
      for ak in self.dep[k]:
        print("%s -> %s;"%(ak,k),file=f)
    print("}",file=f)
    self.log("save pipe diagram at %s"%osp.join(self.options["dirpath"],self.options["dep_log"]))

  def pair_close(self):
    self.log("close init pipe")
    self.pair.close(0)
  def pair_send(self,msg,log=True):
    self.pair.send_pyobj(msg)
    if log:
      self.log("send to master (init pipe)::",msg)

    
  def _inc_running(self,dummy=None):
    self._running.add(dummy)
  def _dec_running(self,dummy=None):
    #print("RN: remove",dummy,self._running)
    #if dummy in self._running:
    self._running.remove(dummy)
  def running(self):
    return len(self._running)

  def killall(self):
    for ch in self.child:
      self.child[ch].kill()
    self.end()

  def end(self):
    self._fin = True
      
  def get_status(self,*labels):
    if not labels:
      labels = self.child.keys()
    return dict([(lb,(self.child[lb].status,self.child[lb].status_db)) for lb in labels if lb in self.child])
  def save_status(self):
    print_job_status(self.get_status(),file=tools.open_in_dir(self.options,self.options["status_log"],"w"))
    self.log("save child status at %s"%osp.join(self.options["dirpath"],self.options["status_log"]))

  def lower(self,*labels):
    llb = []
    for lab in labels:
      if lab in self.child and isinstance(self.child[lab],brrr):
        self.child[lab].lower()
        llb += [lab]
    return llb

  def sanitize_dependency(self,jbi):
    dep = jbi.get_dependency()
    return [get(self.label_alias,d,d) for d in dep]

  def new_child(self,jbi):
    depend = [self.child[tlabel] for tlabel in self.sanitize_dependency(jbi)]
    if getattr(jbi,"is_barrier",lambda : False)():
      ch = brrr(self.pubso.getsockopt_string(zmq.LAST_ENDPOINT),self.pullso.getsockopt_string(zmq.LAST_ENDPOINT),jbi,depend,self.actionqueue,self.log)
    else:  
      ch = chld(self.pubso.getsockopt_string(zmq.LAST_ENDPOINT),self.pullso.getsockopt_string(zmq.LAST_ENDPOINT),jbi,depend,self.actionqueue,self.log)
    self.child[ch.label] = ch
    self.label_alias.update(dict([(al,ch.label) for al in jbi.get_alias()]))
    txt = "add child '%s'"%(jbi.get_label())
    if jbi.get_dependency():
      txt += " after %s"%(",".join(self.sanitize_dependency(jbi)))
    self.delay_dep.update(ch.label,self.sanitize_dependency(jbi))
    self.log(txt)
    action.trgr("all","R",[ch],self._inc_running,label="inc")
    action.trgr("all","SKF",[ch],self._dec_running,label="dec")
    action.trgr("any",child_status,[ch],self.delay_status.update,label="status_log",keep=True)
    action.trgr("any",child_status,[ch],lambda cause:self.log("%s reported %s"%(ch.label,ch.status)),keep=True,label="log")
    action.trgr("all","F",[ch],self.emergency_maker("%s reported failure"%ch.label),label="fail emergency")
    
    return ch


if __name__=="__main__":
  import signal
  signal.signal(signal.SIGINT, lambda x,y:print("got in server"))

  server_exec(sys.argv[1])
