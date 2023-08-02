# define here what is a job
# expect two different ones
#   - python function 
#   - arbitrary executable
from __future__ import print_function
from . import server
from .tools import get_uniq, minitee,mkdir_safe
import subprocess as sbp
import shlex
import time 
from . import pinc_defaults
import os.path as osp
import os
import re
import sys
import stat
import os

class extfile:
  def __init__(self,name,*agrs,**kargs):
    self.name=name
  def close(self):
    pass

class parfile(extfile):
  def __init__(self,name,sep = "=",listtype = "concatenate",listsep = " ",list_counter = "",list_nm="",list_startat=1):
    self.name = get_uniq(name,osp.exists)
    f=open(self.name,"w")
    f.close()
    self.pf = {}
    self.sep = sep
    self.listtype = listtype
    self.listsep = listsep
    self.list_nm = list_nm
    self.list_counter = list_counter
    self.list_startat = list_startat

  def set(self,**ka):
    self.pf.update(ka)
  def set_from(self,pars,*keys):
    self.set(**dict([(k,getattr(pars,k)) for k in keys if k in pars]))

  def close(self):
    f = open(self.name,"w")
    for k,vl in self.pf.items():
      if isinstance(vl,(str,int,float,bool,complex)):
        print("%s %s %s"%(k,self.sep,vl),file=f)
      else:
        nvl = list(vl)
        if self.listtype == "concatenate":
          svl = self.listsep.join(str(v) for v in nvl)
          print("%s %s %s"%(k,self.sep,svl),file=f)
        else:
          for i,v in itemize(nvl):
            print("%s%s%d %s %s"%(k,self.list_nm,i+self.liststartat,self.sep,v),file=f)
          print("%s %s %d"%(self.list_counter,self.sep,len(svl)))
    f.close()


class job:

  def incdb(self,fi,*content):
    f = open(osp.join(self.kargs["dirpath"],fi),"a")
    print(*content,file=f)
    f.close()
  def logdb(self,pid):
    self.incdb(self.__class__.__name__+".db",self.get_label(),pid)

  def wait(self):
    return self.wrk.wait(self)

  def safe(self):
    wrk = self.wrk
    self.wrk = None
    return wrk
  def unsafe(self,buf):
    self.wrk = buf

  def get_dependency(self):
    dep = self.kargs.get("after",())
    if isinstance(dep,str):
      return (dep,)
    return dep
  def get_label(self):
    return self.kargs["label"]
  def get_alias(self):
    return self.kargs["alias"]
  def get_logfile(self):
    return self.kargs["logfile"]

  def set_cmd(self,cmd):
    self.kargs["cmd"] = cmd

  def new_parfile(self,label,**kargs):
    self.kargs["extra"][label] = parfile(self.create_file(".par"),**kargs)
    return self.kargs["extra"][label]

  def new_file(self,label="",ext="",prefix="",suffix="",tmp=False):
    fi = (create_file(ext=ext,prefix=prefix,suffix=suffix,tmp=tmp))
    if label:
      self.kargs["extra"][label] = fi
    return fi


  def __init__(self,wrk,**kargs):
    self.wrk = wrk
    self.kargs = {
      "cmd":"sleep 10",
      "logfile":"",
      "after":(),
      "submit":"",
      "release":"",
      "kill":"",
      "extra":{},
      "alias":[]
    }
    self.kargs.update(pinc_defaults.job_defaults)
    self.kargs.update(kargs)
    self.last_status = ""
    self.sbm = False
    mkdir_safe(self.get_dirpath())

  def add_alias(self,*alias):
    self.kargs["alias"].extend(tuple(alias))

  def get_dirpath(self):
    return osp.join(self.kargs["dirpath"],self.kargs["label"])
  def get_tmp(self):
    return osp.join(self.kargs["dirpath"],"tmp")
  def get_dirortmp(self,tmp):
    if tmp:
      return self.get_tmp()
    return self.get_dirpath()

  def get_cmd(self):
    cmd = self.kargs["cmd"]
    if isinstance(cmd,str):
      cmd = [cmd]

    lcmd = []
    for rcmd in cmd:
      ccmd = rcmd
      if isinstance(rcmd,str):
        ccmd = shlex.split(rcmd)
      for i,cc in enumerate(ccmd):
        for ex,pf in self.kargs["extra"].items():
          if isinstance(pf,extfile):
            pf.close()
            ccmd[i] = cc.replace("@%s"%ex,pf.name)
          else:
            ccmd[i] = cc.replace("@%s"%ex,pf)
      lcmd += [ccmd]
    return lcmd

  def register(self,master):
    buf = self.safe()
    msg = master.add_job(self)
    self.unsafe(buf)
    self.sbm=True
    return msg

  def create_file(self,ext="",prefix="",suffix="",karg_name="",tmp=False):
    dp = self.get_dirortmp(tmp)
    fn = get_uniq(osp.join(dp,prefix+self.kargs["label"]+suffix),lambda l:osp.exists(l+ext))+ext
    if karg_name:
      self.kargs[karg_name] = fn
    return fn 

  def create_srkfiles(self):
    submit_file = self.create_file(".submit","submit")
    release_file = self.create_file(".release","release")
    kill_file = self.create_file(".kill","kill")
    return submit_file,release_file,kill_file

  def make_exec(self,fname,content,shebang="shell"):
    shb = {"shell":"#! /bin/sh",
           "python":"#! %s"%sys.executable}

    content = shb.get(shebang,shebang).strip()+"\n"+content
    f=open(fname,"w")
    print(content,file=f)
    f.close()
    os.chmod(fname,stat.S_IMODE(os.stat(fname)[0]) | stat.S_IXUSR)

  def submit(self,runnercmd):
    # this would submit the code
    # return None or a sbp.Popen object (to be clenedup) and an id string
    return None,""

  def release(self,runnercmd,submitid):
    rchild = sbp.Popen(shlex.split(runnercmd))
    self.logdb(rchild.pid)
    return rchild,submitid

  def kill(self,submitid):
    return

class barrier(job):
  def __init__(self,wrk,**kargs):
    job.__init__(self,wrk,**kargs)
    self.kargs["cmd"] = "sleep 0"
  def is_barrier(self):
    return True

class fakeqsubjob(job):
  
  def __init__(self,wrk,**kargs):
    job.__init__(self,wrk,**kargs)
    
    self.submit_file,self.release_file,self.kill_file = self.create_srkfiles()

  def submit(self,runnercmd):
    self.make_exec(self.submit_file,"echo 1")
    submitid = sbp.check_output([self.submit_file,runnercmd],shell=True).decode("utf-8")
    return None,submitid

  def release(self,runnercmd,submitid):
    self.make_exec(self.release_file,"nohup $2 1> /dev/null 2>/dev/null </dev/null &\necho $1")
    releaseid = sbp.Popen([self.release_file,submitid,runnercmd],stdout = sbp.PIPE)
    assert releaseid.wait()==0
    submitid = releaseid.stdout.read()
    self.logdb(submitid)
    return None,submitid

  def kill(self,submitid):
    self.make_exec(kill_file,"sleep 1")
    sbp.check_output([self.killscript,self.submitid,self.runnercmd],shell=True).decode("utf-8")

_python_cmd = """
import dill as cpic
import types

inputs = open("%s").read()
objs = cpic.loads(inputs)
res = objs[0](*objs[1],**objs[2])
f = open("%s","w")
f.write(cpic.dumps(res))
f.close()
"""

class python_job(job):
  def __init__(self,*args,**kargs):
    import dill
    job.__init__(self,*args,**kargs)

  def safe(self):
    self.build_cmd()
    return job.safe(self)

  def build_cmd(self):
    import dill
    objfile = self.create_file(".objectpickle","objectpickle",tmp=True)
    resfile = self.create_file(".respickle","respickle",tmp=True)
    dill.dump((self.kargs["cmd"][0],self.kargs["cmd"][1],self.kargs["cmd"][2]),open(objfile,"w"))
    cmd = [[sys.executable,"-c",_python_cmd%(objfile,resfile)]]
    self.kargs["cmd"] = cmd

  
  def get_res(self):
    import dill
    res = dill.load(open(self.kargs["respickle"]))
    return res


class qsub(job):
  def __init__(self,wrk,**kargs):
    job.__init__(self,wrk,**kargs)
    qsub_options = {
      "-S": "/bin/bash",
      "-j": "oe",
      "-m": "n",
      "-o":  self.create_file(ext=".qsub.log",tmp=True),
      #"-d": os.getcwd()
    }
    label = self.kargs["label"]
    if len(label)>14:
      label = label[:13]+"X"
    qsub_options["-N"] = label
    qsub_options.update(self.kargs.get("qsub_options",{}))
    self.kargs["qsub_options"] = qsub_options
    self.kargs["qsub_env"] = self.kargs.get("qsub_env",{})
    self.kargs["qsub_before"] = self.kargs.get("qsub_before",[])
    self.kargs["qsub_after"] = self.kargs.get("qsub_after",[])

  def submit(self,runnercmd):
    batch_file = self.create_file(".qsub","qsub")

    txt = ""

    for k in self.kargs["qsub_options"]:
      txt+="#PBS %s %s\n"%(k,self.kargs["qsub_options"][k])
    for k in self.kargs["qsub_env"]:
      txt+="export %s=%s\n"%(k,self.kargs["qsub_env"][k])
    txt+="cd %s\n"%os.getcwd()
    for l in self.kargs["qsub_before"]:
      txt+=l+"\n"
    txt+=runnercmd+"\n"
    for l in self.kargs["qsub_after"]:
      txt+=l+"\n"
    self.make_exec(batch_file,txt)
    submitid = sbp.check_output(["qsub","-h",batch_file]).decode("utf-8")
    
    
    submitid = submitid.split(".")[0]
    self.incdb("qsubid",submitid)
    self.logdb(submitid)
    return None,submitid 

  def release(self,runnercmd,submitid):
    sbp.check_output(["qrls",submitid]).decode("utf-8")
    return None,submitid

  def kill(self,submitid):
    sbp.check_output(["qdel",submitid]).decode("utf-8")

_python_rcmd = """
import sys
sys.path = %s+sys.path
print(sys.path)
import marshal
import types
print("toto")
func_code = marshal.loads(%s)#.decode("base64"))
args = marshal.loads(%s)#.decode("base64"))
kargs = marshal.loads(%s)#.decode("base64"))
print("tata")
func = types.FunctionType(func_code,globals())
func(*args,**kargs)
print ("toto")
"""

def python_cmd(func,args,kargs={},use_sys_path=True):
  import marshal
  #func_s = repr(marshal.dumps(func.__code__).encode("base64"))
  #args_s = repr(marshal.dumps(args).encode("base64"))
  #kargs_s = repr(marshal.dumps(kargs).encode("base64"))
  func_s = repr(marshal.dumps(func.__code__))#.encode("base64"))
  args_s = repr(marshal.dumps(args))#.encode("base64"))
  kargs_s = repr(marshal.dumps(kargs))#.encode("base64"))
  spath = []
  if use_sys_path:
    import sys
    spath = sys.path
  ext = _python_rcmd%(spath,func_s,args_s,kargs_s)
  cmd = [[sys.executable,"-c",ext]]
  return cmd
