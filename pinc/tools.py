from __future__ import print_function
import re
import time
import datetime
import os.path as osp
import os
import stat

def mkdir_safe(path):
  if osp.exists(path):
    return
  else:
    mkdir_safe(osp.dirname(path))
    os.mkdir(path)

def first_exists(paths):
  for p in paths:
    if osp.exists(p):
      return p
  return ""

def get_uniq(label,test):
  if test(label):
    vl = re.findall(".+_(\d+)",label)
    if not vl:
      label = label+"_1"
    else:
      label = "_".join(label.split("_")[:-1]+[str(int(vl[0])+1)])
    return get_uniq(label,test)
  return label


class minitee:
  def __init__(self,fi1,fi2):
    self.fi1 = fi1
    self.fi2 = fi2
    
  def write(self,txt):
    self.fi1.write(txt)
    self.fi2.write(txt)
    self.fi1.flush()
    self.fi2.flush()
  def flush(self):
    self.fi1.flush()
    self.fi2.flush()
    
##class miniq:
##  def __init__(self,li=[]):
##    self.li = []
##  def add(self,act):
##    self.li.append(act)
##    return act
##  def pop(self):
##    return self.li.pop(0)
##  def __nonzero__(self):
##    return bool((self.li))

class delay:
  def __init__(self,delay,func,update_func=None):
    self.delay = delay
    self.timestamp = -1
    self._update = False
    self.func = func
    self.update_func = update_func
  
  def update(self,*args,**kargs):
    self._update = True
    if callable(self.update_func):
      self.update_func(*args,**kargs)

  def __call__(self,*args,**kargs):
    if ((self.timestamp<0) or (time.time() - self.timestamp > self.delay)) and self._update:
      self.func(*args,**kargs)
      self.timestamp = time.time()
      self._update = False

class logger:
  def __init__(self,*files):
    self.backlog = ""
    self.files = []
    self.close = []
    for fi in files:
      self.add_file(fi)
  def __call__(self,*txt,**kargs):
    end = kargs.get("end","\n")
    self.backlog+="%s: "%datetime.datetime.today().isoformat()+" ".join(str(t) for t in txt)+end
    bkl = self.backlog
    for fi in self.files:
      print(bkl,end="",file=fi)
      fi.flush()
      self.backlog = ""

  def write(self,txt):
    self(txt)
  def flush(self):
    pass
  def add_file(self,file):
    close = 0
    if isinstance(file,str):
      file=open(file,"w")
      close = 1
    self.files += [file]
    self.close +=[close]
  def __del__(self):
    for fi,cl in zip(self.files,self.close):
      if cl:
        fi.close()

class bag(set):
  def remove(self,what):
    if what not in self:
      return
    set.remove(self,what)

def open_in_dir(options,filename,rw="r",exc=False):
  f=open(osp.join(options["dirpath"],filename),rw)
  if exc:
    os.chmod(osp.join(options["dirpath"],filename),stat.S_IRWXU)
  return f