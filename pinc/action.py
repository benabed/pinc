

# an action is a message to be send as well as a possible callback function.
# It is registered in an actionloop and all registered actions are fired at the time
# the loop runs
class action:
  def __init__(self,send,label="",msg=[],callback=None,keep=False,log=None):
    self.send=send
    self.label = label
    if isinstance(msg,str):
      msg = (msg,)
    self.msg = tuple(msg)
    self.callback = callback
    self.keep = keep
    self.log = log
  def unkeep(self,dummy):
    self.keep = False
  def __call__(self):
    #print("action",self.label,self.msg)
    self.send((self.label,)+self.msg)
    if callable(self.log):
      self.log("sent to %s :: %s"%(self.label,str(self.msg)))
    if self.callback!=None:
      self.callback()

# holds a list of action. If no action is present at the time of the loop do
# only the default action
class actionloop:
  def __init__(self,send,log,default=None):
    self.ac = []
    self.send = send
    self.log = log
    self.default = default
  def add(self,label="",msg=[],callback=None,keep=False,silent=False):
    nact = action(self.send,label,msg,callback,keep,self.log if not silent else None)
    self.ac.append(nact)
    return nact

  def loop(self):
    if not self.ac and self.default:
      # if no communication is planned, add an heartbeat
      self.add(**self.default)

    # do all actions
    kp = []
    while(self.ac):
      act = self.ac.pop()
      act()
      if act.keep: # save actions that has to be redone many times
        kp+=[act]

    # add kept actions to action list for next iteration of the loop 
    self.ac = kp


# a trigger is an event that will be run as soon as a list of conditions are 
# met for a list of objects
class trgr:

  trg_uniq = 0
  @classmethod
  def uniq(self):
    self.trg_uniq +=1
    return self.trg_uniq

  def __init__(self,typ,event,cld,callback,log=None,label="",keep=False):
    # event is the condition. It is a function that returns True is the condition
    # is met for a given object passed as input.
    # cld holds the list of objects to test

    for cc in cld:
      # the trigger has to be registered at the objects, they will call the 
      # trigger themselves at each status change
      cc.register_trigger(self)

    self.cld = set([cc for cc in cld])
    self.typ = typ
    self.callback = callback
    if callable(event):
      self.event = event
      self.str_event=str(event)
    else:
      self.event = lambda ch:ch.status in event
      self.str_event=event
    self.label = label
    self._log = log
    self.keep = keep
    self.trg_id = self.uniq()
    self.fired = False

    # check if trigger is not already true !
    for cc in list(self.cld):
      if cc in self.cld:
        self(cc)
    
  def log(self,*txt):
    if self._log:
      self._log(str(self),"::",*txt)
  
  def unreg(self,cc):
    if not self.keep:
      cc.unregister_trigger(self)
      self.cld.remove(cc)

  def __call__(self,cc):
    if self.fired and not self.keep:
      return

    if self.event(cc):
      # ok I should remove child from the trigger
      #print(str(self),"fired by %s"%cc.label)
      self.unreg(cc)
      if self.typ == "any" or len(self.cld)==0:
        # I should fire the trigger now !
        self.fired = True
        cld_copy = [c for c in self.cld]
        for c in cld_copy:
          self.unreg(c)
        self.log("do callback",self.callback)
        self.callback(cc.label)

  def __str__(self):
    return ((self.label) if self.label else "")+("(%d) : "%self.trg_id)+"trigger("+self.typ+" "+self.str_event+" "+",".join([cc.label for cc in self.cld])+")"
