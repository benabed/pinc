import time
import sys

print ("in sleeper")

print ("wait for %s sec"%sys.argv[1])

print ("this is a stderr test",file=sys.stderr)

time.sleep(abs(int(sys.argv[1])))

if len(sys.argv)>2:
  print ("got a parfile ! %s"%sys.argv[2])
  pf = open(sys.argv[2])
  for l in pf:
    print l
print ("finished !")
if int(sys.argv[1])<0:
  sys.exit(int(sys.argv[1]))
sys.exit(0)