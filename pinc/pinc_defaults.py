defaults = {
  "path"                : ".",
  "label"               : "pinc",
  "tmpdirname"          : "tmp",
  "logsuffix"           : ".log",
  "server_protocol"     : "ipc",
  "server_address"      : "*",
  "server_loop_timeout" : 1.,           # in sec
  "child_protocol"      : "tcp",
  "child_address"       : "*:*",
  "child_ping_timeout"  : 60.,         # in sec
  "child_ping_delay"    : 50.,          # in sec
  "child_loop_timeout"  : 2.,           # in sec
  "server_log"          : "server.log",
  "emergency_log"       : "emergency.log",
  "status_log"          : "status.log",
  "status_log_delay"    : 30,
  "dep_log"             : "status.dot",
  "dep_log_delay"       : 30,
}

job_defaults = {}
