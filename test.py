#!/usr/bin/python
import os
import signal
import sys
import subprocess
import socket
import time

def wrapper ():
#This is a simple python wrapper for the data aquisition script
#The aim of the wrapper is to redirect standart output and error to
#files, to save information about pid and to handle SIGTERM signal 
#for the correct termination.
    current_hostname = socket.gethostname ()

#Save pid into the $hostname.run file
#if file is already exist, than start is impossible
#previous process has to be removed
    if os.path.isfile (current_hostname+".run"):
        sys.stderr = open (current_hostname+".err.2","w")
        sys.stderr.write ("File "+current_hostname+".run already exists, thus the previous data aquisition is not yet completed.\nAbort.\n")
        sys.stderr.close ()
        sys.exit (-1)
    runfile = open (current_hostname+".run","w")
    runfile.write (str(os.getpid ())+"\n")
    runfile.close ()

  #Open log and error files
    sys.stdout = open (current_hostname+".log","a")
    sys.stderr = open (current_hostname+".err","a")
  #define and set handler to catch SIGTERM signal
  #and save standart output and err correctly
    def handler (signum,frame):
        print "Got the signal No. "+str(signum)
        print "Terminate."
        sys.stderr.write ("Terminated.\n")
        sys.stdout.close ()
        sys.stderr.close ()
        os.system ("rm "+current_hostname+".run")
        sys.exit (0)

    signal.signal (signal.SIGTERM,handler)

#emulate a hard calculation process
    i=0
    while i < 100:
        print i
        i = i + 1
        time.sleep (1)
    handler (-1,0)

def killer ():
    current_hostname = socket.gethostname ()
    try:
        runfile= open (current_hostname+".run")
        for linefile in runfile:
            os.kill (int (linefile),signal.SIGTERM)
    except IOError:
        sys.stderr.write ("Warning, run file is absent.\n")
    return
        
    

def check_init ():
    current_hostname=socket.gethostname ()
    if "ecl01" != current_hostname:
        sys.stderr.write ("Incorrect host. Has to be ecl01\n")
        sys.exit (-1)
    #test_command.py is a script that has to be performed on the all computers
    subprocess.call ("scp "+__file__+" ecl02:"+__file__,shell="/bin/sh")
    return

def read_config ():
    confhosts=[]
    try:
        configfile = open ("config.dat","r")
    except IOError:
        sys.stderr.write ("IOError. Probably config.dat file was not found. Halt.\n")
        sys.exit (-1)
    cprhosts = {}
    for fileline in configfile:
        confhost = int (fileline)
        if (confhost-1)%2 == 0: #1,3,5... 
            crate="a"           #1,3,5... -> 1,2,3 (in cpr)
            cpr=(confhost+1)/2
        else:                   #2,4,6...
            crate="b"           #2,4,6... -> 1,2,3 (in cpr)
            cpr=confhost/2
        try:
            prevflag=cprhosts[cpr]
            if ((prevflag == "a") and (crate == "b")) or ((prevflag == "b") and (crate == "a")):
                cprhosts[cpr]="ab"
            else:
                sys.stderr.write ("Warning! During read the config.dat the same hosts and crates was found.\n")
                sys.stderr.write ("\tReport: cpr = "+str(cpr)+ " crate = "+crate+
                    ", probably in config.dat the number "+str(confhost)+" was repeated at least twice\n")
        except KeyError:        #in this case key means key of the dictionary, not the key from the keyboard :)
            cprhosts[cpr]=crate
    return cprhosts

#Run COMMAND on the all slave machines from the ecl0x
#for ecl02 slaves command has to be 
#ssh ecl02 -t "ssh cpr5014 -t 'hostname >& cpr5014.err > cpr5014.log'"
#for ecl01 slaves command has to be 
#ssh cpr5013 -t 'hostname >& cpr5013.err > cpr5013.log'
def generate_commands (cprhosts, COMMAND):
    shellcommands=[]
    for cpr in cprhosts.keys():
        cprname="cpr50"+str(cpr).zfill(2)
        if cpr <= 13:
            hostname=cprname
            command=" -t ' "+COMMAND +"  & '"
        else:
            hostname="ecl02"
            command=" -t \" ssh "+cprname + " -t ' "+COMMAND+" &  '\""
        shellcommands.append(("ssh "+hostname+" "+command))
    return shellcommands
    #let us start
#start processes
def run_command (COMMAND,detach):
    check_init ()
    cpr_hosts={}
    cpr_hosts=read_config ()
    for shellcommand in generate_commands(cpr_hosts, COMMAND):
        if detach:
            pid = os.system (shellcommand)
        else:
            subprocess.call (shellcommand, shell="/bin/tcsh")
    return 0


if __name__ == "__main__":
    import optparse
    parser = optparse.OptionParser()
#    parser.add_option ("-w", "--wrapper", action="store_true",dest="wrapper")

    (options, args) = parser.parse_args ()
    if (len(args)==0) or (args[0]=="start"):
        run_command ("python test.py wrapper",True)
    elif args[0]=="stop":
        run_command ("python test.py killer",False)
        subprocess.call ("scp ecl02:\"*.{log,err}\" ./",shell="/bin/tcsh")
    elif args [0]=="wrapper":
        wrapper ()
    elif args [0]=="killer":
        killer ()
    elif args [0]=="clean":
        subprocess.call ("rm *.{log,err}",shell="/bin/tcsh")
        subprocess.call ("ssh ecl02 -t 'rm *.{log,err}'",shell="/bin/tcsh")
    else:
        sys.stderr.write ("Incorrect command arguments.")
