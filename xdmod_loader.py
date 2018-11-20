#! /usr/bin/python


#ericmars
#11/2018
#import subprocess
#import datetime

from datetime import date
from datetime import datetime
from datetime import timedelta
from subprocess import call


start_date = date(2015,01,01)
clusters = ('amarel',  'nh3', 'hpcc', 'perceval', 'amarelci', 'amarelg2', 'amareln')

def eachtime(cluster, the_date):
    starttime = the_date + 'T00:00'
    endtime = the_date + 'T23:59'
    # grab the data from slurm
    print("calling sacct with " + startime + " and " + endtime)
    #check = call([ 'sacct', '--allusers', '--parsable2', '--noheader', '--allocations', '--duplicates', '--clusters', cluster, '--format', 'jobid,jobidraw,cluster,partition,account,group,gid,user,uid,submit,eligible,start,end,elapsed,exitcode,state,nnodes,ncpus,reqcpus,reqmem,reqgres,reqtres,timelimit,nodelist,jobname', '--state', 'CANCELLED,COMPLETED,FAILED,NODE_FAIL,PREEMPTED,TIMEOUT', '--starttime', startime, '--endtime', endtime, '>/tmp/ingest.dump' ])
    #        sacct --allusers --parsable2 --noheader --allocations --duplicates --clusters $one --format jobid,jobidraw,cluster,partition,account,group,gid,user,uid,submit,eligible,start,end,elapsed,exitcode,state,nnodes,ncpus,reqcpus,reqmem,reqgres,reqtres,timelimit,nodelist,jobname --state CANCELLED,COMPLETED,FAILED,NODE_FAIL,PREEMPTED,TIMEOUT --starttime 2018-06-29T03:59:37 --endtime 2018-07-10T03:59:37 >/tmp/ingest.dump
    #        xdmod-shredder -r  -f slurm -i /tmp/ingest.dump 
    print("calling the shredder")
    #check = call(['xdmod-shredder','-r','-f','slurm','-i','/tmp/ingest.dump']) 
    print(check)    
    #        xdmod-ingestor
    #print("calling the ingester")
    check = call(['xdmod-ingestor'])
    #print(check)    
    #        service httpd reload
    #print("restarting httpd ")
    check = call(['service', 'httpd','reload'])
    #print(check)    
    #        echo "ingested"
    #print("calling the ")
    #call(['/usr/bin/logger','ingested'])
    #        date
    #print("calling the date ")
    check = call(['date'])
    print(check)    

# how many days to pull from slurm
today = date.today()
number_of_days = (today - start_date).days

# for each day between the begining of 2015 and now
for day in range(number_of_days):
    day_format = start_date + timedelta(days=day)
    print(day_format)
    # and for each cluster we have or had
    for cluster in clusters:
        print('    ' + cluster)
	eachtime(cluster, day_format):
[root@biomech ~]# tmux attach -t0

    print()
    print([ 'sacct', '--allusers', '--parsable2', '--noheader', '--allocations', '--duplicates', '--clusters', cluster, '--format', 'jobid,jobidraw,cluster,partition,account,group,gid,user,uid,submit,eligible,start,end,elapsed,exitcode,state,nnodes,ncpus,reqcpus,reqmem,reqgres,reqtres,timelimit,nodelist,jobname', '--state', 'CANCELLED,COMPLETED,FAILED,NODE_FAIL,PREEMPTED,TIMEOUT', '--starttime', start_time, '--endtime', end_time, '>', '/tmp/ingest.dump' ])
    #check = call([ 'sacct', '--allusers', '--parsable2', '--noheader', '--allocations', '--duplicates', '--clusters', cluster, '--format', 'jobid,jobidraw,cluster,partition,account,group,gid,user,uid,submit,eligible,start,end,elapsed,exitcode,state,nnodes,ncpus,reqcpus,reqmem,reqgres,reqtres,timelimit,nodelist,jobname', '--state', 'CANCELLED,COMPLETED,FAILED,NODE_FAIL,PREEMPTED,TIMEOUT', '--starttime', start_time, '--endtime', end_time, '>', '/tmp/ingest.dump' ])
    input =  'sacct --allusers --parsable2 --noheader --allocations --duplicates --clusters ' + cluster + ' --format jobid,jobidraw,cluster,partition,account,group,gid,user,uid,submit,eligible,start,end,elapsed,exitcode,state,nnodes,ncpus,reqcpus,reqmem,reqgres,reqtres,timelimit,nodelist,jobname --state CANCELLED,COMPLETED,FAILED,NODE_FAIL,PREEMPTED,TIMEOUT --starttime ' + start_time + ' --endtime '+ end_time + ' > /tmp/ingest.dump'
    check = check_output( input, shell=True )
    #        sacct --allusers --parsable2 --noheader --allocations --duplicates --clusters $one --format jobid,jobidraw,cluster,partition,account,group,gid,user,uid,submit,eligible,start,end,elapsed,exitcode,state,nnodes,ncpus,reqcpus,reqmem,reqgres,reqtres,timelimit,nodelist,jobname --state CANCELLED,COMPLETED,FAILED,NODE_FAIL,PREEMPTED,TIMEOUT --starttime 2018-06-29T03:59:37 --endtime 2018-07-10T03:59:37 >/tmp/ingest.dump
    #        xdmod-shredder -r  -f slurm -i /tmp/ingest.dump 
    print()
    print("calling the shredder")
    #check = call(['xdmod-shredder','-r','-f','slurm','-i','/tmp/ingest.dump']) 
    if stat('/tmp/ingest.dump').st_size != 0:

        input = 'xdmod-shredder -r ' + cluster + ' -f slurm -i /tmp/ingest.dump'
        check = check_output(input, shell=True) 
        print(check)    
        #        xdmod-ingestor
        print()
        print("calling the ingester")
        check = call(['xdmod-ingestor'])
        print(check)    
        #        service httpd reload
        print()
        print("restarting httpd ")
        check = call(['service', 'httpd','reload'])
        print(check)    
        #        echo "ingested"
        #print("calling the ")
        #call(['/usr/bin/logger','ingested'])
        #        date
        #print("calling the date ")
        print()
    else:
        print("empty ingest file")
    print(date.today().strftime("%Y-%m-%d %H:%M"))


# how many days to pull from slurm
today = date.today()
number_of_days = (today - start_date).days

# for each day between the begining of 2015 and now
for day in range(number_of_days):
    day_format = start_date + timedelta(days=day)
    print(day_format)
    # and for each cluster we have or had
    for cluster in clusters:
        print('    ' + cluster)
        eachtime(cluster, day_format)
