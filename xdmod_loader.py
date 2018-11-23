#!  /usr/bin/env python2.7


#ericmars
#11/2018
#import subprocess
#import datetime

from os import stat
from datetime import date
from datetime import datetime
from datetime import timedelta
from subprocess import call
from subprocess import check_output 
import os

# TODO: read the list of clusters from the config file in /etc/xdmod
clusters = ('amarel',  'nh3', 'hpcc', 'perceval', 'amarelc', 'amarelg2', 'amareln')
# start date 2015-11-01
# TODO: add argument parsing and the ability to pick dates
start_date = date(2015,11,01)
# TODO: add python file control instead of hard coded linux paths
ingest_file = '/tmp/ingest.dump'
ingest_clean_file = '/tmp/ingest_clean.dump'
# number of pipe characters in a proper line of data
magic_num = 24


def scrub_file():
    """ filter the input file to remove truncated lines which crash the xmod-ingestor. This also removes the short lines that sacct generates for some failed array jobs (which also crash the xdmod ingestor"""  	
    with open(ingest_file, 'r') as ingest:	
        with open(ingest_clean_file, 'w') as clean_ingest:
            for line in ingest: 
                count = line.count('|')
		if count == magic_num:
		    clean_ingest.write(line)

	
def eachtime(cluster, the_date):
    """ for each day, pull data from sacct and write to a file. If the file isn't empty, fully ingest the data and restart httpd"""

    start_time = the_date.strftime('%Y-%m-%d') + 'T00:00'
    end_time = the_date.strftime('%Y-%m-%d') + 'T23:59'
    # grab the data from slurm
    print("########### starting a day ############")
    print("calling sacct with " + start_time + " and " + end_time)
    print()
	
	# TODO: add error checking
    input =  'sacct --allusers --parsable2 --noheader --allocations --duplicates --clusters ' + cluster + ' --format jobid,jobidraw,cluster,partition,account,group,gid,user,uid,submit,eligible,start,end,elapsed,exitcode,state,nnodes,ncpus,reqcpus,reqmem,reqgres,reqtres,timelimit,nodelist,jobname --state CANCELLED,COMPLETED,FAILED,NODE_FAIL,PREEMPTED,TIMEOUT --starttime ' + start_time + ' --endtime '+ end_time + ' > ' + ingest_file
    print(input)
    check = check_output( input, shell=True )
    #        sacct --allusers --parsable2 --noheader --allocations --duplicates --clusters $one --format jobid,jobidraw,cluster,partition,account,group,gid,user,uid,submit,eligible,start,end,elapsed,exitcode,state,nnodes,ncpus,reqcpus,reqmem,reqgres,reqtres,timelimit,nodelist,jobname --state CANCELLED,COMPLETED,FAILED,NODE_FAIL,PREEMPTED,TIMEOUT --starttime 2018-06-29T03:59:37 --endtime 2018-07-10T03:59:37 >/tmp/ingest.dump
    #        xdmod-shredder -r  -f slurm -i /tmp/ingest.dump 
    # if the file has data, lets ingest it
    if stat(ingest_file).st_size != 0:
	print("cleaning the data")
	scrub_file()
    	print()

    	print("calling the shredder")
    	input = 'xdmod-shredder -r ' + cluster + ' -f slurm -i ' + ingest_clean_file
    	check = check_output(input, shell=True) 
    	print(check)    
    	# run the xdmod-ingestor
    	print()
    	print("calling the ingester")
        check = call(['xdmod-ingestor'])
        print(check)    
        # run service httpd reload
        print()
        print("restarting httpd ")
        check = call(['service', 'httpd','reload'])
        print(check)    
        print()
    else:
	print("empty ingest file")
    # print a time stamp
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
