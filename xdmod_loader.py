#!  /usr/bin/env python2.7


#ericmars
#11/2018
#import subprocess
#import datetime

from time import sleep
from os import stat
from datetime import date
from datetime import datetime
from datetime import timedelta
from subprocess import call
from subprocess import check_output 
import os

# TODO add a verbose option which enables ar disables the s commands
# TODO: read the list of clusters from the config file in /etc/xdmod
clusters = ('amarel',  'nh3', 'hpcc', 'perceval', 'amarelc', 'amarelg2', 'amareln')

# TODO: add argument parsing and the ability to pick dates
# original start date 2015-11-01
# for our systems
start_date = date(2015,11,29)
# test start
#start_date = date(2016,02,01)



# our restrart date 2016-02-25
# 2015-12-30
# 2016-03-26
#start_date = date(2016,04,10)

# hpcc was misconfigured at first
skip_until = { 
    "hpcc": date(2016,01,10),
    "nm3": date(2016,01,10),
    "amarel": date(2017,01,10),
    "amarelg": date(2018,07,10),
    "amarelc": date(2018,07,10),
    "amareln": date(2018,07,10),
}
# skip dates with bad data
skip_perceval_dates = set(
	[
        date(2016,02,15),
        date(2016,02,16),
        date(2016,02,17),
        date(2016,02,18),
        date(2016,02,19),
        date(2016,02,20),
        date(2016,02,21),
        date(2016,02,22),
        date(2016,02,23),
        date(2016,02,24),
        date(2016,02,25),
        date(2016,02,26),
        date(2016,02,27),
    ]
    )


# TODO: add python file control instead of hard coded linux paths
ingest_file = '/tmp/ingest.dump'
ingest_clean_file = '/tmp/ingest_clean.dump'

# number of pipe characters in a proper line of data
# the number of pipe characters is a proxy for correctly formed data
# if we don't have all the pipes, it is truncated or an array of jobs
magic_num = 24

# ingest_every_so_many_days is how often we run the ingestion
# for us, this process takes several hours, so in order to catch up 
# I can't run it for every day, since it almost takes a day to ingest!
ingest_every_so_many_days = 4

def ingest():
	# ingest is the most time consuming part by hours!!
    # run the xdmod-ingestor
    print("\ncalling the ingester")
    #check = call(['xdmod-ingestor', '--debug'])
    check = call(['xdmod-ingestor'])
    #print(check)    
    print("done calling the ingester\n")
    # run service httpd reload
    print("restarting httpd ")
    check = call(['service', 'httpd','reload'])
    print(check , " \n")    
    print("done restarting httpd ")


def scrub_file():
    """ filter the input file to remove truncated lines which crash the xmod-ingestor. This also removes the short lines that sacct generates for some failed array jobs (which also crash the xdmod ingestor"""  	
    print("scrub file")
    with open(ingest_file, 'r') as ingest:	
        with open(ingest_clean_file, 'w', 0 ) as clean_ingest:
            for line in ingest: 
                count = line.count('|')
		if count == magic_num:
		    clean_ingest.write(line)

	
def eachtime(cluster, the_date):
    """ for each day, pull data from sacct and write to a file. If the file isn't empty, fully ingest the data and restart httpd"""
    print("each time" + cluster)

    start_time = the_date.strftime('%Y-%m-%d') + 'T00:00'
    end_time = the_date.strftime('%Y-%m-%d') + 'T23:59'
    # grab the data from slurm
    print("########### starting a day ############")
    print("calling sacct with " + start_time + " and " + end_time + "\n")
	
	# TODO: add error checking
    #        sacct --allusers --parsable2 --noheader --allocations --duplicates --clusters $one --format jobid,jobidraw,cluster,partition,account,group,gid,user,uid,submit,eligible,start,end,elapsed,exitcode,state,nnodes,ncpus,reqcpus,reqmem,reqgres,reqtres,timelimit,nodelist,jobname --state CANCELLED,COMPLETED,FAILED,NODE_FAIL,PREEMPTED,TIMEOUT --starttime 2018-06-29T03:59:37 --endtime 2018-07-10T03:59:37 >/tmp/ingest.dump
    input =  'sacct --allusers --parsable2 --noheader --allocations --duplicates --clusters ' + cluster + ' --format jobid,jobidraw,cluster,partition,account,group,gid,user,uid,submit,eligible,start,end,elapsed,exitcode,state,nnodes,ncpus,reqcpus,reqmem,reqgres,reqtres,timelimit,nodelist,jobname --state CANCELLED,COMPLETED,FAILED,NODE_FAIL,PREEMPTED,TIMEOUT --starttime ' + start_time + ' --endtime '+ end_time + ' > ' + ingest_file
    #print(input)
    print("done calling sacct \n")
    check = check_output( input, shell=True )

    # if the file has data, lets ingest it
    if stat(ingest_file).st_size != 0:
        print("cleaning the data")
        scrub_file()

        print("\ncalling the shredder")
        #        xdmod-shredder -r  -f slurm -i /tmp/ingest.dump 
        input = 'xdmod-shredder -r ' + cluster + ' -f slurm -i ' + ingest_clean_file
       	#print( input )
        check = check_output(input, shell=True) 
        #print(check)    
        print("done calling the shredder")
    else:
	    print("empty ingest file")
    # print a time stamp
    print(date.today().strftime("%Y-%m-%d %H:%M"))


# how many days to pull from slurm
today = date.today()
number_of_days = (today - start_date).days

print( "############## starting ############### \n")
print( "today: ", today, " number of days: " , number_of_days, " \n")
# for each day between the begining of 2015 and now
for day in range(number_of_days):
    day_format = start_date + timedelta(days=day)
    print(day_format)
    # and for each cluster we have or had
    for cluster in clusters:
        if cluster == 'hpcc' and skip_hpcc_until < day_format:
        if skip_until[cluster] < day_format:
            print("skipping "+ cluster + ": before start date"
            continue 

        if cluster == 'perceval' and day_format in skip_perceval_dates:
            continue 

        print('    ' + cluster)
        eachtime(cluster, day_format)
        # after loding up a few dayof data, ingest and restart httpd
        #if day != 0 and (ingest_every_so_many_days % day) == 0:
        ingest()
