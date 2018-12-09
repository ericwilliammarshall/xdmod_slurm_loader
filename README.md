# xdmod_slurm_loader
a script to take data from slurmdb and load xdmod for months of data
this was designed as a one-shot script for reloading xdmod with data from slurm, but may be used again if we encounter the same scenario in the future.

XDMod displays data from slurmdb. This script allows the loading off historical data from slurmdb into xdmod. This script figures the number of days from some date until today. And then for each of those days, grab a day's worth of data and have xdmod ingest this. Thus avoiding the problem of asking for too much data from slurmdb. In the past, large requests would crash our slurmdb. This scripts solves that problem for me.

