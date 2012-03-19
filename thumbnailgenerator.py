#!/usr/bin/python

import stat
import re
import signal
import shutil
import math
from os.path import splitext
import datetime
import simplejson as json
import subprocess
import os
from progress_bar import ProgressBar
import sys
import pika
import argparse

sigint_caught=False
debug_mode=True
number_of_posterfiles=8
thumbnail_dimension='160x90'
thumbnail_quality=75
directory='/mnt/s3fs/'
#directory='/tmp/test/'
posterfiledir='/mnt/s3fs/posterfiles/'
#posterfiledir='/tmp/test/posterfiles/'
pidfile = "/tmp/thumbnailgenerator.pid"

metafile = posterfiledir+'meta.js'
tempdir = '/tmp/'
files=os.listdir(directory)

meta = {}

def signal_handler(signal, frame):
	global sigint_caught
	if sigint_caught:
		print 'SIGINT caught, exiting'
		commit_metadata()

		#Clean up PID file
		os.unlink(pidfile)
	
		sys.exit(0)
	else:
		print 'SIGINT caught, quitting after next job'
		sigint_caught=True

def main():
	print "Starting up"

	#Exit on SIGINT
	signal.signal(signal.SIGINT, signal_handler)

	#Setup PID file to see if we're already running
	pid = str(os.getpid())
	if os.path.isfile(pidfile):
		print "%s already exists, exiting" % pidfile
		sys.exit()
	else:
		file(pidfile, 'w').write(pid)

	debug("Opening rMQ connection")
	connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
	channel = connection.channel()
	channel.queue_declare(queue='thumbnailgenerator')
	

	#Load whatever metadata already exists
	if os.path.exists(metafile):
		f = open(metafile,'r')
		meta = json.load(f)
		f.close()

	#Start listening for messages:
	channel.basic_consume(process_msg,queue='thumbnailgenerator',no_ack=True)
	debug("Listening for messages")
	channel.start_consuming()


def process_msg(ch,method,properties,body):
	debug("Received msg: "+body)
	filename = directory + body
	if (os.path.exists(filename)):
		meta[body] = get_metadata(body)
		generate_posterfiles(body)
		commit_metadata()
	else:
		debug(body + " doesn't seem to exist")
	if sigint_caught:
		sys.exit(0)
	debug("Idle")

def commit_metadata():
	debug("Writing metadata to disk")
	#Commit the metadata back to disk
	f = open(metafile,'w')
	json.dump(meta,f)
	f.close()

def generate_posterfiles(filename):
	"""Expects short filename"""
	debug("Generating " + str(number_of_posterfiles) + " posterfiles for" + filename)
	#Figure out the intervals at which we need to take posterfiles
	durations=meta[filename]['duration'].split(":")
	totallength = int((int(durations[0])*3600)+(int(durations[1])*60)+float(durations[2]))
	debug("Copying file to tmp")
	#Dump the video in a tempdirectory to reduce latency
	shutil.copy2(directory+filename,tempdir+filename)
	
	#Set the permissions through chmod
	os.chmod(tempdir+filename,stat.S_IRUSR)


	interval = float(totallength) / (number_of_posterfiles+1);
	intervals = []
	for x in range(number_of_posterfiles):
		intervals.append(interval*(x+1))
	for idx,val in enumerate(intervals):
		posterfile = posterfiledir + filename + "_"+str(idx)+".jpg"
		thumbnail_posterfile = posterfiledir + filename + "_" + str(idx) + ".th.jpg"
		debug("Generating "+posterfile)
		cmd = ["ffmpeg","-i",tempdir+filename,"-an","-ss",str(val),"-f","mjpeg","-qmin","0.8","-qmax","0.8","-t","1","-r","1","-y",posterfile]
		outputs = subprocess.Popen(cmd,stderr=subprocess.PIPE).communicate()[1]
		th_cmd = ["convert",posterfile,"-resize",thumbnail_dimension+"^","-gravity","center","-extent",thumbnail_dimension,"-quality",str(thumbnail_quality),thumbnail_posterfile]
		th_outputs = subprocess.Popen(th_cmd,stderr=subprocess.PIPE).communicate()[1]
	os.remove(tempdir+filename)

def get_metadata(short_filename):
	"""Expects short filename"""
	filename = directory + short_filename
	debug("Extracting metadata from " +short_filename)
	#Grab file info from ffmpeg
	metadata_str = subprocess.Popen(['ffmpeg','-i',filename],stderr=subprocess.PIPE).communicate()[1]
	metadata_parts = re.findall(r'[^,\|\n]+',metadata_str.replace(': ','|'))
	metadata_start_index = 0
	inc=0;
	metadata={}
	val=''
	seen_v_stream=0
	seen_a_stream=0
	metadata['since']=datetime.datetime.fromtimestamp(int(os.path.getmtime(filename))).isoformat()
	for ii in range(len(metadata_parts)):
		val = metadata_parts[ii].strip()
		if val == 'encoder':
			metadata['encoder'] = metadata_parts[ii+1].strip()
		if val == 'WM/ToolName':
			metadata['encoder'] = metadata_parts[ii+1].strip()
		if val == 'Duration':
			metadata['duration'] = metadata_parts[ii+1].strip()
		if val == 'bitrate':
			metadata['total_bitrate'] = metadata_parts[ii+1].strip()
		if 'Stream #' in val:
			streamtype = metadata_parts[ii+1].strip()
			if streamtype == 'Audio' and seen_a_stream == 0:
				seen_a_stream=1
				metadata['a_codec']=metadata_parts[ii+2].strip()
				metadata['a_rate']=metadata_parts[ii+3].strip()
				metadata['a_bitrate']=metadata_parts[ii+6].strip()
			if streamtype == 'Video' and seen_v_stream == 0:
				seen_v_stream=1
				metadata['v_codec']=metadata_parts[ii+2].strip()
				metadata['v_color']=metadata_parts[ii+3].strip()
				metadata['v_dimension']=metadata_parts[ii+4].strip()
				metadata['v_bitrate']=metadata_parts[ii+5].strip()
				metadata['v_fps']=metadata_parts[ii+6].strip()
	return metadata

def debug(msg):
	if debug_mode:
		print msg

#Kick into the main proc
main()
