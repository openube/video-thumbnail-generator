#!/usr/bin/python

import re
import shutil
import math
from os.path import splitext
import datetime
import simplejson as json
import subprocess
import os
from progress_bar import ProgressBar
import sys


number_of_posterfiles=8
thumbnail_dimension='160x90'
thumbnail_quality=75
directory='/mnt/s3fs/'
#directory='/tmp/test/'
posterfiledir='/mnt/s3fs/posterfiles/'
#posterfiledir='/tmp/test/posterfiles/'

metafile = posterfiledir+'meta.js'
tempdir = '/tmp/'
files=os.listdir(directory)

meta = {}

def main():
	print "Starting up"
	#Setup PID file to see if we're already running
	pid = str(os.getpid())
	pidfile = "/tmp/thumbnailgenerator.pid"
	if os.path.isfile(pidfile):
		print "%s already exists, exiting" % pidfile
		sys.exit()
	else:
		file(pidfile, 'w').write(pid)

	#Load whatever metadata already exists
	if os.path.exists(metafile):
		f = open(metafile,'r')
		meta = json.load(f)
		f.close()

	print "Processing all files"
	process_all_files()

	#Commit the metadata back to disk
	f = open(metafile,'w')
	json.dump(meta,f)
	f.close()

	#Clean up PID file
	os.unlink(pidfile)

def process_all_files():
	total_files = len(files)
	current=0;
	p = ProgressBar(total_files)
	#Loop through all files in the directory
	for file in files:
		#Update the progress bar
		current+=1	
		p.update_time(current)
		sys.stdout.write(str(p)+'\r')
		sys.stdout.flush()

		#Extract the filename and extension
		filename,extension = splitext(file)

		#Are we a video file?
		if extension in ['.mp4','.m4v','.mov','.mkv','.wmv']:
			if not file in meta:
				#Extract Metadata from the file
				meta[file]=get_metadata(directory+file)
			if not os.path.exists(posterfiledir+file+'_0.jpg'):
				
				#Figure out the intervals at which we need to take posterfiles
				durations=meta[file]['duration'].split(":")
				totallength = int((int(durations[0])*3600)+(int(durations[1])*60)+float(durations[2]))

				#Dump the video in a tempdirectory to reduce latency
				shutil.copy2(directory+file,tempdir+file)

				interval = float(totallength) / (number_of_posterfiles+1);
				intervals = []
				for x in range(number_of_posterfiles):
					intervals.append(interval*(x+1))
				for idx,val in enumerate(intervals):
					posterfile = posterfiledir+file+"_"+str(idx)+".jpg"
					thumbnail_posterfile = posterfiledir+file+"_"+str(idx)+".th.jpg"
					cmd = ["ffmpeg","-i",tempdir+file,"-an","-ss",str(val),"-f","mjpeg","-qmin","0.8","-qmax","0.8","-t","1","-r","1","-y",posterfile]
					outputs = subprocess.Popen(cmd,stderr=subprocess.PIPE).communicate()[1]
					th_cmd = ["convert",posterfile,"-resize",thumbnail_dimension+"^","-gravity","center","-extent",thumbnail_dimension,"-quality",str(thumbnail_quality),thumbnail_posterfile]
					th_outputs = subprocess.Popen(th_cmd,stderr=subprocess.PIPE).communicate()[1]
				os.remove(tempdir+file)

def get_metadata(filename):
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

#Kick into the main proc
main()
