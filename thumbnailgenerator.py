#!/usr/bin/python

import ftputil
import stat
import re
from os.path import splitext
import signal
import datetime
import simplejson as json
import subprocess
import os
import sys
import pika
import boto

sigint_caught=False
debug_mode=True
number_of_posterfiles=8
thumbnail_dimension='160x90'
thumbnail_quality=75
pidfile = "/tmp/thumbnailgenerator.pid"

ftp_host = ''
ftp_username = ''
ftp_password = ''

metafile = 'posterfiles/meta.js'
tempdir = '/tmp/'
bucket = {}
s3_bucket_name = ''

meta = {}

try:
    from local_settings import *
except ImportError:
    pass


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
	global meta
	global bucket
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

	bucket = boto.connect_s3().get_bucket(s3_bucket_name)

	debug("Opening rMQ connection")
	connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
	channel = connection.channel()
	channel.queue_declare(queue='thumbnailgenerator')
	
	load_metadata()

	#Start listening for messages:
	channel.basic_consume(process_msg,queue='thumbnailgenerator',no_ack=True)
	debug("Listening for messages")
	channel.start_consuming()


def process_msg(ch,method,properties,body):
	global meta
	debug("Received msg: "+body)
	try:
		decoded_msg = json.loads(body)

		if 'command' in decoded_msg:
			if decoded_msg['command'] == 'add':
				filename = decoded_msg['filename']
				if not bucket.get_key(filename) == None and bucket.get_key(filename).exists():
					meta[filename] = get_metadata(filename)
					generate_posterfiles(filename)
					upload_to_ftp(filename)
					commit_metadata()
				else:
					debug(filename + " doesn't seem to exist")
			elif decoded_msg['command'] == 'purgemetadata':
				debug("Purging extraneous metadata")
				meta_to_purge = []
				bucket_key_list = []
				for key in bucket.list():
					bucket_key_list.append(key.name)
				debug("Got all keys. Size ="+str(len(bucket_key_list)))
				for metafile in meta:
					debug("seeing if "+metafile+" is absent")
					if not metafile in bucket_key_list:
						debug("File absent. Purging metadata for "+metafile)
						meta_to_purge.append(metafile)
				for metafile in meta_to_purge:
					meta.pop(metafile)
				commit_metadata()
			elif decoded_msg['command'] == 'purgeftp':
				debug("Purging extraneous FTP Files ")
				bucket_key_list = []
				for key in bucket.list():
					bucket_key_list.append(key.name)
				host = ftputil.FTPHost(ftp_host,ftp_username,ftp_password)
				ftplist = host.listdir(host.curdir)
				for ftpfile in ftplist:
					if not ftpfile in bucket_key_list:
						debug("Deleting "+ftpfile+" from FTP")
						try:
							host.remove(ftpfile)
						except ftputil.ftp_error.PermanentError:
							pass
			elif decoded_msg['command'] == 'updateftp':
				debug("Uploading missing videos to FTP")
				bucket_key_list = []
				for key in bucket.list():
					bucket_key_list.append(key.name)
				host = ftputil.FTPHost(ftp_host,ftp_username,ftp_password)
				ftplist = host.listdir(host.curdir)
				for key in bucket_key_list:
					firstpart,extension = splitext(key)
					#Are we a video file?
					if extension in ['.mp4','.m4v','.mov','.mkv','.wmv','.avi'] and not "/" in key and key not in ftplist:
						upload_to_ftp(key)

			else:
				debug("Message not understood")
		else:
			debug("Message not understood")
	except json.decoder.JSONDecodeError:
		debug("Message not understood. Exception raised decoding.")
	if sigint_caught:
		commit_metadata()
		#Clean up PID file
		os.unlink(pidfile)
		sys.exit(0)
	debug("Idle")

def commit_metadata():
	debug("Writing metadata to disk")
	#Commit the metadata back to disk
	metakey = bucket.get_key(metafile)
	if metakey==None:
		metakey = bucket.new_key(metafile)
	metakey.set_contents_from_string(json.dumps(meta))

def load_metadata():
	global meta
	debug("Loading metadata from disk")
	#Load whatever metadata already exists
	metakey = bucket.get_key(metafile)
	if not metakey == None and metakey.exists():
		metastring = metakey.get_contents_as_string()
		meta = json.loads(metastring)

def generate_posterfiles(filename):
	global meta
	"""Expects short filename"""
	debug("Generating " + str(number_of_posterfiles) + " posterfiles for " + filename)
	#Figure out the intervals at which we need to take posterfiles
	durations=meta[filename]['duration'].split(":")
	totallength = int((int(durations[0])*3600)+(int(durations[1])*60)+float(durations[2]))
	debug("Copying file to tmp")
	#Dump the video in a tempdirectory to reduce latency
	bucket.get_key(filename).get_contents_to_filename(tempdir+filename)
	
	#Set the permissions through chmod
	os.chmod(tempdir+filename,stat.S_IRUSR)


	interval = float(totallength) / (number_of_posterfiles+1);
	intervals = []
	for x in range(number_of_posterfiles):
		intervals.append(interval*(x+1))
	for idx,val in enumerate(intervals):
		posterfile = filename + "_"+str(idx)+".jpg"
		thumbnail_posterfile = filename + "_" + str(idx) + ".th.jpg"
		debug("Generating "+posterfile)
		cmd = ["ffmpeg","-i",tempdir+filename,"-an","-ss",str(val),"-f","mjpeg","-qmin","0.8","-qmax","0.8","-t","1","-r","1","-y",tempdir+posterfile]
		subprocess.Popen(cmd,stderr=subprocess.PIPE).communicate()[1]
		debug("Generating "+thumbnail_posterfile)
		th_cmd = ["convert",tempdir+posterfile,"-resize",thumbnail_dimension+"^","-gravity","center","-extent",thumbnail_dimension,"-quality",str(thumbnail_quality),tempdir+thumbnail_posterfile]
		th_out = subprocess.Popen(th_cmd,stderr=subprocess.PIPE).communicate()[1]
		debug("Uploading "+posterfile)
#		while True:
#			try:
		bucket.new_key('posterfiles/'+posterfile).set_contents_from_filename(tempdir+posterfile)
		bucket.new_key('posterfiles/'+thumbnail_posterfile).set_contents_from_filename(tempdir+thumbnail_posterfile)
#			except socket.error: continue
#			break
	os.remove(tempdir+filename)

ftp_upload_filesize=0
ftp_upload_progress=0

def upload_to_ftp(short_filename):
	"""Expects short filename"""
	global ftp_upload_progress
	global ftp_upload_filesize
	debug("Uploading "+short_filename+" to FTP")
	debug("Caching file from S3")
	filekey = bucket.get_key(short_filename)
	ftp_upload_filesize = filekey.size
	ftp_upload_progress=0
	filekey.get_contents_to_filename(tempdir+short_filename)
	debug("Uploading to FTP")
	host = ftputil.FTPHost(ftp_host,ftp_username,ftp_password)
	host.upload(tempdir+short_filename,short_filename,mode='b',callback=ftpcallback)
	host.close()

def ftpcallback(chunk):
	global ftp_upload_progress
	global ftp_upload_filesize
	ftp_upload_progress = ftp_upload_progress+len(chunk)
	percentage = (float(ftp_upload_progress) / float(ftp_upload_filesize))*100
	print("%.2f" %round(percentage,2))+'%'


def get_metadata(short_filename):
	global meta
	"""Expects short filename"""
	debug("Downloading  " +short_filename+" to temp directory")
	filekey = bucket.get_key(short_filename)
	filename = tempdir+short_filename
	filekey.get_contents_to_filename(filename)
	debug("Extracting metadata from " +short_filename)
	#Grab file info from ffmpeg
	metadata_str = subprocess.Popen(['ffmpeg','-i',filename],stderr=subprocess.PIPE).communicate()[1]
	metadata_parts = re.findall(r'[^,\|\n]+',metadata_str.replace(': ','|'))
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
