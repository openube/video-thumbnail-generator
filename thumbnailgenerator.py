#!/usr/bin/python

import logging
import socket
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
import shutil
from loggly import logglyHandler,JsonFormatter

sigint_caught=False
number_of_posterfiles=10
thumbnail_dimension='160x90'
thumbnail_quality=75
pidfile = "/tmp/thumbnailgenerator.pid"

ftp_host = ''
ftp_username = ''
ftp_password = ''

metafile = 'posterfiles/meta.js'
tempdir = '/tmp/thumbnailgenerator/'
bucket = {}
s3_bucket_name = ''
logglyurl=''
meta = {}

try:
    from local_settings import *
except ImportError:
    pass


def signal_handler(signal, frame):
	global sigint_caught
	if sigint_caught:
		logging.warn('SIGINT caught, exiting')
		commit_metadata()

		#Clean up PID file
		os.unlink(pidfile)
	
		sys.exit(0)
	else:
		logging.warn('SIGINT caught, quitting after next job')
		sigint_caught=True

def main():
	global meta
	global bucket
	logging.basicConfig(filename='thumbnailgenerator.log', level=logging.DEBUG,
			format='%(asctime)s %(module)-12s %(funcName)-2s %(levelname)-8s %(message)s',
			datefmt='%m-%d %H:%M',
			filemode='a')

	console = logging.StreamHandler()
	console.setLevel(logging.DEBUG)
	formatter = logging.Formatter('%(asctime)s - %(module)-12s %(funcName)-2s: %(levelname)-8s %(message)s')
	console.setFormatter(formatter)

	http_handler = logglyHandler(logglyurl)
	http_handler.setLevel(logging.INFO)
	loggly_formatter = JsonFormatter('%(levelname)s %(pathname)s %(module)s %(funcName)s %(asctime)s %(message)s')
	http_handler.setFormatter(loggly_formatter)
	logging.getLogger('').addHandler(http_handler)

	logging.getLogger('').addHandler(console)
	logging.getLogger('boto').setLevel(logging.ERROR)
	logging.info("Starting up")

	#Exit on SIGINT
	signal.signal(signal.SIGINT, signal_handler)

	#Setup PID file to see if we're already running
	pid = str(os.getpid())
	if os.path.isfile(pidfile):
		logging.error("%s already exists, exiting"% pidfile)
		sys.exit()
	else:
		file(pidfile, 'w').write(pid)

	logging.debug("Checking if tempdir %s exists"%tempdir)
	if os.path.exists(tempdir):
		logging.debug("It does, removing it")
		shutil.rmtree(tempdir)
	logging.info("Creating tempdir: %s"%tempdir)
	os.makedirs(tempdir)


	bucket = boto.connect_s3().get_bucket(s3_bucket_name)

	logging.debug("Opening rMQ connection")
	connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
	channel = connection.channel()
	channel.queue_declare(queue='thumbnailgenerator')
	
	load_metadata()

	#Start listening for messages:
	channel.basic_consume(process_msg,queue='thumbnailgenerator',no_ack=True)
	logging.debug("Listening for messages")
	channel.start_consuming()


def process_msg(ch,method,properties,body):
	global meta
	logging.info("Received msg: %s"%body)
	try:
		decoded_msg = json.loads(body)

		if 'command' in decoded_msg:
			if decoded_msg['command'] == 'add':
				filename = decoded_msg['filename']
				if not bucket.get_key(filename) == None and bucket.get_key(filename).exists():# and not filename.startswith("._"):
					logging.debug("Downloading %s to temp directory"%filename)
					filekey = bucket.get_key(filename)
					fullpath = tempdir+filename
					filekey.get_contents_to_filename(fullpath)
					#Set the permissions through chmod
					os.chmod(fullpath,stat.S_IRUSR)

					meta[filename] = get_metadata(filename)
					if meta[filename] == None:
						logging.warn("Metadata is none. Removing and skipping")
						meta.pop(filename)
					else:
						generate_posterfiles(filename)
						upload_to_ftp(filename)
					logging.debug("Removing locally cached %s"%fullpath)
					
					os.unlink(fullpath)
					commit_metadata()
				else:
					logging.error(filename + " doesn't seem to exist")
			elif decoded_msg['command'] == 'purgemetadata':
				logging.info("Purging extraneous metadata")
				meta_to_purge = []
				bucket_key_list = []
				for key in bucket.list():
					bucket_key_list.append(key.name)
				logging.debug("Got all keys. Size = %s"%len(bucket_key_list))
				for metafile in meta:
					logging.debug("seeing if %s is absent"%metafile)
					if not metafile in bucket_key_list:
						logging.info("File absent. Purging metadata for %s"%metafile)
						meta_to_purge.append(metafile)
				for metafile in meta_to_purge:
					meta.pop(metafile)
				commit_metadata()
			elif decoded_msg['command'] == 'purgeftp':
				logging.info("Purging extraneous FTP Files ")
				bucket_key_list = []
				for key in bucket.list():
					bucket_key_list.append(key.name)
				host = ftputil.FTPHost(ftp_host,ftp_username,ftp_password)
				ftplist = host.listdir(host.curdir)
				for ftpfile in ftplist:
					if not ftpfile in bucket_key_list:
						logging.info("Deleting %s from FTP"%ftpfile)
						try:
							host.remove(ftpfile)
						except ftputil.ftp_error.PermanentError:
							pass
			elif decoded_msg['command'] == 'updateftp':
				logging.info("Uploading missing videos to FTP")
				bucket_key_list = []
				for key in bucket.list():
					bucket_key_list.append(key.name)
				host = ftputil.FTPHost(ftp_host,ftp_username,ftp_password)
				ftplist = host.listdir(host.curdir)
				for key in bucket_key_list:
					firstpart,extension = splitext(key)
					#Are we a video file?
					if extension in ['.mp4','.m4v','.mov','.mkv','.wmv','.avi'] and not "/" in key and key not in ftplist:
						logging.debug("Downloading %s to temp directory"%key)
						filekey = bucket.get_key(key)
						fullpath = tempdir+key
						filekey.get_contents_to_filename(fullpath)
						#Set the permissions through chmod
						os.chmod(fullpath,stat.S_IRUSR)
						upload_to_ftp(key)
						os.unlink(fullpath)

			else:
				logging.error("Message not understood")
		else:
			logging.error("Message not understood")
	except json.decoder.JSONDecodeError:
		logging.error("Message not understood. Exception raised decoding.")
#	except:
#		logging.error("Unexpected error: %s"%sys.exc_info()[0])
	if sigint_caught:
		commit_metadata()
		#Clean up PID file
		os.unlink(pidfile)
		sys.exit(0)
	logging.info("Idle")

def commit_metadata():
	logging.info("Writing metadata to disk")
	#Commit the metadata back to disk
	metakey = bucket.get_key(metafile)
	if metakey==None:
		metakey = bucket.new_key(metafile)
	metakey.set_contents_from_string(json.dumps(meta))

def load_metadata():
	global meta
	logging.info("Loading metadata from disk")
	#Load whatever metadata already exists
	metakey = bucket.get_key(metafile)
	if not metakey == None and metakey.exists():
		metastring = metakey.get_contents_as_string()
		meta = json.loads(metastring)

def generate_posterfiles(filename):
	global meta
	"""Expects short filename"""
	logging.info("Generating %s posterfiles for %s"%(number_of_posterfiles, filename))
	#Figure out the intervals at which we need to take posterfiles
	durations=meta[filename]['duration'].split(":")
	totallength = int((int(durations[0])*3600)+(int(durations[1])*60)+float(durations[2]))

	interval = float(totallength) / (number_of_posterfiles+1);
	intervals = []
	for x in range(number_of_posterfiles):
		intervals.append(interval*(x+1))
	for idx,val in enumerate(intervals):
		#Make interval an int
		val = int(val)
		posterfile = filename + "_"+str(idx)+".jpg"
		thumbnail_posterfile = filename + "_" + str(idx) + ".th.jpg"
		logging.info("Generating %s@%s"%(posterfile,val))
		output_success=False
		output_failure=False
		#Try and generate a posterfile at the given time. If failure, add a second until wins.
		attempts=0
		while not output_success and not output_failure:
			cmd = ["ffmpeg","-ss",str(val),"-i",tempdir+filename,"-an","-f","mjpeg","-qmin","0.8","-qmax","0.8","-t","1","-r","1","-y",tempdir+posterfile]
			subprocess.Popen(cmd,stderr=subprocess.PIPE).communicate()[1]
			if os.path.getsize(tempdir+posterfile)==0 and attempts < 30:
				val=val+1
				attempts=attempts+1
				logging.warn("Posterfile was zero sized. Increasing time to %s. Attempt number %s"%(val,attempts))
			elif attempts < 30:
				output_success=True
			else: 
				logging.error("Failed to generate %s"%posterfile)
				output_failure=True
		if output_success:
			logging.info("Generating %s"%thumbnail_posterfile)
			#Use Imagemagick to convert posterfile to thumbnail
			th_cmd = ["convert",tempdir+posterfile,"-resize",thumbnail_dimension+"^","-gravity","center","-extent",thumbnail_dimension,"-quality",str(thumbnail_quality),tempdir+thumbnail_posterfile]
			th_out = subprocess.Popen(th_cmd,stderr=subprocess.PIPE).communicate()[1]
			uploaded = False
			while not uploaded:
				try:
					logging.info("Uploading %s"%posterfile)
					bucket.new_key('posterfiles/'+posterfile).set_contents_from_filename(tempdir+posterfile)
					uploaded=True
				except socket.error:
					logging.error("Socket Error uploading file. Retrying")
					pass
			uploaded = False
			while not uploaded:
				try:
					logging.info("Uploading %s"%thumbnail_posterfile)
					bucket.new_key('posterfiles/'+thumbnail_posterfile).set_contents_from_filename(tempdir+thumbnail_posterfile)
					uploaded = True
				except socket.error:
					logging.error("Socket Error uploading file. Retrying")
					pass
			os.remove(tempdir+thumbnail_posterfile)
			os.remove(tempdir+posterfile)

ftp_upload_filesize=0
ftp_upload_progress=0

def upload_to_ftp(short_filename,overwrite=False):
	"""Expects short filename"""
	global ftp_upload_progress
	global ftp_upload_filesize
	logging.info("Uploading %s to FTP"%short_filename)
	logging.debug("Overwrite forced: %s"%overwrite)
	logging.debug("Reading filesize from S3")
	filekey = bucket.get_key(short_filename)
	ftp_upload_filesize = filekey.size
	ftp_upload_progress=0
	logging.debug("Uploading to FTP")
	host = ftputil.FTPHost(ftp_host,ftp_username,ftp_password)
	if host.path.exists(short_filename):
		logging.debug("File exists on FTP")
	if not overwrite and host.path.exists(short_filename):
		size_on_disk = os.lstat(tempdir+short_filename).st_size
		size_on_server = host.lstat(short_filename).st_size
		logging.info("Size on disk: %s Size on server: %s"%(size_on_disk,size_on_server))
		if size_on_disk != size_on_server:
			logging.info("Different. Re-uploading")
			host.upload(tempdir+short_filename,short_filename,mode='b',callback=ftpcallback)
	else:
		logging.debug("Overwrite flag or lack of FTP presence forces upload")
		host.upload(tempdir+short_filename,short_filename,mode='b',callback=ftpcallback)
	host.close()

def ftpcallback(chunk):
	global ftp_upload_progress
	global ftp_upload_filesize
	ftp_upload_progress = ftp_upload_progress+len(chunk)
	percentage = (float(ftp_upload_progress) / float(ftp_upload_filesize))*100
	logging.debug("%.2f" %round(percentage,2)+'%')


def get_metadata(short_filename):
	global meta
	"""Expects short filename"""
	logging.info("Extracting metadata from %s"%short_filename)
	#Grab file info from ffmpeg
	filename = tempdir+short_filename

	metadata_str = subprocess.Popen(['avconv','-i',filename],stderr=subprocess.PIPE).communicate()[1]
	logging.debug("metadata received %s"%metadata_str)
	if metadata_str.strip().endswith('Invalid data found when processing input'):
		logging.info("Invalid video. Returning None")	
		return None
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

#Kick into the main proc
main()
