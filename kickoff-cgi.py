#!/usr/bin/python

import pika
import cgi
import cgitb; cgitb.enable()
import os
from os.path import splitext

print "Content-type: application/json"
print "Access-Control-Allow-Origin: *"
qs = cgi.FieldStorage()

if "name" not in qs and "type" not in qs:
	print "Status: 404 Not Found"
	print
	print "{'404 Not Found'}"
else:
	connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
	channel = connection.channel()
	if "name" in qs:

		channel.basic_publish(exchange='',routing_key='thumbnailgenerator',body=qs['name'].value)
		print
		print "{'"+str(qs["name"].value)+"'}"
	elif "type" in qs:
		directory='/mnt/s3fs/'
		files=os.listdir(directory)
		allfiles = False
		if qs['type'].value == 'all':
			allfiles = True
		for filename in files:
			#Extract the filename and extension
			firstpart,extension = splitext(filename)
			#Are we a video file?
			print
			print "{"
			print "'"+filename+"',"
			if extension in ['.mp4','.m4v','.mov','.mkv','.wmv']:
				if allfiles or (not allfiles and not os.path.exists(directory+'posterfiles/'+filename+'_0.jpg')):
					channel.basic_publish(exchange='',routing_key='thumbnailgenerator',body=filename)
			print "}"
