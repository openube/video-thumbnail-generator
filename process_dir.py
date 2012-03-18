#!/usr/bin/python


import pika
import os
from os.path import splitext
directory='/mnt/s3fs/'
files=os.listdir(directory)

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
total_files = len(files)
for file in files:
	#Extract the filename and extension
	filename,extension = splitext(file)
	#Are we a video file?
	if extension in ['.mp4','.m4v','.mov','.mkv','.wmv']:
		channel.basic_publish(exchange='',routing_key='thumbnailgenerator',body=file)

connection.close()
