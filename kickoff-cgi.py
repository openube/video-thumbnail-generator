#!/usr/bin/python

import pika
import cgi
import cgitb; cgitb.enable()

print "Content-type: application/json"
qs = cgi.FieldStorage()

if "name" not in qs:
	print "Status: 404 Not Found"
	print
	print "{'404 Not Found'}"
else:
	connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
	channel = connection.channel()
	channel.basic_publish(exchange='',routing_key='thumbnailgenerator',body=qs['name'].value)
	print
	print "{'"+str(qs["name"].value)+"'}"

