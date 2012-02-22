import math
from ftplib import FTP
from os.path import splitext
import subprocess
import os.path

number_of_posterfiles=4

ftp = FTP('ftp.streaming.thomsonreuters2.netdna-cdn.com')
ftp_username = 'streaming.thomsonreuters2'
ftp_password = 'Gadget55'
try:
	ftp.login(ftp_username,ftp_password)
	files = ftp.nlst()
except:
	print "Unexpected error connecting to FTP:" +sys.exc_info()[0]
	ftp.close()
	raise

for file in files:

	print "On the FTP server: "+file
	filename,extension = splitext(file)
	if extension == '.mp4':
		print "File is an mp4 video"
		poster = file+'_0.jpg'
		if poster not in files:
			print "Poster doesn't exist"
			if not os.path.exists(file):
				print "Downloading file"
				ftp.retrbinary('RETR '+file,open(file, 'wb').write)
				print "File download complete"
			lengthout = subprocess.Popen(["ffmpeg","-i",file],stderr=subprocess.PIPE).communicate()[1].split()
			duration = lengthout[lengthout.index('Duration:')+1].rstrip(',')
			durations=duration.split(":")
			totallength = int((int(durations[0])*3600)+(int(durations[1])*60)+float(durations[2]))
			print "Total video length is: "+str(totallength)+"s"

			interval = totallength / (number_of_posterfiles+1);
			intervals = []
			for x in range(number_of_posterfiles):
				intervals.append(interval*(x+1))
			for idx,val in enumerate(intervals):
				posterfile = file+"_"+str(idx)+".jpg"
				print "Generating "+posterfile
				cmd = ["ffmpeg","-i",file,"-an","-ss",str(val),"-f","mjpeg","-t","1","-r","1","-y",posterfile]
				print cmd
				outputs = subprocess.Popen(cmd,stderr=subprocess.PIPE).communicate()[1]
				print outputs
				ftp.storbinary('STOR '+posterfile,open(posterfile,'rb'))

		
ftp.close()

