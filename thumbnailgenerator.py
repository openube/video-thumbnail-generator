from ftplib import FTP
from os.path import splitext
import subprocess
import os.path

ftp = FTP('ftp.streaming.thomsonreuters2.netdna-cdn.com')
ftp_username = 'streaming.thomsonreuters2'
ftp_password = 'Gadget55'
ftp.login(ftp_username,ftp_password)

files = ftp.nlst()

for file in files:
	print file
	filename,extension = splitext(file)
	if extension == '.mp4':
		print "File is a video"
		poster = filename+'_1.jpg'
		if poster not in files:
			print "Poster doesn't exist"
			if not os.path.exists(file):
				ftp.retrbinary('RETR '+file,open(file, 'wb').write)
			print "File download complete"
			lengthout = subprocess.Popen(["ffmpeg","-i",file],stderr=subprocess.PIPE).communicate()[1].split()
			duration = lengthout[lengthout.index('Duration:')+1].rstrip(',')
			durations=duration.split(":")
			totallength = (int(durations[0])*3600)+(int(durations[1])*60)+float(durations[2])
			print durations
			print totallength
		
ftp.close()

