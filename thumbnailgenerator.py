import shutil
import math
from ftplib import FTP
from os.path import splitext
import subprocess
import os

number_of_posterfiles=8
thumbnail_dimension='160x90'
thumbnail_quality=75
directory='/mnt/s3fs/'
posterfiledir='/mnt/s3fs/posterfiles/'
tempdir = '/tmp/'
files=os.listdir(directory)

for file in files:

	print "On S3: "+file
	filename,extension = splitext(file)
	if extension in ['.mp4','.m4v','.mov','.mkv'] and not os.path.exists(posterfiledir+file+'_0.jpg'):
		print "File is a video. Copying to "+tempdir+file
		shutil.copy2(directory+file,tempdir+file)
		print "Finding length:" 
		lengthout = subprocess.Popen(['ffmpeg','-i',tempdir+file],stderr=subprocess.PIPE).communicate()[1].split()
		print lengthout
		duration = lengthout[lengthout.index('Duration:')+1].rstrip(',')
		durations=duration.split(":")
		totallength = int((int(durations[0])*3600)+(int(durations[1])*60)+float(durations[2]))
		print "Total video length is: "+str(totallength)+"s"

		interval = float(totallength) / (number_of_posterfiles+1);
		print "Interval is: "+str(interval)
		intervals = []
		for x in range(number_of_posterfiles):
			intervals.append(interval*(x+1))
		for idx,val in enumerate(intervals):
			print "Posterfile "+str(idx)+" value "+str(val)
			posterfile = posterfiledir+file+"_"+str(idx)+".jpg"
			thumbnail_posterfile = posterfiledir+file+"_"+str(idx)+".th.jpg"
			print "Generating "+posterfile
			cmd = ["ffmpeg","-i",tempdir+file,"-an","-ss",str(val),"-f","mjpeg","-qmin","0.8","-qmax","0.8","-t","1","-r","1","-y",posterfile]
			print cmd
			outputs = subprocess.Popen(cmd,stderr=subprocess.PIPE).communicate()[1]
			print outputs
			th_cmd = ["convert",posterfile,"-resize",thumbnail_dimension+"^","-gravity","center","-extent",thumbnail_dimension,"-quality",str(thumbnail_quality),thumbnail_posterfile]
			print th_cmd
			th_outputs = subprocess.Popen(th_cmd,stderr=subprocess.PIPE).communicate()[1]
			print th_outputs
		os.remove(tempdir+file)

