import re
import shutil
import math
from ftplib import FTP
from os.path import splitext
import simplejson as json
import subprocess
import os

number_of_posterfiles=8
thumbnail_dimension='160x90'
thumbnail_quality=75
directory='/mnt/s3fs/'
#directory='/tmp/test/'
posterfiledir='/mnt/s3fs/posterfiles/'
#posterfiledir='/tmp/test/posterfiles/'
tempdir = '/tmp/'
files=os.listdir(directory)

for file in files:

	print "On S3: "+file
	filename,extension = splitext(file)
	if extension in ['.mp4','.m4v','.mov','.mkv']:
		if not os.path.exists(posterfiledir+file+'_0.jpg') or not os.path.exists(posterfiledir+file+'.js'):
			print "File is a video. Copying to "+tempdir+file
			shutil.copy2(directory+file,tempdir+file)
	
			print "Finding length:" 
			lengthout = subprocess.Popen(['ffmpeg','-i',tempdir+file],stderr=subprocess.PIPE).communicate()[1].split()
			duration = lengthout[lengthout.index('Duration:')+1].rstrip(',')
			durations=duration.split(":")
			totallength = int((int(durations[0])*3600)+(int(durations[1])*60)+float(durations[2]))
			print "Total video length is: "+str(totallength)+"s"
			if not os.path.exists(posterfiledir+file+'.js'):
				print "Extracting metadata:"
				metadata_start_index = lengthout.index('Video:')
				inc=0;
				metadata={}
				prevval=''
				val=''
				metadata['duration']=durations
				while True:
					inc+=1
					prevval=val
					val = lengthout[metadata_start_index+inc]
					if val=='Metadata:':
						break
					else:
						if inc==1:
							metadata['v_codec']=val
						if 'fps' in val:
							metadata['fps']=prevval
						if 'kb/s' in val:
							metadata['vbr']=prevval
						if re.search('\d+x\d+',val):
							metadata['dimension']=val
				f = open(posterfiledir+file+'.js','w')
				json.dump(metadata,f)
				f.close()
	
			if not os.path.exists(posterfiledir+file+'_0.jpg'):
				print "Generating Posterfiles:"
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
					outputs = subprocess.Popen(cmd,stderr=subprocess.PIPE).communicate()[1]
					print outputs
					th_cmd = ["convert",posterfile,"-resize",thumbnail_dimension+"^","-gravity","center","-extent",thumbnail_dimension,"-quality",str(thumbnail_quality),thumbnail_posterfile]
					th_outputs = subprocess.Popen(th_cmd,stderr=subprocess.PIPE).communicate()[1]
					print th_outputs
			os.remove(tempdir+file)

