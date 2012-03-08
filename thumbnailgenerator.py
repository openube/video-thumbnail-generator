import re
import shutil
import math
from os.path import splitext
import datetime
import simplejson as json
import subprocess
import os

number_of_posterfiles=8
thumbnail_dimension='160x90'
thumbnail_quality=75
directory='/mnt/s3fs/'
directory='/tmp/test/'
posterfiledir='/mnt/s3fs/posterfiles/'
posterfiledir='/tmp/test/posterfiles/'

metafile = posterfiledir+'meta.js'
tempdir = '/tmp/'
files=os.listdir(directory)

meta = {}
for file in files:

	print "On S3: "+file
	filename,extension = splitext(file)
	if extension in ['.mp4','.m4v','.mov','.mkv']:
		if not os.path.exists(posterfiledir+file+'_0.jpg') or not os.path.exists(metafile):
			if os.path.exists(metafile):
				f = open(metafile,'r')
				meta = json.load(f)
				f.close()

			print "File is a video. Copying to "+tempdir+file
			shutil.copy2(directory+file,tempdir+file)
			
			print "Finding length:" 
			
			metadata_str = subprocess.Popen(['ffmpeg','-i',tempdir+file],stderr=subprocess.PIPE).communicate()[1]
			metadata_parts = re.findall(r'[^,\|\n]+',metadata_str.replace(': ','|'))

			print "Extracting metadata:"
			metadata_start_index = 0
			inc=0;
			metadata={}
			val=''
			seen_v_stream=0
			seen_a_stream=0
			metadata['since']=datetime.datetime.fromtimestamp(os.path.getmtime(directory+file)).isoformat()
			for ii in range(len(metadata_parts)):
				val = metadata_parts[ii].strip()
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
						


			meta[file]=metadata	

			durations=metadata['duration'].split(":")
			totallength = int((int(durations[0])*3600)+(int(durations[1])*60)+float(durations[2]))
			print "Total video length is: "+str(totallength)+"s"
			
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
f = open(metafile,'w')
json.dump(meta,f)
f.close()
