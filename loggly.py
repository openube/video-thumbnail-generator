import logging, logging.handlers
import simplejson as json,re
from datetime import datetime


class logglyHandler(logging.Handler):
	
	def __init__(self, url):
		logging.Handler.__init__(self)
		self.url = url
		
	def mapLogRecord(self, record):
		return record.__dict__
   
	def emit(self, record):
		try:
			import httplib2
			insert_http = httplib2.Http(timeout=10)

			resp, content = insert_http.request(self.url, "POST", body=self.format(record), headers={'content-type':'application/json'})
		except (KeyboardInterrupt, SystemExit):
			raise
		except:
			self.handleError(record)



class JsonFormatter(logging.Formatter):
	"""A custom formatter to format logging records as json objects"""

	def parse(self):
		standard_formatters = re.compile(r'\((.*?)\)', re.IGNORECASE)
		return standard_formatters.findall(self._fmt)

	def format(self, record):
		"""Formats a log record and serializes to json"""
		mappings = {
			'asctime': create_timestamp,
			'message': lambda r: r.msg,
		}

		formatters = self.parse()

		log_record = {}
		for formatter in formatters:
			try:
				log_record[formatter] = mappings[formatter](record)
			except KeyError:
				log_record[formatter] = record.__dict__[formatter]
		log_record['severity']=log_record['levelname']
		del log_record['levelname']
		if log_record['severity'] == 'WARNING':
			log_record['severity'] == 'WARN'
		return json.dumps(log_record)

def create_timestamp(record):
	"""Creates a human readable timestamp for a log records created date"""
	timestamp = datetime.fromtimestamp(record.created)
	return timestamp.strftime("%y-%m-%d %H:%M:%S,%f")
