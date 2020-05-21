import os
import boto3
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
from multiprocessing.dummy import Pool
from html.parser import HTMLParser

class MyHTMLParser(HTMLParser):

	def __init__(self):
		HTMLParser.__init__(self)
		self.valid = False
		self.year = ''
		self.type = ''
		self.data = []

	def handle_starttag(self, tag, attr):
		if tag.lower() == 'div' and self.valid == False:
			cur_class = ''
			cur_id = ''
			for item in attr:
				if item[0].lower() == 'class':
					cur_class = item[1].lower().strip()
				if item[0].lower() == 'id':
					cur_id = item[1].strip()
			
			if 'tab-pane fade' in cur_class and len(cur_id.split('_')) == 2:
				self.valid = True
				self.year = cur_id[0:4]
				self.type = cur_id[5:]

			
		if tag.lower() == 'a' and self.valid == True:
			for item in attr:
				if item[0].lower() == 'href' and '.' in item[1]:
					self.data.append({
						'path': item[1].strip(),
						'year': self.year,
						'type': self.type
					})

	def handle_endtag(self, tag):
		if tag.lower() == 'div' and self.valid == True:
			self.valid = False

def data_to_s3(data):

	# throws error occured if there was a problem accessing data
	# otherwise downloads and uploads to s3

	source_dataset_base = 'https://www.huduser.gov/'
	
	if data['path'][0] != '/':
		source_dataset_base = source_dataset_base + '/portal/datasets/'

	try:
		response = urlopen(source_dataset_base + data['path'])

	except HTTPError as e:
		raise Exception('HTTPError: ', e.code, data['path'])

	except URLError as e:
		raise Exception('URLError: ', e.reason, data['path'])

	else:
		filename = data['path'].rsplit('/', 1)[1]
		file_location = '/tmp/' + filename

		with open(file_location, 'wb') as f:
			f.write(response.read())

		# variables/resources used to upload to s3
		data_set_name = os.environ['DATA_SET_NAME']
		s3_bucket = os.environ['S3_BUCKET']
		new_s3_key = '{}/dataset/{}/{}/{}'.format(
			data_set_name, data['year'], data['type'], filename)
		s3 = boto3.client('s3')

		s3.upload_file(file_location, s3_bucket, new_s3_key)

		print('Uploaded: ' + filename)

		# deletes to preserve limited space in aws lamdba
		os.remove(file_location)

		# dicts to be used to add assets to the dataset revision
		return {'Bucket': s3_bucket, 'Key': new_s3_key}

def source_dataset():

	source_html_url = 'https://www.huduser.gov/portal/datasets/pis.html'


	try:
		response = urlopen(source_html_url)

	except HTTPError as e:
		raise Exception('HTTPError: ', e.code, source_html_url)

	except URLError as e:
		raise Exception('URLError: ', e.reason, source_html_url)

	else:
		html = response.read().decode()
		parser = MyHTMLParser()
		parser.feed(html)

	# print(parser.data)

	# multithreading speed up accessing data, making lambda run quicker
	with (Pool(50)) as p:
		asset_list = p.map(data_to_s3, parser.data)

	# asset_list is returned to be used in lamdba_handler function
	return asset_list