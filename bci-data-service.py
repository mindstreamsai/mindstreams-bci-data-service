from cortex import Cortex
import boto3
import json
import datetime
import uuid
import time
import sys
# import pdb; pdb.set_trace()

with open('./cortex_creds') as creds_file:
	user = json.load(creds_file)

kinesis_client = boto3.client('kinesis', 'us-east-2')
kinesis_stream_name = "mindstreams-ingestion"
kinesis_shard_count = 1

# function for sending data to Kinesis at the absolute maximum throughput
def send_kinesis(client, stream_name, shard_count, payload):
	kinesisRecords = [] # empty list to store data
	partition_key = payload['sessionId']
	data = json.dumps(payload)
	encodedValues = bytes(data, 'utf-8') # encode the string to bytes
	kinesisRecord = {
		"Data": encodedValues, # binary data block which contains the JSON as bytes
		"PartitionKey": partition_key  # We use sessionId to ensure all MindStream events for this session stay together and in order
	}
	kinesisRecords.append(kinesisRecord)
	response = client.put_records(Records=kinesisRecords, StreamName=stream_name)		


# See docs: https://emotiv.gitbook.io/cortex-api/data-subscription/data-sample-object

field_name_mapping = {
	"eng.isActive": "engagementEnabled",
	"eng": "engagement",
	"exc.isActive": "excitementEnabled",
	"exc": "excitement",
	"lex": "excitementLast1Min",
	"str.isActive": "stressEnabled", 
	"str": "stress", 
	"rel.isActive": "relaxationEnabled", 
	"rel": "relaxation", 
	"int.isActive": "interestEnabled", 
	"int": "interest", 
	"foc.isActive": "focusEnabled", 
	"foc": "focus",
	"COUNTER": "n",
	"INTERPOLATED": "wasInterpolated",
	"AF3": "AF3",
	"T7": "T7",
	"Pz": "PZ",
	"T8": "T8",
	"AF4": "AF4",
	"RAW_CQ": "rawQC",
	"MARKER_HARDWARE": "hasMarkers",
	"MARKERS": "markers",
	"COUNTER_MEMS": "n",
	"INTERPOLATED_MEMS": "wasInterpolated",
	"Q0": "q0",
	"Q1": "q1",
	"Q2": "q2",
	"Q3": "q3",
	"ACCX": "ax",
	"ACCY": "ay",
	"ACCZ": "az",
	"MAGX": "mx",
	"MAGY": "my",
	"MAGZ": "mz",
	"GYROX": "gx",
	"GYROY": "gy",
	"GYROZ": "gz",
	"eyeAct": "eyes",
	"uAct": "upperFace",
	"uPow": "upperFacePower",
	"lAct": "lowerFace",
	"lPow": "lowerFacePower"
}

class Subcribe():

	def __init__(self):
		self.headers = {}
		self.objects = None
		self.count = 0
		self.tick = time.time()
		self.maxEventsInBuffer = 100
		self.stream_types = {}
		self.is_first_for_type = {
			"met": True,
			"eeg": True,
			"fac": True,
			"mot": True,
		}
		self.c = Cortex(user, debug_mode=False)
		self.c.do_prepare_steps()
		self.time_slice = 0
		self.time_data = {
			'engagement': {},
			'excitement': {},
			'stress': {},
			'relaxation': {},
			'interest': {},
			'focus': {}
		}
		self.time_slice_averages = {}


	def add_cognitive_data_to_time_aggregation(self, event):
		right_now = time.time()
		current_time = str(right_now * 1000)
		td = self.time_data
		for key in td.keys():
			new_value = event.get(key, None)
			if new_value:
				td[key][current_time] = new_value

		self.time_slice_averages = {}
		for key in td.keys():
			ds = td[key]
			keepers = {}
			for t in ds.keys():
				last_time = float(t) / 1000
				if (right_now - last_time) <= 30:
					keepers[t] = ds[t]
			td[key] = keepers
			vals = keepers.values()
			self.time_slice_averages[key] = sum(vals) / len(vals)
		event["last_30s"] = self.time_slice_averages


	def prepare_metadata(self):
		metadata = self.c.session_context["result"]	
		del metadata["recordIds"]
		del metadata["recording"]
		del metadata["id"]
		headset = metadata["headset"]
		del headset["motionSensors"]
		del headset["sensors"]
		del metadata["license"]
		del metadata["performanceMetrics"]
		del metadata["stopped"]
		del metadata["streams"]
		if self.c.user_id is None:
			self.c.user_id = metadata["owner"]
		self.c.device_id = headset["id"]
		self.c.component_id = headset["virtualHeadsetId"] or "00000000-0000-0000-0000-000000000000"
		del metadata["owner"]
		del headset["id"]
		del headset["virtualHeadsetId"]


	def create_records_structure(self):
		o = {
			"id": str(uuid.uuid4()),
			"sessionId": self.c.session_id,
			"userId": self.c.user_id,
			"deviceId": self.c.device_id.lower(),
			"componentId": self.c.component_id,
			"ts": datetime.datetime.now().isoformat(),
			"metadata": {
			},
			"eeg": [],
			"cognitive": [],
			"facial": [],
			"motion": [],
		}
		metadata = self.c.session_context["result"]
		o["metadata"] = metadata
		return o

	def publish_records(self):
		my_keys = ['engagement', 'excitement', 'stress', 'relaxation', 'interest', 'focus']
		if len(self.objects['cognitive']) > 0:
			event_data = self.objects
			rows = self.objects['cognitive']
			for x in range(len(rows)):
				row = rows[x]
				vals = []
				for y in range(len(my_keys)):
					k = my_keys[y]
					vals.append(str(row[k]))
				print(','.join(vals))
			# print(json.dumps(event_data, indent=4))
			send_kinesis(kinesis_client, kinesis_stream_name, kinesis_shard_count, event_data) # send it!

		self.objects = self.create_records_structure()
		self.count = 0
		self.tick = time.time()


	def has_all_keys(self, record, keys):
		answer = True
		for k in keys:
			answer = (answer and (k in record)) or False
		return answer


	def add_event(self, event, stream_name, sid):
		if stream_name == 'met':
			self.objects['cognitive'].append(event)			
		elif stream_name == 'eeg':
			self.objects['eeg'].append(event)
		elif stream_name == 'fac':
			self.objects['facial'].append(event)
		elif stream_name == 'mot':
			self.objects['motion'].append(event)


	def map_met(self, event):
		if not event['engagementEnabled']: 
			del event['engagement']
		if not event['excitementEnabled']: 
			del event['excitement'] 
			del event['excitementLast1Min']
		if not event['stressEnabled']: 
			del event['stress']
		if not event['relaxationEnabled']: 
			del event['relaxation']
		if not event['interestEnabled']: 
			del event['interest']
		if not event['focusEnabled']: 
			del event['focus']
		del event['engagementEnabled']
		del event['excitementEnabled']
		del event['stressEnabled']
		del event['relaxationEnabled']
		del event['interestEnabled']
		del event['focusEnabled']
		self.add_cognitive_data_to_time_aggregation(event)
		

	def map_eeg(self, event):
		if not event['hasMarkers']: 
			del event['markers']
			del event['hasMarkers']
		else:
			event['hasMarkers'] = True

		if not event['wasInterpolated']: 
			del event['wasInterpolated']
		else:
			event['wasInterpolated'] = True


	def map_mot(self, e):
		if not e['wasInterpolated']: 
			del e['wasInterpolated']
		else:
			e['wasInterpolated'] = True


	def is_facial_data_redundant(self, event):
		recent_facial_records = self.objects['facial']
		if len(recent_facial_records) <= 0:
			return False
		last_facial_record = recent_facial_records[len(recent_facial_records)-1]
		compare_fields = ["eyes", "upperFace", "upperFacePower", "lowerFace", "lowerFacePower"]
		is_identical = True
		for field in compare_fields:
			if not (event[field] == last_facial_record[field]):
				is_identical = False
		return is_identical


	def is_data_sample_relevant(self, event):
		n = event['n']
		# Get every 10th record using mathematics modulo
		return (n % 5) == 0


	def map_event(self, record, stream_name):
		event = {}
		sid = record['sid']
		time_value = record['time']
		headers = self.headers[stream_name]
		if stream_name in record:
			metrics = record[stream_name]
			for i in range(len(headers)):
				key = headers[i]
				mapped_key = field_name_mapping[key] or key
				val = metrics[i]
				event[mapped_key] = val 
			event['ts'] = time_value

			if stream_name == 'met':
				self.map_met(event)				
			elif stream_name == 'eeg':
				if self.is_data_sample_relevant(event):
					self.map_eeg(event)
				else:
					event = None
			elif stream_name == 'mot':
				if self.is_data_sample_relevant(event):
					self.map_mot(event)
				else:
					event = None
			elif stream_name == 'fac':
				if self.is_facial_data_redundant(event):
					event = None
		return event, sid


	def get_record_type(self, record):
			record_type = None
			if 'eeg' in record: record_type = 'eeg'
			elif 'mot' in record: record_type = 'mot'
			elif 'fac' in record: record_type = 'fac'
			elif 'met' in record: record_type = 'met'	
			return record_type


	def process_headers(self, record):
			header_data = record and record['result'] and record['result']['success']
			for i in range(len(header_data)):
				header = header_data[i]
				cols = header['cols']
				stream_name = header['streamName']
				self.headers[stream_name] = cols


	def on_data_received(self, data):
		try:
			record = json.loads(data)
		except e as Error:
			print(e)
		sid = None
		stream_name = None

		# if the data record has a 'sid' field at the top level, then it is a summary
		record_type = self.get_record_type(record)
		if record_type:
			self.count = self.count + 1
			event, sid = self.map_event(record, record_type)
			if event:
				self.add_event(event, record_type, sid)
				current_time = time.time()
				# if (self.count >= self.maxEventsInBuffer) or ((current_time - self.tick) >= 1):
				if (current_time - self.tick) >= 1:
					self.publish_records()
		else:
			# Otherwise this is a special header record with all of the columns defined
			self.process_headers(record)

			

	def start(self, user_id, streams):
		if self.c.ready_to_use:
			self.c.user_id = user_id
			self.prepare_metadata()
			self.c.add_callback(self.on_data_received)		
			self.count = 0
			self.objects = self.create_records_structure()
			print(','.join(['engagement', 'excitement', 'stress', 'relaxation', 'interest', 'focus']))
			self.c.sub_request(streams)

def main():
	user_id = sys.argv[1]
	print(f"BCI Data Service for {user_id}")

	# streams = ['met', 'fac', 'mot', 'eeg']
	streams = ['met', 'fac']

	s = Subcribe()
	s.start(user_id, streams)

if __name__ == "__main__":
    main()
