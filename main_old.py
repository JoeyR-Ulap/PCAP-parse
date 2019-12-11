import os
import json
import csv
import time

from google.cloud import bigquery
from google.cloud import storage
import pcapkit
from flask import Flask
import psutil


input_bucket_name = "pcap-files"
output_bucket_name = "pcap-output"
directory = "/Users/andrewhuh/Ulap/PCAP-parse/"

# bq_client = bigquery.Client(project="kubeflow-test-260816").from_service_account_json(directory + "kubeflow-test-260816-ccc94a55bcba.json")
storage_client = storage.Client(project="kubeflow-test-260816").from_service_account_json(directory + "kubeflow-test-260816-ccc94a55bcba.json")

# app = Flask(__name__)

# @app.route('/')
def PCAP_analysis():

	for filename in os.listdir(directory):

		if filename.endswith("pcap"):
			print("Filename: ", filename)

			filename_list = filename.split(".")
			filename_no_ext = filename_list[0]
			print("Extracting PCAP file")

			input_bucket = storage_client.get_bucket(input_bucket_name)
			blob = bucket.blob(filename)
			blob.download_to_filename("tmp/pcap/{}".format(filename))
			#Extracting PCAP data from PCAP files
			start = time.time()

			ljson = pcapkit.extract(fin="tmp/pcap/{}".format(filename), fout='tmp/json/{}.json'.format(filename_no_ext), format='json', store=False, engine="dpkt")

			end = time.time()
			total = end - start
			print("It took " + str(total) + " seconds")
			json_data = json.loads(open('out/{}.json'.format(filename_no_ext)).read())
			
			print("Deleting Header")
			# del json_data['Global Header']

			
			with open('revised/{}.json'.format(filename_no_ext), 'w') as f:
				#dumping json data without a header into a new file.
				json.dump(json_data, f)

			#transforming json into newline delimited json for bigquery
			command = os.popen("cat tmp/json/{}.json | jq -c '.[]' > tmp/nd_json/{}.json".format(filename_no_ext, filename_no_ext))
			

			# print("Uploading to GCS")
			output_bucket = storage_client.get_bucket(output_bucket_name)
			blob = bucket.blob(filename_no_ext + "_ND" + '.json')
			blob.upload_from_filename("tmp/nd_json/{}.json".format(filename_no_ext))

			# bq_pcap_table = bq_client.get_table('kubeflow-test-260816.PCAP.test2')
			
			# #grabbing schema from schema.json file
			# json_schema = json.loads(open('schema.json').read())
			
			# job_config = bigquery.LoadJobConfig()
			# job_config.schema = json_schema
			# job_config.source_format = 'NEWLINE_DELIMITED_JSON'
			# job_config.max_bad_records = 200
			# print("Loading to BQ")
			
			# bq_client.load_table_from_uri(
			# 	"gs://pcap-files/" + filename_no_ext + ".json",
			# 	bq_pcap_table, job_config=job_config)


			return "Success"


if __name__ == '__main__':
    # app.run()
    PCAP_analysis()
