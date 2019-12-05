import os
import json
import csv

from google.cloud import bigquery
from google.cloud import storage
import pcapkit

bucket_name = "pcap-files"
directory = "/Users/andrewhuh/ulap/PCAP-parse/"

bq_client = bigquery.Client(project="kubeflow-test-260816").from_service_account_json(directory + "kubeflow-test-260816-6e4b941d52d5.json")
storage_client = storage.Client(project="kubeflow-test-260816").from_service_account_json(directory + "kubeflow-test-260816-6e4b941d52d5.json")

def PCAP_analysis():

	for filename in os.listdir(directory):

		if filename.endswith("pcap"):

			filename_list = filename.split(".")
			filename_no_ext = filename_list[0]
			print("Extracting PCAP file")

			#Extracting PCAP data from PCAP files
			ljson = pcapkit.extract(fin=filename_no_ext, fout='out/{}.json'.format(filename_no_ext), format='json', store=False, engine="dpkt")
			json_data = json.loads(open('out/{}.json'.format(filename_no_ext)).read())
			
			print("Deleting Header")
			del json_data['Global Header']

			
			with open('revised/{}.json'.format(filename_no_ext), 'w') as f:
				#dumping json data without a header into a new file.
				json.dump(json_data, f)

			#transforming json into newline delimited json for bigquery
			command = os.popen("cat revised/test.json | jq -c '.[]' > ND_JSON/test.json")
			

			print("Uploading to GCS")
			bucket = storage_client.get_bucket(bucket_name)
			blob = bucket.blob(filename_no_ext + '.json')
			blob.upload_from_filename(directory + 'ND_JSON/' + filename_no_ext + '.json')

			bq_pcap_table = bq_client.get_table('kubeflow-test-260816.PCAP.test2')
			
			#grabbing schema from schema.json file
			json_schema = json.loads(open('schema.json').read())
			
			job_config = bigquery.LoadJobConfig()
			job_config.schema = json_schema
			job_config.source_format = 'NEWLINE_DELIMITED_JSON'
			job_config.max_bad_records = 200
			print("Loading to BQ")
			
			bq_client.load_table_from_uri(
				"gs://pcap-files/" + filename_no_ext + ".json",
				bq_pcap_table, job_config=job_config)










def main():
	PCAP_analysis()

if __name__ == '__main__':
	main()
