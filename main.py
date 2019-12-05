import os
import json
import csv


import pcapkit


directory = '/Users/andrewhuh/PCAPs/'

def PCAP_analysis():

	for filename in os.listdir(directory):
		if filename.endswith("pcap"):
			filename_list = filename.split(".")
			filename_no_ext = filename_list[0] 
			ljson = pcapkit.extract(fin=filename_no_ext, fout='out/{}.json'.format(filename_no_ext), format='json', store=False, engine="dpkt")
			#json_data = json.loads(open('out/{}.json'.format(filename_no_ext)).read())
			#with open('csv/{}.csv'.format(filename_no_ext), 'w') as outfile:
				#print(json_data)
				#writer = csv.DictWriter(outfile, json_data.keys())
				#print(writer)
				#writer.writeheader()
				#for row in json_data:
				#	writer.writerow(row)
def main():
	PCAP_analysis()

if __name__ == '__main__':
	main()
