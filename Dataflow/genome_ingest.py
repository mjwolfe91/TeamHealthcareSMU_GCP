import apache_beam as beam
import csv
import datetime 

DATETIME_FORMAT='%Y-%m-%dT%H:%M:%S'


def create_genome_row(fields):
    header = 'int64_field_0,id_number,diagnosis,Grb7,HER2,ER,PR,BCL2,SCUBE2,Ki67,STK15,Survivin,CyclinBI,MYBL2,MMPII,CTSL2,CD68,GSTMI,BAG1,Bactin,GAPDH,RPLPO,GUS,TFRC'.split(',')
    featdict = {}
    for name, value in zip(header, fields):
        featdict[name] = value
    #featdict['EVENT_DATA'] = ','.join(fields)
    return featdict



def extract_genome_fields(line):
	fields = line.split(',')
	if fields[0] != 'FL_DATE':
		yield fields

def format_result(myresult):
    (k, v) = myresult
    return '|'.join([k, str(v)])


def run(project, bucket, dataset):
   argv = [
      '--project={0}'.format(project),
      '--job_name=ingest-genome-data' + datetime.datetime.now().strftime('%Y%m%d%H%M%S'),
      '--save_main_session',
      '--staging_location=gs://{0}/data_files/genotype/staging/'.format(bucket),
      '--temp_location=gs://{0}/data_files/genotype/temp/'.format(bucket),
      '--max_num_workers=8',
      '--autoscaling_algorithm=THROUGHPUT_BASED',
      '--runner=DataflowRunner',
      '--no_use_public_ips'
   ]
   genome_raw_files = 'gs://{}/data_files/genotype/*.csv'.format(bucket)
   genome_output = 'gs://{}/data_files/genotype'.format(bucket)
   genome_bq_output = '{}:{}.Genotype'.format(project, dataset)

   pipeline = beam.Pipeline(argv=argv)
   
   
   genome = (pipeline 
      | 'genome:read' >> beam.io.ReadFromText (genome_raw_files)
	  | 'genome:convert fields' >> beam.FlatMap(extract_genome_fields)
	  | 'genome:todict' >> beam.Map(lambda fields: create_genome_row(fields)) 
   )

	#TODO:  add in csv header handling
   (genome 
      | 'genome:tostring' >> beam.Map(lambda jsonEntry: '|'.join(jsonEntry.values())) 
      | 'genome:out' >> beam.io.textio.WriteToText(genome_output)
   )
   

	#write genome to bq
   genome_schema = 'int64_field_0:INTEGER,id_number:INTEGER,diagnosis:STRING,Grb7:INTEGER,HER2:INTEGER,ER:INTEGER,PR:INTEGER,BCL2:INTEGER,SCUBE2:INTEGER,Ki67:INTEGER,STK15:INTEGER,Survivin:INTEGER,CyclinBI:INTEGER,MYBL2:INTEGER,MMPII:INTEGER,CTSL2:INTEGER,CD68:INTEGER,GSTMI:INTEGER,BAG1:INTEGER,Bactin:INTEGER,GAPDH:INTEGER,RPLPO:INTEGER,GUS:INTEGER,TFRC:INTEGER'

   (genome 
      | 'genome:bq out' >> beam.io.WriteToBigQuery(
                              genome_bq_output, schema=genome_schema,
                              write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                              create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
   )

   pipeline.run()

if __name__ == '__main__':
   import argparse
   parser = argparse.ArgumentParser(description='Run pipeline on the cloud')
   parser.add_argument('-p','--project', help='Unique project ID', required=True)
   parser.add_argument('-b','--bucket', help='bucket for intermediate work', required=True)
   parser.add_argument('-d','--dataset', help='BigQuery dataset', required=True)
   args = vars(parser.parse_args())


   print ("Correcting timestamps and writing to BigQuery dataset {}".format(args['dataset']))
  
   run(project=args['project'], bucket=args['bucket'], dataset=args['dataset'])
   #python genome_ingest.py -p smu-msds-7346-summer2019-mld -b raw-biopsy-files -d raw_biopsy_data

