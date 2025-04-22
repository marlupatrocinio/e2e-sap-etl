import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# class PrintRowFn(beam.DoFn):
#     def process(self, element):
#         print(element)
#         yield element

class ParseCSVFn(beam.DoFn):
    def process(self, element):
        values = element.split(',')
        header = ['empresa', 'material', 'valor_total', 'quantidade']
        row = dict(zip(header, values))
        yield row
        
def run():
    input_file = 'gs://etl_sap/sap_extract.csv'
    
    pipeline_options = PipelineOptions(
        runner = 'DataflowRunner',
        project = 'proud-outpost-455911-s8',
        region = 'us-central1',
        temp_location = 'gs://etl_sap/temp',
        staging_location = 'gs://etl_sap/staging',
        job_name = 'etl-sap-job'
    )
    
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'Read CSV' >> beam.io.ReadFromText(input_file, skip_header_lines=1)
            | 'Parse CSV' >> beam.ParDo(ParseCSVFn())
            | 'Print Parsed Rows' >> beam.Map(print)
        )

if __name__ == '__main__':
    run()