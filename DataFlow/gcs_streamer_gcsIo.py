import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
from apache_beam.transforms import Map
from apache_beam.io import ReadFromPubSub, WriteToText
from apache_beam.transforms.util import BatchElements
from apache_beam.transforms.window import FixedWindows
import logging
import csv
import json
import random
from apache_beam.transforms import Map, WindowInto, GroupByKey
from apache_beam import (
    DoFn,
    GroupByKey,
    io,
    ParDo,
    Pipeline,
    PTransform,
    WindowInto,
    WithKeys,
)

# Define your Google Cloud Storage paths
input_topic = 'projects/stately-minutia-399506/subscriptions/csv_topic-sub'
output_bucket = 'gs://dflow-test-b1/output'
output_prefix = 'output'
logging.basicConfig(level=logging.INFO)



class log_data(beam.DoFn):
    def process(self, element):
        try:
            logging.info(element)
        except Exception as e:
            logging.info(f'''element : {element} 
                            
                            ''')
            raise RuntimeError(e)
        

class WriteToGcs(beam.DoFn):
    def __init__(self, output_path):
        self.output_path= output_path
    def process(self, element):
        import json
        import uuid
        from datetime import datetime
        import apache_beam as beam
        try:
            logging.info(f'element: {element}')
            ts_format='%Y%m%d%H%M%S%f'
            current_time=datetime.now().strftime(ts_format)
            shard_id,batch_data=element
            
            
            # logging.info(f'multiline_string: {multiline_string}')
            file_name=f'{self.output_path}_{str(uuid.uuid4())}_{current_time}_{shard_id}.jsonl'


            with beam.io.gcsio.GcsIO().open(file_name, 'w') as f:
                for item in batch_data:
                    line = json.dumps(item)  # Ensure_ascii=False prevents escaping
                    f.write(f'{line}\n'.encode('utf-8'))
                    # f.write(json.dumps(multiline_string).encode('utf-8'))
        except Exception as e:
            logging.info(f'''element : {element} 
                            
                            ''')
            raise RuntimeError(e)
class GroupMessagesByFixedWindows(PTransform):
    """A composite transform that groups Pub/Sub messages based on publish time
    and outputs a list of tuples, each containing a message and its publish time.
    """

    def __init__(self, window_size, num_shards=5):
        # Set window size to 60 seconds.
        self.window_size = int(window_size)
        self.num_shards = num_shards

    def expand(self, pcoll):
        import random
        from datetime import datetime
        import random
        return (
            pcoll
            # Bind window info to each element using element timestamp (or publish time).
            | "Window into fixed intervals"
            >> WindowInto(FixedWindows(self.window_size))
            # | "Add timestamp to windowed elements" >> ParDo(AddTimestamp())
            # Assign a random key to each windowed element based on the number of shards.
            | "Add key" >> WithKeys(lambda _: random.randint(0, self.num_shards - 1))
            # Group windowed elements by key. All the elements in the same window must fit
            # memory for this. If not, you need to use `beam.util.BatchElements`.
            | "Group by key" >> GroupByKey()
        )


class AddTimestamp(DoFn):
    def process(self, element, publish_time=DoFn.TimestampParam):
        """Processes each windowed element by extracting the message body and its
        publish time into a tuple.
        """
        from datetime import datetime
        import random
        yield (
            element,
            datetime.utcfromtimestamp(float(publish_time)).strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            ),
        )

def run():
    # Set up pipeline options
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'stately-minutia-399506'
    google_cloud_options.job_name = 'pubsub-to-gcs-fcs'

    with beam.Pipeline(options=pipeline_options) as p:
        import csv
        
        message = (p | 'Read From PUb/Sub' >> beam.io.ReadFromPubSub(subscription=input_topic)\
               | 'Decode Message' >> beam.Map(lambda x : x.decode('utf-8'))
               )
     
  
        csv_decode = (message |
                    'read from gcs' >> beam.io.ReadAllFromText(skip_header_lines=1,with_filename=True)
                    | 'ParseCSV' >> beam.Map(lambda line: (line[0],next(csv.reader([line[1]], delimiter=',', quotechar='"'))))
                      | 'to dict' >> beam.Map(lambda x:{'file_name':x[0].split('/')[-1],'Index':x[1][0],'User':x[1][1],'First Name':x[1][2],'Last Name':x[1][3],'Sex':x[1][4],'Email':x[1][5],'Phone':x[1][6],'Date of birth':x[1][7]})
                    )
   
        output_file ='gs://dflow-test-b1/output/part'

        # write_to_gcs = (windowed_data | 'write to gcs' >> beam.ParDo(WriteToGcs(output_file)))
        window_data = csv_decode | "Window into" >> GroupMessagesByFixedWindows(10, 10)
        write_to_gcs = (window_data | 'write to gcs' >> beam.ParDo(WriteToGcs(output_file)))
        
     
        # log_el = (window_data | 'log data' >> beam.ParDo(log_data()))

        p.run()
if __name__=='__main__':
    run()