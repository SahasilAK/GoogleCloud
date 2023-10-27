Use Case:
a.Using Google Dataflow and Appache Beam create a streaming pipeline which reads the messages from Pub/Sub. 
b.The message contains gcs URI to a csv file (or anyother format) 
c.Read the CSV file and perform transformation on the data in streaming mode and write the data to bigquery or google cloud storage.

Code Description:
1.gcs_streamer_storage_api : This code will read data form source URI provided by  Pub/Sub . Once read the data will be conveted to python dictionary and required transformation can be performed on this dictionary . Once the required transformation is performed the data is batched into the widnow defined in code and using Storage API data will be wirtten as csv format to gcs. On top of this external table can be created in bigquery.
2.gcs_streamer_gcsIo : This code will read data form source URI provided by  Pub/Sub . Once read the data will be conveted to python dictionary and required transformation can be performed on this dictionary . Once the required transformation is performed the data is batched into the widnow defined in code and using GCSIO from appache beam library data will be wirtten as csv format to gcs. On top of this external table can be created in bigquery.