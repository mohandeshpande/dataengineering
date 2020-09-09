import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from sys import argv

PROJECT_ID = 'my-project-id'
SCHEMA = 'unique_key:INTEGER,case_number:STRING,date:TIMESTAMP,block:STRING,iucr:STRING,primary_type:STRING,description:STRING,location_description:STRING,arrest:BOOLEAN,domestic:BOOLEAN,district:INTEGER,ward:INTEGER,fbi_code:STRING,year:INTEGER,latitude:FLOAT,longitude:FLOAT,location:STRING'
TOPIC = "projects/my-project-86997-286016/topics/csv_to_pub"


def convert_types(data):
    """Converts string values to their appropriate type."""
    import datetime as dt
    if data['unique_key']==None:
        return data
    try:
        ukey = data['unique_key'].split('.')
        ukey = ukey[0]
        data['unique_key'] = int(ukey) if ukey is not '' else None
        yr = data['year'].split('.')
        yr = yr[0]
        data['year'] = int(yr) if yr is not '' else None

        x= data['district'].split('.')
        x = x[0]
        y = data['ward'].split('.')
        y = y[0]
        data['district'] = int(x) 
        data['ward'] = int(y)

         
        data['case_number'] = str(data['case_number']) if data['case_number'] is not '' else None
        data['block'] = str(data['block']) if data['block'] is not '' else None
        data['iucr'] = str(data['iucr']) if data['iucr'] is not '' else None
        data['primary_type'] = str(data['primary_type']) if data['primary_type'] is not '' else None
        data['description'] = str(data['description']) if data['description'] is not '' else None
        data['location_description'] = str(data['location_description']) if data['location_description'] is not '' else None
        data['arrest'] = bool(data['arrest']) if data['arrest'] is not '' else None
        data['domestic'] = bool(data['domestic']) if data['domestic'] is not '' else None
        data['fbi_code'] = str(data['fbi_code']) if data['fbi_code'] is not '' else None
        
        data['latitude'] = float(data['latitude']) if data['latitude'] is not '' else None
        data['longitude'] = float(data['longitude']) if data['longitude'] is not '' else None
        data['location'] = str(data['location']) if data['location'] is not '' else None

        ts = dt.datetime.strptime(data['date'], "%m/%d/%Y %H:%M:%S %p") if data['date'] is not '' else None
        td = ts.strftime("%Y-%m-%d %H:%M:%S")
        data['date']=td
        #upo = dt.datetime.strptime(data['updated_on'], "%m/%d/%Y %H:%M:%S %p") if data['updated_on'] is not '' else None
        
    except ValueError:
        data['unique_key'] = None
        data['year'] = None
        data['block'] = None
        data['district'] = None
        data['ward'] = None
        data['case_number'] = None
        data['iucr'] = None
        data['primary_type'] = None
        data['description'] = None
        data['location_description'] = None
        data['arrest'] = None
        data['domestic'] = None
        data['fbi_code'] = None
        data['latitude'] = None
        data['longitude'] = None
        data['location'] = None
        data['date'] = None

    return data


def format_types(data):
    if(len(data)>=23):
        if(len(data)>23):
            loc_data = data[22]+','+data[23]
            loc_data = loc_data[1:-1]
        else:
            loc_data = '' 
        x = dict([("unique_key",data[1]),("case_number",data[2]),("date", data[3]),("block",data[4]),("iucr", data[5]),("primary_type", data[6]),("description", data[7]),("location_description", data[8]),("arrest", data[9]),("domestic", data[10]),("district", data[12]),("ward", data[13]),("fbi_code", data[15]),("year", data[18]),("latitude", data[20]),("longitude", data[21]),("location", loc_data)])
    else:
        x = dict([("unique_key",None),("case_number",None),("date", None),("block", None),("iucr", None),("primary_type", None),("description", None),("location_description", None),("arrest", None),("domestic", None),("district", None),("ward", None),("fbi_code", None),("year", None),("latitude", None),("longitude", None),("location", None)])

    return x

def del_unwanted_cols(data):
    """Delete the unwanted columns"""
    del data['first_val']
    del data['block']
    del data['Description']
    del data['location_description']
    del data['beat']
    del data['community_area']
    del data['x_coordinate']
    del data['y_coordinate']
    return data

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    known_args = parser.parse_known_args(argv)

    p = beam.Pipeline(options=PipelineOptions())

    (p | 'ReadData' >> beam.io.ReadFromPubSub(topic=TOPIC).with_output_types(bytes)
       | "Decode" >> beam.Map(lambda x: x.decode('utf-8'))
       | 'SplitData' >> beam.Map(lambda x: x.split(','))
       | 'FormatToDict' >> beam.Map(format_types) 
       | 'ChangeDataType' >> beam.Map(convert_types)
       | 'filterRows' >> beam.Filter(lambda x: x['unique_key']!=None)
       | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
           '{0}:final_project_db.ccd_new_tb_1'.format(PROJECT_ID),
           schema=SCHEMA,
           write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
    result = p.run()
