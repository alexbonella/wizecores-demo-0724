import json
import time
import pandas as pd
from utils_video import *
import boto3
import os

# Initialize the S3 client
s3 = boto3.client('s3')

# Configura el cliente de MediaConvert
mediaconvert_client = boto3.client('mediaconvert', region_name='us-east-1')
media_conv_role = os.environ['MEDIA_CONV_ROLE']

def read_json_from_s3(bucket_name, file_key):
    try:
        # Get the object from the S3 bucket
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        
        # Read the content of the file
        content = response['Body'].read().decode('utf-8')
        
        # Parse the JSON content
        json_content = json.loads(content)
        
        return json_content
    
    except Exception as e:
        print(f"Error reading the file: {e}")
        return None

def lambda_handler(event, context):
    
    bucket = event['bucket_destiny']
    job_name = event['job_name']
    input_file = f"s3://{event['bucket_origin']}/video-input/{event['job_name']}.mp4"
    output_file = f's3://{bucket}/video-output/{job_name}/{job_name}_little'
    file_key = event['transcribe_path']
    object_key = event['bedrock_phrases']
    
    
    #print(file_key)
    json_content = read_json_from_s3(bucket, file_key)
    json_content = json.dumps(json_content, indent=4)
    data = json.loads(json_content) 
    
    
    
    # Extraer los items
    items = data['results']['items']

    df = get_items(items)
    df = df[df['type'] != 'punctuation']
    if df is not None:
        df = df.reset_index(drop=True)
    else:
        print("El DataFrame es None. Verifica la fuente de datos.")
    
    
    # Download the subtitles file from S3
    tmp_file_path = f'/tmp/{job_name}.txt'
    s3.download_file(bucket, object_key, tmp_file_path)
    
    with open(tmp_file_path, 'r') as f:
        next(f)
        text_content = f.read()
    
    phrases  = get_phrases(text_content)
    print(phrases)
    # Process the text content
    df_phrases_f = get_slot_times(phrases,df)
    print(df_phrases_f)
    clips = get_clips(df_phrases_f)
    print(clips)
    
    
    
    job_settings = {
            'Inputs': [
                {
                    'TimecodeSource': 'ZEROBASED',
                    'VideoSelector': {},
                    'AudioSelectors': {
                        'Audio Selector 1': {
                            'DefaultSelection': 'DEFAULT'
                        }
                    },
                    'FileInput': input_file,
                    'InputClippings': clips
                }
            ],
            'OutputGroups': [
                {
                    'Name': 'File Group',
                    'Outputs': [
                        {
                            'ContainerSettings': {
                                'Container': 'MP4',
                            },
                            'VideoDescription': {
                                'CodecSettings': {
                                    'Codec': 'H_264',
                                    'H264Settings': {
                                        'MaxBitrate': 5000000,
                                        'RateControlMode': 'QVBR',
                                        'QualityTuningLevel': 'SINGLE_PASS',
                                        'QvbrSettings': {
                                            'QvbrQualityLevel': 8
                                        }
                                    }
                                },
                                'Width': 1920,
                                'Height': 1080
                            },
                            'AudioDescriptions': [
                                {
                                    'CodecSettings': {
                                        'Codec': 'AAC',
                                        'AacSettings': {
                                            'Bitrate': 96000,
                                            'CodingMode': 'CODING_MODE_2_0',
                                            'SampleRate': 48000
                                        }
                                    }
                                }
                            ]
                        }
                    ],
                    'OutputGroupSettings': {
                        'Type': 'FILE_GROUP_SETTINGS',
                        'FileGroupSettings': {
                            'Destination': output_file
                        }
                    }
                }
            ]
        }
    
    #response = mediaconvert_client.create_job(Settings=job_settings)
    
    # Ejecuta el trabajo de MediaConvert
    response_video_job = mediaconvert_client.create_job(
        Role=media_conv_role,  # Asegúrate de usar el ARN correcto de tu rol
        Settings=job_settings
    )
    
    print('JOB RUN SUCCESFULLY')
    print('Job created:', response_video_job['Job']['Id'])
    
    
    
    metadata_file = {
        
        "bucket_origin": event['bucket_origin'],
        "bucket_destiny":bucket,
        "job_name":job_name,
        "input_file":input_file,
        "output_file":f"{output_file}.mp4",
        "status": "created"
        
    }

    # TODO implement
    return metadata_file
