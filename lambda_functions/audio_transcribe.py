import boto3
import json
import os
import time

s3 = boto3.client('s3', region_name='us-east-1')
transcribe = boto3.client('transcribe', region_name='us-east-1')
S3_BUCKET_DESTINY = os.environ['S3_BUCKET_DESTINY']

def lambda_handler(event, context):
    # Obtener el nombre del bucket y la clave del objeto del evento
    
    try :
        s3_source_bucket = event['detail']['bucket']['name']
        s3_source_key = event['detail']['object']['key']
    except:
        s3_source_bucket = event['bucket_destiny']
        s3_source_key = f"video-output{event['output_file'].split('video-output')[1]}"
        
        
    # Imprimir el nombre del bucket y la clave del objeto
    print(f"Bucket: {s3_source_bucket}, Key: {s3_source_key}")
    
    job_name = s3_source_key.split('.')[0].split('/')[-1]
    job_uri = f's3://{s3_source_bucket}/{s3_source_key}'
    
    try:
        response = transcribe.start_transcription_job(
            TranscriptionJobName=job_name,
            Media={'MediaFileUri': job_uri},
            MediaFormat='mp4',
            IdentifyLanguage=True,
            LanguageOptions=['en-US', 'es-ES', 'fr-FR'],
            OutputBucketName=S3_BUCKET_DESTINY,
            OutputKey=f'subtitles/{job_name}/',
            Subtitles={ 
              "Formats": [ "srt" ],
              "OutputStartIndex": 1
            }
        )
        
        print('TRANSCRIBE RUN SUCCESFULLY')
        
    except Exception as e:
        print(f"Error: {e}")
    
    
    metadata_file = {
        
        "bucket_origin": s3_source_bucket,
        "bucket_destiny":S3_BUCKET_DESTINY,
        "video_uploaded": s3_source_key,
        "subtiltes_path":f'subtitles/{job_name}/{job_name}.srt',
        "transcribe_path":f'subtitles/{job_name}/{job_name}.json',
        "status": "created"
        
    } 
    
    return metadata_file
