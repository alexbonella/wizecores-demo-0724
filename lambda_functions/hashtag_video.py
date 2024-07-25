import json
import os
import subprocess
import shlex
import boto3
import re
from utils_lambda import *

s3 = boto3.client('s3')


def lambda_handler(event, context):
    
    s3_source_bucket = event['bucket_destiny'] 
    s3_source_key = f"video-output{event['output_file'].split('video-output')[1]}"
    job_name = s3_source_key.split('.')[0].split('/')[-1].replace('_little_subtitles','')
    hashtag_path = f"bedrock_answer/{job_name}/{job_name}_hastag.txt"
    print(job_name)
    print(hashtag_path)
    
    SIGNED_URL_TIMEOUT = 60
    s3_source_signed_url = s3.generate_presigned_url('get_object',
        Params={'Bucket': s3_source_bucket, 'Key': s3_source_key},
        ExpiresIn=SIGNED_URL_TIMEOUT)
    
    # Download the subtitles file from S3
    tmp_file_path = f'/tmp/{job_name}.txt'
    s3.download_file(s3_source_bucket, hashtag_path, tmp_file_path)
    
    with open(tmp_file_path, 'r') as f:
        next(f)
        text_content = f.readlines()
        
    hashtag = get_hashtag(text_content[-1])
    print(hashtag)
    
    
    #Download Fonts
    s3_font = 'lambda-layers/fonts/Roboto_Mono/'
    local_dir = f"/tmp/fonts/{s3_font.split('/')[-1]}"
    
    # Create local directory if it doesn't exist
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)
        
    # List objects in the specified S3 folder
    response = s3.list_objects_v2(Bucket=s3_source_bucket, Prefix=s3_font)
    
    # Check if the folder contains objects
    if 'Contents' in response:
        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith('/'):
                continue  # Skip any sub-folder if exists

            local_file_path = os.path.join(local_dir, os.path.basename(key))

            # Download the file
            s3.download_file(s3_source_bucket, key, local_file_path)
            #print(f'Download Complete {local_file_path}-{key}')
    

    ffmpeg_path = "/opt/bin/ffmpeg"
    font = "/tmp/fonts/RobotoMono-Bold.ttf"
    
    #fmpeg_cmd = f'''opt/bin/ffmpeg -i {s3_source_signed_url} -vf "drawtext=fontfile={local_dir}static/RobotoMono-Bold.ttf:text='{hashtag}':fontsize=24:fontcolor=white:x=W-tw:y=40" /temp/{job_name}_final.mp4'''
    #ffmpeg_cmd = f'''{ffmpeg_path} -i {s3_source_signed_url} -vf "drawtext=fontfile={font}:text='{hashtag}':fontsize=24:fontcolor=white:x=W-tw:y=40" /temp/{job_name}_final.mp4'''
    fmpeg_cmd = f'''{ffmpeg_path} -i {s3_source_signed_url} \
       -vf "drawtext=fontfile={font}:text='\{hashtag}':fontsize=48:fontcolor=black:box=1:boxcolor=white:x=W-tw:y=40" \
       -y /tmp/{job_name}_final.mp4'''
       
    command1 = shlex.split(fmpeg_cmd)
    run_command = subprocess.run(command1, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print('ERRORES AQUI')
    print(f'stdout errors {run_command.stdout.decode()}')
    print(f'stderr errors {run_command.stderr.decode()}')
    
    archivos = os.listdir('/tmp/')
    print(f'Video aqui {archivos}')
    
    path_components = s3_source_key.split('/')[:2]
    s3_destination_filename = '/'.join(path_components[:2])
    
    #Put video in Destiny
    #s3.put_object(Body=run_command.stdout, Bucket=s3_source_bucket, Key=s3_destination_filename)
    final_video = f'/tmp/{job_name}_final.mp4'
    
    s3.upload_file(final_video, s3_source_bucket, Key=f'{s3_destination_filename}/{job_name}_final.mp4')
    #aws_cp_command = f'aws s3 cp {final_video} s3://{s3_source_bucket}/{s3_destination_filename}/{job_name}_final.mp4'
    #run_aws_command = subprocess.run(aws_cp_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    
    metadata_file = {
        
        "bucket_origin": event['bucket_origin'],
        "bucket_destiny":s3_source_bucket,
        "job_name":job_name,
        "output_file":f'{s3_destination_filename}/{job_name}_final.mp4',
        "hashtag_inserted":hashtag,
        "status": "created"
        
    }
    
    success_message =  f'Hey Datexland! the video 👉 {metadata_file["output_file"]} just been uploaded in your {metadata_file["bucket_destiny"]} Bucket'
    
    # TODO implement
    return success_message
