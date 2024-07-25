import boto3
import json
import os
import time


s3 = boto3.client('s3', region_name='us-east-1')
transcribe = boto3.client('transcribe')
# Configura el cliente de MediaConvert
mediaconvert_client = boto3.client('mediaconvert', region_name='us-east-1')
media_conv_role = os.environ['MEDIA_CONV_ROLE']

def lambda_handler(event, context):
    # TODO implement
    
    bucket = event['bucket_destiny'] 
    s3_source_key = f"video-output{event['output_file'].split('video-output')[1]}" #video-output/shakira_video_full/shakira_video_full_little.mp4
    
    job_name = s3_source_key.split('.')[0].split('/')[-1]
    input_file = event['output_file']
    output_file = f's3://{bucket}/video-output/{job_name}/{job_name}_subtitles'
    subtitles = F"s3://{bucket}/subtitles/{job_name}/{job_name}.srt"
    
    print(input_file)
    
    
    transcribe.start_transcription_job(
            TranscriptionJobName=job_name,
            Media={'MediaFileUri': input_file},
            MediaFormat='mp4',
            IdentifyLanguage=True,
            LanguageOptions=['en-US', 'es-ES', 'fr-FR'],
            OutputBucketName=bucket,
            OutputKey=f'subtitles/{job_name}/',
            Subtitles={ 
              "Formats": [ "srt" ],
              "OutputStartIndex": 1
            }
        )
      
    print('TRANSCRIBE RUN SUCCESFULLY')
    time.sleep(60)
    
    # Define the job settings for MediaConvert
    job_settings = {
            'Inputs': [
            {
                'FileInput': input_file,  # Replace with your input video file path
                'VideoSelector': {},
                'AudioSelectors': {
                    'Audio Selector 1': {
                        'DefaultSelection': 'DEFAULT'
                    }
                },
                'CaptionSelectors': {
                    'Caption Selector 1': {
                        'SourceSettings': {
                            'SourceType': 'SRT',
                            'FileSourceSettings': {
                                'SourceFile': subtitles  # Replace with your first subtitle file path (SRT)
                            }
                        }
                    },
                }
            }
            ],
            'OutputGroups': [
                {
                    'Name': 'File Group',
                    'Outputs': [
                        {
                            'ContainerSettings': {
                                'Container': 'MP4'
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
                            ],
                            'CaptionDescriptions': [
                                {
                                    'CaptionSelectorName': 'Caption Selector 1',
                                    'DestinationSettings': {
                                        'DestinationType': 'BURN_IN',
                                        'BurninDestinationSettings': {
                                            'FontSize': 24,
                                            'OutlineColor': 'BLACK',
                                            'ShadowColor': 'NONE',
                                            'TeletextSpacing': 'FIXED_GRID',
                                            'FontColor': 'WHITE',
                                            'FontOpacity': 255,
                                            'OutlineSize': 2
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
    
    # Ejecuta el trabajo de MediaConvert
    response_video_job = mediaconvert_client.create_job(
        Role=media_conv_role,  # Asegúrate de usar el ARN correcto de tu rol
        Settings=job_settings
    )
    
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
