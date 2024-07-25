import json
import time
import pandas as pd


def get_clips(df_phrases_f):
  
  clips = []

  # Iterate through DataFrame rows
  for i, row in df_phrases_f.iterrows():
      start_time = row['start']
      end_time = row['end']
      
      partes_s = str(start_time).split(".")
      parte_entera_s = int(partes_s[0])
      parte_decimal_s = float("0." + partes_s[1])
      
      
      partes_e = str(end_time).split(".")
      parte_entera_e = int(partes_e[0])
      parte_decimal_e = float("0." + partes_e[1])
      #print(parte_decimal)
  
      # Format start and end times
      formatted_start_time = "{:02d}:{:02d}:{:02d}:{:02.0f}".format(int(start_time // 3600), int((start_time % 3600) // 60), int(start_time % 60), parte_decimal_s)
      #formatted_end_time = "{:02d}:{:02d}:{:02d}:{:02d}".format(int(end_time // 3600), int((end_time % 3600) // 60), int(end_time % 60), int(round(((end_time % 60) % int((end_time % 60))) * 100,1)))
      formatted_end_time = "{:02d}:{:02d}:{:02d}:{:02.0f}".format(int(end_time // 3600), int((end_time % 3600) // 60), int(end_time % 60),parte_decimal_e)

  
      # Format start and end times
      #formatted_start_time = "{:02d}:{:02d}:{:02d}:{:02d}".format(int(start_time // 3600), int((start_time % 3600) // 60), seconds_module(start_time)[0], seconds_module(start_time)[1])
      #formatted_end_time = "{:02d}:{:02d}:{:02d}:{:02d}".format(int(end_time // 3600), int((end_time % 3600) // 60), int(end_time % 60), int(round(((end_time % 60) % int((end_time % 60))) * 100,1)))
      
  
  
  
      # Create the JSON object
      json_object = {
          "StartTimecode": formatted_start_time,
          "EndTimecode": formatted_end_time
      }
  
      # Append to clips list
      clips.append(json_object)
      
  return clips

def get_slot_times(phrases,df):
  
  df_phrases = []
  relevant_phrases = []
  start = []
  end = []
  
  for phrase in phrases:
  
    try:
      df_temp = get_accuracy_words(phrase.replace(',','').replace('.','').replace('"','').strip().split(),df)
      df_temp.reset_index(inplace =True,drop=True)
      df_phrases.append(df_temp)
      start.append(float(df_temp['start_time'][0]))
      end.append(float(df_temp['end_time'][len(df_temp)-1]))
      relevant_phrases.append(phrase)
      print(phrase)
    except:
      pass
    
  df_phrases_f = {
      'relevant_phrases': relevant_phrases,
      'start': start,
      'end': end
  }
  
  df_phrases_f = pd.DataFrame(df_phrases_f)
  
  return df_phrases_f

def get_items(items):
  # Crear listas para cada columna
  types = []
  contents = []
  confidences = []
  start_times = []
  end_times = []

  # Llenar las listas con los datos
  for item in items:
      types.append(item.get('type', ''))
      content = item['alternatives'][0].get('content', '')
      contents.append(content)
      confidence = item['alternatives'][0].get('confidence', '') if item['type'] == 'pronunciation' else ''
      confidences.append(confidence)
      start_times.append(item.get('start_time', ''))
      end_times.append(item.get('end_time', ''))

  # Crear el DataFrame
  df = pd.DataFrame({
      "type": types,
      "content": contents,
      "confidence": confidences,
      "start_time": start_times,
      "end_time": end_times
  })

  return df

def get_phrases(text):

  """
  This fucntion takes a text string as input and returns a list of phrases.
  """
  
  phrases = text.split('\n')
  phrases = phrases[1:]
  phrases = [phrase.strip()[2:-1] for phrase in phrases]
  filtered_phrases = [phrase.strip() for phrase in phrases if len(phrase) >= 2]

  return filtered_phrases

def get_accuracy_words(words,df):

  """
  This function takes a list of words and a DataFrame as input and returns a DataFrame of matching words.
  """
  
  n = len(words)
  for i in range(len(df) - n + 1):
      if list(df['content'].iloc[i:i + n].str.lower()) == [word.lower() for word in words]:
          return df.iloc[i:i + n]
  return None

def convert_seconds_to_minutes(seconds):
    """
    This function takes a number of seconds as input and returns a tuple of minutes and remaining seconds.
    """
    seconds_float = float(seconds)  
    minutes = int(seconds_float // 60)
    remaining_seconds = seconds_float % 60
    return minutes, int(remaining_seconds)

def seconds_module(time_value):

  """
  This function takes a number of seconds as input and returns a tuple of minutes and remaining seconds.
  """
  seconds = 0
  miliseconds = 0

  if (time_value < 1 ): 
    miliseconds = 0
  else :
    seconds = int(time_value % 60)
    miliseconds = int(round(((time_value % 60) % int((time_value % 60))) * 100,1))
  
  return seconds,miliseconds
