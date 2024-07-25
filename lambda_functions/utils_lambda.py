import json
import time
import re

def get_hashtag(text):

  """
  This function takes a string as input and returns the hashtag.
  """

  #hashtag = text.split('\n')

  # Remove the initial "Here are the 5 most important phrases from the interview:" line
  #hashtag_list = hashtag[1:]
  #filtered_hashtags = [hashtag.strip() for hashtag in hashtag_list if len(hashtag) >= 2]
  
  filtered_hashtags = text.split()

  if filtered_hashtags[0].startswith('#'):
      
      result_hash = f"{filtered_hashtags[0]}"
  else:
      
      result_hash = f"#{filtered_hashtags[0]}"
      
  
  return result_hash

def get_phrases(text):

  """
  This fucntion takes a text string as input and returns a list of phrases.
  """
  
  phrases = text.split('\n')
  phrases = phrases[1:]
  phrases = [phrase.strip()[2:-1] for phrase in phrases]
  filtered_phrases = [phrase.strip() for phrase in phrases if len(phrase) >= 2]

  return filtered_phrases
