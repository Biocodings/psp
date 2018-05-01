import os
import requests

API_key = os.environ["API_KEY"]
BASE_API_URL = os.environ["API_URL"]



def post_update_to_proteomics_clue(url_suffix, id, payload):
    api_url = BASE_API_URL + "/" + id + "/" + url_suffix

    headers = {'user_key': API_key}

    r = requests.put(api_url,json=payload,headers=headers)
    print r.text
    if r.ok:
        print "successfully updated API at: {}".format(api_url)
    else:
        print "failed to update API at: {} with response: {}".format(api_url, r.text)
