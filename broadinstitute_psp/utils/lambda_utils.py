import os
import requests

def post_update_to_proteomics_clue(url_suffix, id, payload):
    """

    :param url_suffix: must include leading "/"
    :param id: api entry id
    :param payload: json to put to API

    """
    api_key = os.environ["API_KEY"]
    base_api_url = os.environ["API_URL"]

    api_url = base_api_url + "/" + id  + url_suffix

    headers = {'user_key': api_key}

    r = requests.put(api_url,json=payload,headers=headers)
    print r.text
    if r.ok:
        print "successfully updated API at: {}".format(api_url)
    else:
        print "failed to update API at: {} with response: {}".format(api_url, r.text)
    return r