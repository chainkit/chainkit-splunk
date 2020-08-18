
# encoding = utf-8

import os
import sys
import time
import datetime
import json
from splunklib import client, results
from datetime import datetime, timedelta
import requests
import hashlib

'''
    IMPORTANT
    Edit only the validate_input and collect_events functions.
    Do not edit any other part in this file.
    This file is generated only once when creating the modular input.
'''
'''
# For advanced users, if you want to create single instance mod input, uncomment this method.
def use_single_instance_mode():
    return True
'''

def validate_input(helper, definition):
    """Implement your own validation logic to validate the input stanza configurations"""
    # This example accesses the modular input variable
    # username = definition.parameters.get('username', None)
    # password = definition.parameters.get('password', None)
    # storage = definition.parameters.get('storage', None)
    # api = definition.parameters.get('api', None)
    pass

def collect_events(helper, ew):
    """Implement your data collection logic here

    # The following examples get the arguments of this input.
    # Note, for single instance mod input, args will be returned as a dict.
    # For multi instance mod input, args will be returned as a single value.
    opt_username = helper.get_arg('username')
    opt_password = helper.get_arg('password')
    opt_storage = helper.get_arg('storage')
    opt_api = helper.get_arg('api')
    # In single instance mode, to get arguments of a particular input, use
    opt_username = helper.get_arg('username', stanza_name)
    opt_password = helper.get_arg('password', stanza_name)
    opt_storage = helper.get_arg('storage', stanza_name)
    opt_api = helper.get_arg('api', stanza_name)

    # get input type
    helper.get_input_type()

    # The following examples get input stanzas.
    # get all detailed input stanzas
    helper.get_input_stanza()
    # get specific input stanza with stanza name
    helper.get_input_stanza(stanza_name)
    # get all stanza names
    helper.get_input_stanza_names()

    # The following examples get options from setup page configuration.
    # get the loglevel from the setup page
    loglevel = helper.get_log_level()
    # get proxy setting configuration
    proxy_settings = helper.get_proxy()
    # get account credentials as dictionary
    account = helper.get_user_credential_by_username("username")
    account = helper.get_user_credential_by_id("account id")
    # get global variable configuration
    global_userdefined_global_var = helper.get_global_setting("userdefined_global_var")

    # The following examples show usage of logging related helper functions.
    # write to the log for this modular input using configured global log level or INFO as default
    helper.log("log message")
    # write to the log using specified log level
    helper.log_debug("log message")
    helper.log_info("log message")
    helper.log_warning("log message")
    helper.log_error("log message")
    helper.log_critical("log message")
    # set the log level for this modular input
    # (log_level can be "debug", "info", "warning", "error" or "critical", case insensitive)
    helper.set_log_level(log_level)

    # The following examples send rest requests to some endpoint.
    response = helper.send_http_request(url, method, parameters=None, payload=None,
                                        headers=None, cookies=None, verify=True, cert=None,
                                        timeout=None, use_proxy=True)
    # get the response headers
    r_headers = response.headers
    # get the response body as text
    r_text = response.text
    # get response body as json. If the body text is not a json string, raise a ValueError
    r_json = response.json()
    # get response cookies
    r_cookies = response.cookies
    # get redirect history
    historical_responses = response.history
    # get response status code
    r_status = response.status_code
    # check the response status, if the status is not sucessful, raise requests.HTTPError
    response.raise_for_status()

    # The following examples show usage of check pointing related helper functions.
    # save checkpoint
    helper.save_check_point(key, state)
    # delete checkpoint
    helper.delete_check_point(key)
    # get checkpoint
    state = helper.get_check_point(key)

    # To create a splunk event
    helper.new_event(data, time=None, host=None, index=None, source=None, sourcetype=None, done=True, unbroken=True)
    """

    '''
    # The following example writes a random number as an event. (Multi Instance Mode)
    # Use this code template by default.
    import random
    data = str(random.randint(0,100))
    event = helper.new_event(source=helper.get_input_type(), index=helper.get_output_index(), sourcetype=helper.get_sourcetype(), data=data)
    ew.write_event(event)
    '''

    '''
    # The following example writes a random number as an event for each input config. (Single Instance Mode)
    # For advanced users, if you want to create single instance mod input, please use this code template.
    # Also, you need to uncomment use_single_instance_mode() above.
    import random
    input_type = helper.get_input_type()
    for stanza_name in helper.get_input_stanza_names():
        data = str(random.randint(0,100))
        event = helper.new_event(source=input_type, index=helper.get_output_index(stanza_name), sourcetype=helper.get_sourcetype(stanza_name), data=data)
        ew.write_event(event)
    '''
    opt_username = helper.get_arg('username')
    opt_password = helper.get_arg('password')
    opt_storage = helper.get_arg('storage')
    opt_api = helper.get_arg('api')
    opt_search_name = helper.get_arg("search_name")
    opt_query = helper.get_arg("query")
    opt_earliest_time = int(helper.get_arg("earliest_time"))
    opt_latest_time = int(helper.get_arg("latest_time"))
    # Splunk Account
    opt_global_account = helper.get_arg("global_account")
    opt_export_logs = helper.get_arg("export_logs")
    
    data = opt_username
    res = {}

    proxy_settings = helper.get_proxy()
    
    
    username = opt_global_account.get("username")
    password = opt_global_account.get("password")
    
    HOST = os.getenv("SPLUNK_HOST", "localhost")
    PORT = int(os.getenv("SPLUNK_PORT", "8089"))
    now = datetime.now()
    now = now - timedelta(seconds=now.second)
    service = client.connect(
        host=HOST,
        port=PORT,
        username=username,
        password=password)
    
    earliest_time = (now - timedelta(seconds=-opt_earliest_time)).strftime(
        '%Y-%m-%dT%H:%M:%S')
    latest_time = (now - timedelta(seconds=-opt_latest_time)).strftime('%Y-%m-%dT%H:%M:%S')
    
    kwargs_export = {"earliest_time": earliest_time,
                     "latest_time": latest_time,
                     "search_mode": "normal",
                     "preview": False}
    
    exportsearch_results = service.jobs.export(str(opt_query), **kwargs_export)
    
    reader = results.ResultsReader(exportsearch_results)
    
    logs = []
    
    if opt_api == "register":

        for result in reader:
            if isinstance(result, dict):
                logs += [list(result)]
        
        logs.sort()
        length = len(logs)
        if opt_export_logs == "raw_data":
            logs = str(logs)
        elif opt_export_logs == "no_space":
            logs = str(logs).replace(" ", "")
        _hash = make_hash(logs)

        logindata = login(opt_username, opt_password)
        entityId = register(logindata, _hash, opt_storage)
        
        res = {}

        res["hash"] = str(_hash)
        res["query"] = opt_query
        res["title"] = opt_search_name
        _time = (datetime.now()).strftime('%Y-%m-%dT%H:%M:%S')
        res["running_script"] = _time
        res["assetId"] = entityId.get("assetId")
        res["earliest_time"] = earliest_time
        res["latest_time"] = latest_time
        res["length"] = length
        res["export_logs"] = opt_export_logs
        
        _event = helper.new_event(source=helper.get_input_type(), index=helper.get_output_index(), sourcetype=helper.get_sourcetype(), data=json.dumps(res))
        ew.write_event(_event)
        
        
    if opt_api == "verify":
        
        for result in reader:
            
            if isinstance(result, dict):
                
                dict_res = result["_raw"]
                dict_res = json.loads(dict_res)
                
                assetId = dict_res["assetId"]
                earliest_time = dict_res["earliest_time"]
                latest_time = dict_res["latest_time"]
                query_hash = dict_res["query"]
                title = dict_res["title"]
                export_logs = dict_res["export_logs"]
                reg_time = result["_time"]
                
                kwargs_export = {"earliest_time": earliest_time,
                                 "latest_time": latest_time,
                                 "search_mode": "normal",
                                 "preview": False
                                 }
                
                searchquery_export = query_hash
                
                exportsearch_results = service.jobs.export(str(searchquery_export), **kwargs_export)
                reader = results.ResultsReader(exportsearch_results)
                logs =[]
                for result in reader:
                    if isinstance(result, dict):
                        logs += [list(result)]

                
                logs.sort()
                length = len(logs)
                if export_logs == "raw_data":
                    logs = str(logs)
                elif export_logs == "no_space":
                    logs = str(logs).replace(" ", "")
                _hash = make_hash(logs)
                
                logindata = login(opt_username, opt_password)
                response = verify(logindata, assetId, _hash, opt_storage)
                verified = response.get("verified")
                res = {}
                res["run_script"] = (datetime.now()).strftime('%Y-%m-%dT%H:%M:%S')
                res["verified"] = verified
                res["assetId"] = assetId
                res["earliest_time"] = earliest_time
                res["latest_time"] = latest_time
                res["hash"] = _hash
                res["query"] = query_hash
                res["title"] = title
                res["reg_time"] = reg_time
                res["length"] = length
                res["export_logs"] = export_logs
                
                event = helper.new_event(source=helper.get_input_type(), index=helper.get_output_index(), sourcetype=helper.get_sourcetype(), data=json.dumps(res))
                ew.write_event(event)


def login(username, password):
    """Construct an ProvenanceValidator object by logging in to the
    Pencildata server.
       Both the username and the password arguments may be given as str.
       Password bytes sequence will be submitted to the server encoded in
    base64. After a successful authentication, the login_data property is
    populated by a dictionary that (among other things) contains the
    user's AccessToken. If the authentication fails, an exception is raised
    (actually a HTTPError: Bad Request)."""

    url = 'https://api.pencildata.com/token'
    data = {'userId': username, 'password': password}
    head = {"Content-Type": "application/json"}
    res = requests.request("POST", url, data=json.dumps(data), headers=head)

    return res.json()
    
def make_hash(val):
    hash_object = hashlib.sha256()
    hash_object.update(val.encode('utf-8'))
    return hash_object.hexdigest()

### This function is to register hash to blockchain
def register(login_data, hash, storage="pencil"):
    """Register a file (by its SHA-256 hash) in your Pencildata account.
    Warning: this method does not check if the file hash exists in the
    registers. It returns the asset id for the file.

    Arguments:
    login_data: Access Token
    storage: 'public' or 'private'. Whether to store the file entry in the
    public or in the private database at the PencilDATA server."""

    datajson = {}

    datajson["hash"] = hash
    datajson["storage"] = storage
    url = "https://api.pencildata.com/register/"

    head = {"Content-Type": "application/json",
            "Authorization": "Bearer {0}".format(login_data['data']['accessToken'])}  # Request HTTP headers
    res = requests.request("POST", url, data=json.dumps(datajson), headers=head)

    return res.json()

### This function is to verify hash against hash in blockchain
def verify(login_data, asset_id, hash, storage="pencil"):
    """Register a file (by its SHA-256 hash) in your Pencildata account.
    Warning: this method does not check if the file hash exists in the
    registers. It returns the asset id for the file.

    Arguments:
    file: file name or a file object. If file is given as a file-like
    object, this method advances the current position of the file until
    its end, but it does not close the file-like object
    storage: 'public' or 'private'. Whether to store the file entry in the
    public or in the private database at the PencilDATA server."""
    datajson = {}
    datajson["hash"] = hash
    datajson["storage"] = storage
    url = "https://api.pencildata.com/verify/" + str(asset_id)
    head = {"Content-Type": "application/json",
            "Authorization": "Bearer {0}".format(login_data['data']['accessToken'])}  # Request HTTP headers
    res = requests.request("GET", url, params=datajson, headers=head)
    return res.json()


