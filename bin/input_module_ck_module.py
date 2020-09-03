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


def validate_input(helper, definition):
    pass


def collect_events(helper, ew):
    opt_search_name = helper.get_arg('name')
    opt_username = helper.get_arg('username')
    opt_password = helper.get_arg('password')
    opt_storage = helper.get_arg('storage')
    opt_api = helper.get_arg('api')
    opt_search_name = helper.get_arg("search_name")
    opt_query = helper.get_arg("query")
    opt_earliest_time = int(helper.get_arg("earliest_time"))
    opt_latest_time = int(helper.get_arg("latest_time"))
    opt_export_logs = helper.get_arg("export_logs")
    opt_endpoint = helper.get_arg("endpoint")

    input_data = {}
    input_data["opt_username"] = opt_username
    input_data["opt_password"] = opt_password
    input_data["opt_storage"] = opt_storage
    input_data["opt_export_logs"] = opt_export_logs
    input_data["opt_query"] = opt_query
    input_data["opt_search_name"] = opt_search_name
    input_data["opt_endpoint"] = opt_endpoint

    HOST = os.getenv("SPLUNK_HOST", "localhost")
    PORT = int(os.getenv("SPLUNK_PORT", "8089"))
    now = datetime.now()
    now = now - timedelta(seconds=now.second)

    sessionKey = helper.context_meta['session_key']

    service = client.connect(
        host=HOST,
        port=PORT,
        token=sessionKey)

    earliest_time = (now - timedelta(seconds=-opt_earliest_time + 60)).strftime(
        '%Y-%m-%dT%H:%M:%S')
    latest_time = (now - timedelta(seconds=-opt_latest_time + 60)).strftime('%Y-%m-%dT%H:%M:%S')
    input_data["earliest_time"] = earliest_time
    input_data["latest_time"] = latest_time

    kwargs_export = {"earliest_time": earliest_time,
                     "latest_time": latest_time,
                     "search_mode": "normal",
                     "preview": False}

    exportsearch_results = service.jobs.export(str(opt_query), **kwargs_export)
    reader = results.ResultsReader(exportsearch_results)

    if opt_api == "register":
        register_api(reader, input_data, helper, ew)
    if opt_api == "verify":
        verify_api(reader, input_data, service, helper, ew)


def register_api(reader, input_data, helper, ew):
    logs = []
    for result in reader:
        if isinstance(result, dict):
            logs += [list(result)]

    logs.sort()
    length = len(logs)
    opt_export_logs = input_data["opt_export_logs"]
    opt_username = input_data["opt_username"]
    opt_password = input_data["opt_password"]
    opt_storage = input_data["opt_storage"]
    opt_endpoint = input_data["opt_endpoint"]

    if opt_export_logs == "raw_data":
        logs = str(logs)
    elif opt_export_logs == "no_space":
        logs = str(logs).replace(" ", "")
    _hash = make_hash(logs)

    input_type = helper.get_input_type()
    logindata = login(opt_username, opt_password, opt_endpoint)
    entityId = register(logindata, _hash, opt_endpoint, opt_storage)

    res = {}
    res["hash"] = str(_hash)
    res["query"] = input_data["opt_query"]
    res["title"] = input_data["opt_search_name"]
    _time = (datetime.now()).strftime('%Y-%m-%dT%H:%M:%S')
    res["running_script"] = _time
    res["assetId"] = entityId.get("assetId")
    res["earliest_time"] = input_data["earliest_time"]
    res["latest_time"] = input_data["latest_time"]
    res["length"] = length
    res["export_logs"] = opt_export_logs

    event = helper.new_event(source=helper.get_input_type(), index=helper.get_output_index(),
                             sourcetype=helper.get_sourcetype(), data=json.dumps(res))
    ew.write_event(event)


def verify_api(reader, input_data, service, helper, ew):
    opt_username = input_data["opt_username"]
    opt_password = input_data["opt_password"]
    opt_storage = input_data["opt_storage"]
    opt_endpoint = input_data["opt_endpoint"]
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

            _latest_time = datetime.strptime(latest_time, '%Y-%m-%dT%H:%M:%S')
            kwargs_export = {"earliest_time": earliest_time,
                             "latest_time": latest_time,
                             "search_mode": "normal",
                             "preview": False
                             }

            searchquery_export = query_hash

            exportsearch_results = service.jobs.export(str(searchquery_export), **kwargs_export)
            reader = results.ResultsReader(exportsearch_results)
            logs = []
            for result in reader:
                if isinstance(result, dict):
                    logs += [list(result)]
            res = {}
            logs.sort()
            length = len(logs)
            if export_logs == "raw_data":
                logs = str(logs)
            elif export_logs == "no_space":
                logs = str(logs).replace(" ", "")
            hash = make_hash(logs)

            logindata = login(opt_username, opt_password, opt_endpoint)
            response = verify(logindata, assetId, hash, opt_endpoint, opt_storage)
            verified = response.get("verified")

            res["verified"] = verified
            res["assetId"] = assetId
            res["_time"] = (datetime.now()).strftime('%Y-%m-%dT%H:%M:%S')
            res["earliest_time"] = earliest_time
            res["latest_time"] = latest_time
            res["hash"] = hash
            res["query"] = query_hash
            res["title"] = title
            res["run_script"] = (datetime.now()).strftime('%Y-%m-%dT%H:%M:%S')
            res["length"] = length
            res["export_logs"] = export_logs

            event = helper.new_event(source=helper.get_input_type(), index=helper.get_output_index(),
                                     sourcetype=helper.get_sourcetype(), data=json.dumps(res))
            ew.write_event(event)


def login(username, password, opt_endpoint):
    """Construct an ProvenanceValidator object by logging in to the
    Pencildata server.
       Both the username and the password arguments may be given as str.
       Password bytes sequence will be submitted to the server encoded in
    base64. After a successful authentication, the login_data property is
    populated by a dictionary that (among other things) contains the
    user's AccessToken. If the authentication fails, an exception is raised
    (actually a HTTPError: Bad Request)."""
    url = "{}{}".format(opt_endpoint, "/token/")
    data = {'userId': username, 'password': password}
    head = {"Content-Type": "application/json"}
    res = requests.request("POST", url, data=json.dumps(data), headers=head)
    return res.json()


def make_hash(val):
    hash_object = hashlib.sha256()
    hash_object.update(val.encode('utf-8'))
    return hash_object.hexdigest()


### This function is to register hash to blockchain
def register(login_data, hash, opt_endpoint, storage="pencil"):
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
    url = "{}{}".format(opt_endpoint, "/register/")
    head = {"Content-Type": "application/json",
            "Authorization": "Bearer {0}".format(login_data['data']['accessToken'])}  # Request HTTP headers
    res = requests.request("POST", url, data=json.dumps(datajson), headers=head)
    return res.json()


### This function is to verify hash against hash in blockchain
def verify(login_data, asset_id, hash, opt_endpoint, storage="pencil"):
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
    url = "{}{}{}".format(opt_endpoint, "/verify/", str(asset_id))
    head = {"Content-Type": "application/json",
            "Authorization": "Bearer {0}".format(login_data['data']['accessToken'])}  # Request HTTP headers
    res = requests.request("GET", url, params=datajson, headers=head)
    return res.json()