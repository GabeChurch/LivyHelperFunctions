import re
import pandas as pd
from IPython.display import display, HTML
import requests
import textwrap
import json
import time


def getResultProgress(result):
    res_js = result.json()
    try:
        result_prog_status = (res_js['progress'] != 1.0)
        return result_prog_status
    except:
        result_prog_status = (res_js['statements'][-1]['progress'] != 1.0 and res_js['statements'][-1]['state'] == 'running')
        return result_prog_status

# If the code returns False for available, error, or cancelled and stop. If it returns 1.0 but still running we also return False
def getResultProgress2(result):
    res_js = result.json()
    try:
        result_prog_status = (res_js['state'] != 'available' or res_js['state'] != 'error' or res_js['state'] != 'cancelled')
        #result_prog_status = (res_js['state'] != 'running')
        return result_prog_status
    except:
        result_prog_status = (res_js['statements'][-1]['progress'] != 1.0 and res_js['statements'][-1]['state'] == 'running')
        return result_prog_status
    
#Catches the bug caused from progress = 1.0 and still running
def getResultProgress3(result):
    res_js = result.json()
    try:
        result_prog_status = (res_js['state'] != 'available' or res_js['state'] != 'error' or res_js['state'] != 'cancelled')
        return result_prog_status
    except:
        result_prog_status = (res_js['statements'][-1]['state'] == 'running') 
        return result_prog_status

def pollRequest(r, statements_url, data, headers, host):
    time.sleep(1)
    try:
        statement_url2 = host + r.headers['location'] 
        r2 = requests.get(statement_url2, headers=headers)
        return r2
    except:
        statement_url2=statements_url
        r2 = requests.get(statement_url2, headers=headers)
        return r2

def executeLivy(command):
    sessionNumber = global_session_id
    stringIn = """"""
    if (input_type=='scala'):
        targetCode = command
    else:
        strCommand = 'd = ' + command + "\n%json d\n"
        targetCode = textwrap.dedent(strCommand)
    session_url = host + "/sessions/" + sessionNumber
    statements_url = session_url + '/statements'
    headers = {'Content-Type': 'application/json'}
    data = {'code': targetCode}
    r = requests.post(statements_url, data=json.dumps(data), headers=headers)
    #if the result is not ready we poll until it is checking every 2 seconds
    while (getResultProgress(r)) :
        r = pollRequest(r, statements_url, data, headers, host)
        time.sleep(1)
    else:
        while (getResultProgress2(r)) :
            r = pollRequest(r, statements_url, data, headers, host)
            time.sleep(1)
        else:
            try:
                json_output = r.json()
                output = json_output['statements'][-1]["output"] 
                status = output["status"]
                if status == 'error':
                    raise SyntaxError(output["ename"] + "\n" + "\n".join(output["traceback"]))
                else:
                    final = r.json()['statements'][-1]["output"]["data"]
                    return (list(final.values())[0])
            except SyntaxError as SE:
                raise SyntaxError(SE)
            else:    
                final = r.json()["output"]["data"]
                return (list(final.values())[0])
            
def executeLivyDebug(command):
    sessionNumber = global_session_id
    stringIn = """"""
    if (input_type=='scala'):
        targetCode = command
    else:
        strCommand = 'd = ' + command + "\n%json d\n"
        targetCode = textwrap.dedent(strCommand)
    session_url = host + "/sessions/" + sessionNumber
    statements_url = session_url + '/statements'
    headers = {'Content-Type': 'application/json'}
    data = {'code': targetCode}
    r = requests.post(statements_url, data=json.dumps(data), headers=headers)
    #if the result is not ready we poll until it is checking every 2 seconds
    while (getResultProgress(r)) :
        r = pollRequest(r, statements_url, data, headers, host)
        time.sleep(1)
    else:
        while (getResultProgress2(r)) :
            r = pollRequest(r, statements_url, data, headers, host)
            time.sleep(1)
        else:
            try:
                final = r #.json()['statements'][-1]["output"]["data"]
                return final #(list(final.values())[0])
            except:
                final = r #r.json()["output"]["data"]
                return final #(list(final.values())[0])

def htmlOut(command):
    sessionNumber = global_session_id
    stringIn = """"""
    if (input_type=='scala'):
        targetCode = "println(" + command + ")"
    else:
        strCommand = 'd = ' + command + "\n%json d\n"
        targetCode = textwrap.dedent(strCommand)
    session_url = host + "/sessions/" + sessionNumber
    statements_url = session_url + '/statements'
    headers = {'Content-Type': 'application/json'}
    data = {'code': targetCode}
    r = requests.post(statements_url, data=json.dumps(data), headers=headers)
    #if the result is not ready we poll until it is checking every 2 seconds
    while (getResultProgress(r)) :
        r = pollRequest(r, statements_url, data, headers, host)
        time.sleep(1)
    else:
        while (getResultProgress2(r)) :
            r = pollRequest(r, statements_url, data, headers, host)
            time.sleep(1)
        else:
            while (getResultProgress3(r)) :
                r = pollRequest(r, statements_url, data, headers, host)
                time.sleep(1)
            else:
                try:
                    json_output = r.json()
                    output = json_output['statements'][-1]["output"] 
                    status = output["status"]
                    if status == 'error':
                        raise SyntaxError(output["ename"] + "\n" + "\n".join(output["traceback"]))
                    else:
                        final = r.json()['statements'][-1]["output"]["data"]
                        return HTML(list(final.values())[0])
                except SyntaxError as SE:
                    raise SyntaxError(SE)
                else:    
                    final = r.json()["output"]["data"]
                    return HTML(list(final.values())[0])    

from io import StringIO
import pandas as pd

def getLivy(command):
    sessionNumber = global_session_id
    stringIn = """"""
    if (input_type=='scala'):
        targetCode = "println(" + command + ")"
    else:
        strCommand = 'd = ' + command + "\n%json d\n"
        targetCode = textwrap.dedent(strCommand)
    session_url = host + "/sessions/" + sessionNumber
    statements_url = session_url + '/statements'
    headers = {'Content-Type': 'application/json'}
    data = {'code': targetCode}
    r = requests.post(statements_url, data=json.dumps(data), headers=headers)
    #if the result is not ready we poll until it is checking every 2 seconds
    while (getResultProgress(r)) :
        r = pollRequest(r, statements_url, data, headers, host)
        time.sleep(1)
    else:
        while (getResultProgress2(r)) :
            r = pollRequest(r, statements_url, data, headers, host)
            time.sleep(1)
        else:
            try:
                json_output = r.json()
                output = json_output['statements'][-1]["output"] 
                status = output["status"]
                if status == 'error':
                    raise SyntaxError(output["ename"] + "\n" + "\n".join(output["traceback"]))
                else:
                    final = r.json()['statements'][-1]["output"]["data"]
                    return (list(final.values())[0])
            except SyntaxError as SE:
                raise SyntaxError(SE)
            else:    
                final = r.json()["output"]["data"]
                return (list(final.values())[0])
            

def getLivyPar(command, global_session_id):
    sessionNumber = global_session_id
    stringIn = """"""
    if (input_type=='scala'):
        targetCode = "println(" + command + ")"
    else:
        strCommand = 'd = ' + command + "\n%json d\n"
        targetCode = textwrap.dedent(strCommand)
    session_url = host + "/sessions/" + sessionNumber
    statements_url = session_url + '/statements'
    headers = {'Content-Type': 'application/json'}
    data = {'code': targetCode}
    r = requests.post(statements_url, data=json.dumps(data), headers=headers)
    #if the result is not ready we poll until it is checking every 2 seconds
    while (getResultProgress(r)) :
        r = pollRequest(r, statements_url, data, headers, host)
        time.sleep(1)
    else:
        while (getResultProgress2(r)) :
            r = pollRequest(r, statements_url, data, headers, host)
            time.sleep(1)
        else:
            while (getResultProgress3(r)) :
                r = pollRequest(r, statements_url, data, headers, host)
                time.sleep(1)
            else:
                try:
                    json_output = r.json()
                    output = json_output['statements'][-1]["output"] 
                    status = output["status"]
                    if status == 'error':
                        raise SyntaxError(output["ename"] + "\n" + "\n".join(output["traceback"]))
                    else:
                        final = r.json()['statements'][-1]["output"]["data"]
                        return (list(final.values())[0])
                except SyntaxError as SE:
                    raise SyntaxError(SE)
                else:    
                    final = r.json()["output"]["data"]
                    return (list(final.values())[0])    

def executeLivyPar(command, global_session_id):
    sessionNumber = global_session_id
    stringIn = """"""
    if (input_type=='scala'):
        targetCode = command
    else:
        strCommand = 'd = ' + command + "\n%json d\n"
        targetCode = textwrap.dedent(strCommand)
    session_url = host + "/sessions/" + sessionNumber
    statements_url = session_url + '/statements'
    headers = {'Content-Type': 'application/json'}
    data = {'code': targetCode}
    r = requests.post(statements_url, data=json.dumps(data), headers=headers)
    #if the result is not ready we poll until it is checking every 2 seconds
    while (getResultProgress(r)) :
        r = pollRequest(r, statements_url, data, headers, host)
        time.sleep(1)
    else:
        while (getResultProgress2(r)) :
            r = pollRequest(r, statements_url, data, headers, host)
            time.sleep(1)
        else:
            while (getResultProgress3(r)) :
                r = pollRequest(r, statements_url, data, headers, host)
                time.sleep(1)
            else:
                try:
                    json_output = r.json()
                    output = json_output['statements'][-1]["output"] 
                    status = output["status"]
                    if status == 'error':
                        raise SyntaxError(output["ename"] + "\n" + "\n".join(output["traceback"]))
                    else:
                        final = r.json()['statements'][-1]["output"]["data"]
                        return (list(final.values())[0])
                except SyntaxError as SE:
                    raise SyntaxError(SE)
                else:    
                    final = r.json()["output"]["data"]
                    return (list(final.values())[0])        

            
def executeLivyParDebug(command, sessionNumber):
    stringIn = """"""
    if (input_type=='scala'):
        targetCode = command
    else:
        strCommand = 'd = ' + command + "\n%json d\n"
        targetCode = textwrap.dedent(strCommand)
    session_url = host + "/sessions/" + sessionNumber
    statements_url = session_url + '/statements'
    headers = {'Content-Type': 'application/json'}
    data = {'code': targetCode}
    r = requests.post(statements_url, data=json.dumps(data), headers=headers)
    #if the result is not ready we poll until it is checking every 2 seconds
    while (getResultProgress(r)) :
        r = pollRequest(r, statements_url, data, headers, host)
        time.sleep(1)
    else:
        while (getResultProgress2(r)) :
            r = pollRequest(r, statements_url, data, headers, host)
            time.sleep(1)
        else:
            try:
                final = r #.json()['statements'][-1]["output"]["data"]
                return final #(list(final.values())[0])
            except:
                final = r #r.json()["output"]["data"]
                return final #(list(final.values())[0])            
            

def toPandasLocal(dfname):
    dfstatement = dfname + """.columns.mkString(",") ++ "\\n " ++ """ + dfname + """.collect().map(row => row.mkString(",")).mkString("\\n ")"""
    localdata = StringIO(get_livy(dfstatement))
    df = pd.read_csv(localdata, sep=",")
    return df

from IPython.core.display import HTML
import binascii
from io import BytesIO
import matplotlib.pyplot as plt
import numpy as np
import base64

def capturePlotHTML(plotFunction): 
    #// open IO object
    sio3 = BytesIO() 
    #// print raw canvas data to IO object
       #//fig.canvas.print_png(sio2)
    #// new method
    plotFunction()
    plt.savefig(sio3)
    sio3.seek(0)
    data_uri = base64.b64encode(sio3.read()).decode('ascii')
    html_out = '<html><head></head><body>'
    #html_out += '<img src="data:image/png;base64,{0}" align="left">'.format(data_uri)
    #updated for smart resize
    html_out += '<img style="max-width:100%;max-height:100vh;margin: auto;" src="data:image/png;base64,{0}" align="left">'.format(data_uri)
    html_out += '</body></html>'
    #//prevents plot from showing 
    plt.close()
    return (HTML(html_out))


from concurrent import futures
def executeLivyConcurrent(passed_func, livy_session_numbers):
    executor = futures.ThreadPoolExecutor(max_workers=len(livy_session_numbers))
    futures_map3 = executor.map(passed_func, livy_session_numbers)
    return list(futures_map3)

## requires livy_session_numbers variable in python 
def groupByLivyGroup(scalaint_array_name, livy_session_numbers): 
    groups_raw = get_livy_par('''{
        val target_ts = ''' + scalaint_array_name +  '''
        val groupsizes = ((target_ts.length) / ''' + str(len(livy_session_numbers)) + ''') + 1
        val target_ts_groups = target_ts.grouped(groupsizes)
        val returned = target_ts_groups.toArray.map(x => {x.toArray.mkString(",")}).mkString("-")
        returned
    }''', livy_session_numbers[0]).rstrip().split("-")

    prepped_ts_groups = list(map(lambda tsExecuteGroup: tsExecuteGroup.split(","), ts_groups_raw))
    scala_prepped_ts_groups = list(map(lambda prepped_ts_group: '''"''' + '''","'''.join( list(map(lambda tsNum : tsNum.strip(), prepped_ts_group))) + '''"''', prepped_ts_groups))
    ts_livy_session_combo = []
    for i in list(range(0, len(scala_prepped_ts_groups))):
        ts_livy_session_combo.append((scala_prepped_ts_groups[i], livy_session_numbers[i]))
    return ts_livy_session_combo

def killLivy(sessionNumber):
    session_url = host + "/sessions/" + str(sessionNumber)
    statements_url = session_url
    headers = {'Content-Type': 'application/json'}
    r = requests.delete(statements_url, headers=headers)
    return str(r)

def toPandasLocal(dfname):
    dfstatement = dfname + """.columns.mkString(",") ++ "\\n " ++ """ + dfname + """.collect().map(row => row.mkString(",")).mkString("\\n ")"""
    localdata = StringIO(get_livy(dfstatement))
    df = pd.read_csv(localdata, sep=",")
    return df
