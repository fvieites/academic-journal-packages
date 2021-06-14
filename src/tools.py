import os
import requests
import pandas as pd
import json


def downloadKbart(url,filename,folder):
    '''
    Download a kbart file having the url
    '''
    downloadedKbart = {'completed':False,'log':'', 'filepath':None, 'content':None}
    try:
        if url:
            headers= loadHeaders()
            resp = requests.get(url, headers=headers)
            downloadedKbart['log'] = '{0} {1}'.format(resp.status_code,resp.reason)
            if resp.text:
                downloadedKbart['content'] = resp.text             
            if resp.status_code != 200:
                return(downloadedKbart)
            if not os.path.exists(folder):
                os.makedirs(folder)    
            filepath = os.path.join(folder,filename)
            if os.path.exists(filepath):
                os.remove(filepath)
            file = open(filepath,"wb")
            file.write(resp.text.encode('utf-8'))
            file.close()
            downloadedKbart['filepath'] = filepath
            downloadedKbart['completed'] = True
    except Exception as e:
        downloadedKbart['log'] = downloadedKbart['log'] + '\n{0}'.format(str(e))
    return(downloadedKbart)

def loadHeaders():
    headers = {}
    headers['User-Agent'] = 'Mozilla/5.0' # some sites (Wiley) require an user agent
    return(headers)

def isKbartDownloaded(filename,folder):
    filepath = os.path.join(folder,filename)
    retvalue = os.path.isfile(filepath)
    return(retvalue)

def obtainKabart(url,code,folder):
    filename = getFileName(code)
    if not isKbartDownloaded(filename,folder):
        downloadedKbart = downloadKbart(url,filename,folder)
        if downloadedKbart['completed']:
            print('Download completed: {0} : {1}'.format(code, downloadedKbart['filepath']))
            return(downloadedKbart['filepath'])
        else:
            print('Error downloading {0} : {1}'.format(code, downloadedKbart['log']))
    else:
        print('Kbart path from stored file {0} : {1}'.format(code, filename))
        filepath = os.path.join(folder,filename)
        return(filepath)
    return(None)

def getFolderPath(folder):
    cwd = os.getcwd()
    return(os.path.join(cwd,folder))

def getFileName(name):
    return name +'.txt'

def getKbartAsDf(url,name,code,folder):
    kbartPath = obtainKabart(url,code,folder)
    if kbartPath:
        df_all = pd.read_csv(kbartPath, sep='\t', header=0)
        df_all['_package'] = code
        df_all['_package_name'] = name 
        df_all['_publication_status'] = 'inactive'
        df_all.loc[df_all['date_last_issue_online'].isna(),'_publication_status'] = 'active'
        return(df_all)
    return(None)

def loadJobs(jobsfile):
    xl = pd.ExcelFile(jobsfile)
    df = xl.parse(xl.sheet_names[0])
    if 'active' in df.columns:
        return(df.loc[df['active']==1])
    return(df)

def doKbartJobs():
    cfg = loadConfig()
    jobs = loadJobs(cfg['jobs_file'])
    folder = cfg['data_folder']
    dfs = []
    for j in jobs.index:
        if jobs.iloc[j]['active'] == 1 and jobs.iloc[j]['type'] == 'kbart':
            df_tmp = getKbartAsDf(jobs.iloc[j]['url'],jobs.iloc[j]['name'],jobs.iloc[j]['code'],folder)
            dfs.append(df_tmp)
    df = pd.concat(dfs)
    return(df)

def runSummarize(df = None):
    if df is None:
        df = doKbartJobs()
    output_df = df[['_package','access_type','_publication_status','publication_title']].groupby(['_package','access_type','_publication_status']).agg({'publication_title':'count'})
    print(output_df)

def runSummarize2(df_all):
    packages = list(df_all['_package'].unique())
    for i,package in enumerate(packages):
        df = df_all.loc[df_all['_package']== package]
        print(str(df['_package_name'][0]))
        print('\t{0} - {1} - Number of titles in the kbart file:{2}'.format(i,package,len(df.index)))
        
        #titles without closed publication (active)
        df_actives = df[df['date_last_issue_online'].isna()]
        print('\t{0} - {1} - Number of active titles:{2}'.format(i,package,len(df_actives.index)))

        df_actives_free = df_actives[df_actives['access_type'] == 'F' ]
        print('\t{0} - {1} - Number of free (e.g. Open Access or Free Access) active titles:{2}'.format(i,package,len(df_actives_free.index)))

        df_actives_paid = df_actives[df_actives['access_type'] == 'P' ]
        print('\t{0} - {1} - Number of paid (e.g. subscription) active titles:{2}'.format(i,package,len(df_actives_paid.index)))


def loadConfig():
    config = {}
    with open('config.json') as json_data:
        config = json.load(json_data)
    return(config)



