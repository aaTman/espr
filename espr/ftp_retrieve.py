from ftplib import FTP 
import re
import os
from datetime import datetime
from time import sleep
import logging
import numpy as np
import sys

logging.basicConfig(filename='output.log', 
                level=logging.INFO, 
                format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

def most_recent_dir(ftp):
    ftpdir_list = ftp.nlst()
    new_dir_int = max([int(re.search(r'\d+', ftpdir).group()) for ftpdir in ftpdir_list])
    new_dir = [n for n in ftpdir_list if str(new_dir_int) in n][0]
    return new_dir

def ftp_login(site='ftp.ncep.noaa.gov',sleep_time=30):
    try:
        ftp = FTP(site)
        ftp.login()
    except Exception as e:
        logging.error(f'{e} error, retrying in {sleep_time} sec...')
        sleep(sleep_time)
        ftp = FTP(site)
        ftp.login()
    return ftp

def ftp_change(ftp_dir='./', sleep_time=30):
    ls_prev = set()
    while True:
        ftp = ftp_login(sleep_time=sleep_time)
        logging.info(ftp.lastresp)
        ftp.cwd(ftp_dir)
        ftp.cwd(most_recent_dir(ftp))
        ls = set(ftp.nlst())
        ftp.quit()
        add, rem = ls-ls_prev, ls_prev-ls
        if add or rem: yield add, rem, ftp
        ls_prev = ls
        sleep(sleep_time)

def notify_changes(sleep_time=30):
    for add, rem, ftp in ftp_change('pub/data/nccf/com/gens/prod'):
        datenow = datetime.now().strftime('%Y %m %d %H:%m')
        logging.info(f'{datenow}')
        if len(add) > 0:
            logging.info('\n'.join('+ %s' % str(i) for i in add))
            retr_files(add, ftp, ftpdir='pub/data/nccf/com/gens/prod')
        if len(rem) > 0:
            logging.info('\n'.join('- %s' % str(i) for i in rem))
        sleep(sleep_time)
    datenow = datetime.now().strftime('%Y %m %d %H:%m')
    logging.info(f'exiting script at {datenow}')

def link_builder(query_dict):
    assert 'lat' in query_dict; 'latitude must be in dict'
    assert 'lon' in query_dict; 'longitude must be in dict'
    
def loop_and_download(stat, fhours, ftp, http=True, query_dict={}):
    assert stat in ['mean','sprd']; 'stat must be mean or sprd'
    if stat == 'mean':
        gestat = 'geavg'
        save_name = 'mean'
    file_valid_list = [n for n in file_list if np.logical_and('geavg' in n, '.idx' not in n)]
    file_valid_list = [n for n in file_valid_list if np.logical_and(int(n[-3:]) <= 168, int(n[-3:]) % 6 == 0)]
    file_valid_list = [n for n in file_valid_list if '011ab' not in n]
    for n in file_valid_list:
        fname = 'gefs_mean_'+n[-3:]+'.grib2'
        try:
            with open(fname, 'wb') as f:
                # Define the callback as a closure so it can access the opened 
                # file in local scope
                def callback(data):
                    f.write(data)
                ftp.retrbinary(f'RETR {fname}', callback)
        except Exception as e:
            logging.error(f'{n} failure, {e}')
            pass

def retr_files(add, ftp, ftpdir='/'):
    ftp = ftp_login()
    ftp.cwd(ftpdir)
    import pdb; pdb.set_trace()
    while True:
        try:
            ftp.cwd(most_recent_dir(ftp))
        except AttributeError:
            continue
    if 'atmos' in ftp.nlst():
        ftp.cwd('atmos')
    if 'pgrb2sp25' in ftp.nlst():
        ftp.cwd('pgrb2sp25')
    assert 'pgrb2sp25' in ftp.pwd(); f'not in file directory, currently in {ftp.pwd()}'
    file_list = ftp.nlst()
    file_valid_list = [n for n in file_list if np.logical_and('geavg' in n, '.idx' not in n)]
    file_valid_list = [n for n in file_valid_list if np.logical_and(int(n[-3:]) <= 168, int(n[-3:]) % 6 == 0)]
    file_valid_list = [n for n in file_valid_list if '011ab' not in n]
    for n in file_valid_list:
        fname = 'gefs_mean_'+n[-3:]+'.grib2'
        try:
            with open(fname, 'wb') as f:
                # Define the callback as a closure so it can access the opened 
                # file in local scope
                def callback(data):
                    f.write(data)
                ftp.retrbinary(f'RETR {fname}', callback)
        except Exception as e:
            logging.error(f'{n} failure, {e}')
            pass
    file_valid_list = [n for n in file_list if np.logical_and('gespr' in n, '.idx' not in n)]
    file_valid_list = [n for n in file_valid_list if np.logical_and(int(n[-3:]) <= 168, int(n[-3:]) % 6 == 0)]
    for n in file_valid_list:
        fname = 'gefs_sprd_'+n[-3:]+'.grib2'
        try:
            ftp.retrbinary(f"RETR {n}", open(temp_store + fname, 'wb').write)
        except:
            with open(log_directory + 'retrieval_log.txt', "a") as f:
                f.write(n + ' failure at ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S')+'\n')
            break
    with open(log_directory + 'retrieval_log.txt', "a") as f:
        f.write('completed at ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S')+'\n')
if __name__ == '__main__':
    notify_changes()
