from ftplib import FTP 
import numpy as np
from datetime import datetime
import paths as ps 
import os
import hsa

def retr_files(ftp, temp_store):
    log_directory = ps.log_directory
    file_list = ftp.nlst()
    file_valid_list = [n for n in file_list if np.logical_and('geavg' in n, '.idx' not in n)]
    file_valid_list = [n for n in file_valid_list if np.logical_and(int(n[-3:]) <= 168, int(n[-3:]) % 6 == 0)]
    file_valid_list = [n for n in file_valid_list if '011ab' not in n]
    for n in file_valid_list:
        fname = 'gefs_mean_'+n[-3:]+'.grib2'
        try:
            ftp.retrbinary("RETR " + n, open(temp_store + fname, 'wb').write)
        except:
            with open(log_directory + 'retrieval_log.txt', "a") as f:
                f.write(n + ' failure at ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S')+'\n')
            break
    file_valid_list = [n for n in file_list if np.logical_and('gespr' in n, '.idx' not in n)]
    file_valid_list = [n for n in file_valid_list if np.logical_and(int(n[-3:]) <= 168, int(n[-3:]) % 6 == 0)]
    for n in file_valid_list:
        fname = 'gefs_sprd_'+n[-3:]+'.grib2'
        try:
            ftp.retrbinary("RETR " + n, open(temp_store + fname, 'wb').write)
        except:
            with open(log_directory + 'retrieval_log.txt', "a") as f:
                f.write(n + ' failure at ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S')+'\n')
            break
    with open(log_directory + 'retrieval_log.txt', "a") as f:
        f.write('completed at ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S')+'\n')

    

