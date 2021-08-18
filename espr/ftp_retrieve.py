from ftplib import FTP 
import re
import os
from datetime import datetime
from time import sleep
import logging

logging.basicConfig(filename='output.log', level=logging.WARNING)

def most_recent_dir(ftp):
    ftpdir_list = ftp.nlst()
    new_dir = max([int(re.search(r'\d+', ftpdir).group()) for ftpdir in ftpdir_list])
    return new_dir

def ftp_login(site='ftp.ncep.noaa.gov',sleep_time=30):
    try:
        ftp = FTP(site)
        ftp.login()
    except Exception as e:
        logging.error(f'{e} error, retrying soon')
        sleep(sleep_time)
        ftp = FTP(site)
        ftp.login()
    return ftp

def changemon(ftp_dir='./', sleep_time=30):
    ls_prev = set()

    while True:
        ftp = ftp_login(sleep_time=sleep_time)
        ftp.cwd(most_recent_dir(ftp))
        ftp.cwd(most_recent_dir(ftp))
        ls = set(ftp.nlst(ftp_dir))
        ftp.quit()
        add, rem = ls-ls_prev, ls_prev-ls
        if add or rem: yield add, rem
        ls_prev = ls
        sleep(sleep_time)

def notify_changes(sleep_time=30):
    for add, rem in changemon('pub/data/nccf/com/gens/prod'):
        datenow = datetime.now().strftime('%Y %m %d %H:%m')
        logging.warning(f'{datenow}')
        logging.warning('\n'.join('+ %s' % str(i) for i in add))
        logging.warning('\n'.join('- %s' % str(i) for i in rem))
        sleep(sleep_time)

if __name__ == '__main__':
    notify_changes()
