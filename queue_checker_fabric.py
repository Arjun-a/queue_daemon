#!/usr/bin/python
import os
import re
import beanstalkc
import logging
from time import sleep

FORMAT = '%(asctime)s - %(levelname)s - %(message)s'

cd_dir = '/var/log/'
logging.basicConfig(
                format = FORMAT,
                filename = cd_dir + "daemon.log",
                level = logging.DEBUG
)


queue = []
workers = {}
mapping = {}

def fetch_fabric_queues(queue_list):
        file_loc = '/home/vagrant/www/fabric/src/Practo/PractoAppBundle/Queue/AbstractQueue.php'
        file_new = open(file_loc)

        for line in file_new :
                if 'const' in line :
                        xtract = re.findall(r'"(.*?)"', line)
                        if xtract :
                                queue_list.append(xtract[0])

        file_new.close()

def fetch_fabric_workers(workers_dict) :
        loc = "/etc/supervisor/conf.d/fabric.conf"
        file_workers = open(loc)

        for line in file_workers :
                if "[program:" in line :
                        worker = re.findall(r"\:([A-Za-z0-9_]+)\]", line)
                        command = next(file_workers)
                        workers_dict[worker[0]] = command

        file_workers.close()

def queue_worker_mapper(queue_list, workers_dict, mapping) :

        i=0
        length = len(queue_list)
        unprocessd = []
        mapp = {}

        while i < length :
                os.chdir('/home/vagrant/www/fabric')
                loc = os.popen(" git grep -i " + queue_list[i] + " | awk -F: '/.php/ {print $1}'| xargs grep 'setName' | awk -F:'|' 'NR==1 {print $1;x=$1} NR>1 {if ($1 != x) {print $1;x=$1}}'").read()
                loc = loc.rstrip()
                try:
                        file_new = open(loc, "r")
                except:
                        unprocessd.append(queue_list[i])
                        i = i + 1
                        continue
                for line in file_new :
                        if "setName" in line :
                                hit_line = line
                                hit_line = hit_line.rstrip()
                                extract = re.findall(r"'(.*?)'", hit_line, re.DOTALL)
                                if extract :
                                        mapp[queue_list[i]] = extract[0]
                i = i + 1

                file_new.close()


        for key,val in mapp.items() :
                for keys,values in workers_dict.items() :
                        if mapp[key] in values :
                                mapping['dev-' + key] = 'fabric:0' + keys

def job_fabric(mapping) :

        beanstalk = beanstalkc.Connection(host='localhost', port=11300)

        while True :
                for tube in beanstalk.tubes() :
                        if mapping.has_key(tube) :
                                logging.debug('Checking for queue : %s', tube)
                                if beanstalk.stats_tube(tube)['total-jobs']==0 :
                                        os.system('sudo supervisorctl stop ' + mapping[tube])
                                        logging.warning('Stopping Worker : %s', mapping[tube])
                                else:
                                        os.system('sudo supervisorctl restart ' + mapping[tube])
                                        logging.warning('Starting Worker : %s', mapping[tube])
                sleep(1)






fetch_fabric_queues(queue)
fetch_fabric_workers(workers)
queue_worker_mapper(queue, workers, mapping)
job_fabric(mapping)


