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



queue = {}
workers = {}
mapping = {}

def fetch_ray_queues(queue_list):
        file_loc = '/home/vagrant/www/ray/library/Practo/Queue.php'
        file_new = open(file_loc)

        for line in file_new :
                if 'const' in line :
                        key = re.findall(r'const (.+?) ', line)
                        value = re.findall(r"'(.*?)'", line)
                        if key :
                                if value :
                                        queue_list[key[0]] = value[0]
        file_new.close()

def fetch_ray_workers(workers_dict) :
        loc = "/etc/supervisor/conf.d/ray_app.conf"
        file_workers = open(loc)

        for line in file_workers :
                if "[program:" in line :
                        worker = re.findall(r"\:([A-Za-z0-9_]+)\]", line)
                        command = next(file_workers)
                        comm = re.findall(r'php (.+?) ',command)
                        if comm :
                                workers_dict[worker[0]] = comm[0]

        file_workers.close()

def queue_worker_mapper(queue_list, workers_dict, mapping) :

        unprocessd = []
        mapp = {}

        for q_pseudo, q in queue_list.items() :
                os.chdir('/home/vagrant/www/ray')
                loc = os.popen("git grep " + q_pseudo  + " | awk -F : '{print $1}' | xargs grep '$practoQueue->receiveMessage();' | awk -F:'|' 'NR==1 {print $1;x=$1} NR>1 {if ($1 != x) {print $1;x=$1}}'").read()
                loc = loc.rstrip()
                file_loc = loc.split('\n')
                i=0
                found = 0
                num = len(file_loc)
                if num != 0 :
                        while i<num :
                                if file_loc[i] :
                                        file_new = open(file_loc[i])
                                        for line in file_new :
                                                if '$practoQueue = Practo_Queue::getInstance()' in line :
                                                        if q_pseudo in next(file_new) :
                                                                mapp[q] = file_loc[i]
                                                                found = 1
                                                                break

                                        file_new.close()
                                if found == 1 :
                                        break
                                else :
                                        i = i + 1


        for work, file_name in workers_dict.items() :
                for q,loc in mapp.items() :
                        if file_name in loc :
                                mapping['dev-' + q] = 'ray_app:0' + work


def job_ray(mapping) :

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




fetch_ray_queues(queue)
fetch_ray_workers(workers)
queue_worker_mapper(queue, workers, mapping)
job_ray(mapping)
