# -*- coding: utf-8 -*-
from gzip import open as _gzopen
from traceback import print_exc as _print_exc
from time import process_time as _process_time
from os import getcwd as _getcwd
import copy

""" write the Log file and print Effectiveness """
def func_produce_result(li, start):

    Total_wait_t, Total_wait_rate, Total_turnaround_t = 0, 0, 0
    with open('Utilization based moldable job scheduling exce Log.txt', 'w') as wp:
        for job in li:
            if isinstance(job, str):
                wp.write(job)
            else:
                Total_wait_t += job['Wait_t']
                Total_wait_rate += job['Waiting_rate']
                Total_turnaround_t = Total_turnaround_t + job['Wait_t'] + job['Run_t']
                wp.write(
                    f"Job number: {job['Job_num']}\nSubmit Time: {job['Sub_t']} Wait Time: {job['Wait_t']} "\
                    f"Start Time: {job['start_t']} Run Time: {job['Run_t']} Finish Time: {job['Finish_t']}\n"\
                    f"Idle Process: {job['idle_pro']} Allocated Process: {job['allocated_pro']}\n{'-'*101}\n"\
                )
         
        print(
            f'spend {_process_time() - start} seconds\nTotal waiting time: {Total_wait_t}\n'\
            f'Total waiting rate: {Total_wait_rate}\nAverage waiting time: {Total_wait_t/count}\n'\
            f'Average waiting rate: {Total_wait_rate/count}\nAverage turnaround time: {Total_turnaround_t/count}\n'
        )

def _speedup(n):
    global alpha
    return 1 / ((1-alpha)+alpha/n)

def T1(Tn, speedup):
    return Tn * speedup

def efficiency(speedup, n):
    return speedup / n

def Utilization(Ready_queue, current_time):
    tmp = 0
    for i in Ready_queue:
            ti = i.get('T1')/_speedup(i.get('allocated_pro'))
            tmp += ti*i.get('allocated_pro')*efficiency(_speedup(i.get('allocated_pro')), i.get('allocated_pro'))
    tmp2 = max(Ready_queue, key=lambda x:x.get('expected finish time')).get('expected finish time') - current_time
    tmp1 = resource_process * tmp2
    return tmp / tmp1

def MLS(Ready_queue, running_queue, current_time):
  
    Ready_queue_length = len(Ready_queue)
    tmp = list.copy(running_queue)
    profile = list.copy(running_queue)
    current_idle_pro = resource_process
    
    if not running_queue:
        pass
    else:
        tmp1 = sum([job.get('allocated_pro')  for job in running_queue  if job.get('start_t') <= current_time])
        current_idle_pro =  current_idle_pro - tmp1
    i = 0
    while True:
        if (Ready_queue[i].get('allocated_pro') <= current_idle_pro and 
            tmp and min(tmp, key=lambda x:x.get('expected finish time'))['expected finish time'] > current_time or not tmp):
            Ready_queue[i]['start_t'] =  current_time
            Ready_queue[i]['expected finish time'] = current_time + Ready_queue[i].get('T1')/_speedup(Ready_queue[i].get('allocated_pro'))
            current_idle_pro -= Ready_queue[i].get('allocated_pro')
            profile.append(Ready_queue[i])
            tmp.append(Ready_queue[i])
            if i == Ready_queue_length-1:
                break
            else:
                i+=1
        else:
            current_time = min(tmp, key=lambda x:x.get('expected finish time')).get('expected finish time')
            Earliest_Finish_job_list = (data for data in tmp if data.get('expected finish time') == current_time)
            for job in Earliest_Finish_job_list:
                tmp.remove(job)
                current_idle_pro += job.get('allocated_pro')

    return profile

""" simulate process from Job_queue (New state) to Ready_queue (Ready state) """
def ready_status(Job_queue, profile, Ready_queue, Running_queue, log, current_time, idle_pro):
 
    if Job_queue and not Ready_queue and not Running_queue: 
        current_time = Job_queue[0].get('Sub_t')
    tmp = list.copy(Job_queue)
    c_t = current_time
    var_flag = 0
    for job in tmp:
        if  job.get('Sub_t') <= c_t:
            current_time = job.get('Sub_t')
            if Running_queue and min(Running_queue, key=lambda x:x.get('Finish_t')).get('Finish_t') > current_time or not Running_queue:
                Ready_queue.append(job)
                Job_queue.remove(job)
                var_flag = 1
                profile = utilization_based_moldable(Ready_queue, Running_queue, current_time)
                profile, Ready_queue, Running_queue, log, idle_pro = scheduling(
                    profile, Ready_queue, Running_queue, log, current_time, idle_pro)
            else:
                break
        else:
            if (var_flag == 0 and Running_queue and min(Running_queue, key=lambda x:x.get('Finish_t')).get('Finish_t') > current_time or not        Running_queue):
                profile, Ready_queue, Running_queue, log, idle_pro = scheduling(
                    profile, Ready_queue, Running_queue, log, current_time, idle_pro)
            break

    return Job_queue, profile, Ready_queue, Running_queue, log, idle_pro

""" simulate process from Ready state to Run state """
def scheduling(profile, Ready_queue, Running_queue, log, current_time, idle_pro):
    
    Ready_queue = sorted(Ready_queue, key=lambda x:x['start_t'])
    tmp = list.copy(Ready_queue)
    for job in tmp:
        if job.get('start_t') == current_time:
            job['Finish_t'] = job.get('expected finish time')
            job['Run_t'] = job.get('expected finish time') - current_time
            job['Wait_t'] = current_time - job.get('Sub_t')
            job['Waiting_rate'] = job.get('Wait_t')/job.get('Run_t')
            idle_pro -= job.get('allocated_pro')
            job['idle_pro'] = idle_pro
            Running_queue.append(job)
            Ready_queue.remove(job)
            log.append(job)

    return profile, Ready_queue, Running_queue, log, idle_pro

def utilization_based_moldable(Ready_queue, Running_queue, current_time):
    
    for job in Ready_queue:
        job['allocated_pro'] = 1
        job['T1'] = T1(job.get('Run_t'), _speedup(job.get('Req_p')))

    profile = MLS(Ready_queue, Running_queue, current_time)
    reg_profile = copy.deepcopy(profile)

    utilization = Utilization(Ready_queue, current_time)
    while True:
        modified = False
        tmp = [data.get('expected finish time') for data in profile]
        tmp1 = [data.get('start_t') for data in profile if data.get('start_t') >= current_time]
        time_instant = sorted(set(tmp)|set(tmp1))
        
        for time_t in time_instant:
            s_list = [job for job in profile if job.get('start_t') == time_t]
            np_t = resource_process - sum([data.get('allocated_pro') for data in profile if data.get('start_t') <= time_t and data.get('expected finish time') > time_t])
            if np_t > 0 and s_list:
                while np_t > 0:
                    for job in s_list:
                        job['efficiency'] = efficiency(_speedup(job.get('allocated_pro')), job.get('allocated_pro'))
                    max(s_list, key=lambda x:x.get('efficiency'))['allocated_pro']+= 1
                    np_t -= 1
                modified = True
                profile = MLS(Ready_queue, Running_queue, current_time)
                tmp_utilization = Utilization(Ready_queue, current_time)
                break

        if modified == False:
            latest_finish_time_job = max(Ready_queue, key=lambda x:x.get('expected finish time'))
            if latest_finish_time_job.get('allocated_pro') < resource_process:
                max(Ready_queue, key=lambda x:x.get('expected finish time'))['allocated_pro'] += 1
                modified = True
            profile = MLS(Ready_queue, Running_queue, current_time)
            tmp_utilization = Utilization(Ready_queue, current_time)

        if utilization >= tmp_utilization:
            profile = reg_profile
            break
        else:
            reg_profile = copy.deepcopy(profile)
            utilization = tmp_utilization

    return profile

""" simulate process from Run state to Terminated state """
def running_status(Job_queue, profile, Ready_queue, Running_queue, log, idle_pro):
    
    # 找到目前最早跑完的Job，將時間戳設成該Job Finish time
    current_time =min(Running_queue, key=lambda x:x.get('Finish_t')).get('Finish_t')

    # 利用前述的時間戳檢查在running過程中是否有Job應該到Ready_queue(Ready state)
    Job_queue, profile, Ready_queue, Running_queue, log, idle_pro = ready_status(
        Job_queue, profile, Ready_queue, Running_queue, log, current_time, idle_pro)
    current_time = min(Running_queue, key=lambda x:x.get('Finish_t')).get('Finish_t')
    Earliest_Finish_job_list = (data for data in Running_queue if data.get('Finish_t') == current_time)

    # 將最早跑完的Job從Running_queue(Run state)移除，最後歸還用完的CPU
    for job in Earliest_Finish_job_list:
        tmp1 = f"Job number: {job.get('Job_num')} terminated, return {job.get('allocated_pro')} process\n"
        tmp2 = " ".join([str(i.get('Job_num')) for i in Ready_queue if i.get('Sub_t') <= current_time])
        if tmp2 != '':
            tmp2 = f'Job number: {tmp2} in the Ready queue\n'
        s = f"{tmp1}{tmp2}{'-'*101}\n"
        log.append(s)
        Running_queue.remove(job)
        idle_pro += job.get('allocated_pro')

    return Job_queue, profile, Ready_queue, Running_queue, log, current_time, idle_pro   

if __name__ == "__main__":

    try:
        Job_queue, Ready_queue, Running_queue, profile, log = [], [], [], [], []
        resource_process = int(input("resource process:"))
        alpha = float(input("alpha:"))
        current_time, count, idle_pro = 0, 0, resource_process
        
        file_path = _getcwd() + '\\SDSC-SP2-1998-4.2-cln.swf.gz'
        with _gzopen(file_path, 'r') as fp:
            for line in fp:
                i = line.split()
                if(bytes.decode(i[0]) != ';' and (bytes.decode(i[10]) == '1' or bytes.decode(i[10]) == '0')):
                    count += 1
                    # Job Number, Submit Time, Run Time, Requested Number of Processors
                    Job_queue.append({'Job_num': int(bytes.decode(i[0])),
                                      'Sub_t': int(bytes.decode(i[1])),
                                      'Run_t': int(bytes.decode(i[3])),
                                      'Req_p': int(bytes.decode(i[7]))})
            print("total Job: %d" %count)
        
        start = _process_time()

        while True:
            Job_queue, profile, Ready_queue, Running_queue, log, idle_pro = ready_status(
                Job_queue, profile, Ready_queue, Running_queue, log, current_time, idle_pro)            
            Job_queue, profile, Ready_queue, Running_queue, log, current_time, idle_pro = running_status(
                Job_queue, profile, Ready_queue, Running_queue, log, idle_pro)
            if not Job_queue and not Running_queue and not Ready_queue:
                break
        
        func_produce_result(log, start)
      
    except:   
        _print_exc()