# -*- coding: utf-8 -*-
from gzip import open as _gzopen
from traceback import print_exc as _print_exc
from time import process_time as _process_time
import os
import threading

def _IOwirte(li):

    with open('Utilization based moldable job scheduling exce Log.txt', 'w') as wp:
        for job in li:
            if isinstance(job, str):
                wp.write(job)
            else:
                wp.write("Job number: %d\n"\
                        "Submit Time: %d Wait Time: %d Start Time: %d Run Time: %d Finish Time: %d\n"\
                        "Idle Process: %d\n"\
                        "-----------------------------------------------------------------------------------------------------\n"\
                        %(job['Job_num'], job['Sub_t'], job['Wait_t'], job['start_t'], job['Run_t'], job['Finish_t'], job['idle_pro']))

def Print_Screen(li, Total_wait_t, Total_wait_rate, start):

    for job in li:
        if isinstance(job, str):
            continue
        else:
            Total_wait_t += job['Wait_t']
            Total_wait_rate += job['Waiting_rate']
            
    print(f'spend {_process_time() - start} seconds\nTotal waiting time: {Total_wait_t}\nTotal waiting rate: {Total_wait_rate}\n'\
            f'Average waiting time: {Total_wait_t/count}\nAverage waiting rate: {Total_wait_rate/count}\n')

def Sort(sub_li, key):
    return(sorted(sub_li, key = lambda x: x[key]))

def _speedup(n):
    global alpha
    return 1/((1-alpha)+alpha/n)

def T1(Tn, speedup):
    return Tn*speedup

def efficiency(speedup, n):
    return speedup/n

def Utilization(profile, current_time):
    return sum([(i['Finish_t'] - current_time)*i['allocated_pro']*efficiency(_speedup(n), n)\
    if i['start_t'] < current_time else (i['expected finish time'] - i['start_t'])*i['allocated_pro']*efficiency(_speedup(n), n)  for i in profile])\
    (128*(max(profile, key = lambda x:x['expected finish time']) - current_time))

def MLS(Waiting_queue, profile):
    
    profile_length = len(profile)
    tmp = list.copy(Waiting_queue)
    if profile_length != 0:
        profile = Sort(profile, 'expected finish time')        
        time_instant = [data['expected finish time'] for data in profile]    
    else:
        time_instant = [Waiting_queue['Sub_t']]

    idle_pro = [{i :128 - sum([data['allocated_pro'] for data in profile if data.get('start_t') <= i and data.get('expected finish time') > i])} for i in s_list]    
        
    for i in range(len(idle_pro)):
        process = idle_pro[i][time_instant[i]]
        for job in tmp:
            if job['allocated_pro'] <= process:
                job['start_t'], job['expected finish time'] = instant, instant + job['T1']/_speedup(job['allocated_pro'])
                process -= job['allocated_pro']
                idle_pro.append({job['expected finish time'] : 128 - sum([data['allocated_pro'] for data in profile if data.get('start_t') <= job['expected finish time'] and data.get('expected finish time') > job['expected finish time']])})
                profile.append(job)
                Waiting_queue.remove(job)
                idle_pro = Sort(idle_pro, 'expected finish time') 
        if len(Waiting_queue) != 0:
            tmp = list.copy(Waiting_queue)
        else:
            break
        
    return profile

"""
模擬process從Job_queue(New state) -> Waiting_queue(Ready state)
"""
def ready_status(Job_queue, profile, Running_queue, log, current_time, idle_pro):

    if len(Job_queue) == 0:
        profile, Running_queue, log, idle_pro = FCFS_scheduling(profile, Running_queue, log, current_time, idle_pro)
    elif len(profile) == 0 and len(Running_queue) == 0: 
        current_time = Job_queue[0]['Sub_t']
    tmp = list.copy(Job_queue)
    c_t = current_time
    for job in tmp:
        if job['Sub_t'] <= c_t:
            current_time = job['Sub_t']
            if len(Running_queue) != 0 and min(Running_queue, key=lambda x:x['Finish_t'])['Finish_t'] > current_time or len(Running_queue) == 0:
                Waiting_queue.append(job)
                Job_queue.remove(job)
                profile = utilization_based_moldable(Waiting_queue, Running_queue, current_time)
                # 目前到這
                profile, Running_queue, log, idle_pro = FCFS_scheduling(profile, Running_queue, log, current_time, idle_pro)
            else:
                break
        else:
            profile, Running_queue, log, idle_pro = FCFS_scheduling(profile, Running_queue, log, current_time, idle_pro)
            break

    return Job_queue, profile, Running_queue, log, idle_pro

"""
從(Waiting_queue) -> schedular dispatch 確定是否可以Running，可以的話進入Running_queue，不行則是繼續放置在Waiting_queue等待下個時間點
"""
def FCFS_scheduling(profile, Running_queue, log, current_time, idle_pro):
    
    tmp = list.copy(profile)

    for i in range(profile_length):
        job = tmp[i]
        if job['Req_p'] <= idle_pro and job['Finish_t'] == 0:
            job['start_t'], job['Pred_T'], job['Wait_t'], job['Finish_t'] = current_time, current_time + job['Req_T'], current_time - job['Sub_t'], current_time + job['Run_t']
            job['Waiting_rate'] = job['Wait_t']/job['Run_t']
            idle_pro -= job['Req_p']
            job['idle_pro'] = idle_pro
            Running_queue.append(job)
            log.append(job)
        else:
            break

    return profile, Running_queue, log, idle_pro

def utilization_based_moldable(Waiting_queue, Running_queue, current_time):

    for job in Waiting_queue:
        job['allocated_pro'] = 1
        job['T1'] = T1(job['Run_t'], speedup(job['Req_p']))
        
    profile = MLS(Waiting_queue, Running_queue)
    utilization = Utilization(profile, current_time)
    while True:
        modified = False;
        tmp = [data['expected finish time'] for data in profile]
        time_instant = sorted(set(tmp), key = tmp.index)
        for time_t in time_instant:
            s_list = [data for data in profile if data['start_t'] == time_t]
            np_t = 128 - sum([data['allocated_pro'] for data in profile if data['start_t'] < time_t and data['expected finish time'] > time_t])
            if np_t > 0 and len(s_list) != 0:
                while np_t > 0:
                    for job in s_list:
                        job['efficiency'] = efficiency(_speedup(n), n)
                    max_efficiency_job = max(s_list, key = lambda x:x['efficiency'])
                    max_efficiency_job['allocated_pro'] += 1
                    np_t -= 1
                modified = True;

        if modified = False:
            latest_finish_time_job = max(profile, key = lambda x:x['expected finish time'])
            latest_finish_time_job['allocated_pro'] += 1
        
        tmp_profile = MLS(Waiting_queue, Running_queue)
        tmp_utilization = Utilization(profile, current_time)
        
        if utilization >= tmp_utilization:
            break
        else:
            profile = tmp_profile
            utilization = tmp_utilization

    return profile

def running_status(Job_queue, profile, Running_queue, log, idle_pro):
    
    # 找到開始run job中最早跑完的，現在時間就是這個時間點
    min_Job = min(Running_queue, key=lambda x:x['Finish_t'])
    current_time = min_Job['Finish_t']
    
    # 知道目前第一個跑完的job finish時間點回去檢查在他running過程中是否有job應該到Waiting_queue接著排程看會不會job可以進running
    Job_queue, profile, Running_queue, log, idle_pro = ready_status(Job_queue, profile, Running_queue, log, current_time, idle_pro)
    min_Job = min(Running_queue, key=lambda x:x['Finish_t'])
    current_time = min_Job['Finish_t']
    Earliest_Finish_job_list = [data for data in Running_queue if data.get('Finish_t') == current_time]
    
    # 將最早跑完的job從Running_queue移除，最後歸還用完的CPU
    for job in Earliest_Finish_job_list:
        Job_num, Req_p = job['Job_num'], job['Req_p']
        tmp1 = f'Job number: {Job_num} terminated, return {Req_p} process\n'
        tmp2 = " ".join([str(i['Job_num']) for i in profile if i['Sub_t'] <= current_time and i['Finish_t'] != 0])
        if tmp2 != '':
            tmp2 = f'Job number: {tmp2} in the Waiting queue\n'
        s = f'{tmp1}{tmp2}-----------------------------------------------------------------------------------------------------\n'
        log.append(s)
        Running_queue.remove(job)
        profile.remove(job)
        idle_pro += job['Req_p']

    return Job_queue, profile, Running_queue, log, current_time, idle_pro   

if __name__ == "__main__":

    try:
        Job_queue = []
        Running_queue = []
        profile = []
        log = []

        idle_pro = int(input("resource process:"))
        alpha = float(input("alpha:"))
        current_time, count, Total_wait_t, Total_wait_rate, back_job_count = 0, 0, 0, 0, 0
        # 讀檔
        file_path = os.getcwd() + '\SDSC-SP2-1998-4.2-cln.swf.gz'
        with _gzopen(file_path, 'r') as fp:
            for line in fp:
                i = line.split()
                if(bytes.decode(i[0]) != ';' and (bytes.decode(i[10]) == '1' or bytes.decode(i[10]) == '0')):
                    count += 1
                    # Job Number, Submit Time, Run Time, Requested Number of Processors
                    Job_queue.append({'Job_num': int(bytes.decode(i[0])),
                                      'Sub_t': int(bytes.decode(i[1])),
                                      'Run_t': int(bytes.decode(i[3])),
                                      'Req_p': int(bytes.decode(i[7])),
                                      'Finish_t': 0})
            print("total Job: %d" %count)
        
        start = _process_time()

        while True:
            Job_queue, profile, Running_queue, log, idle_pro = ready_status(Job_queue, profile, Running_queue, log, current_time, idle_pro)            
            Job_queue, profile, Running_queue, log, current_time, idle_pro = running_status(Job_queue, profile, Running_queue, log, idle_pro)
            if len(Job_queue) == 0 and len(profile) == 0 and len(Running_queue) == 0:
                break
        
        thread1 = threading.Thread(target = _IOwirte, args = (log,))
        thread2 = threading.Thread(target = Print_Screen, args = (log, Total_wait_t, Total_wait_rate, start, back_job_count))

        thread1.start()
        thread2.start()

        thread1.join()
        thread2.join()
      
    except:
        _print_exc()
