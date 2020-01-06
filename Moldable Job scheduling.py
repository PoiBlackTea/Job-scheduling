# -*- coding: utf-8 -*-
from gzip import open as _gzopen
from traceback import print_exc as _print_exc
from time import process_time as _process_time
from os import getcwd as _getcwd
from threading import Thread as _Thread
import copy

def _IOwirte(li):

    with open('Utilization based moldable job scheduling exce Log.txt', 'w') as wp:
        for job in li:
            if isinstance(job, str):
                wp.write(job)
            else:
                wp.write("Job number: %d\n"\
                        "Submit Time: %d Wait Time: %d Start Time: %d Run Time: %d Finish Time: %d\n"\
                        "Idle Process: %d Allocated Process: %d\n"\
                        "-----------------------------------------------------------------------------------------------------\n"\
                        %(job['Job_num'], job['Sub_t'], job['Wait_t'], job['start_t'], job['Run_t'], job['Finish_t'], job['idle_pro'], job['allocated_pro']))

def Print_Screen(li, start):

    Total_wait_t, Total_wait_rate, Total_turnaround_t = 0, 0, 0
    for job in li:
        if isinstance(job, str):
            continue
        else:
            Total_wait_t += job['Wait_t']
            Total_wait_rate += job['Waiting_rate']
            Total_turnaround_t = Total_turnaround_t + job['Wait_t'] + job['Run_t']
            
            
    print(f'spend {_process_time() - start} seconds\nTotal waiting time: {Total_wait_t}\nTotal waiting rate: {Total_wait_rate}\n'\
            f'Average waiting time: {Total_wait_t/count}\nAverage waiting rate: {Total_wait_rate/count}\n'\
            f'Average turnaround time: {Total_turnaround_t/count}')

def Sort(sub_li, key):
    return(sorted(sub_li, key = lambda x: x[key]))

def _speedup(n):
    global alpha
    return 1/((1-alpha)+alpha/n)

def T1(Tn, speedup):
    return Tn*speedup

def efficiency(speedup, n):
    return speedup/n

def Utilization(Waiting_queue, current_time):
    tmp = 0
    for i in Waiting_queue:
            ti = i.get('T1')/_speedup(i.get('allocated_pro'))
            tmp += ti*i.get('allocated_pro')*efficiency(_speedup(i.get('allocated_pro')), i.get('allocated_pro'))
    tmp1 = resource_process*(max(Waiting_queue, key = lambda x:x.get('expected finish time')).get('expected finish time') - current_time)
    return tmp/tmp1

def MLS(waiting_queue, running_queue, current_time):
  
    waiting_queue_length = len(waiting_queue)
    tmp = list.copy(running_queue)
    profile = list.copy(running_queue)
    current_idle_pro = resource_process
    
    if not running_queue:
        pass
    else:
        current_idle_pro =  current_idle_pro - sum([job.get('allocated_pro')  for job in running_queue  if job.get('start_t') <= current_time])
    i = 0
    while True:
        if waiting_queue[i].get('allocated_pro') <= current_idle_pro and (tmp and min(tmp, key=lambda x:x.get('expected finish time'))['expected finish time'] > current_time or not tmp):
            waiting_queue[i]['start_t'], waiting_queue[i]['expected finish time'] = current_time, current_time + waiting_queue[i].get('T1')/_speedup(waiting_queue[i].get('allocated_pro'))
            current_idle_pro -= waiting_queue[i].get('allocated_pro')
            profile.append(waiting_queue[i])
            tmp.append(waiting_queue[i])
            if i == waiting_queue_length-1:
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

"""
模擬process從Job_queue(New state) -> Waiting_queue(Ready state)
"""
def ready_status(Job_queue, profile, Waiting_queue, Running_queue, log, current_time, idle_pro):

    if not Job_queue:
        profile, Waiting_queue, Running_queue, log, idle_pro = scheduling(profile, Waiting_queue, Running_queue, log, current_time, idle_pro)
    elif not Waiting_queue and not Running_queue: 
        current_time = Job_queue[0].get('Sub_t')
    tmp = list.copy(Job_queue)
    c_t = current_time
    flag = 0
    for job in tmp:
        if  job.get('Sub_t') <= c_t:
            current_time = job.get('Sub_t')
            if Running_queue and min(Running_queue, key=lambda x:x.get('Finish_t')).get('Finish_t') > current_time or not Running_queue:
                Waiting_queue.append(job)
                Job_queue.remove(job)
                flag = 1
                profile = utilization_based_moldable(Waiting_queue, Running_queue, current_time)
                profile, Waiting_queue, Running_queue, log, idle_pro = scheduling(profile, Waiting_queue, Running_queue, log, current_time, idle_pro)
            else:
                break
        else:
            if flag == 0:
                profile, Waiting_queue, Running_queue, log, idle_pro = scheduling(profile, Waiting_queue, Running_queue, log, current_time, idle_pro)
            break

    return Job_queue, profile, Waiting_queue, Running_queue, log, idle_pro

"""
從(Waiting_queue) -> schedular dispatch 確定是否可以Running，可以的話進入Running_queue，不行則是繼續放置在Waiting_queue等待下個時間點
"""
def scheduling(profile, Waiting_queue, Running_queue, log, current_time, idle_pro):

    profile_length = len(profile)
    # scheduling 後可能出現是要先等job finish後再開始的情況
    if Running_queue and min(Running_queue, key=lambda x:x.get('Finish_t')).get('Finish_t') > current_time or not Running_queue:
        for i in range(profile_length):
            if  profile[i].get('start_t') ==  current_time and profile[i] not in Running_queue:
                profile[i]['Finish_t'], profile[i]['Run_t'] = profile[i].get('expected finish time'),  profile[i].get('expected finish time') - current_time
                profile[i]['Wait_t'] = current_time - profile[i].get('Sub_t')
                profile[i]['Waiting_rate'] = profile[i].get('Wait_t')/profile[i].get('Run_t')
                idle_pro -= profile[i].get('allocated_pro')
                profile[i]['idle_pro'] = idle_pro
                Running_queue.append(profile[i])
                Waiting_queue.remove(next((item for item in Waiting_queue if item.get("Job_num") == profile[i].get('Job_num')), None))
                log.append(profile[i])
                # print(profile[i])

    return profile, Waiting_queue, Running_queue, log, idle_pro

def utilization_based_moldable(Waiting_queue, Running_queue, current_time):
    
    Waiting_queue_length = len(Waiting_queue)
    for i in range(Waiting_queue_length):
        Waiting_queue[i]['allocated_pro'] = 1
        Waiting_queue[i]['T1'] = T1(Waiting_queue[i].get('Run_t'), _speedup(Waiting_queue[i].get('Req_p')))

    profile = MLS(Waiting_queue, Running_queue, current_time)
    reg_profile = [copy.deepcopy(i) for i in profile]

    utilization = Utilization(Waiting_queue, current_time)
    while True:
        modified = False
        tmp = [data.get('expected finish time') for data in profile]
        tmp1 = [data.get('start_t') for data in profile if data.get('start_t') >= current_time]
        time_instant = sorted(set(tmp)|set(tmp1))
        
        for time_t in time_instant:
            s_list = [profile[i] for i in range(len(profile)) if profile[i].get('start_t') == time_t]
            np_t = resource_process - sum([data.get('allocated_pro') for data in profile if data.get('start_t') <= time_t and data.get('expected finish time') > time_t])
            if np_t > 0 and s_list:
                while np_t > 0:
                    for i in range(len(s_list)):
                        s_list[i]['efficiency'] = efficiency(_speedup(s_list[i].get('allocated_pro')), s_list[i].get('allocated_pro'))
                    max(s_list, key = lambda x:x.get('efficiency'))['allocated_pro']+= 1
                    np_t -= 1
                modified = True
                profile = MLS(Waiting_queue, Running_queue, current_time)
                tmp_utilization = Utilization(profile, current_time)
                break

        if modified == False:
            latest_finish_time_job = max(Waiting_queue, key = lambda x:x.get('expected finish time'))
            if latest_finish_time_job.get('allocated_pro') < resource_process:
                max(Waiting_queue, key = lambda x:x.get('expected finish time'))['allocated_pro'] += 1
                modified = True
            profile = MLS(Waiting_queue, Running_queue, current_time)
            tmp_utilization = Utilization(Waiting_queue, current_time)

        if utilization >= tmp_utilization:
            profile = reg_profile
            break
        else:
            reg_profile = [copy.deepcopy(i) for i in profile]
            utilization = tmp_utilization

    return profile

def running_status(Job_queue, profile, Waiting_queue, Running_queue, log, idle_pro):
    
    # 找到開始run job中最早跑完的，現在時間就是這個時間點
    current_time =min(Running_queue, key=lambda x:x.get('Finish_t')).get('Finish_t')

    # 知道目前第一個跑完的job finish時間點回去檢查在他running過程中是否有job應該到Waiting_queue接著排程看會不會job可以進running
    Job_queue, profile, Waiting_queue, Running_queue, log, idle_pro = ready_status(Job_queue, profile, Waiting_queue, Running_queue, log, current_time, idle_pro)
    min_Job = min(Running_queue, key=lambda x:x.get('Finish_t'))
    current_time = min_Job.get('Finish_t')
    Earliest_Finish_job_list = (data for data in Running_queue if data.get('Finish_t') == current_time)

    # 將最早跑完的job從Running_queue移除，最後歸還用完的CPU
    for job in Earliest_Finish_job_list:
        Job_num, allocated_pro = job['Job_num'], job['allocated_pro']
        tmp1 = f'Job number: {Job_num} terminated, return {allocated_pro} process\n'
        tmp2 = " ".join([str(i.get('Job_num')) for i in Waiting_queue])
        if tmp2 != '':
            tmp2 = f'Job number: {tmp2} in the Waiting queue\n'
        s = f'{tmp1}{tmp2}-----------------------------------------------------------------------------------------------------\n'
        log.append(s)
        if job.get('idle_pro') > resource_process or  job.get('Wait_t') < 0:
            print('error')
        Running_queue.remove(job)
        profile.remove(job)
        idle_pro += job.get('allocated_pro')

    return Job_queue, profile, Waiting_queue, Running_queue, log, current_time, idle_pro   

if __name__ == "__main__":

    try:
        Job_queue, Waiting_queue, Running_queue, profile, log = [], [], [], [], []
        resource_process = int(input("resource process:"))
        alpha = float(input("alpha:"))
        current_time, count, idle_pro = 0, 0, resource_process
        # 讀檔
        file_path = _getcwd() + '\SDSC-SP2-1998-4.2-cln.swf.gz'
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
            Job_queue, profile, Waiting_queue, Running_queue, log, idle_pro = ready_status(Job_queue, profile, Waiting_queue, Running_queue, log, current_time, idle_pro)            
            Job_queue, profile, Waiting_queue, Running_queue, log, current_time, idle_pro = running_status(Job_queue, profile, Waiting_queue, Running_queue, log, idle_pro)
            if not Job_queue and not Running_queue and not Waiting_queue:
                break
        
        thread1 = _Thread(target = _IOwirte, args = (log,))
        thread2 = _Thread(target = Print_Screen, args = (log, start))

        thread1.start()
        thread2.start()

        thread1.join()
        thread2.join()
      
    except:   
        _print_exc()