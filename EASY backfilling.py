# -*- coding: utf-8 -*-
from gzip import open as _gzopen
from traceback import print_exc as _print_exc
from time import process_time as _process_time
from os import getcwd as _getcwd
from threading import Thread as _Thread

""" write the Log file and print Effectiveness """
def func_produce_result(li, start, back_job_count=0):

    Total_wait_t, Total_wait_rate, Total_turnaround_t = 0, 0, 0
    with open('EASY_Backfilling exce Log.txt', 'w') as wp:
        for job in li:
            if isinstance(job, str):
                wp.write(job)
            else:
                Total_wait_t += job.get('Wait_t')
                Total_wait_rate += job.get('Waiting_rate')
                Total_turnaround_t = Total_turnaround_t + job.get('Wait_t') + job.get('Run_t')
                wp.write(f"Job number: {job.get('Job_num')}\nSubmit Time: {job.get('Sub_t')} "\
                    f"Wait Time: {job.get('Wait_t')} Start Time: {job.get('start_t')} Run Time: {job.get('Run_t')}"\
                    f"Finish Time: {job.get('Finish_t')}\nRequested Time: {job.get('Req_T')} "\
                    f"Predict Time: {job.get('Pred_T')} Requested Number of Processors: {job.get('Req_p')}\n"\
                    f"Idle Process: {job.get('idle_pro')}\n{'-'*101}\n"
                )
        print(f'spend {_process_time() - start} seconds\nTotal waiting time: {Total_wait_t}\n'\
            f'Total waiting rate: {Total_wait_rate}\nAverage waiting time: {Total_wait_t/count}\n'\
            f'Average waiting rate: {Total_wait_rate/count}\nBackfilling Job number: {back_job_count}\n'\
            f'Average turnaround time: {Total_turnaround_t/count}\n'
        )

def Sort(sub_li, key):

    return(sorted(sub_li, key = lambda x: x[key]))

""" simulate process from Job_queue (New state) to Ready_queue (Ready state) """
def ready_status(Job_queue, Ready_queue, Running_queue, log, current_time, idle_pro):

    # 發生在沒有Job會submit了，但Ready_queue和Running_queue可能還有Job處理中
    if not Job_queue:
        Ready_queue, Running_queue, log, idle_pro = FCFS_scheduling(
            Ready_queue, Running_queue, log, current_time, idle_pro)
    # Ready_queue和Running_queue是0，i.e., 一開始還沒有Job submit或者運行過程中剛好Job完成但還沒new Job submit的時間點，模擬直接 # 抓Job_queue第一個job丟進Ready_queue並將時間設定成該Job的Submit Time    
    elif not Ready_queue and not Running_queue: 
        current_time = Job_queue[0].get('Sub_t')
    # 檢查是否有這個時間點內的Job應該進Ready_queue且已經沒有Job從Running_queue(Run state) -> Terminated state
    tmp = list.copy(Job_queue)
    c_t = current_time
    var_flag = 0
    for job in tmp:
        if job.get('Sub_t') <= c_t:
            current_time = job.get('Sub_t')
            if Running_queue and min(Running_queue, key=lambda x:x.get('Finish_t')).get('Finish_t') > current_time or not Running_queue:
                Ready_queue.append(job)
                Job_queue.remove(job)
                Ready_queue, Running_queue, log, idle_pro = FCFS_scheduling(
                    Ready_queue, Running_queue, log, current_time, idle_pro)
                var_flag = 1
            else:
                break
        else:
            if not var_flag:
                Ready_queue, Running_queue, log, idle_pro = FCFS_scheduling(
                    Ready_queue, Running_queue, log, current_time, idle_pro)
            break

    return Job_queue, Ready_queue, Running_queue, log, idle_pro

""" simulate process from Ready state to Run state """
def FCFS_scheduling(Ready_queue, Running_queue, log, current_time, idle_pro):
    
    # 開始檢查Ready_queue的Job，將資源夠跑的Job放進Running_queue(Run state)
    tmp = list.copy(Ready_queue)

    for job in tmp:
        if job['Req_p'] <= idle_pro:
            job['start_t'], job['Pred_T'], job['Wait_t'], job['Finish_t'] = (
                current_time, current_time + job.get('Req_T'), current_time - job.get('Sub_t'), current_time + job.get('Run_t'))
            job['Waiting_rate'] = job.get('Wait_t')/job.get('Run_t')
            idle_pro -= job.get('Req_p')
            job['idle_pro'] = idle_pro
            Ready_queue.remove(job)
            Running_queue.append(job)
            log.append(job)
        # 因為是順序執行因此遇到第一個無法執行的call EASY_Backfilling
        else:
            Ready_queue, Running_queue, log, idle_pro = EASY_Backfilling(
                Ready_queue, Running_queue, log, current_time, idle_pro)
            break

    return Ready_queue, Running_queue, log, idle_pro

"""
EASY Backfilling Algorithm
"""
def EASY_Backfilling(Ready_queue, Running_queue, log, current_time, idle_pro):

    tmp = list.copy(Ready_queue)
    Ready_queue_length = len(Ready_queue)
    currently_free_nodes = idle_pro
    Running_queue = Sort(Running_queue, 'Pred_T')
    global backfilling_job_count

    for job in Running_queue:
        currently_free_nodes += job.get('Req_p')
        if currently_free_nodes >= Ready_queue[0].get('Req_p') :
            extra_nodes = currently_free_nodes - Ready_queue[0].get('Req_p')
            Ready_queue[0]['shadow_time'] = job.get('Pred_T')
            break

    for i in range(1, Ready_queue_length):
        job = tmp[i]
        if min(Running_queue, key=lambda x:x.get('Finish_t')).get('Finish_t') <= current_time:
            break
        elif job['Req_p'] <= idle_pro:
            job['Pred_T'] = current_time + job.get('Req_T')
            if job.get('Pred_T') <= Ready_queue[0].get('shadow_time'):
                pass
            elif job.get('Req_p') <= extra_nodes:
                extra_nodes -= job.get('Req_p')
            else:
                continue

            job['start_t'], job['Wait_t'], job['Finish_t'] = (
                current_time, current_time - job.get('Sub_t'), current_time + job.get('Run_t'))
            job['Waiting_rate'] = job.get('Wait_t')/job.get('Run_t')
            idle_pro -= job.get('Req_p')
            job['idle_pro'] = idle_pro
            Ready_queue.remove(job)
            Running_queue.append(job)
            log.append(job)
            backfilling_job_count += 1
        else:
            continue

    return Ready_queue, Running_queue, log, idle_pro

""" simulate process from Run state to Terminated state """
def running_status(Job_queue, Ready_queue, Running_queue, log, idle_pro):
    
    # 找到目前最早跑完的Job，將時間戳設成該Job Finish time
    current_time =  min(Running_queue, key=lambda x:x.get('Finish_t')).get('Finish_t')
    
    # 利用前述的時間戳檢查在running過程中是否有Job應該到Ready_queue(Ready state)
    Job_queue, Ready_queue, Running_queue, log, idle_pro = ready_status(
        Job_queue, Ready_queue, Running_queue, log, current_time, idle_pro)
    current_time =  min(Running_queue, key=lambda x:x.get('Finish_t')).get('Finish_t')
    Earliest_Finish_job_list = (data for data in Running_queue if data.get('Finish_t') == current_time)
    
    # 將最早跑完的Job從Running_queue(Run state)移除，最後歸還用完的CPU
    for job in Earliest_Finish_job_list:
        Job_num, Req_p = job.get('Job_num'), job.get('Req_p')
        tmp1 = f'Job number: {Job_num} terminated, return {Req_p} process\n'
        tmp2 = " ".join([str(i.get('Job_num')) for i in Ready_queue if i.get('Sub_t') <= current_time])
        if tmp2 != '':
            tmp2 = f'Job number: {tmp2} in the Ready queue\n'
        s = f"{tmp1}{tmp2}{'-'*101}\n"
        log.append(s)
        Running_queue.remove(job)
        idle_pro += job.get('Req_p')

    return Job_queue, Ready_queue, Running_queue, log, current_time, idle_pro   

if __name__ == "__main__":

    try:
        # initalize，log list是用來紀錄執行過程
        Job_queue, Running_queue, Ready_queue, log = [], [], [], []        
        current_time, count, backfilling_job_count = 0, 0, 0

        idle_pro = int(input("resource process:"))
        # 讀檔
        file_path = _getcwd() + '\\SDSC-SP2-1998-4.2-cln.swf.gz'
        with _gzopen(file_path, 'r') as fp:
            for line in fp:
                i = line.split()
                if(bytes.decode(i[0]) != ';' and (bytes.decode(i[10]) == '1' or bytes.decode(i[10]) == '0')):
                    count += 1
                    # Job Number, Submit Time, Run Time, Requested Number of Processors, Requested Time
                    Job_queue.append(
                        {
                            'Job_num': int(bytes.decode(i[0])),
                                      'Sub_t': int(bytes.decode(i[1])),
                                      'Run_t': int(bytes.decode(i[3])),
                                      'Req_p': int(bytes.decode(i[7])),
                                      'Req_T': int(bytes.decode(i[8]))
                        }
                    )

        print(f'Total Job: {count}')
        
        start = _process_time()

        while True:
            Job_queue, Ready_queue, Running_queue, log, idle_pro = ready_status(
                Job_queue, Ready_queue, Running_queue, log, current_time, idle_pro)            
            Job_queue, Ready_queue, Running_queue, log, current_time, idle_pro = running_status(
                Job_queue, Ready_queue, Running_queue, log, idle_pro)
            if not Job_queue and not Ready_queue and not Running_queue:
                break
        
        func_produce_result(log, start, backfilling_job_count)

    except:
        _print_exc()