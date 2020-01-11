# -*- coding: utf-8 -*-
from gzip import open as _gzopen
from traceback import print_exc as _print_exc
from time import process_time as _process_time
from os import getcwd as _getcwd

"""
模擬process從Job_queue(New state) -> Waiting_queue(Ready state)
"""
def ready_status(Job_queue, Waiting_queue, Running_queue, log, current_time, idle_pro):

    # 發生在沒有Job會submit了，但Waiting_queue和Running_queue可能還有Job處理中
    if not Job_queue:
        Waiting_queue, Running_queue, log, idle_pro = FCFS_scheduling(
            Waiting_queue, Running_queue, log, current_time, idle_pro)
    # Waiting_queue和Running_queue是0，i.e., 一開始還沒有Job submit或者運行過程中剛好Job完成但還沒new Job submit的時間點，模擬直接 # 抓Job_queue第一個job丟進Waiting_queue並將時間設定成該Job的Submit Time    
    elif not Waiting_queue and not Running_queue: 
        current_time = Job_queue[0].get('Sub_t')
    # 檢查是否有這個時間點內的Job應該進Waiting_queue且已經沒有Job從Running_queue(Run state) -> Terminated state
    tmp = list.copy(Job_queue)
    c_t = current_time
    var_flag = 0
    for job in tmp:
        if job.get('Sub_t') <= c_t:
            current_time = job.get('Sub_t')
            if Running_queue and min(Running_queue, key=lambda x:x.get('Finish_t')).get('Finish_t') > current_time or not         Running_queue:
                Waiting_queue.append(job)
                Job_queue.remove(job)
                Waiting_queue, Running_queue, log, idle_pro = FCFS_scheduling(
                    Waiting_queue, Running_queue, log, current_time, idle_pro)
                var_flag = 1
            else:
                break
        else:
            if not var_flag:
                Waiting_queue, Running_queue, log, idle_pro = FCFS_scheduling(
                    Waiting_queue, Running_queue, log, current_time, idle_pro)
            break

    return Job_queue, Waiting_queue, Running_queue, log, idle_pro

"""
從Waiting_queue(Ready state) 經過scheduler確定是否可以Running_queue(Run state)，可以的話進入Running_queue，不行則是繼續放置在Waiting_queue等待下個時間點
"""
def FCFS_scheduling(Waiting_queue, Running_queue, log, current_time, idle_pro):
    
    # 開始檢查Waiting_queue的Job，將資源夠跑的Job放進Running_queue(Run state)
    tmp = list.copy(Waiting_queue)
    Waiting_queue_length = len(Waiting_queue)

    for i in range(Waiting_queue_length):
        job = tmp[i]
        if job['Req_p'] <= idle_pro:
            job['start_t'], job['Wait_t'], job['Finish_t'] = (
                current_time, current_time - job.get('Sub_t'), current_time + job.get('Run_t'))
            job['Waiting_rate'] = job.get('Wait_t')/job.get('Run_t')
            idle_pro -= job.get('Req_p')
            job['idle_pro'] = idle_pro
            Waiting_queue.remove(job)
            Running_queue.append(job)
            log.append(job) 
        else:
            break

    return Waiting_queue, Running_queue, log, idle_pro

"""
找到目前在Running_queue(Run state)中最早完成的Job並回去檢查是不是有應該在這個時間點內進入的Job，最後將最早結束的Job移除歸還process
"""
def running_status(Job_queue, Waiting_queue, Running_queue, log, idle_pro):
    
    # 找到目前最早跑完的Job，將時間戳設成該Job Finish time
    current_time = min(Running_queue, key=lambda x:x.get('Finish_t')).get('Finish_t')
    
    # 利用前述的時間戳檢查在running過程中是否有Job應該到Waiting_queue(Ready state)
    Job_queue, Waiting_queue, Running_queue, log, idle_pro = ready_status(
        Job_queue, Waiting_queue, Running_queue, log, current_time, idle_pro)
    current_time = min(Running_queue, key=lambda x:x.get('Finish_t')).get('Finish_t')
    Earliest_Finish_job_list = (data for data in Running_queue if data.get('Finish_t') == current_time)
    
    # 將最早跑完的Job從Running_queue(Run state)移除，最後歸還用完的CPU
    for job in Earliest_Finish_job_list:
        Job_num, Req_p = job.get('Job_num'), job.get('Req_p')
        tmp1 = f'Job number: {Job_num} terminated, return {Req_p} process\n'
        tmp2 = " ".join([str(i.get('Job_num')) for i in Waiting_queue if i.get('Sub_t') <= current_time])
        if tmp2 != '':
            tmp2 = f'Job number: {tmp2} in the Waiting queue\n'
        s = f"{tmp1}{tmp2}{'-'*101}\n"
        log.append(s)
        Running_queue.remove(job)
        idle_pro += job.get('Req_p')
        
    return Job_queue, Waiting_queue, Running_queue, log, current_time, idle_pro   

if __name__ == "__main__":

    try:
        # initalize，log list是用來紀錄執行過程
        Job_queue, Running_queue, Waiting_queue, log = [], [], [], []
        current_time, count, Total_wait_t, Total_wait_rate, Total_turnaround_t = 0, 0, 0, 0, 0

        idle_pro = int(input("resource process:"))
        # 讀檔
        file_path = _getcwd() + '\\SDSC-SP2-1998-4.2-cln.swf.gz'
        with _gzopen(file_path, 'r') as fp:
            for line in fp:
                i = line.split()
                if(bytes.decode(i[0]) != ';' and (bytes.decode(i[10]) == '1' or bytes.decode(i[10]) == '0')):
                    count += 1
                    # Job Number, Submit Time, Run Time, Requested Number of Processors
                    Job_queue.append(
                        {
                            'Job_num': int(bytes.decode(i[0])),
                            'Sub_t': int(bytes.decode(i[1])),
                            'Run_t': int(bytes.decode(i[3])),
                            'Req_p': int(bytes.decode(i[7]))
                        }
                    )

        print(f'Total Job: {count}')
        
        start = _process_time()

        while True:
            Job_queue, Waiting_queue, Running_queue, log, idle_pro = ready_status(
                Job_queue, Waiting_queue, Running_queue, log, current_time, idle_pro)            
            Job_queue, Waiting_queue, Running_queue, log, current_time, idle_pro = running_status(
                Job_queue, Waiting_queue, Running_queue, log, idle_pro)
            if not Job_queue and not Waiting_queue and not Running_queue:
                break
        
        # 輸出log list
        with open('FCFS exce Log.txt', 'w') as wp:
            for job in log:
                if isinstance(job, str):
                    wp.write(job)
                else:
                    Total_wait_t += job.get('Wait_t')
                    Total_wait_rate += job.get('Waiting_rate')
                    Total_turnaround_t = Total_turnaround_t + job.get('Wait_t') + job.get('Run_t')
                    wp.write(
                        f"Job number: {job.get('Job_num')}\nSubmit Time: {job.get('Sub_t')} "\
                        f"Wait Time: {job.get('Wait_t')} Start Time: {job.get('start_t')} "\
                        f"Run Time: {job.get('Run_t')} Finish Time: {job.get('Finish_t')}\n"\
                        f"Requested Number of Processors: {job.get('Req_p')} Idle Process: {job.get('idle_pro')}\n"\
                        f"{'-'*101}\n"
                    )

        print(
            f'spend {_process_time() - start} seconds\nTotal waiting time: {Total_wait_t}\n'\
            f'Total waiting rate:{Total_wait_rate}\nAverage waiting time: {Total_wait_t/count}\n'\
            f'Average waiting rate: {Total_wait_rate/count}\nAverage turnaround time: {Total_turnaround_t/count}'
        )
    except:
        _print_exc()