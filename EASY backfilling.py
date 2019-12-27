# -*- coding: utf-8 -*-
from gzip import open as _gzopen
from traceback import print_exc as _print_exc
from time import process_time as _process_time
from os import getcwd as _getcwd
from threading import Thread as _Thread

def _IOwirte(li):

    with open('EASY_Backfilling exce Log.txt', 'w') as wp:
        for job in li:
            if isinstance(job, str):
                wp.write(job)
            else:
                wp.write("Job number: %d\n"\
                        "Submit Time: %d Wait Time: %d Start Time: %d Run Time: %d Finish Time: %d\n"\
                        "Requested Time: %d Predict Time: %d Requested Number of Processors: %d\n"\
                        "Idle Process: %d\n"\
                        "-----------------------------------------------------------------------------------------------------\n"\
                        %(job['Job_num'], job['Sub_t'], job['Wait_t'], job['start_t'], job['Run_t'], job['Finish_t'], job['Req_T'], job['Pred_T'], job['Req_p'], job['idle_pro']))

def Print_Screen(li, Total_wait_t, Total_wait_rate, start, back_job_count):

    for job in li:
        if isinstance(job, str):
            continue
        else:
            Total_wait_t += job['Wait_t']
            Total_wait_rate += job['Waiting_rate']
            
    print(f'spend {_process_time() - start} seconds\nTotal waiting time: {Total_wait_t}\nTotal waiting rate: {Total_wait_rate}\n'\
            f'Average waiting time: {Total_wait_t/count}\nAverage waiting rate: {Total_wait_rate/count}\nBackfilling Job number: {back_job_count}\n')

def Sort(sub_li, key):
    return(sorted(sub_li, key = lambda x: x[key]))

"""
call這個function 在以下幾種狀態 
1.模擬一開始process or job 從Job_queue(New state) -> ready status(Waiting_queue)
2.每當有job 從running status -> terminated status 檢查這個時間內是否有job 應該從Job_queue(New state) -> ready status(Waiting_queue) -> schedular dispatch
以上兩個動作最後皆需排程
"""
def ready_status(Job_queue, Waiting_queue, Running_queue, log, current_time, idle_pro):

    if len(Job_queue) == 0:
        Waiting_queue, Running_queue, log, idle_pro = FCFS_scheduling(Waiting_queue, Running_queue, log, current_time, idle_pro)
    # Waiting_queue和Running_queue是0，i.e., 一開始還沒有job submit或者運行過程中剛好job完成但還沒new job submit的時間點，模擬直接抓Job_queue第一個job丟進Waiting_queue並將時間設定成該job的Submit Time
    # 第一種狀態可能發生的情形
    elif len(Waiting_queue) == 0 and len(Running_queue) == 0: 
        current_time = Job_queue[0]['Sub_t']
    # 檢查是否有這個時間段內的job應該進Waiting_queue
    # 第一種狀態檢查是否有同時間submit的job
    # 第二種可能發生的情形
    tmp = list.copy(Job_queue)
    c_t = current_time
    for job in tmp:
        if job['Sub_t'] <= c_t:
            current_time = job['Sub_t']
            if len(Running_queue) != 0 and min(Running_queue, key=lambda x:x['Finish_t'])['Finish_t'] > current_time or len(Running_queue) == 0:
                Waiting_queue.append(job)
                Job_queue.remove(job)
                Waiting_queue, Running_queue, log, idle_pro = FCFS_scheduling(Waiting_queue, Running_queue, log, current_time, idle_pro)
            else:
                break
        else:
            Waiting_queue, Running_queue, log, idle_pro = FCFS_scheduling(Waiting_queue, Running_queue, log, current_time, idle_pro)
            break

    return Job_queue, Waiting_queue, Running_queue, log, idle_pro

"""
從(Waiting_queue) -> schedular dispatch 確定是否可以Running，可以的話進入Running_queue，不行則是繼續放置在Waiting_queue等待下個時間點
"""
def FCFS_scheduling(Waiting_queue, Running_queue, log, current_time, idle_pro):
    
    # 開始檢查Waiting_queue的job，資源夠跑且時間符合的的job放進Running_queue
    tmp = list.copy(Waiting_queue)
    Waiting_queue_length = len(Waiting_queue)

    for i in range(Waiting_queue_length):
        job = tmp[i]
        if job['Req_p'] <= idle_pro:
            job['start_t'], job['Pred_T'], job['Wait_t'], job['Finish_t'] = current_time, current_time + job['Req_T'], current_time - job['Sub_t'], current_time + job['Run_t']
            job['Waiting_rate'] = job['Wait_t']/job['Run_t']
            idle_pro -= job['Req_p']
            job['idle_pro'] = idle_pro
            Waiting_queue.remove(job)
            Running_queue.append(job)
            log.append(job)
            
        # 因為是順序執行因此遇到第一個無法執行的call EASY_Backfilling
        else:
            Waiting_queue, Running_queue, log, idle_pro = EASY_Backfilling(Waiting_queue, Running_queue, log, current_time, idle_pro)
            break

    return Waiting_queue, Running_queue, log, idle_pro

def EASY_Backfilling(Waiting_queue, Running_queue, log, current_time, idle_pro):

    tmp = list.copy(Waiting_queue)
    Waiting_queue_length = len(Waiting_queue)
    Running_queue_length = len(Running_queue)
    tmp1 = idle_pro
    Running_queue = Sort(Running_queue, 'Pred_T')
    global back_job_count

    for sort_order in range(Running_queue_length):
        tmp1 += Running_queue[sort_order]['Req_p']
        if tmp1 >= Waiting_queue[0]['Req_p']:
            extra_nodes = tmp1 - Waiting_queue[0]['Req_p']
            Waiting_queue[0]['shadow_time'] = Running_queue[sort_order]['Pred_T']
            break

    for i in range(1, Waiting_queue_length):
        job = tmp[i]
        if min(Running_queue, key=lambda x:x['Finish_t'])['Finish_t'] <= current_time:
            break
        elif job['Req_p'] <= idle_pro:
            job['Pred_T'] = current_time + job['Req_T']
            if job['Pred_T'] <= Waiting_queue[0]['shadow_time']:
                pass
            elif job['Req_p'] <= extra_nodes:
                extra_nodes -= job['Req_p']
            else:
                continue

            job['start_t'], job['Wait_t'], job['Finish_t'] = current_time, current_time - job['Sub_t'], current_time + job['Run_t']
            job['Waiting_rate'] = job['Wait_t']/job['Run_t']
            idle_pro -= job['Req_p']
            job['idle_pro'] = idle_pro
            Waiting_queue.remove(job)
            Running_queue.append(job)
            log.append(job)
            back_job_count += 1
        else:
            continue    

    return Waiting_queue, Running_queue, log, idle_pro

def running_status(Job_queue, Waiting_queue, Running_queue, log, idle_pro):
    
    # 找到開始run job中最早跑完的，現在時間就是這個時間點
    min_Job = min(Running_queue, key=lambda x:x['Finish_t'])
    current_time = min_Job['Finish_t']
    
    # 知道目前第一個跑完的job finish時間點回去檢查在他running過程中是否有job應該到Waiting_queue接著排程看會不會job可以進running
    Job_queue, Waiting_queue, Running_queue, log, idle_pro = ready_status(Job_queue, Waiting_queue, Running_queue, log, current_time, idle_pro)
    min_Job = min(Running_queue, key=lambda x:x['Finish_t'])
    current_time = min_Job['Finish_t']
    Earliest_Finish_job_list = [data for data in Running_queue if data.get('Finish_t') == current_time]
    
    # 將最早跑完的job從Running_queue移除，最後歸還用完的CPU
    for job in Earliest_Finish_job_list:
        Job_num, Req_p = job['Job_num'], job['Req_p']
        tmp1 = f'Job number: {Job_num} terminated, return {Req_p} process\n'
        tmp2 = " ".join([str(i['Job_num']) for i in Waiting_queue if i['Sub_t'] <= current_time])
        if tmp2 != '':
            tmp2 = f'Job number: {tmp2} in the Waiting queue\n'
        s = f'{tmp1}{tmp2}-----------------------------------------------------------------------------------------------------\n'
        log.append(s)
        Running_queue.remove(job)
        idle_pro += job['Req_p']

    return Job_queue, Waiting_queue, Running_queue, log, current_time, idle_pro   

if __name__ == "__main__":

    try:
        Job_queue = []
        Running_queue = []
        Waiting_queue = []
        log = []

        idle_pro = int(input("resource process:"))
        current_time, count, Total_wait_t, Total_wait_rate, back_job_count = 0, 0, 0, 0, 0
        # 讀檔
        file_path = _getcwd() + '\SDSC-SP2-1998-4.2-cln.swf.gz'
        with _gzopen(file_path, 'r') as fp:
            for line in fp:
                i = line.split()
                if(bytes.decode(i[0]) != ';' and (bytes.decode(i[10]) == '1' or bytes.decode(i[10]) == '0')):
                    count += 1
                    # Job Number, Submit Time, Run Time, Requested Number of Processors, Queue Number先不加
                    Job_queue.append({'Job_num': int(bytes.decode(i[0])),
                                      'Sub_t': int(bytes.decode(i[1])),
                                      'Run_t': int(bytes.decode(i[3])),
                                      'Req_p': int(bytes.decode(i[7])),
                                      'Req_T': int(bytes.decode(i[8])),
                                      'shadow_time': 0})
            print("total Job: %d" %count)
        
        start = _process_time()

        while True:
            Job_queue, Waiting_queue, Running_queue, log, idle_pro = ready_status(Job_queue, Waiting_queue, Running_queue, log, current_time, idle_pro)            
            Job_queue, Waiting_queue, Running_queue, log, current_time, idle_pro = running_status(Job_queue, Waiting_queue, Running_queue, log, idle_pro)
            if len(Job_queue) == 0 and len(Waiting_queue) == 0 and len(Running_queue) == 0:
                break
        
        thread1 = _Thread(target = _IOwirte, args = (log,))
        thread2 = _Thread(target = Print_Screen, args = (log, Total_wait_t, Total_wait_rate, start, back_job_count))

        thread1.start()
        thread2.start()

        thread1.join()
        thread2.join()
      
    except:
        _print_exc()
