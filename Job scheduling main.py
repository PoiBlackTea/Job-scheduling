# -*- coding: utf-8 -*-
import queue
from gzip import open as _gzopen
from traceback import print_exc as _print_exc
from time import process_time as _process_time
import os

"""
call這個function 在以下幾種狀態 
1.模擬一開始process or job 從Job_queue(New state) -> ready status(Waiting_queue)
2.每當有job 從running status -> terminated status 檢查這個時間內是否有job 應該從Job_queue(New state) -> ready status(Waiting_queue) -> schedular dispatch
以上兩個動作最後皆需排程
"""
def ready_status(Job_queue, Waiting_queue, Running_queue, current_time, idle_pro):

    # Job_queue是0，代表所有Job都在Waiting_queue或者Running_queue
    # 第二種狀態時可能發生的情形
    if len(Job_queue) == 0:
        return Job_queue, Waiting_queue, Running_queue, idle_pro
    # Waiting_queue和Running_queue是0，i.e., 一開始還沒有job submit或者運行過程中剛好job完成但還沒new job submit的時間點，模擬直接抓Job_queue第一個job丟進Waiting_queue並將時間設定成該job的Submit Time
    # 第一種狀態可能發生的情形
    if len(Waiting_queue) == 0 and len(Running_queue) == 0:
        job = Job_queue[0]
        current_time = job['Sub_t']
    # 檢查是否有這個時間段內的job應該進Waiting_queue
    # 第一種狀態檢查是否有同時間submit的job
    # 第二種可能發生的情形
    tmp = list.copy(Job_queue)
    c_t = current_time
    for job in tmp:
        if job['Sub_t'] <= c_t:
            current_time = job['Sub_t']
            job['event_type'] = 'ready'
            Waiting_queue.append(job)
            Job_queue.remove(job)
            Waiting_queue, Running_queue, idle_pro = FCFS_scheduling(Waiting_queue, Running_queue, current_time, idle_pro)
        else:
            # 主要為了當Running_queue = 0且Waiting_queue != 0 時
            Waiting_queue, Running_queue, idle_pro = FCFS_scheduling(Waiting_queue, Running_queue, c_t, idle_pro)
            break

    return Job_queue, Waiting_queue, Running_queue, idle_pro

"""
從(Waiting_queue) -> schedular dispatch 確定是否可以Running，可以的話進入Running_queue，不行則是繼續放置在Waiting_queue等待下個時間點
"""
def FCFS_scheduling(Waiting_queue, Running_queue, current_time, idle_pro):
    
    # 開始檢查Waiting_queue的job，資源夠跑且時間符合的的job放進Running_queue
    tmp = list.copy(Waiting_queue)
    for job in tmp:
        if job['Req_p'] <= idle_pro:
            if job['Sub_t'] <= current_time:
                job['start_t'] = current_time
            else:
                job['start_t'] = job['Sub_t']

            job['Wait_t'] = job['start_t'] - job['Sub_t']
            job['Waiting_rate'] = job['Wait_t']/job['Run_t']
            job['Finish_t'] = job['start_t'] + job['Run_t']
            idle_pro -= job['Req_p']
            job['idle_pro'] = idle_pro
            job['event_type'] = 'running'
            Waiting_queue.remove(job)
            Running_queue.append(job)
            
        # 因為是FCFS所以一遇到剩餘資源不夠run的就可以跳出迴圈
        else:
            break

    return Waiting_queue, Running_queue, idle_pro

"""
分成幾個部分進行
1.先找到目前Running_queue中最早結束的job，將時間軸設定成這個點
2.利用這個時間軸去call ready_status function 將在這個job運行過程中進入的job丟入Waiting_queue
3.ready_status內會call FCFS_scheduling function確認是否還有資源能讓job跑
4.回到running_status function 會確認Running_queue長度是否還是一樣，不一樣代表有可以run的job剛剛進入，那就需要再call running_status function 找到新的job first Finish time
5.釋放最早結束job的資源
"""
def running_status(Job_queue, Waiting_queue, Running_queue, Terminated_queue, idle_pro):
    
    tmp = list.copy(Running_queue)
    c_t = Running_queue[0]['Finish_t']
    # 找到開始run job中最早跑完的，現在時間就是這個時間點
    for job in tmp:
        if c_t >= job['Finish_t']:
            c_t = job['Finish_t']
    
    # 將最早跑完的job從Running_queue移除並加入到Terminated_queue，最後歸還用完的CPU
    for job in tmp:
        if c_t == job['Finish_t']:
            # 知道目前第一個跑完的job finish時間點回去檢查在他running過程中是否有job應該到Waiting_queue接著排程看會不會job可以進running
            Job_queue, Waiting_queue, Running_queue, idle_pro = ready_status(Job_queue, Waiting_queue, Running_queue, c_t, idle_pro)
            # 如果有進Running_queue的job那要重新確認最快結束的job是哪一個，得到最早完成的job finish time
            if Running_queue != tmp:
                c_t = Running_queue[0]['Finish_t']
                for job in Running_queue:
                    if c_t >= job['Finish_t']:
                        c_t = job['Finish_t']
                        
            else:
                Running_queue.remove(job)
                Terminated_queue.put(job)
                idle_pro += job['Req_p']
        
    return Job_queue, Waiting_queue, Running_queue, Terminated_queue, c_t, idle_pro   

if __name__ == "__main__":

    try:
        Job_queue = []
        Running_queue = []
        Waiting_queue = []
        Terminated_queue = queue.Queue()

        idle_pro = int(input("resource process:"))
        current_time, count, Total_wait_t, Total_wait_rate = 0, 0, 0, 0
        # 讀檔
        file_path = os.getcwd() + '\SDSC-SP2-1998-4.2-cln.swf.gz'
        with _gzopen(file_path, 'r') as fp:
            for line in fp:
                i = line.split()
                if(bytes.decode(i[0]) != ';' and (bytes.decode(i[10]) == '1' or bytes.decode(i[10]) == '0')):
                    count += 1
                    # Job Number, Submit Time, Run Time, Requested Number of Processors, Queue Number先不加
                    Job_queue.append({'Job_num': bytes.decode(i[0]),
                                      'Sub_t': int(bytes.decode(i[1])),
                                      'Run_t': int(bytes.decode(i[3])),
                                      'Req_p': int(bytes.decode(i[7])),
                                      'event_type': 'sub',
                                      'Wait_t': 0.0,
                                      'Waiting_rate': 0.0,
                                      'start_t': 0,
                                      'Finish_t' : 0,
                                      'idle_pro' : 0})
            print("total Job: %d" %count)
        
        start = _process_time()

        while True:
            Job_queue, Waiting_queue, Running_queue, idle_pro = ready_status(Job_queue, Waiting_queue, Running_queue, current_time, idle_pro)            
            Job_queue, Waiting_queue, Running_queue, Terminated_queue, current_time, idle_pro = running_status(Job_queue, Waiting_queue, Running_queue, Terminated_queue, idle_pro)
            if len(Job_queue) == 0 and len(Waiting_queue) == 0 and len(Running_queue) == 0:
                break

        with open('result.txt', 'w') as wp:
            for i in range(Terminated_queue.qsize()):
                job = Terminated_queue.get()
                Total_wait_t += job['Wait_t']
                wp.write("job_num: %s, job_Sub: %d, job_start:  %d, job_Finish: %d\n" %(job['Job_num'], job['Sub_t'], job['Sub_t']+job['Wait_t'], job['Finish_t']))
                Total_wait_rate += job['Waiting_rate']
                

        print("spend %f seconds \n"\
                "Total waiting time: %d\n"\
                "Total waiting rate: %d\n"\
                "Average waiting time: %d\n"\
                "Average waiting rate: %d"\
                %(_process_time() - start, Total_wait_t, Total_wait_rate, Total_wait_t/count, Total_wait_rate/count))
        
    except:
        _print_exc()
