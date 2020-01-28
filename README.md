# Job scheduling

## 研究所課程-平行程式作業

## 作業內容

從 **Paralled Workloads Archive** 中利用 **SDSC SP2 Workload log** 實作出下列三種的 **Job Scheduler** 的模擬程式
1. **First Come, First Served (FCFS)**
2. **EASY Backfilling**
3. **Moldable Job Scheduling**

## 作業要求

以下皆為Workload log 中的欄位名，括號內的數字是指在欄位的位置
* **FCFS** 使用欄位 **Job Number(1)**, **Submit Time(2)**, **Run Time(4)**, **Requested Number of Processors(8)**
* **EASY Backfilling** 使用欄位 **Job Number(1)**, **Submit Time(2)**, **Run Time(4)**, **Requested Number of Processors(8)**, **Requested Time(9)**
* **Moldable Job Scheduling** 使用欄位 **Job Number(1)**, **Submit Time(2)**, **Run Time(4)**, **Requested Number of Processors(8)**

## 演算法實作參考論文

* **EASY Backfilling** 參考自 **Utilization, Predictability, Workloads, and User Runtime Estimates in Scheduling the IBM SP2 with Backfilling**
* **Moldable Job Scheduling** 參考自 **Utilization-Based Moldable Job Scheduling for HPC as a Service**
