# Sstable 測試後有問題,目前猜測是format以及block未成功寫入導致offset出問題

- [x] LSM Tree similar sql and store the data on the file and push to S3
  - [x] MemTable (Red black tree)
  ```text
  值先寫入Wal log裡面記錄同時記錄到硬盤然後到一定數量一次給memTable
  ```
  - [x] IMemTable
  - [x] Open
  - [x] Wal
    - [x] Write the log 

```text
open->lsm->walManager-> write log if log count > maxCount -> memtable
if memtable size > maxSize -> imemtable
if imemtable size > maxSize -> ssTable
ssTable -> write file -> LSM Tree
```
  
- [x] Use Kubernetes to deploy the environment 
  - EKS cluster
  
- [x] Use Docker to build the image for program
  - Container 

- [x] Use gitOPS to build the CI/CD stream
  - CI: Git action
  - CD: GitOPS (ArgoCD)

- [x] EC2 to push the website on domain with nginx to load balance

- [x] Helm Chart to manage the environment 

```
Ingress control -> LB(DNS) -> Export
```

## 使用工具

### 生產環境
- deploy: k8s cluster (on ec2)
- load balance: nginx
- build: docker
- ci: git action
- cd: argo cd
- storage: aws s3 bucket

### 本地測試
- api test: postman
- aws: localstack

### program
- language: golang,yaml
- 程式目地: 使用這創建本地db(基於lsm tree完成),對db進行操作後可以push到雲端,使用者也可以查看雲端上的db
- why lsm: 考慮到db有可能有大量的內容,使用一般io讀寫對內存造成較大負擔,使用lsm tree的思想,先預寫log到wal中,再寫入到memtable如果memtable中的內容大小超過的話建立只讀表並且重新生成memtable

### 未來發展
- 實現類似leveldb中的version迭代
- 完善lsm tree的壓縮功能
- 對資料表有更大的操作或存取認證
