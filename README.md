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
