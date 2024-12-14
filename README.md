- [ ] LSM Tree similar sql and store the data on the file and push to S3
  - [x] MemTable (Red black tree)
  ```text
  值先寫入Wal log裡面記錄同時記錄到硬盤然後到一定數量一次給memTable
  ```
  - [x] IMemTable
  - [ ] SSTable
    - [ ] Write the .db file 
  - [ ] Open
  - [x] Wal
    - [ ] Write the log 

```text
open->lsm->walManager-> write log if log count > maxCount -> memtable
if memtable size > maxSize -> imemtable
if imemtable size > maxSize -> ssTable
ssTable -> write file -> LSM Tree
```
  
- [ ] Use Kubernetes to deploy the environment 
  - EKS cluster
  
- [ ] Use Docker to build the image for program
  - Container 

- [ ] Use gitOPS to build the CI/CD stream
  - CI: Git action
  - CD: GitOPS (ArgoCD)

- [ ] EC2 to push the website on domain with nginx to load balance

- [ ] Helm Chart to manage the environment 
- [ ] pbd->public table, pdb ->private

```
Ingress control -> LB(DNS) -> Export
```
