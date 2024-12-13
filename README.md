- [ ] LSM Tree similar sql and store the data on the file and push to S3
  - [x] MemTable (Red black tree)
  - [x] IMemTable
  - [ ] SSTable
    - [ ] Write the .db file 
  - [ ] Open
  - [x] Wal
    - [ ] Write the log 
  
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