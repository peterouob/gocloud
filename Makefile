build:
	docker build -t peter123ouob/mdb:v1 .
	docker buildx build --platform linux/amd64 -t peter123ouob/mdb:v1.2 .
push:
	docker push peter123ouob/mdb:v1
run:
	docker run -p 8089:8089 -it peter123ouob/mdb:v1
install_eks:
	eksctl create cluster --name mdb-cluster --region us-east-1
apply:
	cd  ~/Documents/go_cloud/k8s/manifest
	kubectl apply -f deployment.yaml
	kubectl apply -f service.yaml
	kubectl apply -f ingress.yaml
k8getnode:
	kubectl get nodes
	kubectl get nodes -o wide
k8geting:
	kubectl get ing
k8getsvc:
	kubectl get svc
k8edit:
	kubectl edit svc mdb
k8getpods:
	kubectl get pods
nginx:
	kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.12.0-beta.0/deploy/static/provider/aws/deploy.yaml
	kubectl get pods -n ingress-nginx
	kubectl get edit -n ingress-nginx-controller-6568cc55cd-gnpmp -n ingress-nginx