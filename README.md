
### TODOs
+ (DONE) Keyrange subscription (through ECS)
+ Data request response
+ Test scripts
+ Report


### Docker login
```shell
docker login gitlab.lrz.de:5005
```

### Build
```shell
# For Mac (local testing)
docker build -t gitlab.lrz.de:5005/cdb-23/gr6/ms4/kv-client --target kv_client .

docker build -t gitlab.lrz.de:5005/cdb-23/gr6/ms4/kv-server --target kv_server .
docker build -t gitlab.lrz.de:5005/cdb-23/gr6/ms4/ecs-server --target ecs_server .

# For Linux (before pushing to gitlab)
docker buildx build --platform linux/amd64 -t gitlab.lrz.de:5005/cdb-23/gr6/ms4/kv-client --target kv_client .

docker buildx build --platform linux/amd64 -t gitlab.lrz.de:5005/cdb-23/gr6/ms4/kv-server --target kv_server .
docker buildx build --platform linux/amd64 -t gitlab.lrz.de:5005/cdb-23/gr6/ms4/ecs-server --target ecs_server .
```

### Test locally
```shell
docker run --rm --network=my-network --name client -it gitlab.lrz.de:5005/cdb-23/gr6/ms4/kv-client

docker run --rm --network=my-network --name server -it gitlab.lrz.de:5005/cdb-23/gr6/ms4/kv-server
docker run --rm --network=my-network --name server -it gitlab.lrz.de:5005/cdb-23/gr6/ms4/ecs-server

docker run -p 35573:35573 gitlab.lrz.de:5005/cdb-23/gr6/ms4/kv-server -a 0.0.0.0 -p 35573 -ll DEBUG -d process_data -s FIFO -c 10
```


### Push docker image to gitlab
```shell
# Before pushing, make sure that you build for Linux
docker push gitlab.lrz.de:5005/cdb-23/gr6/ms4/kv-client

docker push gitlab.lrz.de:5005/cdb-23/gr6/ms4/kv-server
docker push gitlab.lrz.de:5005/cdb-23/gr6/ms4/ecs-server



docker run -it --rm --name ms4-gr6-client --network ms4/gr6 gitlab.lrz.de:5005/cdb-23/gr6/ms4/kv-client -a ms4-gr6-kv-client


docker run --rm -p 43787:43787 --name ms4-gr6-ecs-server --network ms4/gr6 gitlab.lrz.de:5005/cdb-23/gr6/ms4/ecs-server -a 0.0.0.0 -p 43787 -ll FINEST

KV0 Stdio 
docker run --rm -p 46507:46507 --name ms4-gr6-kv-server0 --network ms4/gr6 gitlab.lrz.de:5005/cdb-23/gr6/ms4/kv-server -a ms4-gr6-kv-server0 -p 46507 -ll INFO -d process_data -s FIFO -c 10 -b ms4-gr6-ecs-server:43787

KV1 Stdio 
docker run --rm -p 34145:34145 --name ms4-gr6-kv-server1 --network ms4/gr6 gitlab.lrz.de:5005/cdb-23/gr6/ms4/kv-server -a ms4-gr6-kv-server1 -p 34145 -ll INFO -d process_data -s FIFO -c 10 -b ms4-gr6-ecs-server:43787

KV2 Stdio 
docker run --rm -p 42547:42547 --name ms4-gr6-kv-server2 --network ms4/gr6 gitlab.lrz.de:5005/cdb-23/gr6/ms4/kv-server -a ms4-gr6-kv-server2 -p 42547 -ll INFO -d process_data -s FIFO -c 10 -b ms4-gr6-ecs-server:43787

KV3 Stdio 
docker run --rm -p 43371:43371 --name ms4-gr6-kv-server3 --network ms4/gr6 gitlab.lrz.de:5005/cdb-23/gr6/ms4/kv-server -a ms4-gr6-kv-server3 -p 43371 -ll INFO -d process_data -s FIFO -c 10 -b ms4-gr6-ecs-server:43787

KV4 Stdio 
docker run --rm -p 41855:41855 --name ms4-gr6-kv-server4 --network ms4/gr6 gitlab.lrz.de:5005/cdb-23/gr6/ms4/kv-server -a ms4-gr6-kv-server4 -p 41855 -ll INFO -d process_data -s FIFO -c 10 -b ms4-gr6-ecs-server:43787

```