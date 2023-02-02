# shellcast_analysis

### Development Environment
#### Docker
* Docker installation: https://docs.docker.com/get-docker/
* Cloud Auth SQL Proxy

##### Mac
Connecting Docker container to locally connected Cloud SQL instance. Initially tried Unix socket connection but it didn't work.
Therefore, this instruction is for TCP connection.

config.ini [gcp.local] section: HOST = docker.for.mac.localhost
curl -o cloud_sql_proxy https://dl.google.com/cloudsql/cloud_sql_proxy.darwin.386
chmod +x cloud_sql_proxy
gcloud auth application-default login
./cloud_sql_proxy -instances={*instance name*}=tcp:3306


##### Notes:
How to install wgrib2 in Docker
https://qiita.com/KentoDodo/items/c8f1dc7fb902c07e817e
Install gcc, gfortran and zlib1g-dev
