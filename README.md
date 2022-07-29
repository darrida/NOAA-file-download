# NOAA-file-download

## Figured out in Prefect 2.0
- [x] prefect login
- [x] prefect agent launch
- [x] prefect deployment build
- [x] prefect deployment apply
- [x] create and push docker image to GitHub (ghcr.io)
- [x] create docker-container configuration block for shared image
- [x] create s3 storage block (perhaps one per group project)

## Storage
- create S3 bucket (sub folders can be specified in different blocks as target storage buckets as well)
- create Prefect S3 Storage block (put in S3 folder path, and credentials)

## Authenticate with Prefect and start agent
```shell
poetry run prefect cloud login
# entry key when prompted
echo $PAT | sudo docker login ghcr.io --username phanatic --password-stdin  # maybe needed
poetry run prefect agent start 0100d1d1-91e3-4f0a-a1a1-00d73790dead
```

## Authenticate docker, build, and push image
```shell
echo $PAT | sudo docker login ghcr.io --username phanatic --password-stdin
sudo docker build . -t ghcr.io/darrida/noaa_flows:latest
sudo docker push ghcr.io/darrida/noaa_flows:latest
```
- Create Prefect Docker-Container block
  - Important information:
    - Image [name] (includes partial path -- i.e., `ghcr.io/darrida/noaa_flows:latest`)
    - Auto Remove: true
    - Stream Output: true
    - Type: docker-container  (maybe needed)

# Authenticate with Prefect, build, apply flow
```shell
poetry run prefect cloud login
# entry key when prompted
poetry run prefect deployment build ./noaa_file_download/flow.py:noaa_file_download -sb s3/prefect-noaa -ib docker-container/shared-noaa-flows-latest -n noaa-file-download
poetry run prefect deployment apply deployment.yaml
```