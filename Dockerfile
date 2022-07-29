FROM prefecthq/prefect:2.0.0-python3.10

RUN python3 -m pip install --upgrade pip
RUN pip install tqdm pandas lxml fs

# sudo docker build . -t ghcr.io/darrida/noaa_flows:latest
# sudo docker push ghcr.io/darrida/noaa_flows:latest