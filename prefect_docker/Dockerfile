FROM prefecthq/prefect:latest 

WORKDIR /nhl_example

ADD . .

RUN pip install .
RUN pip install "prefect[github]"

RUN prefect auth login -k YOUR_REFECT_LOGIN_API_KEY