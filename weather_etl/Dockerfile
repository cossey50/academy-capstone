FROM public.ecr.aws/datamindedacademy/capstone:v3.4.1-hadoop-3.3.6-v1

COPY requirements.txt /app/requirements.txt

WORKDIR /app

USER 0

RUN python3 -m pip install --upgrade pip && pip install -r requirements.txt

COPY weather_etl.py ./weather_etl.py

ENTRYPOINT [ "python3" ]
CMD [ "weather_etl.py"]