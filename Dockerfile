FROM public.ecr.aws/docker/library/python:3.7

WORKDIR /usr

ENV PYTHONPATH=/usr

COPY requirements.txt ./

RUN pip3 install --no-cache-dir -r requirements.txt

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)

COPY src/ ./src

CMD [ "streamlit", "run", "src/streamlit_app.py" ]