FROM python:3.9.16-slim

WORKDIR /src

COPY /src .

RUN pip install -r requirements.txt

ENV PYTHONPATH=/src

CMD ["python", "job_board/pipeline/job_board_pipeline.py"]