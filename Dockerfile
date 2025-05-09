FROM python:3.11

WORKDIR /app

ENV PYTHONUTF8=1

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "etl.py"]
