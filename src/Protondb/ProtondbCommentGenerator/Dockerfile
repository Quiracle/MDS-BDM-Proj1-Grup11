FROM pytorch/pytorch:2.2.0-cuda11.8-cudnn8-runtime

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY ProtondbCommentGenerator.py .

CMD ["python", "ProtondbCommentGenerator.py"]
