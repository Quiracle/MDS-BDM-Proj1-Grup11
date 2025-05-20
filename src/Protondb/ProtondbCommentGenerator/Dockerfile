FROM pytorch/pytorch:2.2.0-cuda11.8-cudnn8-runtime

WORKDIR /app

# Copy and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy scripts
COPY ProtondbCommentGenerator.py .
COPY ReviewClassifier.py .

# Ensure HF token is set at runtime via environment variable
# Run both scripts in sequence
CMD ["sh", "-c", "python ProtondbCommentGenerator.py && python ReviewClassifier.py"]
