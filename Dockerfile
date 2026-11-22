FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN --mount=type=secret,id=build_ca if [ -f /run/secrets/build_ca ]; then export PIP_CERT=/run/secrets/build_ca; fi; pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["sh","-ec","alembic upgrade head && exec python -m bot.main"]
