FROM python:3.11-slim

WORKDIR /app

# Install Pennsieve Agent
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl netcat-openbsd && \
    curl -fsSL -o /tmp/pennsieve.deb https://github.com/Pennsieve/pennsieve-agent/releases/download/1.8.12/pennsieve_1.8.12_amd64.deb && \
    dpkg -i /tmp/pennsieve.deb && \
    rm /tmp/pennsieve.deb && \
    apt-get purge -y curl && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

ENV PATH="/opt/pennsieve:${PATH}"

COPY requirements.txt ./
RUN pip install -r requirements.txt
COPY clients/ ./clients/
COPY cleaners/ ./cleaners/
COPY config.py process.py handler.py entrypoint.sh ./
RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
CMD ["python", "process.py"]
