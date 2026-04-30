FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    wget \
    default-jre \
    && rm -rf /var/lib/apt/lists/*

RUN wget https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein \
    && mv lein /usr/local/bin/lein \
    && chmod a+x /usr/local/bin/lein \
    && lein version

WORKDIR /app

COPY ../requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Create non-root user to avoid permission issues with mounted volumes if needed
# but for simplicity in docker-compose, we use root to install dependencies.
CMD ["/bin/bash"]
