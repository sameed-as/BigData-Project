FROM python:3.8-slim

RUN apt-get update && apt-get install -y \
    wget \
    default-jre \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

RUN wget https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein \
    && mv lein /usr/local/bin/lein \
    && chmod a+x /usr/local/bin/lein \
    && lein version

WORKDIR /app

COPY requirements.txt /app/
# Install all packages except streamparse first, then install thriftpy2
# and streamparse without deps to avoid the broken 'thriftpy' C extension
RUN grep -v 'streamparse' requirements.txt > /tmp/requirements_base.txt && \
    pip install --no-cache-dir -r /tmp/requirements_base.txt && \
    pip install --no-cache-dir thriftpy2 && \
    pip install --no-cache-dir --no-deps streamparse==3.15.1

# Create non-root user to avoid permission issues with mounted volumes if needed
# but for simplicity in docker-compose, we use root to install dependencies.
CMD ["/bin/bash"]
