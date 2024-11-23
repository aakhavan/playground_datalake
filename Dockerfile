FROM apache/airflow:2.10.3

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -

# Copy the pyproject.toml and poetry.lock files
COPY pyproject.toml poetry.lock /opt/airflow/

# Install dependencies using Poetry
RUN cd /opt/airflow && poetry config virtualenvs.create false && poetry install

# Set the entrypoint to use the Airflow entrypoint
ENTRYPOINT ["/entrypoint"]