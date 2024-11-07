FROM --platform=linux/amd64 python:3.12
WORKDIR /code
# python dependencies
RUN pip install pipenv
COPY Pipfile .
RUN pipenv install --skip-lock
COPY ./ .
ENTRYPOINT ["pipenv", "run", "python", "main.py"]