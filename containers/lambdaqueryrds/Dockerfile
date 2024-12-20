FROM public.ecr.aws/lambda/python:3.7

COPY app.py ${LAMBDA_TASK_ROOT}

RUN pip install psycopg2-binary  --target "${LAMBDA_TASK_ROOT}"

CMD [ "app.lambda_handler" ] 