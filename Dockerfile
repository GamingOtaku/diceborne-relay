FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir "websockets>=13"
COPY server.py .
EXPOSE 8765
HEALTHCHECK --interval=30s --timeout=5s CMD python -c "import urllib.request,os;urllib.request.urlopen(\"http://127.0.0.1:%s/health\" % os.environ.get(\"PORT\",\"8765\"),timeout=3)" || exit 1
CMD ["python", "server.py"]
