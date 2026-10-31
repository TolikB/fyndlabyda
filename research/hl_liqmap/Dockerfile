FROM funding-research:py312
RUN pip install -q --root-user-action=ignore --disable-pip-version-check websockets==15.0.1
WORKDIR /app
COPY collector.py selftest_collector.py /app/
CMD ["python", "-u", "collector.py"]
