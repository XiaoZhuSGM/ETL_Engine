FROM lambci/lambda:build-python3.6
WORKDIR /var/task
RUN virtualenv .venv
RUN /bin/bash -c "source .venv/bin/activate"
ENTRYPOINT ["/bin/bash"]
