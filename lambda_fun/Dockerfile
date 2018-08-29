FROM lambci/lambda:build-python3.6
WORKDIR /var/task
ADD docker_hd/requirements_base.txt .
RUN virtualenv .venv
RUN /bin/bash -c "source .venv/bin/activate && pip3 install -r requirements_base.txt -i https://pypi.douban.com/simple"

ENTRYPOINT ["/bin/bash"]
