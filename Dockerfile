
# Create image with docker buildx build -t dea-vectoriser .

# Thankyou https://uwekorn.com/2021/03/01/deploying-conda-environments-in-docker-how-to-do-it-right.html

FROM mambaorg/micromamba:0.13.1
COPY environment.yaml /root/environment.yaml
RUN --mount=type=cache,target=/opt/conda/pkgs micromamba install -y -n base -f /root/environment.yaml && \
#    micromamba clean --all --yes && \
    rm -rf /opt/conda/include && \
    rm -rf /opt/conda/conda-meta  && \
    rm -rf /opt/conda/lib/libpython3.9.so.1.0 && \
    rm -rf /opt/conda/bin/pdf* && rm -rf /opt/conda/bin/postgres && rm -rf /opt/conda/bin/x86_64-conda-linux-gnu-ld && \
    rm -rf /opt/conda/share/{locale,poppler,doc} && \
    find /opt/conda/lib/python3.9/site-packages/scipy -name 'tests' -type d -exec rm -rf '{}' '+' && \
    find /opt/conda/lib/python3.9/site-packages/numpy -name 'tests' -type d -exec rm -rf '{}' '+' && \
    find /opt/conda/lib/python3.9/site-packages/pandas -name 'tests' -type d -exec rm -rf '{}' '+' && \
    find /opt/conda/lib/python3.9/site-packages -name '*.pyx' -delete && \
    find /opt/conda/ -name '*.a' -delete && \
    find /opt/conda/ -name '__pycache__' -type d -exec rm -rf '{}' '+' && \
    rm -rf /opt/conda/lib/python3.9/site-packages/{sklearn,matplotlib,bokeh} && \
#    rm -rf /opt/conda/lib/libpoppler* && \
    bash -O extglob -c 'rm -rf /opt/conda/lib/python3.9/site-packages/botocore/data/!(sqs,sns,s3)'



COPY dust-v0.5.4-x86_64-unknown-linux-gnu/dust /bin/dust
COPY dist /dist
RUN pip install dist/*.whl
