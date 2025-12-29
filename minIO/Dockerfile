from minio/minio:RELEASE.2025-09-07T16-13-09Z

COPY init-minio.sh /usr/local/bin/init-minio.sh
RUN chmod +x /usr/local/bin/init-minio.sh

ENTRYPOINT ["sh", "-c", "/usr/local/bin/init-minio.sh"]