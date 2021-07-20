 FROM gcr.io/distroless/static:latest
 LABEL maintainers="Kubernetes Authors"
 LABEL description="In-use protecter"
 ARG binary=./bin/in-use-protector

 COPY ${binary} in-use-protector
 ENTRYPOINT ["/in-use-protector"]
