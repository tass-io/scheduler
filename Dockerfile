FROM golang:latest as builder
WORKDIR /code
ADD . /code
RUN go env -w GO111MODULE=on && go env -w GOPROXY=https://goproxy.cn,direct && go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o app .
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o runtime ./pkg/initial/wrapper/main.go

FROM centos as prod
EXPOSE 50001
WORKDIR /tass/
COPY --from=0 /code/runtime .
RUN chmod +x /tass/runtime
WORKDIR /root/
COPY --from=0 /code/app .
RUN chmod +x /root/app
ENTRYPOINT ["/root/app"]
