# docker build -t pgturtle .

# see https://link.medium.com/Ra2kvVysZ7
# investigate https://github.com/GoogleContainerTools/distroless later

#FROM golang:1.6.4-alpine AS builder
FROM golang:1.14.4-alpine3.12 AS builder
RUN apk --no-cache add ca-certificates

COPY . /go/src/github.com/debackerl/pgturtle
WORKDIR /go/src/github.com/debackerl/pgturtle
#RUN ls -lh . && go version && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -installsuffix cgo -ldflags="-w -s" -o /bin/pgturtle . && ls -lh /bin/pgturtle
RUN ls -lh . && go version && GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o /bin/pgturtle . && ls -lh /bin/pgturtle

RUN addgroup -S pgturtle -g 1001 && \
    adduser -S pgturtle -u 1001 -g pgturtle

FROM alpine:3.12

RUN apk --no-cache add python3 

WORKDIR /
COPY --from=builder /etc/group /etc/passwd /etc/shadow /etc/
COPY --from=builder /bin/pgturtle /bin/pgturtle

#USER pgturtle

CMD ["/bin/pgturtle", "--config", "/etc/pgturtle.conf"]
