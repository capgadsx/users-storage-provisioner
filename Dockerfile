FROM golang:alpine as builder-container
RUN apk add --no-cache git
RUN go get -v github.com/golang/dep/cmd/dep
WORKDIR /go/src/users-storage-provisioner
ADD src/Gopkg.lock Gopkg.lock
ADD src/Gopkg.toml Gopkg.toml
RUN dep ensure -v --vendor-only
ADD src src
RUN ln -sv ../src/lib vendor/lib
RUN CGO_ENABLED=0 GOOS=linux go build -v -o provisioner src/*/*.go

FROM scratch
LABEL maintainer="Carlos Ponce <cponce@alumnos.inf.utfsm.cl>"
WORKDIR /root
COPY --from=builder-container /go/src/users-storage-provisioner/provisioner /root/provisioner
CMD ["/root/provisioner"]