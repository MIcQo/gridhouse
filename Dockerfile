FROM golang:1.25 AS build

ARG VERSION=latest
ENV VERSION=$VERSION
ENV APP_EXECUTABLE="/go/bin/gridhouse"

WORKDIR /go/src/gridhouse
COPY . .

RUN go mod download
RUN make build-current

# Now copy it into our base image.
FROM gcr.io/distroless/static-debian12
COPY --from=build /go/bin/gridhouse /
ENTRYPOINT ["/gridhouse"]