FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /bin/worker ./cmd/worker

FROM alpine:latest
COPY --from=build /bin/worker /bin/worker
ENTRYPOINT ["/bin/worker"]
