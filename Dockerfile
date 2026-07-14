FROM golang:1.26-alpine AS build

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o /job-distributed .

FROM alpine:3.22
COPY --from=build /job-distributed /usr/local/bin/job-distributed

CMD ["job-distributed"]
