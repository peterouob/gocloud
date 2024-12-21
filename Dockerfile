FROM golang:1.23 AS base

WORKDIR /app

COPY go.mod .

RUN go mod download

COPY . .

ARG TARGETARCH
RUN GOARCH=$TARGETARCH go build -o main .

FROM gcr.io/distroless/base

COPY --from=base /app/main .

EXPOSE 8080

CMD ["./main"]