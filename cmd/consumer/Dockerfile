FROM golang:1.23-alpine

WORKDIR /app
COPY . .
#COPY go.mod go.sum
RUN CGO_ENABLED=0 GOOS=linux
RUN go build -mod=vendor -o main .
CMD ["./main"]