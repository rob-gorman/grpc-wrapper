# Build stage
FROM golang:1.18-alpine AS build

WORKDIR /app
COPY . .

RUN go mod download

RUN CGO_ENABLED=0 go build -o /wrapper .

## Deploy
FROM scratch

COPY --from=build /wrapper /wrapper

ENTRYPOINT ["/wrapper"]