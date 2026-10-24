FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY cmd cmd
COPY internal internal
RUN go test ./...
RUN CGO_ENABLED=0 \
    go build -a -installsuffix cgo \
    -ldflags "-extldflags \"-static\" -s -w" \
    -tags 'netgo osusergo' \
    -o nodestatd ./cmd/nodestatd

FROM scratch
COPY --from=builder /app/nodestatd /nodestatd
EXPOSE 8080
ENTRYPOINT ["/nodestatd"]
