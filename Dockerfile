# =====================================================================
# Stage 1: Build the Magefile Environment
# =====================================================================
FROM golang:1.25.9-alpine AS builder

# Install core build tools and git
RUN apk add --no-cache git gcc musl-dev

WORKDIR /app

# Cache dependencies first
COPY go.mod go.sum ./
RUN go mod download

# Copy the entire source tree
COPY . .

# Move into the cmd directory where magefile.go lives
WORKDIR /app/cmd

# Compile into the current directory as "p2p-app" to avoid absolute path permissions issues
RUN CGO_ENABLED=0 GOOS=linux go run github.com/magefile/mage -compile p2p-app

# =====================================================================
# Stage 2: Minimal Production Runtime
# =====================================================================
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata

# Create secure non-root runtime environment
RUN addgroup -S p2puser && adduser -S p2puser -G p2puser
WORKDIR /home/p2puser

# Pull the compiled app binary from the builder's /app/cmd directory
COPY --from=builder /app/cmd/p2p-app .

# Pre-create data folder for keys generated via LoadOrGenerateKey
RUN mkdir -p data && chown -R p2puser:p2puser /home/p2puser
USER p2puser

# Expose standard libp2p connection protocols
EXPOSE 4001/tcp 4001/udp 4002/tcp 4003/udp

# Set binary as entrypoint
ENTRYPOINT ["./p2p-app"]