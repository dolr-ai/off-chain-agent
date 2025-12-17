#!/bin/bash
# Server setup script for bare metal deployment
# Run this once on each server to prepare it for deployment

set -e

echo "=== Setting up bare metal server for offchain-agent ==="

# Update system
echo "Updating system packages..."
apt-get update && apt-get upgrade -y

# Install Docker
echo "Installing Docker..."
if ! command -v docker &> /dev/null; then
    curl -fsSL https://get.docker.com | sh
    systemctl enable docker
    systemctl start docker
fi

# Install Docker Compose (v2)
echo "Installing Docker Compose..."
if ! docker compose version &> /dev/null; then
    apt-get install -y docker-compose-plugin
fi

# Install required tools
echo "Installing required tools..."
apt-get install -y rsync curl wget

# Create deployment directory
echo "Creating deployment directory..."
mkdir -p /home/offchain-agent
chmod 755 /home/offchain-agent

# Enable IPv6 for Docker (required for some gRPC scenarios)
echo "Configuring Docker for IPv6..."
cat > /etc/docker/daemon.json <<EOF
{
  "ipv6": true,
  "fixed-cidr-v6": "fd00:dead:beef::/48",
  "experimental": true,
  "ip6tables": true
}
EOF

# Restart Docker to apply IPv6 settings
systemctl restart docker

# Configure firewall (if ufw is installed)
if command -v ufw &> /dev/null; then
    echo "Configuring firewall..."
    ufw allow 22/tcp   # SSH
    ufw allow 80/tcp   # HTTP (Caddy redirect)
    ufw allow 443/tcp  # HTTPS (gRPC over TLS)
    ufw allow 443/udp  # HTTP/3 QUIC
    ufw --force enable
fi

# Set up log rotation for Docker
echo "Setting up Docker log rotation..."
cat > /etc/docker/daemon.json <<EOF
{
  "ipv6": true,
  "fixed-cidr-v6": "fd00:dead:beef::/48",
  "experimental": true,
  "ip6tables": true,
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "100m",
    "max-file": "3"
  }
}
EOF

systemctl restart docker

echo ""
echo "=== Server setup complete! ==="
echo ""
echo "Next steps:"
echo "1. Generate SSH key for GitHub Actions: ssh-keygen -t ed25519 -C 'github-actions-offchain'"
echo "2. Add the public key to /root/.ssh/authorized_keys"
echo "3. Add the private key as a GitHub secret (BAREMETAL_SERVER_X_SSH_KEY)"
echo "4. Add the server IP as a GitHub secret (BAREMETAL_SERVER_X_IP)"
echo "5. Add the OFFCHAIN_FLY_API_TOKEN secret to GitHub"
echo ""
echo "Deployment directory: /home/offchain-agent"
