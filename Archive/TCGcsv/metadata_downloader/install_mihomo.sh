#!/bin/bash

# Mihomo (Clash Meta) Installation Script
# Author: Claude Code Assistant
# Description: Download and install mihomo with system service

set -e  # Exit on error

echo "=== Mihomo (Clash Meta) Installation Script ==="

# Configuration
MIHOMO_VERSION="v1.19.12"
MIHOMO_USER="mihomo"
MIHOMO_DIR="/etc/mihomo"
MIHOMO_LOG_DIR="/var/log/mihomo"
MIHOMO_BIN="/usr/local/bin/mihomo"
DOWNLOAD_URL="https://github.com/MetaCubeX/mihomo/releases/download/${MIHOMO_VERSION}/mihomo-linux-amd64-compatible-${MIHOMO_VERSION}.gz"

echo "Installing mihomo version: ${MIHOMO_VERSION}"
echo "Download URL: ${DOWNLOAD_URL}"

# Create mihomo user if it doesn't exist
if ! id "${MIHOMO_USER}" &>/dev/null; then
    echo "Creating mihomo user..."
    sudo useradd --system --no-create-home --shell /bin/false ${MIHOMO_USER}
else
    echo "User ${MIHOMO_USER} already exists"
fi

# Create directories
echo "Creating directories..."
sudo mkdir -p ${MIHOMO_DIR}
sudo mkdir -p ${MIHOMO_LOG_DIR}
sudo mkdir -p /tmp/mihomo-install

# Download mihomo
echo "Downloading mihomo binary..."
cd /tmp/mihomo-install
wget -q --show-progress "${DOWNLOAD_URL}" -O mihomo.gz

# Extract and install
echo "Installing mihomo binary..."
gunzip mihomo.gz
sudo chmod +x mihomo
sudo mv mihomo ${MIHOMO_BIN}

# Set ownership
echo "Setting permissions..."
sudo chown -R ${MIHOMO_USER}:${MIHOMO_USER} ${MIHOMO_DIR}
sudo chown -R ${MIHOMO_USER}:${MIHOMO_USER} ${MIHOMO_LOG_DIR}

# Verify installation
echo "Verifying installation..."
${MIHOMO_BIN} -v

echo "✅ Mihomo binary installed successfully at ${MIHOMO_BIN}"

# Create systemd service file
echo "Creating systemd service..."
sudo tee /etc/systemd/system/mihomo.service > /dev/null <<EOF
[Unit]
Description=Mihomo (Clash Meta) Proxy Server
Documentation=https://wiki.metacubex.one/
After=network.target network-online.target nss-lookup.target
Wants=network.target

[Service]
Type=simple
User=${MIHOMO_USER}
Group=${MIHOMO_USER}
ExecStart=${MIHOMO_BIN} -d ${MIHOMO_DIR}
ExecReload=/bin/kill -HUP \$MAINPID
Restart=on-failure
RestartSec=5s
LimitNOFILE=1000000
LimitNPROC=1000000

# Security settings
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
ReadWritePaths=${MIHOMO_LOG_DIR}
ReadWritePaths=${MIHOMO_DIR}

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd
echo "Reloading systemd..."
sudo systemctl daemon-reload

# Enable service (but don't start yet - we need config first)
echo "Enabling mihomo service..."
sudo systemctl enable mihomo

# Cleanup
rm -rf /tmp/mihomo-install

echo ""
echo "=== Installation Complete ==="
echo "📁 Config directory: ${MIHOMO_DIR}"
echo "📄 Log directory: ${MIHOMO_LOG_DIR}"
echo "🔧 Binary location: ${MIHOMO_BIN}"
echo "🚀 Service name: mihomo"
echo ""
echo "Next steps:"
echo "1. Create configuration file at ${MIHOMO_DIR}/config.yaml"
echo "2. Start service with: sudo systemctl start mihomo"
echo "3. Check status with: sudo systemctl status mihomo"
echo "4. View logs with: sudo journalctl -u mihomo -f"
echo ""
echo "✅ Mihomo installation completed successfully!"