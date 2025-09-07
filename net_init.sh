#!/bin/bash
# Master DHCP setup for DMC. Restores system back to DHCP on cleanup.

RESOLVED_STOPPED=false
ETH_IF=""

# --- Find first physical wired Ethernet ---
for iface in /sys/class/net/en*; do
    [ "$iface" = "lo" ] && continue
    [ -d "/sys/class/net/$iface/wireless" ] && continue
    ETH_IF=$(basename "$iface")
    echo "Wired Ethernet interface found: $ETH_IF"
    break
done

if [ -z "$ETH_IF" ]; then
    echo "No physical wired Ethernet interface found."
    echo "Please manually set a static IPv4 for your NIC and set ETH_IF in the script."
    exit 1
fi

# --- Ask user for network configuration ---
read -p "Enter static IP for master node: " MASTER_IP
read -p "Enter subnet mask (e.g., 24 for 255.255.255.0): " MASTER_MASK
read -p "Enter DHCP range start: " DHCP_START
read -p "Enter DHCP range end: " DHCP_END

# --- Cleanup function ---
cleanup() {
    echo "Restoring system..."
    
    # Restore NetworkManager DHCP
    if [ -n "$ETH_IF" ]; then
        echo "Restoring $ETH_IF to DHCP..."
        sudo nmcli con mod "$CON_NAME" ipv4.method auto
        sudo nmcli con up "$CON_NAME"

        echo "$ETH_IF restored to DHCP."
    fi

    # Restore systemd-resolved
    if [ "$RESOLVED_STOPPED" = true ]; then
        echo "Restoring systemd-resolved..."
        sudo systemctl enable systemd-resolved
        sudo systemctl start systemd-resolved
        sudo ln -sf /run/systemd/resolve/stub-resolv.conf /etc/resolv.conf
        echo "System DNS restored."
    fi
}

# --- Stop systemd-resolved if port 53 busy ---
if sudo lsof -i :53 | grep -q LISTEN; then
    echo "Port 53 in use. Stopping systemd-resolved..."
    sudo systemctl stop systemd-resolved
    sudo systemctl disable systemd-resolved
    sudo rm -f /etc/resolv.conf
    echo "nameserver 8.8.8.8" | sudo tee /etc/resolv.conf
    RESOLVED_STOPPED=true
else
    RESOLVED_STOPPED=false
fi

# --- Set static IP using NetworkManager ---
echo "Assigning static IP $MASTER_IP/$MASTER_MASK to $ETH_IF..."
CON_NAME=$(nmcli -t -f NAME,DEVICE con show --active | grep "$ETH_IF" | cut -d: -f1)
# fallback if not active:
if [ -z "$CON_NAME" ]; then
    CON_NAME=$(nmcli -t -f NAME,DEVICE con show | grep "$ETH_IF" | cut -d: -f1)
fi
sudo nmcli con mod "$CON_NAME" ipv4.addresses "$MASTER_IP/$MASTER_MASK"
sudo nmcli con mod "$CON_NAME" ipv4.method manual
sudo nmcli con up "$CON_NAME"

# --- Install dnsmasq if missing ---
if ! command -v dnsmasq >/dev/null 2>&1; then
    echo "Installing dnsmasq..."
    sudo apt update && sudo apt install -y dnsmasq
fi

# --- Backup & configure dnsmasq ---
sudo cp /etc/dnsmasq.conf /etc/dnsmasq.conf.bak
sudo tee /etc/dnsmasq.conf > /dev/null <<EOF
interface=$ETH_IF
dhcp-range=$DHCP_START,$DHCP_END,12h
bind-interfaces
EOF

sudo systemctl restart dnsmasq
sudo systemctl enable dnsmasq

echo "Master setup complete."
echo "Press Enter to restore system DNS and network..."
read -r
cleanup
