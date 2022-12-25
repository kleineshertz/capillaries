# Prometheus server (assuming node exporter also running on it)
# https://www.digitalocean.com/community/tutorials/how-to-install-prometheus-on-ubuntu-16-04

# Expecting
# PROMETHEUS_VERSION=2.41.0
# PROMETHEUS_TARGETS=\'localhost:9100\',\'10.5.0.2:9100\'

# Create users
sudo useradd --no-create-home --shell /bin/false prometheus

# Before we download the Prometheus binaries, create the necessary directories for storing Prometheus’ files and data. Following standard Linux conventions, we’ll create a directory in /etc for Prometheus’ configuration files and a directory in /var/lib for its data.
sudo mkdir /etc/prometheus
sudo mkdir /var/lib/prometheus

# Now, set the user and group ownership on the new directories to the prometheus user.
sudo chown prometheus:prometheus /etc/prometheus
sudo chown prometheus:prometheus /var/lib/prometheus

# Downloading Prometheus
PROMETHEUS_DL_FILE=prometheus-$PROMETHEUS_VERSION.linux-amd64
cd ~
sudo rm -f $PROMETHEUS_DL_FILE.gz
echo Downloading https://github.com/prometheus/prometheus/releases/download/v$PROMETHEUS_VERSION/$PROMETHEUS_DL_FILE.tar.gz
curl -LO https://github.com/prometheus/prometheus/releases/download/v$PROMETHEUS_VERSION/$PROMETHEUS_DL_FILE.tar.gz
if [ "$?" -ne "0" ]; then
    echo Cannot download, exiting
    exit $?
fi
tar xvf $PROMETHEUS_DL_FILE.tar.gz

# Copy the two binaries to the /usr/local/bin directory.

sudo cp $PROMETHEUS_DL_FILE/prometheus /usr/local/bin/
sudo cp $PROMETHEUS_DL_FILE/promtool /usr/local/bin/

# Set the user and group ownership on the binaries to the prometheus user created in Step 1.
sudo chown prometheus:prometheus /usr/local/bin/prometheus
sudo chown prometheus:prometheus /usr/local/bin/promtool

# Copy the consoles and console_libraries directories to /etc/prometheus.
sudo cp -r $PROMETHEUS_DL_FILE/consoles /etc/prometheus
sudo cp -r $PROMETHEUS_DL_FILE/console_libraries /etc/prometheus

# Set the user and group ownership on the directories to the prometheus user. Using the -R flag will ensure that ownership is set on the files inside the directory as well.
sudo chown -R prometheus:prometheus /etc/prometheus/consoles
sudo chown -R prometheus:prometheus /etc/prometheus/console_libraries

# Lastly, remove the leftover files from your home directory as they are no longer needed.
rm -rf $PROMETHEUS_DL_FILE.tar.gz $PROMETHEUS_DL_FILE

PROMETHEUS_YAML_FILE=/etc/prometheus/prometheus.yml

sudo rm -f $PROMETHEUS_YAML_FILE
sudo touch $PROMETHEUS_YAML_FILE
sudo chown -R prometheus:prometheus $PROMETHEUS_YAML_FILE

echo "global:" | sudo tee -a $PROMETHEUS_YAML_FILE
echo "  scrape_interval: 15s" | sudo tee -a $PROMETHEUS_YAML_FILE
echo "scrape_configs:" | sudo tee -a $PROMETHEUS_YAML_FILE
echo "  - job_name: 'prometheus'" | sudo tee -a $PROMETHEUS_YAML_FILE
echo "    scrape_interval: 5s" | sudo tee -a $PROMETHEUS_YAML_FILE
echo "    static_configs:" | sudo tee -a $PROMETHEUS_YAML_FILE
echo "      - targets: ['localhost:9090']" | sudo tee -a $PROMETHEUS_YAML_FILE
echo "  - job_name: 'node_exporter'" | sudo tee -a $PROMETHEUS_YAML_FILE
echo "    scrape_interval: 5s" | sudo tee -a $PROMETHEUS_YAML_FILE
echo "    static_configs:" | sudo tee -a $PROMETHEUS_YAML_FILE
echo "      - targets: [$PROMETHEUS_TARGETS]" | sudo tee -a $PROMETHEUS_YAML_FILE
  
PROMETHEUS_SERVICE_FILE=/etc/systemd/system/prometheus.service

sudo rm -f $PROMETHEUS_SERVICE_FILE
sudo touch $PROMETHEUS_SERVICE_FILE

echo "[Unit]" | sudo tee -a $PROMETHEUS_SERVICE_FILE
echo "Description=Prometheus server" | sudo tee -a $PROMETHEUS_SERVICE_FILE
echo "Wants=network-online.target" | sudo tee -a $PROMETHEUS_SERVICE_FILE
echo "After=network-online.target" | sudo tee -a $PROMETHEUS_SERVICE_FILE
echo "" | sudo tee -a $PROMETHEUS_SERVICE_FILE
echo "[Service]" | sudo tee -a $PROMETHEUS_SERVICE_FILE
echo "User=prometheus" | sudo tee -a $PROMETHEUS_SERVICE_FILE
echo "Group=prometheus" | sudo tee -a $PROMETHEUS_SERVICE_FILE
echo "Type=simple" | sudo tee -a $PROMETHEUS_SERVICE_FILE
echo "ExecStart=/usr/local/bin/prometheus --config.file /etc/prometheus/prometheus.yml --storage.tsdb.path /var/lib/prometheus/ --storage.tsdb.retention=60d --web.console.templates=/etc/prometheus/consoles --web.console.libraries=/etc/prometheus/console_libraries" | sudo tee -a $PROMETHEUS_SERVICE_FILE
echo "" | sudo tee -a $PROMETHEUS_SERVICE_FILE
echo "[Install]" | sudo tee -a $PROMETHEUS_SERVICE_FILE
echo "WantedBy=multi-user.target" | sudo tee -a $PROMETHEUS_SERVICE_FILE

sudo systemctl daemon-reload

sudo systemctl start prometheus
sudo systemctl status prometheus
curl http://localhost:9090
if [ "$?" -ne "0" ]; then
    echo Cannot check localhost:9090
    exit $?
fi