# dependency for fastparquet
echo "customized emr boostrap start now:"
date >> /home/hadoop/bslog.txt
sudo yum install -y python3-devel
sudo yum install -y git

# git config for aws codecommit
git config --global credential.helper '!aws codecommit credential-helper $@'
git config --global credential.UseHttpPath true
git config --global user.name 'Qian Li'
git config --global user.email 'mzqia@amazon.com'

# git clone repos
echo "cloning StudiosResearchDev repo" >> /home/hadoop/bslog.txt
git clone https://git-codecommit.us-east-1.amazonaws.com/v1/repos/StudiosResearchDev /home/hadoop/StudiosResearchDev >> /home/hadoop/bslog.txt 2>&1
echo "cloning Portfolio-Optimization repo" >> /home/hadoop/bslog.txt
git clone https://git-codecommit.us-east-1.amazonaws.com/v1/repos/Portfolio-Optimization /home/hadoop/Portfolio-Optimization >> /home/hadoop/bslog.txt 2>&1

echo "sourcing the .awsalias from .bashrc" >> /home/hadoop/bslog.txt
echo "source /home/hadoop/StudiosResearchDev/.awsalias" >> /home/hadoop/.bashrc

# nodejs for jupyterlab extensions
curl -sL https://rpm.nodesource.com/setup_12.x | sudo -E bash
sudo yum install -y nodejs
sudo pip3 install pip --upgrade

echo "before customized bootstrap, here is the python3 pip pkg version" >> /home/hadoop/bslog.txt
sudo /usr/local/bin/pip3 freeze >> /home/hadoop/bslog.txt

echo "starting of EMR bootstrap, pip-3.6 install pkgs now:" >> /home/hadoop/bslog.txt
date >> /home/hadoop/bslog.txt
sudo /usr/local/bin/pip3 install botocore==1.17.25 --upgrade
sudo /usr/local/bin/pip3 install boto3==1.14.25 --upgrade
sudo /usr/local/bin/pip3 install numpy==1.19.1 --upgrade
sudo /usr/local/bin/pip3 install scipy==1.5.1
sudo /usr/local/bin/pip3 install pandas==1.0.5
sudo /usr/local/bin/pip3 install argparse
sudo /usr/local/bin/pip3 install tqdm
sudo /usr/local/bin/pip3 install csvkit
sudo /usr/local/bin/pip3 install matplotlib==3.3.0
sudo /usr/local/bin/pip3 install plotly==4.9.0
sudo /usr/local/bin/pip3 install seaborn==0.10.1
sudo /usr/local/bin/pip3 install s3fs==0.4.2
sudo /usr/local/bin/pip3 install scikit-learn==0.23.1
sudo /usr/local/bin/pip3 install findspark==1.4.2
sudo /usr/local/bin/pip3 install py4j==0.10.9
sudo /usr/bin/python3 -m pip install pyarrow==2.0.0
sudo /usr/local/bin/pip3 install grpcio==1.30.0
sudo /usr/local/bin/pip3 install protobuf==3.12.2
sudo /usr/local/bin/pip3 install csvkit

# install and configure jupyter
# installing jupyter to /usr/local/bin
sudo /usr/local/bin/pip3 install jupyter==1.0.0
sudo /usr/local/bin/pip3 install jupyterlab==2.2.0

echo "customized python packages installation finished, here is the version:" >> /home/hadoop/bslog.txt
sudo /usr/local/bin/pip3 freeze >> /home/hadoop/bslog.txt
date >> /home/hadoop/bslog.txt

echo "Install git lab extensions now:" >> /home/hadoop/bslog.txt
date >> /home/hadoop/bslog.txt
# install jupyterlab extension
sudo /usr/local/bin/pip3 install jupyterlab-git==0.20.0
sudo /usr/local/bin/jupyter labextension install @jupyterlab/toc

echo "Configuring jupyter lab server now:" >> /home/hadoop/bslog.txt
date >> /home/hadoop/bslog.txt
# root path of notebook/lab session
mkdir -p /home/hadoop/notebookdir
aws s3 cp s3://studiosresearch-applications/notebook/SparkViaJupyterDemo.ipynb /home/hadoop/notebookdir

mkdir -p /home/hadoop/.jupyter
touch ls /home/hadoop/.jupyter/jupyter_notebook_config.py
JUPYTER_PASSWORD="Goliath16"
HASHED_PASSWORD=$(python3 -c "from notebook.auth import passwd; print(passwd('$JUPYTER_PASSWORD'))")
echo "c.NotebookApp.password = u'$HASHED_PASSWORD'" >> /home/hadoop/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.open_browser = False" >> /home/hadoop/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.ip = '*'" >> /home/hadoop/.jupyter/jupyter_notebook_config.py

echo "configuring jupyter deamon and store at /etc/systemd/system/jpl.service now:" >> /home/hadoop/bslog.txt
date >> /home/hadoop/bslog.txt

# change from upstart service using initctl in amzn linux 1 to systemctl in amzn linux2
sudo cat << EOF > /home/hadoop/jpl.service
[Unit]
Description=Host Jupyter Lab with spark config

[Service]
User=hadoop
Group=hadoop
Environment=SPARK_HOME=/usr/lib/spark
Environment=PYSPARK_DRIVER_PYTHON=/usr/local/bin/jupyter
Environment='PYSPARK_DRIVER_PYTHON_OPTS=lab --port=8889'
Environment=PYSPARK_PYTHON=/usr/bin/python3
Environment=JAVA_HOME=/etc/alternatives/jre
Type=simple
WorkingDirectory=/home/hadoop
ExecStart=/usr/bin/pyspark
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo mv /home/hadoop/jpl.service /etc/systemd/system/jpl.service
sudo chown root:root /etc/systemd/system/jpl.service
sudo systemctl enable jpl
sudo systemctl daemon-reload
sudo systemctl start jpl
sleep 5
sudo systemctl status jpl >> /home/hadoop/bslog.txt

export ARROW_PRE_0_15_IPC_FORMAT=1
sudo -E env | grep ARROW_PRE_0_15_IPC_FORMAT >> /home/hadoop/bslog.txt

echo "bootstrap finished:" >> /home/hadoop/bslog.txt
date >> /home/hadoop/bslog.txt