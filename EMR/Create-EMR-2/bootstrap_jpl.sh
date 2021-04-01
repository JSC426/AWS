echo "starting of EMR bootstrap, pip-3.6 install pkgs now:" >> /home/hadoop/bslog.txt
date >> /home/hadoop/bslog.txt
sudo pip-3.6 install botocore --upgrade
sudo pip-3.6 install boto3
sudo pip-3.6 install numpy
sudo pip-3.6 install scipy
sudo pip-3.6 install pandas
sudo pip-3.6 install argparse
sudo pip-3.6 install matplotlib
sudo pip-3.6 install plotly
sudo pip-3.6 install seaborn
sudo pip-3.6 install s3fs
sudo pip-3.6 install scikit-learn
sudo pip-3.6 install findspark
sudo pip-3.6 install py4j
sudo pip-3.6 install grpcio
sudo pip-3.6 install protobuf

# install and configure jupyter
# installing jupyter to /usr/local/bin
sudo pip-3.6 install jupyter
sudo pip-3.6 install jupyterlab

echo "Pip installation finished, install git and node now:" >> /home/hadoop/bslog.txt
date >> /home/hadoop/bslog.txt
# # nodejs for jupyterlab extensions
sudo yum install -y gcc-c++ make git
curl -sL https://rpm.nodesource.com/setup_12.x | sudo -E bash -
sudo yum install -y nodejs

# git config for aws codecommit
git config --global credential.helper '!aws codecommit credential-helper $@'
git config --global credential.UseHttpPath true


echo "Install git lab extensions now:" >> /home/hadoop/bslog.txt
date >> /home/hadoop/bslog.txt
# install jupyterlab extension
sudo pip-3.6 install jupyterlab-git
sudo /usr/local/bin/jupyter labextension install @jupyterlab/toc

echo "Configuring jupyter lab server now:" >> /home/hadoop/bslog.txt
date >> /home/hadoop/bslog.txt
# root path of notebook/lab session
mkdir -p /home/hadoop/notebookdir
aws s3 cp s3://studiosresearch-applications/notebook/SparkViaJupyterDemo.ipynb /home/hadoop/notebookdir
# if installed with --user instead of sudo, jupyter will be in /home/hadoop/.local/bin, need to be add to PATH
# echo "export PATH=/home/hadoop/.local/bin:$PATH" >> /home/hadoop/.bash_profile

mkdir -p /home/hadoop/.jupyter
touch ls /home/hadoop/.jupyter/jupyter_notebook_config.py
JUPYTER_PASSWORD="Goliath16"
HASHED_PASSWORD=$(python3 -c "from notebook.auth import passwd; print(passwd('$JUPYTER_PASSWORD'))")
echo "c.NotebookApp.password = u'$HASHED_PASSWORD'" >> /home/hadoop/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.open_browser = False" >> /home/hadoop/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.ip = '*'" >> /home/hadoop/.jupyter/jupyter_notebook_config.py

# writing a config file with bash script in it
# keep it running in a background daemon with Upstart
# By setting env variables, pyspark will trigger a jupyter lab session
# change it accordingly to switch to pyspark-python shell or jupyter notebook
# githubrepo: "https://gist.github.com/nicor88/5260654eb26f6118772551861880dd67"
# blog:       "https://bytes.babbel.com/en/articles/2017-07-04-spark-with-jupyter-inside-vpc.html"


echo "configuring jupyter deamon now:" >> /home/hadoop/bslog.txt
date >> /home/hadoop/bslog.txt

sudo cat << EOF > /home/hadoop/jupyter.conf
description "Jupyter"
author      "babbel-data-eng"

start on runlevel [2345]
stop on runlevel [016]

respawn
respawn limit 0 10

chdir /home/hadoop/notebookdir

script
  		sudo su - hadoop > /var/log/jupyter.log 2>&1 << BASH_SCRIPT
	export SPARK_HOME=/usr/lib/spark
	export PYSPARK_DRIVER_PYTHON=/usr/local/bin/jupyter
	export PYSPARK_DRIVER_PYTHON_OPTS="lab --port=8889 --log-level=INFO"
	export PYSPARK_PYTHON=/usr/bin/python3
	export JAVA_HOME=/etc/alternatives/jre
    # kick off jupyter lab
    pyspark
  	   BASH_SCRIPT

end script
EOF

# move it to right location
sudo mv /home/hadoop/jupyter.conf /etc/init/
sudo chown root:root /etc/init/jupyter.conf

# be sure that jupyter daemon is registered in initctl
sudo initctl reload-configuration

# start jupyter daemon
sudo initctl start jupyter
sleep 20
sudo initctl restart jupyter


echo "bootstrap finished:" >> /home/hadoop/bslog.txt
date >> /home/hadoop/bslog.txt