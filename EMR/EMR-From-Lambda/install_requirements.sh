# Python Packages
sudo pip install numpy
sudo pip install scipy
sudo pip install pandas
sudo pip install matplotlib
sudo pip install botocore --upgrade
sudo pip install s3fs
sudo pip install seaborn

sudo pip install tensorflow
sudo pip install keras
sudo pip install jupyter-client
sudo pip install ipykernel
sudo pip install grpcio
sudo pip install protobuf


# install git for aws code-commit
sudo yum install git -y
# setup credential helper, enabling git credential helper to send the path to repositories
sudo git config --global credential.helper '!aws codecommit credential-helper $@'
sudo git config --global credential.UseHttpPath true

