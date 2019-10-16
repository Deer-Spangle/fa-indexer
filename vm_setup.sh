## First steps
# 1) Create f1-micro VM, with debian 10
# 2) Add ssh public key
# 3) Copy this file to vm_setup.sh on the VM
# 4) chmod +x vm_setup.sh
# 5) ./vm_setup.sh
sudo apt install git screen python3-pip
echo "alias s='screen -dr'" > .bash_aliases
git clone https://github.com/joshcoales/fa-indexer
cd fa-indexer
pip3 install -r requirements.txt
cat > config.json <<- EOM
{
  "LOGIN_COOKIE": {
    "a": "",
    "b": ""
  },
  "START": 24000000,
  "END": 25000000
}
EOM
nano config.json