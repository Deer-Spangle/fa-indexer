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