# Contributor

COMP90024 Project 2 2023

- Aobo Li (1172339)
- Pavith Samarakoon (1297058)
- Zhihao Liang (1367102)
- Jiqiang Chen (1171420)
- Yijun Liu (1132416)

## Mastodon Toot streaming
This directory contains mastodon's streaming script, mastodon folder contains required script for mastodon streaming downloaded from [mastodon.py](https://pypi.org/project/Mastodon.py/#files). Initial attempt uses secrets.sh to store secret ReSTful API key, but intergrated into stream_mastodon.py for convenience. Streamed toots are processed and stored into out CouchDB instance. 

For more information, checkout stream_mastodon.py file or our report. 
