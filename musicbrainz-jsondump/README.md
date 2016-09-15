# Export Album Data from MusicBrainz

All the code is in the notebook: [Notebook.ipynb](Notebook.ipynb)

## Installation

```
git clone https://github.com/smlz/musicbrainz-jsondump.git
cd musicbrainz-jsondump/
virtualenv -p /usr/bin/python3 .venv
source .venv/bin/activate
pip install notebook SQLAlchemy psycopg2 PyLyrics
```

#### Get a MusicBrainz VM
Get it from here:
https://musicbrainz.org/doc/MusicBrainz_Server/Setup

... and run it using VirtualBox.

For the VM do:
* Add a NAT-port mapping from 6543 to 5432 (in VirtualBox)
* Set listen_address to '*' in /etc/postgresql/9.3/main/postgresql.conf
* Restart PostgreSQL (service postgresql restart)

## Run the Notebook

```
./.venv/bin/jupyter notebook
```
