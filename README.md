optimus_ingestor
================

Ingests data from the edX data package into usable database stored structres


Optimus Ingestor
========
The injestor is a daemon which runs a series of services for ingesting the edX data package.
Each service scans a data directory for specific data and ingests it into either a MySQL or MongoDB
database.  These scans are run and repeated every 5 minutes.  The daemon also provides a REST API to
read the current state of the ingestion.  

The ingested databases are designed to work in conjunction with the UQx-API application: https://github.com/UQ-UQx/uqx_api


Requirements
---------------------
To use the iptocountry service you will need to install the GeoIP2Country.mmdb (available from https://www.maxmind.com/en/country)
```bash
cp GeoIP2-Country.mmdb [BASE_PATH]/services/iptocountry/lib/
```

Installation
---------------------
[BASE_PATH] is the path where you want the injestor installed (such as /var/injestor)

Clone the repository
```bash
git clone https://github.com/UQ-UQx/injestor.git [BASE_PATH]
```
Install pip requirements
```bash
sudo apt-get install libxml2-dev libxslt1-dev python-dev
pip install -r requirements.txt
```
Set injestor configuration
```bash
cp [BASE_PATH]/config.example.py [BASE_PATH]/config.py
vim [BASE_PATH]/config.py
[[EDIT THE VALUES]]
```
Install init.d script (optional, only tested on Centos6.5)
```bash
ln [BASE_PATH]/init.d/injestor /etc/init.d/injestor
```
Run injestor
```bash
/etc/init.d/injestor start
(or) [BASE_PATH]/injestor.py start
```
If you need to test a particular service, you can run
```bash
[BASE_PATH]/test.py [[SERVICENAME]]
```
If you wish to deploy the ingestor to a server, you can use the supplied fab (http://www.fabfile.org/) script (once the config is set)
```
fab prepare deploy
```

Architecture
---------------------

Running Tests
---------------------
Currently the project is at an early stage and does not have reliable tests created.

License
---------------------
This project is licensed under the terms of the MIT license.

How to Contribute
---------------------
Currently the injestor project is at a very early stage and unlikely to accept pull requests
in a timely fashion as the structure may change without notice.
However feel free to open issues that you run into and we can look at them ASAP.

Contact
---------------------
The best contact point apart from opening github issues or comments is to email 
technical@uqx.uq.edu.au