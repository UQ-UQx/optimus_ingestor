optimus_ingestor
================

Ingests data from the edX data package into usable database stored structres


Optimus Ingestor
========
The optimus ingestor is a daemon which runs a series of services for ingesting the edX data package.
Each service scans a data directory for specific data and saves its file reference.  Later, the service ingests it into either a MySQL or MongoDB
database.  These scans are run and repeated every 5 minutes.  The daemon also provides a REST API to
read the current state of the ingestion.  

The ingested databases are designed to work in conjunction with the UQx-API application: https://github.com/UQ-UQx/uqx_api


Requirements
---------------------
- Python 2.7
- MySQL with local-infile (see below)

Make sure that your MySQL installation can use 'local-infile' (see http://dev.mysql.com/doc/refman/5.1/en/server-system-variables.html#sysvar_local_infile)

To use the iptocountry service you will need to install the GeoIP2Country.mmdb (available from https://www.maxmind.com/en/country)
```bash
cp GeoIP2-Country.mmdb [BASE_PATH]/services/iptocountry/lib/
```

The files extracted from the edX research package should be organised in the following manner

```
/data
    /clickstream_logs
        /latest (symlink)
        /2014-01-01
        /2014-01-07
    /database_state
        /latest (symlink)
        /2014-01-01
        /2014-01-07
```

The ingestor will look at these latest symlinked directories


Installation
---------------------
[BASE_PATH] is the path where you want the injestor installed (such as /var/ingestor)

Clone the repository
```bash
git clone https://github.com/UQ-UQx/optimus_ingestor.git [BASE_PATH]
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
Edit the courses which will be ingested (note, keep 'default' and 'person_course')
```bash
vim [BASE_PATH]/courses.py
[[EDIT THE VALUES IN 'EDX_DATABASES']]
```
Run injestor
```bash
/etc/init.d/ingestor
(or) [BASE_PATH]/ingestor.py
```
If you wish to deploy the ingestor to a server, you can use the supplied fab (http://www.fabfile.org/) script (once the config is set)
```
fab prepare deploy
```

Extraction Example
---------------------
The optimus ingestor does not deal with the initial extraction of data from the edX s3 repository.  However an example script (based on the UQx extraction process)
can be found in extract_data.example.sh.  

Architecture
---------------------
The architecture of the Optimus Ingestor is a service-based application which has two aspects.  Firstly, the Ingestor calls each service and provides them
with an array of files from the exported edX data.  Each service replies with which files they are interested in, and the references to these files are stored 
in a MySQL database.  The service then is run and looks for uningested files in the database, and ingests them through the run() method.  Services can check the
queue of uningested data for other services, to establish when a service should be run (data dependencies).  

The flow of the data is as follows:
![Optimus Ingestor](/README_ARCHITECTURE_IMAGE.png?raw=true "Optimus Ingestor")

Service Logic
---------------------
The service logic is as follows:
 - Clickstream - Ingests clicks into MongoDB (no requirements)
 - Database State - Updates the SQL tables from the SQL dumps (no requirements)
 - Discussion Forums - Ingests discussion forum tables into MongoDB (no requirements)
 - IP To Country - Updates Mongo records with Country attributes where IP is present (requires complete clickstream)
 - Time Finder - Updates Mongo records with Date object attributes where a date string is present (requires complete clickstream)
 - Person Course - Generates Person Course SQL table (requires complete IPToCountry and Timefinder)

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