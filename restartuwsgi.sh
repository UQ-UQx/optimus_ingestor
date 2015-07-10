#!/bin/sh

killall -s INT /usr/local/bin/uwsgi
/etc/init/uwsgi.conf
