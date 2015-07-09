#!/usr/bin/python
# coding=utf-8
''' Running the stand-alone wsgi module '''
from __future__ import absolute_import, print_function, division

from web import app
application = app.wsgi_app

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)
