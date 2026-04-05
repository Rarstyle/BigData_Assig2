#!/usr/bin/env python3
import os
import runpy
import sys


ROOT = os.path.dirname(os.path.abspath(__file__))
APP_DIR = os.path.join(ROOT, "app")

os.chdir(APP_DIR)
sys.path.insert(0, APP_DIR)
runpy.run_path(os.path.join(APP_DIR, "query.py"), run_name="__main__")
