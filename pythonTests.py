#!/usr/bin/python3
import subprocess
from time import sleep
import os
import shutil
from datetime import datetime
import yaml
import numpy as np
from android_controller import AndroidController

UE = AndroidController()
UE.get_lockScreen_status()