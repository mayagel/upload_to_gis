# author: yagel maimon
# time: 04/12/2025 09:20
# description: script to get blocks_and_parcels table from central-catalog and upload to gis postgres
# steps:
# 1. Connect to central-catalog database
# 2. Query blocks_and_parcels table
# 3. process data and create temp gdb
# 4. Upload data to GIS postgres

import sys
import os
import json

imports_dirname = os.path.dirname(__file__)
main_dirname = os.path.dirname(imports_dirname)
sys.path.append(main_dirname)

# mess with DB
from pathlib import Path
from dataclasses import dataclass
from datetime import date
from tqdm import tqdm
from queries.q_blocks_and_parcels import *

from common.db_operations import DatabaseOperations 
from common.config import DATA_GOV_ZIP_URL, THRESHOLD, FORCE
from common.data_processor import DataProcessor, DataGovObj




