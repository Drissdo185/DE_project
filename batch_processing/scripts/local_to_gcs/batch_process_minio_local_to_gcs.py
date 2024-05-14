# Load minio local to GCS

import os
import pickle
import re
import sys
from io import BytesIO
from pathlib import Path
from typing import Optional, Union

import numpy as np
from minio import Minio
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, when
from pyspark.sql.types import IntegerType, MapType, StringType
from tqdm import tqdm
from utils.helpers import load_cfg



# MinIO config
CFG_FILE = "../..config.yaml"