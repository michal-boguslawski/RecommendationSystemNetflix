from dotenv import load_dotenv
import os
import pandas as pd
from pathlib import Path
import pyarrow.fs as fs


env_path = os.path.join(Path(__file__).parent.parent.absolute(), ".env")
load_dotenv(env_path)


def get_s3_config_for_pandas():
    return fs.S3FileSystem(
            access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
            secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
            endpoint_override=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),  # MinIO endpoint
        )

def load_pandas_df_from_s3(bucket_name: str, file_path: str) -> pd.DataFrame:
    s3fs = get_s3_config_for_pandas()
    df = pd.read_parquet(f"{bucket_name}/{file_path}", filesystem=s3fs)
    return df
