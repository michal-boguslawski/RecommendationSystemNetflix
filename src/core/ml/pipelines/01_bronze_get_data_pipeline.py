import os
import sys
from ..data.ingest import main


if __name__ == "__main__":
    # download dataset
    dataset_path = sys.argv[1] if len(sys.argv) > 1 else "https://www.kaggle.com/api/v1/datasets/download/netflix-inc/netflix-prize-data"
    local_path = sys.argv[2] if len(sys.argv) > 2 else "data/bronze/"
    bucket_name = os.getenv("MINIO_BUCKET_NAME", None)
    main(dataset_path, local_path, bucket_name)
