import sys
import wget
from zipfile import ZipFile


if __name__ == "__main__":
    # download dataset
    dataset_path = sys.argv[1] if len(sys.argv) > 1 else "https://www.kaggle.com/api/v1/datasets/download/netflix-inc/netflix-prize-data"
    local_file_path = sys.argv[2] if len(sys.argv) > 2 else "data/raw/"
    file_path = wget.download(dataset_path, local_file_path)
    
    with ZipFile(file_path, "r") as obj:
        obj.extractall(local_file_path)
