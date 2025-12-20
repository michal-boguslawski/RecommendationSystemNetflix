import os
from ..data.preprocess import preprocess_neflix_user_data_multiple_files, preprocess_netflix_movie_data


if __name__ == "__main__":
    print(20 * "=", "Preprocessing Netflix data", 20 * "=")
    BUCKET = os.getenv("MINIO_BUCKET_NAME", "recommendation-system")
    OUTPUT_PATH = "s3a://recommendation-system/data/silver/netflix_user_data/v1"
    preprocess_neflix_user_data_multiple_files(
        bucket_name=BUCKET,
        file_name="combined_data",
        output_path=OUTPUT_PATH
    )
    # preprocess_netflix_user_data_file()
    preprocess_netflix_movie_data()
    print(20 * "=", "Done preprocessing data!", 20 * "=")
