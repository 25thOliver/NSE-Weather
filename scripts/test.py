import s3fs, os
from dotenv import load_dotenv

load_dotenv("../.env")
fs = s3fs.S3FileSystem(
    key=os.getenv("MINIO_ACCESS_KEY"),
    secret=os.getenv("MINIO_SECRET_KEY"),
    client_kwargs={"endpoint_url": os.getenv("MINIO_ENDPOINT")},
)

print(fs.ls("nse-weather/staging/weather"))
