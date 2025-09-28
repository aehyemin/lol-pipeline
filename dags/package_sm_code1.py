#sagemaker는 로컬 pc의 파일을 읽을 수 없기 때문에,
#s3에 업로드된 압출 파일을 가져가서 사용한다. 이 과정을 airflow가 자동으로 처리하도록



import os, tarfile, boto3, time


def make_tar(source_dir: str, out_path:str):
    with tarfile.open(out_path, "w:gz") as tar:
        for name in ["sm_train.py","requirements.txt", "__init__.py"]:
            p = os.path.join(source_dir, name)
            if os.path.exists(p):
                tar.add(p, arcname=name)

def package_and_upload(**context):
    s3_bucket = "my-riot-ml-pipeline-project"
    s3_prefix="sm-code"
    src_dir= "/opt/airflow/ml"
    os.makedirs("/opt/airflow/tmp", exist_ok=True)
    ts= time.strftime("%Y%m%d-%H%M%S")
    tar_local = f"/opt/airflow/tmp/sm_code_{ts}.tar.gz"
    make_tar(src_dir, tar_local)
    s3 = boto3.client("s3", region_name="ap-northeast-2")
    key = f"{s3_prefix}/sm_code_{ts}.tar.gz"
    s3.upload_file(tar_local, s3_bucket, key)

    return f"s3://{s3_bucket}/{key}"