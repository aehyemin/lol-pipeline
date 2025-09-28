
from sagemaker.sklearn import SKLearn
import sagemaker
import os ,boto3

role_arn = "arn:aws:iam::174919262157:role/riot-pipeline-role"  

boto_sess = boto3.Session(region_name="ap-northeast-2")
sm_sess  = sagemaker.Session(boto_session=boto_sess)
est = SKLearn(
    entry_point="sm_train.py",
    source_dir="ml",
    role=role_arn,
    framework_version="1.2-1",
    py_version="py3",
    instance_type="ml.m5.large",
    instance_count=1,
    sagemaker_session=sm_sess,
    hyperparameters={
        "secret-name": "riot/snowflake/train"
    },
  
    output_path="s3://my-riot-ml-pipeline-project/sm-artifacts/",
    code_location="s3://my-riot-ml-pipeline-project/sm-code/",
)
est.fit(wait=True)
print("complete")


