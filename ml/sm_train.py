# import os
# import json
# import boto3
# import pandas as pd
# import snowflake.connector
# import lightgbm as lgb
# import joblib
# import argparse


# def get_snowflake_creds(secret_name: str, region: str):
#     sm = boto3.client("secretsmanager", region_name=region)
#     resp = sm.get_secret_value(SecretId=secret_name)
#     payload = resp.get("SecretString") or resp["SecretBinary"].decode()
#     return json.loads(payload)

# def sm_train(args):

#     region = os.environ.get("AWS_REGION", "ap-northeast-2")
#     secret = get_snowflake_creds(args.secret_name, region)
#     user = secret["SNOWFLAKE_USER"]
#     pwd  = secret["SNOWFLAKE_PASSWORD"]
#     acct = secret["SNOWFLAKE_ACCOUNT"]
#     wh   = secret.get("SNOWFLAKE_WAREHOUSE","COMPUTE_WH")
#     db   = secret.get("SNOWFLAKE_DATABASE","RIOT_DB")
#     sch  = secret.get("SNOWFLAKE_SCHEMA","MART")

#     #데이터로드
#     conn = snowflake.connector.connect(
#         user=user, password=pwd, account=acct,
#         warehouse=wh, database=db, schema=sch
#     )
#     df = pd.read_sql("SELECT * FROM RIOT_DB.MART.VIEW_FEATURE_FOR_MODEL_A;", conn)
#     conn.close()

#     df.dropna(inplace=True)
#     df.columns = [c.upper() for c in df.columns]
#     print("데이터로드", df.shape)


# #모델학습

#     x = df.filter(like="_DIF")
#     y = df["BLUE_TEAM_WIN"].astype(int)

#     model = lgb.LGBMClassifier(random_state=12)
#     model.fit(x, y)
#     print("모델 학습 완료")

#     #모델저장
#     model_path = os.path.join(args.model_dir, "model.joblib")
#     print(f"모델 저장{model_path}")


# if __name__ == "__main__":
#     p = argparse.ArgumentParser()
#     p.add_argument("--secret-name", type=str, default="riot/snowflake/train")
#     args = p.parse_args()
#     sm_train(args)

import os, json, boto3, argparse, traceback
import pandas as pd
import snowflake.connector
import lightgbm as lgb
import joblib
import os, json, boto3, argparse, traceback, re
import pandas as pd
import snowflake.connector
import lightgbm as lgb
import joblib

def get_secret(name, region):
    sm = boto3.client("secretsmanager", region_name=region)
    val = sm.get_secret_value(SecretId=name)
    s = val.get("SecretString") or val["SecretBinary"].decode()
    return json.loads(s)

def _normalize_account(acct: str) -> str:
    acct = acct.strip()
    acct = re.sub(r"^https?://", "", acct, flags=re.I)
    acct = acct.replace(".snowflakecomputing.com", "")
    acct = acct.replace(".aws", "")
    return acct

def sm_train(args):
    region = os.environ.get("AWS_REGION", "ap-northeast-2")
    print(f"[INFO] AWS region: {region}")
    print(f"[INFO] Secret name: {args.secret_name}")

    sec = get_secret(args.secret_name, region)
    safe_print = {k: ("***" if "PASS" in k.upper() else v) for k, v in sec.items()}
    print(f"[INFO] Loaded secret keys: {list(safe_print.keys())}")
    print(f"[INFO] Secret (safe): {safe_print}")

    user = sec["SNOWFLAKE_USER"]
    pwd  = sec["SNOWFLAKE_PASSWORD"]
    wh   = sec.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    db   = sec.get("SNOWFLAKE_DATABASE", "RIOT_DB")
    sch  = sec.get("SNOWFLAKE_SCHEMA", "MART")

    raw_acct = sec.get("SNOWFLAKE_ACCOUNT", "")
    if not raw_acct:
        raise ValueError("SNOWFLAKE_ACCOUNT must be set in the secret.")
    acct = _normalize_account(raw_acct)

    sf_reg = (sec.get("SNOWFLAKE_REGION") or "").strip() or None


    conn_kwargs = dict(
        user=user,
        password=pwd,
        warehouse=wh,
        database=db,
        schema=sch,
        account=acct,        
       
    )

    if sf_reg and "." not in acct:
        conn_kwargs["region"] = sf_reg

    print("[INFO] Final Snowflake params (safe): "
          f"account={conn_kwargs.get('account')}, region={conn_kwargs.get('region')}, "
          f"wh={wh}, db={db}, sch={sch}")

    print("[INFO] Connecting to Snowflake...")
    conn = snowflake.connector.connect(**conn_kwargs)
    print("[INFO] Connected.")

    query = "SELECT * FROM RIOT_DB.MART.VIEW_FEATURE_FOR_MODEL_A;"
    df = pd.read_sql(query, conn)
    conn.close()


    df.dropna(inplace=True)
    df.columns = [c.upper() for c in df.columns]

    x = df.filter(like="_DIF")
    y = df["BLUE_TEAM_WIN"].astype(int)

    print("[INFO] Training LightGBM...")
    model = lgb.LGBMClassifier(random_state=42)
    model.fit(x, y)
    print("[INFO] Training done.")

    out_dir = os.environ.get("SM_MODEL_DIR", "/opt/ml/model")
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "model.joblib")
    joblib.dump(model, path)
    print(f"[INFO] Saved model to: {path}")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--secret-name", type=str, default="riot/snowflake/train")
    a = p.parse_args()
    try:
        sm_train(a)
    except Exception:
        print("[ERROR] Training failed with exception below:")
        traceback.print_exc()
        raise
