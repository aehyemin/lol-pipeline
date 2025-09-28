import os, io, tarfile, json, re, boto3
import numpy as np
import pandas as pd, joblib, snowflake.connector
import shap
from typing import Dict, List

ROLES = ["TOP", "JNG", "MID", "BOT", "SUP"]

def normalize_account(act: str)-> str:
    act = act.strip()
    act = re.sub(r"^https?://", "", act, flags=re.I)
    act = act.replace(".snowflakecomputing.com", "").replace(".aws", "")
    return act

def get_secret_manager(name, region="ap-northeast-2"):
    sm = boto3.client("secretsmanager", region_name=region)
    val = sm.get_secret_value(SecretId=name)
    s = val.get("SecretString") or val["SecretBinary"].decode()
    return json.loads(s)


def load_model_from_s3(model_artifact_s3: str, region:str = "ap-northeast-2"):
    s3 = boto3.client("s3", region_name=region)
    bucket, key = model_artifact_s3[5:].split("/", 1)
    byts = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    with tarfile.open(fileobj=io.BytesIO(byts), mode="r:gz") as tar:
        model = None
        for m in tar.getmembers():
            if m.name.endswith("model.joblib"):
                model = joblib.load(tar.extractfile(m))
                break
        if model is None:
            raise RuntimeError("model.joblib not found in model.tar.gz")
        return model
    
def role_groups(columns: List[str]) -> Dict[str,List[int]]:
    role_to_idx = {r: [] for r in ROLES}
    upcols = [c.upper() for c in columns]

    MAP = [
        ("TOP", ["TOP"]),
        ("JNG", ["JNG", "JUNGLE"]),
        ("MID", ["MID", "MIDDLE"]),
        ("BOT", ["BOT", "BOTTOM", "ADC"]),
        ("SUP", ["SUP", "UTILITY", "SUPPORT"]),
    ]    
    for i, c in enumerate(upcols):
        for role, keys in MAP:
            if any(k in c for k in keys):
                role_to_idx[role].append(i)
                break
    return role_to_idx


def compute_role_contrib(ds:str, model_artifact_s3:str, **_):
    model = load_model_from_s3(model_artifact_s3)

    secret = get_secret_manager("riot/snowflake/train")
    act = normalize_account(secret["SNOWFLAKE_ACCOUNT"])

    conn_kwargs = dict(
        user=secret["SNOWFLAKE_USER"],
        password=secret["SNOWFLAKE_PASSWORD"],
        account=act,
        warehouse=secret.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=secret.get("SNOWFLAKE_DATABASE", "RIOT_DB"),
        schema=secret.get("SNOWFLAKE_SCHEMA", "MART"),
    )
    if secret.get("SNOWFLAKE_REGION") and "." not in act:
        conn_kwargs["region"] = secret["SNOWFLAKE_REGION"]
    conn = snowflake.connector.connect(**conn_kwargs)

    # 3) ds 피처 로드 (학습과 동일 뷰)
    sql = f"""
        SELECT * FROM RIOT_DB.MART.VIEW_FEATURE_FOR_MODEL_A
        WHERE DS = TO_DATE('{ds}','YYYY-MM-DD')
    """
    df = pd.read_sql(sql, conn)

    if df.empty:
        conn.close()
        return f"No rows to explain for ds={ds}"

    df.columns = [c.upper() for c in df.columns]
    x = df.filter(like="_DIF")
    if x.empty:
        conn.close()
        return "No *_DIF columns found"


    groups = role_groups(list(x.columns))

   
    explainer = shap.TreeExplainer(model)  
    shap_values = explainer.shap_values(x)
    if isinstance(shap_values, list):     
        shap_values = shap_values[1]

    abs_shap = np.abs(shap_values) 
    total_abs = abs_shap.sum(axis=1) 

   
    out_rows = []
    for idx, row in df.reset_index(drop=True).iterrows():
        denom = total_abs[idx]
        if denom == 0:
            share = 100.0 / len(ROLES)
            for r in ROLES:
                out_rows.append((row["DS"], row["MATCHID"], r, round(share, 2)))
            continue

        for r in ROLES:
            cols = groups.get(r, [])
            contrib = abs_shap[idx, cols].sum() / denom * 100.0 if cols else 0.0
            out_rows.append((row["DS"], row["MATCHID"], r, round(float(contrib), 2)))

    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS RIOT_DB.MART.ROLE_WIN_CONTRIB (
            DS DATE,
            MATCHID STRING,
            ROLE STRING,
            CONTRIB_PCT FLOAT
        )
    """)
    cur.execute("DELETE FROM RIOT_DB.MART.ROLE_WIN_CONTRIB WHERE DS = TO_DATE(%s)", (ds,))
    cur.executemany(
        "INSERT INTO RIOT_DB.MART.ROLE_WIN_CONTRIB (DS, MATCHID, ROLE, CONTRIB_PCT) VALUES (TO_DATE(%s), %s, %s, %s)",
        out_rows
    )
    cur.close()
    conn.close()

   
    s = pd.DataFrame(out_rows, columns=["DS","MATCHID","ROLE","CONTRIB_PCT"])
    daily = s.groupby("ROLE")["CONTRIB_PCT"].mean().sort_values(ascending=False).round(2)
    print("[ROLE AVG CONTRIBUTION % for ds]", ds)
    for r, v in daily.items():
        print(f"{r:<4} {v:>6.2f}")

    return f"{len(out_rows)} rows into ROLE_WIN_CONTRIB for ds={ds}"