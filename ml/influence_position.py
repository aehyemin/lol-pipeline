import os
import pandas as pd
import snowflake.connector
import lightgbm as lgb
import shap
import numpy as np
from dotenv import load_dotenv
from sklearn.model_selection import train_test_split

load_dotenv()

def load_data_from_snowflake_a():
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse="COMPUTE_WH",
        database="RIOT_DB",
        schema="MART"
    )
    df = pd.read_sql("SELECT * FROM RIOT_DB.MART.VIEW_FEATURE_FOR_MODEL_A;", conn)
    conn.close()

    df.dropna(inplace=True)
    df.columns = [c.upper() for c in df.columns]
    print("데이터로드", df.shape)
    return df

def influence_position_model(df: pd.DataFrame):

    x = df.filter(like="_DIF")
    y = df["BLUE_TEAM_WIN"].astype(int)

    model = lgb.LGBMClassifier(random_state=12)
    model.fit(x, y)
    print("모델 학습 완료")

 
    explainer = shap.TreeExplainer(model)
    sv = explainer.shap_values(x)
    if isinstance(sv, list):
        sv = sv[1] 
    shap_df = pd.DataFrame(np.abs(sv), columns=x.columns)

    feature_groups = {
        "TOP": ["TOP_SOLOKILLS_DIF", "TOP_PLATES_DIF", "TOP_KILLS_DIF"],
        "JNG": ["JUNGLE_DRAGON_DIF", "JUNGLE_GOLD_RATE_DIF",
                "JUNGLE_KILLPARTICIPATION_DIF", "JUNGLE_RIFTHERALD_DIF", "JUNGLE_BARONKILLS_DIF"],
        "MID": ["MID_KILLPARTICIPATION_DIF", "MID_KILLS_DIF", "MID_DAMAGE_RATE_DIF"],
        "BOT": ["BOTTOM_DAMAGE_RATE_DIF", "BOTTOM_GOLD_RATE_DIF", "BOTTOM_KILLPARTICIPATION_DIF"],
        "SUP": ["SUP_EFFECTIVEHEALANDSHIELDING_DIF", "SUP_CC_RATE_DIF",
                "SUP_VISION_PER_MIN_DIF", "SUP_DAMAGE_RATE_DIF", "SUP_KILLPARTICIPATION_DIF", "SUP_WARDS_PM_DIF"],
    }


    pos_impact = pd.DataFrame()
    for pos, feats in feature_groups.items():
        use_feats = [f for f in feats if f in shap_df.columns]
        if not use_feats:
            continue
        pos_impact[pos] = shap_df[use_feats].sum(axis=1)


    row_sum = pos_impact.sum(axis=1).replace(0, np.nan)
    final_influ_per = pos_impact.div(row_sum, axis=0).fillna(0)

    final_avg = (final_influ_per.mean() * 100).round(2).sort_values(ascending=False)
    print(final_avg.to_string() + "%")
    return final_avg

if __name__ == "__main__":
    df = load_data_from_snowflake_a()
    influence_position_model(df)
