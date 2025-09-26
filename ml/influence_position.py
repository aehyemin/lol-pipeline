import os
import pandas as pd
import snowflake.connector
import lightgbm as lgb
import shap
import numpy as np
from dotenv import load_dotenv

load_dotenv()

def load_data_from_snowflake_a():
    #데잍터 로드
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse="COMPUTE_WH",
        database="RIOT_DB",
        schema="MART"
    )
    query = """
            SELECT * FROM RIOT_DB.MART.VIEW_FEATURE_FOR_MODEL_A;
        """ 
    df = pd.read_sql(query, conn)
    conn.close()

    df.dropna(inplace=True)
    df.columns = [col.upper() for col in df.columns]
    print("데이터로드")
    return df


def influece_position_model(df:pd.DataFrame):
    x = df.filter(like='_DIF')
    y = df['BLUE_TEAM_WIN']

    model_a = lgb.LGBMClassifier(random_state=12)
    model_a.fit(x,y)
    print("모델 학습")

    explainer = shap.TreeExplainer(model_a)
    shap_values = explainer.shap_values(x)

    shap_df = pd.DataFrame(np.abs(shap_values), columns=x.columns)

    feature_groups = {
        "TOP": ["TOP_SOLOKILLS_DIF", "TOP_PLATES_DIF"],
        "JNG": ["JUNGLE_DRAGON_DIF", "JUNGLE_RIFTHERALD_DIF", "JUNGLE_BARONKILLS_DIF"],
        "MID": ["MID_KILLPARTICIPATION_DIF"],
        "BOT": ["BOTTOM_TEAMDAMAGEPER_DIF"],
        "SUP": ["SUP_VISIONPER_DIF"]
    }

    pos_impact = pd.DataFrame()

    for pos, feats in feature_groups.items():
        if all(f in shap_df.columns for f in feats):
            pos_impact[pos] = shap_df[feats].sum(axis=1) / len(feats)
        

    final_influ_per = pos_impact.div(pos_impact.sum(axis=1), axis=0)

    final_avg = final_influ_per.mean() * 100

    print(final_avg.sort_values(ascending=False).round(2).to_string() + "%")



if __name__ == "__main__":
    df = load_data_from_snowflake_a()
    final_results = influece_position_model(df)