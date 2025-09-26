import os
import pandas as pd
import snowflake.connector
from sklearn.model_selection import train_test_split
import lightgbm as lgb
import shap
import numpy as np
import matplotlib.pyplot as plt
from sklearn.metrics import accuracy_score
import mlflow
from dotenv import load_dotenv
load_dotenv()


# 데이터 로드
def load_data_from_snowflake():
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse="COMPUTE_WH",
        database="RIOT_DB",
        schema="MART"
    )
    query = """
        SELECT * FROM RIOT_DB.MART.VIEW_PLAYER_FEATURE
        WHERE DS >= DATEADD(day, -90, CURRENT_DATE)
        """
    df = pd.read_sql(query, conn)
    conn.close()
    df.columns = [col.upper() for col in df.columns]
    df.dropna(inplace=True)
    return df


#라벨 추가, 모델 학습, shap분석
def analyze_model_indi(df: pd.DataFrame, top_n=10, samples=300):
    df["TEAMPOSITION"] = df["TEAMPOSITION"].astype('category') #범주형으로 바꿔줌
    feature_except = [
        "DS", "MATCHID", "TEAMID", "PARTICIPANTID", "GAMEDURATION",
        "WIN",  
        "TOP_LABEL", "JUNGLE_LABEL", "MID_LABEL", "ADC_LABEL", "SUP_LABEL", "kills", 
        "KILLS", "ASSISTS", "DEATHS" 
    ]
    features = [col for col in df.columns if col not in feature_except]

    position_to_label = {
        "TOP": "TOP_LABEL",
        "JUNGLE": "JUNGLE_LABEL",
        "MIDDLE": "MID_LABEL",
        "BOTTOM": "ADC_LABEL",
        "UTILITY": "SUP_LABEL"
    }

    key_feature = {}

    for pos, label_col in position_to_label.items():
        print(f"{pos} len {len(df[df['TEAMPOSITION'] == pos])}")

        df_pos = df[df["TEAMPOSITION"] == pos]

        if len(df_pos) < samples:
            print("샘플 값이 모자람")
            continue
         
        x = df_pos[features]
        y = df_pos[label_col]

        x_train, x_test, y_train, y_test = train_test_split(
            x,y, test_size=0.2, random_state=29)
        # test_size, train_size, random_state,  RandomState,

        model = lgb.LGBMClassifier(random_state=29) #모델 인스턴스 생성
        model.fit(x_train, y_train)

        #shap
        explainer = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(x_test) #기여도가 담긴 배열

        if isinstance(shap_values, list):
            shap_values = shap_values[1]

        print("SHAP shape", shap_values.shape)


        shap_importance = np.abs(shap_values).mean(axis=0)
        importance_df = pd.DataFrame({
            "feature": x_test.columns,
            "importance": shap_importance
        }).sort_values("importance", ascending=False)

        top_feature = importance_df.head(top_n)["feature"].tolist()
        key_feature[pos] = top_feature

    return key_feature
    
if __name__ == "__main__":
    player_stats_df = load_data_from_snowflake()
    important_features = analyze_model_indi(player_stats_df, top_n=10)
    print("핵심 피처")
    for pos, feats in important_features.items():
        print(f"{pos}: {feats}")