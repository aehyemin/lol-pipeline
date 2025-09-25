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

#모델 학습, shap분석
def analyze_model(df: pd.DataFrame):
    df["TEAMPOSITION"] = df["TEAMPOSITION"].astype('category') #범주형으로 바꿔줌
    feature_except = ['DS', "MATCHID", "TEAMID", "PARTICIPANTID", "WIN"]
    features = [col for col in df.columns if col not in feature_except]
    x = df[features]
    y = df["WIN"]

    x_train, x_test, y_train, y_test = train_test_split(x,y, test_size=0.2, random_state=29)
    # test_size, train_size, random_state,  RandomState,
    model = lgb.LGBMClassifier(random_state=29) #모델 인스턴스 생성
    model.fit(x_train, y_train)
    print("학습완료")

    #shap
    explainer = shap.TreeExplainer(model)

    shap_values = explainer.shap_values(x_test) #기여도가 담긴 배열
    print("SHAP shape", shap_values.shape)

    key_feature = {}
    positions = x_test["TEAMPOSITION"].cat.categories

    for pos in positions:
        pos_idx = x_test["TEAMPOSITION"] == pos
        if pos_idx.sum() == 0:
            continue
        pos_shap = shap_values[pos_idx.values, :]  #해당포지션 shap값
        shap_importance = np.abs(pos_shap).mean(axis=0)  #중요도 평균 피처
        top3 = x_test.columns[np.argsort(shap_importance)[-3:][::-1]].tolist()

        key_feature[pos] = top3


        
    
    print("포지션별 top3")
    for pos, feature in key_feature.items():
        print(f"{pos}: {feature}")
    return key_feature

if __name__ == "__main__":
    player_stats_df = load_data_from_snowflake()
    important_feature = analyze_model(player_stats_df)

