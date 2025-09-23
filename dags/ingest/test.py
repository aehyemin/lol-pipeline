import pandas as pd

# ◀◀ pandas가 더 많은 행과 열을 보여주도록 옵션 설정
pd.set_option('display.max_rows', 15)       # 최대 50줄까지 표시
pd.set_option('display.max_columns', None)  # ◀◀ 모든 컬럼을 표시하도록 설정
pd.set_option('display.width', 1000)        # ◀◀ 한 줄의 너비를 넓게 설정하여 줄바꿈 방지

# S3 경로를 폴더까지만 지정하면 Spark가 저장한 모든 part- 파일을 알아서 읽어옵니다.
s3_path = "s3://my-riot-ml-pipeline-project/staging/match_player_stats/ds=2025-09-22/"

try:
    # s3fs를 사용하여 S3 경로의 파케이 파일을 바로 DataFrame으로 읽기
    df = pd.read_parquet(s3_path)

    print("### 데이터 확인 (14줄, 모든 컬럼) ###")
    print(df.head(14))

    print("\n### 데이터 구조 (스키마) ###")
    df.info()

    print(f"\n총 {len(df)}개의 행이 성공적으로 로드되었습니다.")

except Exception as e:
    print(f"데이터를 읽는 중 오류가 발생했습니다: {e}")