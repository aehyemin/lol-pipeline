# LoL 포지션 영향력 분석 MLOps 파이프라인

리그 오브 레전드의 게임 데이터를 기반으로, 각 포지션이 승리에 미치는 영향을 정량적으로 분석하는 End-to-End MLOps 파이프라인 프로젝트입니다.

## 아키텍쳐
<img width="435" height="400" alt="스크린샷 2025-09-30 오후 4 26 34" src="https://github.com/user-attachments/assets/0a5aa5c8-0b4f-400c-ba0e-1126c32e6008" />

**데이터 수집**:  
  Airflow `PythonOperator` Riot API 호출 → 원본 JSON을 S3에 저장  
    
**데이터 변환**:  
  Airflow `SparkSubmitOperator` → Spark 클러스터에서 JSON → Parquet 변환 후 **S3**에 다시 저장 
  
**데이터 적재 및 마트화**:  
  Snowflake Stream & Task → S3 적재 감지 후 Raw 테이블 → 최종 분석용 Mart 뷰 생성  
  
**모델 학습**:  
  Airflow `SageMakerTrainingOperator` → SageMaker에서 학습 → 결과 모델을 S3에 저장  
  
**모델 분석 및 결과 저장**:  
  Airflow `PythonOperator` → 학습된 모델 기반 SHAP 분석 → 최종 결과를 Snowflake에 저장  



  ---


## 핵심 구현

### end to end MLOps 자동화
  - Airflow DAG 로 데이터 수집 -> 전처리 -> 모델 학습 -> 해석 -> 결과 저장까지 전체 자동화

### 포지션별 영향력 모델링(SHAP 기반 해석)
  - 2-Stage 모델링 기법 적용:
      - 1차 모델: 포지션별 핵심 지표 선별
      - 2차 모델: 팀 간 격차 학습 -> 포지션별 영향력 산출
 
### 클라우드 학습 및 분석 자동화
  - 학습 코드 패키징 -> S3 저장 -> SageMaker에서 자동 학습
  - 모델 기반 SHAP 분석 -> 결과 snowflake 저장

### 컨테이너 기반 개발 환경
  - Docker Compose로 Airflow, spark 실행 환경 통합

---


포지션별 핵심 피처 선별



<p align="center">
  <img src="https://github.com/user-attachments/assets/653e78a2-645d-4d89-aa07-b6c9124d9c2f" width="45%" />
  <img src="https://github.com/user-attachments/assets/4b4311e8-0ed6-4e79-b848-16f1b13bb44e" width="45%" />
</p>



<p align="center">
 <img src="https://github.com/user-attachments/assets/13659ea3-cbd4-42c1-b444-e0e6a2a58c32" width="45%"/>
 <img src="https://github.com/user-attachments/assets/58597b6a-6714-4447-b6b4-d970b98edb3d" width="45%"/>
 <img src="https://github.com/user-attachments/assets/5f9d494e-66b7-4683-b198-0f94a04d15e5" width="45%"/>
</p>


핵심 피처 기반 팀 간 격차 계산 -> SHAP 기법 모델 분석

<img width="555" height="160" alt="image" src="https://github.com/user-attachments/assets/798666ef-aa31-4cc0-8fdb-a167816880c2" />


---

## 기술 심화: 실시간 골드 격차 처리 및 시각화 파이프라인

- 실시간 데이터 스트리밍 아키텍처 설계 :실제 LoL경기 타임라인 JSON 데이터를 일정 속도로 Kafka 토픽에 전송
- Kafka & Spark Structured Streaming을 활용한 실시간 집계
- Streamlit 기반 실시간 시각화 대시보드 구현



  <img width="802" height="804" alt="Screenshot from 2025-10-13 21-59-35" src="https://github.com/user-attachments/assets/8df8c085-aa3e-4f62-ad6e-9f5b91c21b9f" />


## 사용 스택

- **Data Engineering**: Apache Airflow, Apache Spark, Kafka, Snowflake, AWS S3  
- **ML / MLOps**: AWS SageMaker, SHAP, LightGBM, Scikit-learn  
- **Infra / Etc.**: Docker, Python, Git, AWS 



