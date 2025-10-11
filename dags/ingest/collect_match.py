import os, time, json, datetime, requests, boto3, pendulum
from datetime import timezone
# from airflow.models import Variable
# from airflow.exceptions import AirflowFailException
# from airflow.exceptions import AirflowFailException

# RIOT_API_KEY=os.environ['RIOT_API_KEY']
# AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY_ID']
# AWS_SECRET_ACCESS_KEY=os.environ['AWS_SECRET_ACCESS_KEY']
# AWS_DEFAULT_REGION=os.environ['AWS_DEFAULT_REGION']
# AWS_S3_BUCKET=os.environ['AWS_S3_BUCKET']

# 1. 챌린저 + 그랜드마스터 + 마스터 유저의 정보를 가져온다
# 2. 유저들의 puuid를 사용해 최근 경기 matchid 목록을 가져온다
# 3. matchid를 사용해 상세 경기 데이터를 가져온다.
# 5. 특정 티어의 모든 유저에 대해 n경기를 수집하고 s3에 업로드

# {
#     "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
#     "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
#     "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
#     "Origin": "https://developer.riotgames.com",
#     "X-Riot-Token": ""
# }

#api 요청

def request_riot_api(url:str, max_retries=1, backoff_factor: float = 0.5):
    api_key = os.getenv("RIOT_API_KEY") 
    headers = {"X-Riot-Token": api_key}
    for i in range(max_retries):
        try:
            response = requests.get(url, headers=headers)

            if response.status_code == 429:
                time.sleep(1.2)
                continue


            elif response.status_code == 200:
                return response.json()
            
            else:
                print(f"Error:{response.status_code} for URL: {url}")
                return None
            
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            return None
    return None


# 1. 챌린저 + 그랜드마스터 + 마스터 유저의 정보를 가져온다

def get_summoners_puuid(tier:str) -> list:
    url = f"https://kr.api.riotgames.com/lol/league/v4/{tier.lower()}/by-queue/RANKED_SOLO_5x5"
    print(f"DEBUG: Requesting URL -> {url}") 
    league_data = request_riot_api(url)

    if league_data and "entries" in league_data:
        puuid = [entry['puuid'] for entry in league_data['entries']]
        print(f"유저의 길이{len(puuid)}")
        return puuid
    
    print("failed_get_puuid")
    return []


# 2. 유저들의 puuid를 사용해 최근 경기 matchid 목록을 가져온다
def get_matchid_by_puuid(puuids: list, ds=None, count: int = 20, limit_users: int | None = None, **kwargs) -> list:
    all_matchids = set()
    if limit_users is not None:
        limit_users = int(limit_users)
        puuids = puuids[:limit_users]
    count = int(count)
    start_time = None
    end_time = None
    if ds:
        execution_date = pendulum.parse(ds)
        start_time = int(execution_date.start_of('day').timestamp())
        end_time = int(execution_date.end_of('day').timestamp())

    for puuid in puuids:
        url = f"https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?start=0&count={count}"
        
        if start_time is not None:
            url += f"&startTime={start_time}"
        if end_time is not None:
            url += f"&endTime={end_time}"
        
        print(f"DEBUG: Requesting URL -> {url}")

        match_ids = request_riot_api(url)
        
        if match_ids:
            all_matchids.update(match_ids)
            
        time.sleep(1.2)

    print(f"Total unique match: {len(all_matchids)}")
    return list(all_matchids)
    
# 3. matchid를 사용해 상세 경기 데이터를 가져온다.
def get_match_by_matchid(matchid: str) -> dict | None:
    url = f"https://asia.api.riotgames.com/lol/match/v5/matches/{matchid}"
    match_detail = request_riot_api(url)
    return match_detail




#4. s3에 업로드
def upload_to_s3(match_detail:dict, matchId:str, game_date:str):
    bucket = os.getenv("AWS_S3_BUCKET")
    region = os.getenv("AWS_DEFAULT_REGION")
    s3 = boto3.client("s3", region_name=region)  
   
    key = f"raw/match/ds={game_date}/{matchId}.json"
    s3.put_object(
                Bucket=bucket, Key=key,
                Body=json.dumps(match_detail),
                ContentType="application/json"
                    )
    print(f"Uploaded to s3://{bucket}/{key}")

# 메인함수
def run_riot_pipeline(match_ids: list, **kwargs):
    print(f"excution_date {len(match_ids)}")

    upload_success = 0
    for matchid in match_ids:

        match_detail = get_match_by_matchid(matchid)
        if match_detail:
            try:
                # "info": {"gameStartTimestamp": 1758123399538, "gameEndTimestamp": 1758125263112}
                # 게임 시작 시간을 기준으로 파티션 나누기
                game_start_time = match_detail["info"]["gameStartTimestamp"]
                game_datetime_utc = datetime.datetime.fromtimestamp(game_start_time / 1000, tz=timezone.utc)
                game_date = game_datetime_utc.strftime('%Y-%m-%d')

                upload_to_s3(match_detail, matchid, game_date)
                upload_success += 1
            except (KeyError, TypeError) as e:
                print(f" main function error :{e}")

        time.sleep(1.2)
    print(f"success upload to s3 {upload_success}")



################################timeline###############################
def get_timeline_by_matchid(match_ids: list, **context):
    bucket = os.getenv("AWS_S3_BUCKET")
    region = os.getenv("AWS_DEFAULT_REGION")
    s3 = boto3.client("s3", region_name=region)  
    ds = context["ds"]
    
    success_cnt = 0
    for match_id in match_ids:
        timeline_data = request_riot_api(f"https://asia.api.riotgames.com/lol/match/v5/matches/{match_id}/timeline")
    
        if timeline_data:
            try:
                key = f"raw/match_timeline/ds={ds}/{match_id}.json"

                s3.put_object(
                            Bucket=bucket, Key=key,
                            Body=json.dumps(timeline_data),
                            ContentType="application/json"
                            )
                print(f"Uploaded to timeline data to s3://{bucket}/{key}")
                success_cnt += 1
            except Exception as e:
                print(f"timeline failed !!")
            time.sleep(1.2)
    print(f"finish {success_cnt}/{len(match_ids)}")
