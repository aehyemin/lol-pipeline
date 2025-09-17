from datetime import datetime, timezone, timedelta

# 1. 주어진 밀리초 타임스탬프 (Riot API에서 받은 값)
game_end_timestamp_ms = 1757169398692

# 2. 초(second) 단위 타임스탬프로 변환 (밀리초 값을 1000으로 나눔)
#    파이썬의 datetime.fromtimestamp 함수는 초 단위로 작동하기 때문입니다.
timestamp_in_seconds = game_end_timestamp_ms / 1000

# 3. UTC(협정 세계시) 기준으로 날짜/시간 객체 생성
#    - 모든 타임스탬프는 기본적으로 UTC 기준입니다.
datetime_utc = datetime.fromtimestamp(timestamp_in_seconds, tz=timezone.utc)

# 4. 한국 시간(KST, UTC+9)으로 변환
#    - UTC 시간에 9시간을 더해 한국 시간대를 만듭니다.
kst_timezone = timezone(timedelta(hours=9))
datetime_kst = datetime_utc.astimezone(kst_timezone)

# 5. 보기 좋은 형식으로 결과 출력
print(f"Original Millisecond Timestamp: {game_end_timestamp_ms}")
print("-" * 40)
print(f"UTC (Coordinated Universal Time): {datetime_utc.strftime('%Y-%m-%d %H:%M:%S %Z')}")
print(f"KST (Korean Standard Time):     {datetime_kst.strftime('%Y-%m-%d %H:%M:%S %Z')}")

# "날짜"만 YYYY-MM-DD 형식으로 추출하고 싶을 경우
date_only_kst = datetime_kst.strftime('%Y-%m-%d')
print(f"KST Date Only:                    {date_only_kst}")