# lol-pipeline



MPS_RAW 데이터에서 필요한 데이터 테이블 생성

CREATE OR REPLACE TABLE MART.PLAYER_STATS (
    ds DATE,
    matchId STRING,
    teamId NUMBER,
    participantId NUMBER,
    teamPosition STRING,  
    win BOOLEAN,

  
    kills NUMBER,
    deaths NUMBER,
    assists NUMBER,
    kda FLOAT,
    goldEarned NUMBER,
    goldPerMinute FLOAT,
    totalDamageDealtToChampions NUMBER,
    totalDamageTaken NUMBER,
    totalMinionsKilled NUMBER,
    soloKills NUMBER,
    killParticipation FLOAT,
    totalTimeCCDealt FLOAT,
    visionScore NUMBER,
    gameDuration NUMBER,


    dragonTakedowns NUMBER,
    baronKills NUMBER,
    baronTakedowns NUMBER,
    riftHeraldTakedowns NUMBER,
    turretPlatesTaken NUMBER,
    turretTakedowns NUMBER,
    inhibitorTakedowns NUMBER,
    objectivesStolen NUMBER,

    teamDamagePercentage FLOAT,
    visionScorePerMinute FLOAT,
    dmg_per_min FLOAT,
    cs_per_min FLOAT,
    effectiveHealAndShielding NUMBER,
    wardsPlaced NUMBER,

    top_label BOOLEAN,
    jungle_label BOOLEAN,
    mid_label BOOLEAN,
    adc_label BOOLEAN,
    sup_label BOOLEAN
  
    
);

############################################################
테이터를 넣어주고 task생성

CREATE OR REPLACE TASK RIOT_DB.MART.TASK_PLAYER_STATS 
    WAREHOUSE=COMPUTE_WH
    SCHEDULE='USING CRON 10 17 * * * UTC' --표준시17시10분에실행
    WHEN SYSTEM$STREAM_HAS_DATA('RIOT_DB.RAW.PLAYER_STATS_RAW_INSERT')
    
AS
INSERT INTO MART.PLAYER_STATS (
    ds, matchId, teamId, participantId, teamPosition, win,
    kills, deaths, assists, kda, goldEarned, goldPerMinute,
    totalDamageDealtToChampions, totalDamageTaken, totalMinionsKilled, soloKills,
    killParticipation, totalTimeCCDealt, visionScore, gameDuration,
    dragonTakedowns, baronKills, baronTakedowns, riftHeraldTakedowns,
    turretPlatesTaken, turretTakedowns, inhibitorTakedowns, objectivesStolen,
    teamDamagePercentage, visionScorePerMinute, dmg_per_min, cs_per_min,
    effectiveHealAndShielding,wardsPlaced ,
    top_label, jungle_label, mid_label, adc_label, sup_label
)
SELECT 
    TO_DATE(ds) AS ds,
    v:"matchId"::STRING AS matchId,
    v:"teamId"::NUMBER AS teamId,
    v:"participantId"::NUMBER AS participantId,
    v:"teamPosition"::STRING AS teamPosition,
    v:"win"::BOOLEAN AS win,


    v:"kills"::NUMBER AS kills,
    v:"deaths"::NUMBER AS deaths,
    v:"assists"::NUMBER AS assists,
    v:challenges."kda"::FLOAT AS kda,
    v:"goldEarned"::NUMBER AS goldEarned,
    v:challenges."goldPerMinute"::FLOAT AS goldPerMinute,
    v:"totalDamageDealtToChampions"::NUMBER AS totalDamageDealtToChampions,
    v:"totalDamageTaken"::NUMBER AS totalDamageTaken,
    v:"totalMinionsKilled"::NUMBER AS totalMinionsKilled,
    v:challenges."soloKills"::NUMBER AS soloKills,
    v:challenges."killParticipation"::FLOAT AS killParticipation,
    v:"totalTimeCCDealt"::FLOAT AS totalTimeCCDealt,
    v:"visionScore"::NUMBER AS visionScore,
    v:"gameDuration"::NUMBER AS gameDuration,


    v:challenges."dragonTakedowns"::NUMBER AS DRAGONTAKEDOWNS,
    v:"baronKills"::NUMBER AS BARONKILLS,
    v:challenges."baronTakedowns"::NUMBER AS BARONTAKEDOWNS,
    v:challenges."riftHeraldTakedowns"::NUMBER AS RIFTHERALDTAKEDOWNS,
    v:challenges."turretPlatesTaken"::NUMBER AS TURRETPLATESTAKEN,
    v:challenges."turretTakedowns"::NUMBER AS TURRETTAKEDOWNS,
    v:challenges."inhibitorTakedowns"::NUMBER AS INHIBITORTAKEDOWNS,
    v:challenges."objectivesStolen"::NUMBER AS OBJECTIVESSTOLEN,


    v:challenges."teamDamagePercentage"::FLOAT AS teamDamagePercentage,
    v:challenges."visionScorePerMinute"::FLOAT AS visionScorePerMinute,


    DIV0(v:"totalDamageDealtToChampions"::NUMBER, v:"gameDuration"::NUMBER / 60) AS dmg_per_min,
    DIV0(v:"totalMinionsKilled"::NUMBER, v:"gameDuration"::NUMBER / 60) AS cs_per_min,

    v:challenges."effectiveHealAndShielding"::NUMBER AS effectiveHealAndShielding,
    v:"wardsPlaced"::NUMBER AS wardsPlaced,

    CASE 
        WHEN v:"teamPosition" = 'TOP' 
             AND (v:challenges."soloKills"::NUMBER > 0 OR v:challenges."turretPlatesTaken"::NUMBER >= 2)
        THEN TRUE ELSE FALSE 
    END AS top_label,
    CASE 
      WHEN v:"teamPosition" = 'JUNGLE'
           AND (
             (COALESCE(v:challenges."dragonTakedowns"::NUMBER,0)
              + COALESCE(v:challenges."riftHeraldTakedowns"::NUMBER,0) >= 2)::INT
           + (v:challenges."killParticipation"::FLOAT >= 0.40)::INT
           + (COALESCE(v:"baronKills"::NUMBER,0) > 0)::INT
           ) >= 2
      THEN TRUE ELSE FALSE
    END AS jungle_label,

    CASE 
        WHEN v:"teamPosition" = 'MIDDLE'
             AND v:challenges."killParticipation"::FLOAT >= 0.5
             AND v:challenges."teamDamagePercentage"::FLOAT >= 0.23 
        THEN TRUE ELSE FALSE 
    END AS mid_label,

    CASE 
        WHEN v:"teamPosition" = 'BOTTOM'
             AND (v:challenges."teamDamagePercentage"::FLOAT >= 0.25 
                  OR DIV0(v:"totalDamageDealtToChampions"::NUMBER, v:"gameDuration"::NUMBER / 60) 
                     > AVG(DIV0(v:"totalDamageDealtToChampions"::NUMBER, v:"gameDuration"::NUMBER / 60)) 
                       OVER (PARTITION BY v:"matchId", v:"teamId"))
        THEN TRUE ELSE FALSE 
    END AS adc_label,
    
    CASE 
      WHEN v:"teamPosition" = 'UTILITY'
           AND (
               (v:challenges."visionScorePerMinute" >
                   PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY v:challenges."visionScorePerMinute")
                   OVER (PARTITION BY v:"matchId", v:"teamId"))::INT
             + (v:challenges."killParticipation"::FLOAT >= 0.35)::INT
             + (v:challenges."effectiveHealAndShielding" >= 1000 
                 OR v:"totalTimeCCDealt" >
                   AVG(v:"totalTimeCCDealt") OVER (PARTITION BY v:"matchId", v:"teamId"))::INT
           ) >= 2
      THEN TRUE ELSE FALSE
    END AS sup_label

FROM RIOT_DB.RAW.MPS_RAW
WHERE 
    v:"teamPosition" IS NOT NULL 
    AND v:"teamPosition" != '' 
    AND v:"gameDuration" >= 900;



##################################################
b모델(각자 포지션별로 중요한 지표)을 학습하기 위한 뷰


CREATE OR REPLACE VIEW RIOT_DB.MART.VIEW_PLAYER_FEATURE_B AS 
WITH TEAM_FEATURE AS (
    SELECT
        matchId,
        teamId,
        SUM(totalDamageDealtToChampions) AS team_damage,
        SUM(goldEarned) AS team_gold,
        SUM(visionScore) AS team_vision,
        SUM(totalMinionsKilled) AS team_minion,
        SUM(totalTimeCCDealt) AS team_cc,
        SUM(effectiveHealAndShielding) AS team_heal
    FROM RIOT_DB.MART.PLAYER_STATS
    GROUP BY matchId, teamId
)
SELECT  
    p.ds,
    p.matchId,
    p.teamId,
    p.participantId,
    p.teamPosition,
    p.win,


    p.top_label,
    p.jungle_label,
    p.mid_label,
    p.adc_label,
    p.sup_label,


    p.kills,
    p.deaths,
    p.assists,
    p.kda,
    p.goldEarned,
    p.goldPerMinute,
    p.totalDamageDealtToChampions,
    p.totalDamageTaken,
    p.totalMinionsKilled,
    p.soloKills,
    p.killParticipation,
    p.totalTimeCCDealt,
    p.visionScore,
    p.effectiveHealAndShielding,
    p.wardsPlaced,


    p.DRAGONTAKEDOWNS,
    p.BARONKILLS,
    p.RIFTHERALDTAKEDOWNS,
    p.TURRETPLATESTAKEN,
    p.TURRETTAKEDOWNS,


    DIV0(p.totalMinionsKilled, p.gameDuration / 60) AS cs_per_min,
    DIV0(p.totalDamageDealtToChampions, p.gameDuration / 60) AS dmg_per_min,
    DIV0(p.visionScore, p.gameDuration / 60) AS vision_per_min,
    DIV0(p.goldEarned, p.gameDuration / 60) AS gold_per_min,
    DIV0(p.totalTimeCCDealt, NULLIF(t.team_cc,0)) AS cc_rate,
    DIV0(p.effectiveHealAndShielding, NULLIF(t.team_heal,0)) AS heal_rate,


    DIV0(p.totalMinionsKilled, NULLIF(t.team_minion,0)) AS cs_rate,
    DIV0(p.totalDamageDealtToChampions, NULLIF(t.team_damage,0)) AS damage_rate,
    DIV0(p.goldEarned, NULLIF(t.team_gold,0)) AS gold_rate, 
    DIV0(p.visionScore, NULLIF(t.team_vision,0)) AS vision_rate,
    DIV0(p.wardsPlaced, p.gameDuration / 60) AS wards_pm

    

FROM RIOT_DB.MART.PLAYER_STATS p
JOIN TEAM_FEATURE t
    ON p.matchId = t.matchId 
   AND p.teamId = t.teamId;




---------------------------------------------------------------------------------------


모델 a를 위한 뷰


CREATE OR REPLACE VIEW RIOT_DB.MART.VIEW_FEATURE_FOR_MODEL_A AS

WITH TEAM_FEATURES AS (
    SELECT 
        MATCHID,
        TEAMID,
        WIN,
        
        MAX(CASE WHEN TEAMPOSITION='TOP' THEN SOLOKILLS ELSE 0 END) AS TOP_SOLOKILLS,
        MAX(CASE WHEN TEAMPOSITION='TOP' THEN TURRETPLATESTAKEN ELSE 0 END) AS TOP_PLATE,
        MAX(CASE WHEN TEAMPOSITION='TOP' THEN KILLS ELSE 0 END) AS TOP_KILLS,
        

        
        MAX(CASE WHEN TEAMPOSITION='JUNGLE' THEN DRAGONTAKEDOWNS ELSE 0 END) AS JUNGLE_DRAGON,
        MAX(CASE WHEN TEAMPOSITION='JUNGLE' THEN GOLD_RATE ELSE 0 END) AS JUNGLE_GOLD_RATE,
        MAX(CASE WHEN TEAMPOSITION='JUNGLE' THEN KILLPARTICIPATION ELSE 0 END) AS JUNGLE_KILLPARTICIPATION,
        MAX(CASE WHEN TEAMPOSITION='JUNGLE' THEN RIFTHERALDTAKEDOWNS ELSE 0 END) AS JUNGLE_RIFTHERALD,
        MAX(CASE WHEN TEAMPOSITION='JUNGLE' THEN BARONKILLS ELSE 0 END) AS JUNGLE_BARONKILLS,
        
        

        
        MAX(CASE WHEN TEAMPOSITION='MIDDLE' THEN KILLPARTICIPATION ELSE 0 END) AS MID_KILLPARTICIPATION,
        MAX(CASE WHEN TEAMPOSITION='MIDDLE' THEN KILLS ELSE 0 END) AS MID_KILLS,
        MAX(CASE WHEN TEAMPOSITION='MIDDLE' THEN DAMAGE_RATE ELSE 0 END) AS MID_DAMAGE_RATE,
        

        MAX(CASE WHEN TEAMPOSITION='BOTTOM' THEN DAMAGE_RATE ELSE 0 END) AS BOTTOM_DAMAGE_RATE,
        MAX(CASE WHEN TEAMPOSITION='BOTTOM' THEN GOLD_RATE ELSE 0 END) AS BOTTOM_GOLD_RATE,
        MAX(CASE WHEN TEAMPOSITION='BOTTOM' THEN KILLPARTICIPATION ELSE 0 END) AS BOTTOM_KILLPARTICIPATION,
        



        MAX(CASE WHEN TEAMPOSITION='UTILITY' THEN EFFECTIVEHEALANDSHIELDING ELSE 0 END) AS SUP_EFFECTIVEHEALANDSHIELDING,
        MAX(CASE WHEN TEAMPOSITION='UTILITY' THEN CC_RATE ELSE 0 END) AS SUP_CC_RATE,
        MAX(CASE WHEN TEAMPOSITION='UTILITY' THEN VISION_PER_MIN ELSE 0 END) AS SUP_VISION_PER_MIN,
        MAX(CASE WHEN TEAMPOSITION='UTILITY' THEN DAMAGE_RATE ELSE 0 END) AS SUP_DAMAGE_RATE,
        MAX(CASE WHEN TEAMPOSITION='UTILITY' THEN KILLPARTICIPATION ELSE 0 END) AS SUP_KILLPARTICIPATION,
        MAX(CASE WHEN TEAMPOSITION='UTILITY' THEN wards_pm ELSE 0 END) AS SUP_WARDS_PM

        
        
        
    FROM RIOT_DB.MART.VIEW_PLAYER_FEATURE_B 
    GROUP BY MATCHID, TEAMID, WIN
),

BLUE_TEAM AS (SELECT * FROM TEAM_FEATURES WHERE TEAMID=100),
RED_TEAM AS (SELECT * FROM TEAM_FEATURES WHERE TEAMID=200)

SELECT 
    b.MATCHID,
    b.WIN AS BLUE_TEAM_WIN,
    
    (b.TOP_SOLOKILLS - r.TOP_SOLOKILLS) AS TOP_SOLOKILLS_DIF,
    (b.TOP_PLATE - r.TOP_PLATE) AS TOP_PLATES_DIF,
    (b.TOP_KILLS - r.TOP_KILLS) AS TOP_KILLS_DIF,
    
    
    (b.JUNGLE_DRAGON - r.JUNGLE_DRAGON) AS JUNGLE_DRAGON_DIF,
    (b.JUNGLE_GOLD_RATE - r.JUNGLE_GOLD_RATE) AS JUNGLE_GOLD_RATE_DIF,
    (b.JUNGLE_KILLPARTICIPATION - r.JUNGLE_KILLPARTICIPATION) AS JUNGLE_KILLPARTICIPATION_DIF,
    (b.JUNGLE_RIFTHERALD - r.JUNGLE_RIFTHERALD) AS JUNGLE_RIFTHERALD_DIF,
    (b.JUNGLE_BARONKILLS - r.JUNGLE_BARONKILLS) AS JUNGLE_BARONKILLS_DIF,
    

        
    (b.MID_KILLPARTICIPATION - r.MID_KILLPARTICIPATION) AS MID_KILLPARTICIPATION_DIF,
    (b.MID_KILLS - r.MID_KILLS) AS MID_KILLS_DIF,
    (b.MID_DAMAGE_RATE - r.MID_DAMAGE_RATE) AS MID_DAMAGE_RATE_DIF,
    
  
    (b.BOTTOM_DAMAGE_RATE - r.BOTTOM_DAMAGE_RATE) AS BOTTOM_DAMAGE_RATE_DIF,
    (b.BOTTOM_GOLD_RATE - r.BOTTOM_GOLD_RATE) AS BOTTOM_GOLD_RATE_DIF,
    (b.BOTTOM_KILLPARTICIPATION - r.BOTTOM_KILLPARTICIPATION) AS BOTTOM_KILLPARTICIPATION_DIF,
    

    (b.SUP_EFFECTIVEHEALANDSHIELDING - r.SUP_EFFECTIVEHEALANDSHIELDING) AS SUP_EFFECTIVEHEALANDSHIELDING_DIF,
    (b.SUP_CC_RATE - r.SUP_CC_RATE) AS SUP_CC_RATE_DIF,
    (b.SUP_VISION_PER_MIN - r.SUP_VISION_PER_MIN) AS SUP_VISION_PER_MIN_DIF,
    (b.SUP_DAMAGE_RATE - r.SUP_DAMAGE_RATE) AS SUP_DAMAGE_RATE_DIF,
    (b.SUP_KILLPARTICIPATION - r.SUP_KILLPARTICIPATION) AS SUP_KILLPARTICIPATION_DIF,
    (b.SUP_WARDS_PM - r.SUP_WARDS_PM) AS SUP_WARDS_PM_DIF
    

FROM BLUE_TEAM b
JOIN RED_TEAM r ON b.MATCHID = r.MATCHID;


