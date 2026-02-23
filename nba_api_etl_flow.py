"""
nba_etl_flow.py

Requisitos en .env
MONGO_URI
MONGO_DB opcional default nba
MONGO_COLLECTION opcional default scoreboard_games
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from typing import Tuple, Optional

import pandas as pd
from dotenv import load_dotenv
from nba_api.stats.endpoints import scoreboardv3
from prefect import flow, task, get_run_logger
from pymongo import MongoClient, UpdateOne


def _get_target_date(target_date: Optional[str]) -> date:
    """
    target_date puede ser None o un string YYYYMMDD
    Si es None usa ayer
    """
    if target_date is None:
        return date.today() - timedelta(days=1)

    if len(target_date) != 8 or not target_date.isdigit():
        raise ValueError("target_date debe ser YYYYMMDD, ejemplo 20260221")

    yyyy = int(target_date[0:4])
    mm = int(target_date[4:6])
    dd = int(target_date[6:8])
    return date(yyyy, mm, dd)


@task(retries=6, retry_delay_seconds=60)
def extract_scoreboard(game_date: date) -> Tuple[pd.DataFrame, pd.DataFrame]:
    logger = get_run_logger()
    logger.info(f"Extract: scoreboard for {game_date.isoformat()}")

    sb = scoreboardv3.ScoreboardV3(
        league_id="00",
        game_date=game_date,
    )

    game_header = sb.game_header.get_data_frame()
    line_score = sb.line_score.get_data_frame()

    logger.info(f"Extract: game_header {len(game_header)} rows | line_score {len(line_score)} rows")
    return game_header, line_score


@task
def transform(game_header: pd.DataFrame, line_score: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info("Transform: starting...")

    gh = game_header.copy()
    ls = line_score.copy()

    if gh.empty or ls.empty:
        logger.info("Transform: there are no games for the given date.")
        return pd.DataFrame()

    gh_idx = gh.set_index("gameId")

    ls["home_tricode"] = (
        ls["gameId"]
        .map(gh_idx["gameCode"])
        .astype(str)
        .str.split("/")
        .str[1]
        .str[-3:]
    )
    ls["away_tricode"] = (
        ls["gameId"]
        .map(gh_idx["gameCode"])
        .astype(str)
        .str.split("/")
        .str[1]
        .str[:3]
    )

    ls["is_home"] = ls["teamTricode"] == ls["home_tricode"]
    ls["side"] = ls["is_home"].map({True: "home", False: "away"})

    ls_small = ls[
        [
            "gameId",
            "side",
            "teamId",
            "teamCity",
            "teamName",
            "teamTricode",
            "wins",
            "losses",
            "score",
        ]
    ].copy()

    game_level_df = (
        ls_small
        .set_index(["gameId", "side"])
        .unstack("side")
    )
    game_level_df.columns = [f"{col}_{side}" for col, side in game_level_df.columns]
    game_level_df = game_level_df.reset_index()

    game_scores_df = game_level_df.merge(
        gh[
            [
                "gameId",
                "gameStatus",
                "gameStatusText",
                "period",
                "gameClock",
                "gameTimeUTC",
                "gameEt",
            ]
        ],
        on="gameId",
        how="left",
    )

    game_scores_df["etl_date"] = date.today().isoformat()

    logger.info(f"Transform: {len(game_scores_df)} rows returned.")
    return game_scores_df


@task(retries=2, retry_delay_seconds=60)
def load_to_mongo(df: pd.DataFrame) -> dict:
    logger = get_run_logger()
    logger.info("Load: starting...")

    if df is None or df.empty:
        logger.info("Load: No data to load.")
        return {"ok": True, "inserted": 0, "modified": 0, "processed": 0}

    load_dotenv()

    mongo_uri = os.getenv("MONGO_URI")
    mongo_db = os.getenv("MONGO_DB", "nba")
    mongo_collection = os.getenv("MONGO_COLLECTION", "scoreboard_games")

    if not mongo_uri:
        raise ValueError("There is no MONGO_URI in the .env file")

    client = MongoClient(mongo_uri)
    col = client[mongo_db][mongo_collection]

    records = df.to_dict("records")
    ops = [UpdateOne({"gameId": r["gameId"]}, {"$set": r}, upsert=True) for r in records]

    result = col.bulk_write(ops, ordered=False)

    summary = {
        "ok": True,
        "inserted": int(result.upserted_count),
        "modified": int(result.modified_count),
        "processed": int(len(ops)),
        "db": mongo_db,
        "collection": mongo_collection,
    }

    logger.info(f"Load: {summary}")
    return summary


@flow(name="nba_daily_scoreboard_etl")
def nba_daily_scoreboard_etl(target_date: Optional[str] = None) -> dict:
    """
    target_date opcional en formato YYYYMMDD
    si no se pasa, usa ayer
    """
    game_date = _get_target_date(target_date)
    game_header_df, line_score_df = extract_scoreboard(game_date)
    scores_df = transform(game_header_df, line_score_df)
    return load_to_mongo(scores_df)


if __name__ == "__main__":
    nba_daily_scoreboard_etl()