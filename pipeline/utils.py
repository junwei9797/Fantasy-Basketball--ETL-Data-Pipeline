import pytz
from datetime import datetime,timedelta, timezone

def get_nba_season_id(date):
    """
    Given a date (datetime.date or datetime.datetime), return the NBA season string ID, e.g., '2023-24'
    """
    year = date.year
    if date.month >= 10:  # NBA season starts in October
        start_year = year
        end_year = year + 1
    else:
        start_year = year - 1
        end_year = year

    return f"{start_year}-{str(end_year)[-2:]}"


def get_nba_est_date(cutoff_hour_et=6):
    """
    Returns the NBA game date in US Eastern Time,
    adjusted based on an Eastern Time cutoff (default 6 AM ET).
    """
    eastern = pytz.timezone('US/Eastern')
    now_et = datetime.now(timezone.utc).astimezone(eastern)
    # If it's before the cutoff in ET, subtract a day
    if now_et.hour < cutoff_hour_et:
        game_date = (now_et - timedelta(days=1)).date()
    else:
        game_date = now_et.date()

    return game_date.strftime("%Y-%m-%d")
