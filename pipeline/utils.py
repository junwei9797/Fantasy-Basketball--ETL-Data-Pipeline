import pytz
from datetime import datetime,timedelta, timezone,date

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


def get_nba_est_date(date_input=None,cutoff_hour_et=6):
    """
    Returns the NBA game date in US Eastern Time,
    adjusted based on an Eastern Time cutoff (default 6 AM ET).
    """
    eastern = pytz.timezone('US/Eastern')
    if date_input is None:
        dt_utc = datetime.now(timezone.utc)
    elif isinstance(date_input, str):
        try:
            # Handle date-only string
            dt_parsed = datetime.fromisoformat(date_input)
            if dt_parsed.tzinfo is None:
                dt_parsed = dt_parsed.replace(tzinfo=timezone.utc)
            dt_utc = dt_parsed.astimezone(timezone.utc)
        except ValueError:
            raise ValueError("String date must be in ISO format like 'YYYY-MM-DD' or 'YYYY-MM-DDTHH:MM:SS'")

    elif isinstance(date_input, date) and not isinstance(date_input, datetime):
        # Handle date object (assume midnight UTC)
        dt_utc = datetime.combine(date_input, datetime.min.time(), tzinfo=timezone.utc)

    elif isinstance(date_input, datetime):
        if date_input.tzinfo is None:
            dt_utc = date_input.replace(tzinfo=timezone.utc)
        else:
            dt_utc = date_input.astimezone(timezone.utc)
    else:
        raise ValueError("date must be None, a datetime object, or an ISO format string")

    dt_et = dt_utc.astimezone(eastern)
    # If it's before the cutoff in ET, subtract a day
    if dt_et.hour < cutoff_hour_et:
        game_date = (dt_et - timedelta(days=1)).date()
    else:
        game_date = dt_et.date()

    return game_date.strftime("%Y-%m-%d")
