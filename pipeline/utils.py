
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