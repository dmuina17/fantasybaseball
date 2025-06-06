import os
from fastapi import FastAPI, BackgroundTasks, Query
import pandas as pd
from pybaseball import pitching_stats, batting_stats, statcast_batter, statcast_pitcher
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from contextlib import asynccontextmanager
import uvicorn

# Global variables for caching data
cached_pitcher_data = None
cached_hitter_data = None
last_updated = None

scheduler = BackgroundScheduler()

def clean_data_column(column):
    cleaned = []
    for item in column:
        if isinstance(item, str):
            cleaned_values = item.replace('$', ' ').split()
            cleaned.append(float(cleaned_values[0]) if cleaned_values else None)
        elif isinstance(item, (int, float)):
            cleaned.append(float(item))
        else:
            cleaned.append(None)
    return cleaned

def fetch_and_process_data():
    """Fetch and process baseball data, updating global variables."""
    global cached_pitcher_data, cached_hitter_data, last_updated
    selected_season = "2025"
    
    print("Fetching and processing data...")

    # Pitcher stats
    pitcher_data = pitching_stats(int(selected_season), int(selected_season), qual=0)
    
    # Clean the data columns (e.g., ERA, WHIP)
    pitcher_data['ERA'] = clean_data_column(pitcher_data['ERA'])
    pitcher_data['WHIP'] = clean_data_column(pitcher_data['WHIP'])
    
    pitcher_data_filtered = pitcher_data[pitcher_data['IP'] > 0]

    # Ensure all columns are numeric
    pitcher_numeric = pitcher_data_filtered.drop(columns=['Name', 'Team']).apply(pd.to_numeric, errors='coerce')
    
    # Calculate mean and std for the pitcher stats
    pitcher_stats_mean = pitcher_numeric.mean()
    pitcher_stats_std = pitcher_numeric.std()
    
    # Calculate Z-scores
    pitcher_z_scores = (pitcher_numeric - pitcher_stats_mean) / pitcher_stats_std

    # Invert ERA and WHIP
    for col in ['ERA', 'WHIP']:
        if col in pitcher_z_scores.columns:
            pitcher_z_scores[col] *= -1

    # Combine name and team back
    pitcher_z_scores = pd.concat([pitcher_data_filtered[['Name', 'Team']].reset_index(drop=True), pitcher_z_scores.reset_index(drop=True)], axis=1)
    pitcher_z_scores['Total Z-Score'] = pitcher_z_scores.drop(columns=['Name', 'Team']).sum(axis=1)
    
    cached_pitcher_data = pitcher_z_scores.sort_values(by='Total Z-Score', ascending=False)

    # Hitter stats
    hitter_data = batting_stats(int(selected_season), int(selected_season), qual=0)
    
    # Clean the data column (e.g., AVG)
    hitter_data['AVG'] = clean_data_column(hitter_data['AVG'])
    
    hitter_data_filtered = hitter_data[hitter_data['PA'] > 0]
    
    # Ensure all columns are numeric
    hitter_numeric = hitter_data_filtered.drop(columns=['Name', 'Team']).apply(pd.to_numeric, errors='coerce')
    
    # Calculate mean and std for the hitter stats
    hitter_stats_mean = hitter_numeric.mean()
    hitter_stats_std = hitter_numeric.std()
    
    # Calculate Z-scores
    hitter_z_scores = (hitter_numeric - hitter_stats_mean) / hitter_stats_std

    # Adjust AVG based on PA
    hitter_z_scores['AVG'] = (hitter_data_filtered['AVG'] - hitter_stats_mean['AVG']) / (hitter_stats_std['AVG'] / (hitter_data_filtered['PA'] ** 0.5))

    # Combine name and team back
    hitter_z_scores = pd.concat([hitter_data_filtered[['Name', 'Team']].reset_index(drop=True), hitter_z_scores.reset_index(drop=True)], axis=1)
    hitter_z_scores['Total Z-Score'] = hitter_z_scores.drop(columns=['Name', 'Team']).sum(axis=1)
    
    cached_hitter_data = hitter_z_scores.sort_values(by='Total Z-Score', ascending=False)

    last_updated = datetime.now()
    print(f"✅ Data updated at {last_updated}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("⏳ Scheduling initial data fetch...")
    # Schedule first data fetch 2 seconds after app start
    scheduler.add_job(fetch_and_process_data, "date", run_date=datetime.now() + timedelta(seconds=2))
    # Schedule future updates every 24 hours
    scheduler.add_job(fetch_and_process_data, "interval", hours=24, next_run_time=datetime.now() + timedelta(minutes=1))
    scheduler.start()
    yield
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan)

@app.get("/update_data")
def update_data(background_tasks: BackgroundTasks):
    """Trigger a manual data update in the background."""
    background_tasks.add_task(fetch_and_process_data)
    return {"message": "✅ Data update started in the background."}

@app.get("/pitchers")
def get_pitchers():
    if cached_pitcher_data is None:
        return {"error": "⚠️ Data not loaded yet"}
    pitchers = cached_pitcher_data[['Name', 'Team', 'Total Z-Score', 'W', 'ERA', 'WHIP', 'SO', 'SV', 'HLD']].head(100).to_dict(orient="records")
    return pitchers

@app.get("/hitters")
def get_hitters():
    if cached_hitter_data is None:
        return {"error": "⚠️ Data not loaded yet"}
    hitters = cached_hitter_data[['Name', 'Team', 'Total Z-Score', 'R', 'HR', 'RBI', 'SB', 'AVG']].head(100).to_dict(orient="records")
    return hitters

@app.get("/player/{player_name}")
def get_player(player_name: str):
    """Fetch player stats from cached data."""
    if cached_pitcher_data is None or cached_hitter_data is None:
        return {"error": "⚠️ Data not loaded yet"}

    player_stats = cached_pitcher_data[cached_pitcher_data['Name'] == player_name]
    if player_stats.empty:
        player_stats = cached_hitter_data[cached_hitter_data['Name'] == player_name]
    if not player_stats.empty:
        return player_stats.iloc[0].to_dict()
    return {"error": "❌ Player not found"}

@app.get("/filter_stats")
def filter_stats(
    season: int = Query(..., ge=2015),
    timeframe: str = Query(...),
    type: str = Query(...),
):
    """
    Filter raw Statcast stats by season and timeframe.
    Timeframe options: 'last_week', 'last_2_weeks', 'last_month'
    Type options: 'pitcher' or 'hitter'
    """
    end_date = datetime.today()
    days_map = {
        "last_week": 7,
        "last_2_weeks": 14,
        "last_month": 30
    }

    if timeframe not in days_map:
        return {"error": "❌ Invalid timeframe. Use: 'last_week', 'last_2_weeks', or 'last_month'."}

    start_date = end_date - timedelta(days=days_map[timeframe])
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    try:
        if type == "pitcher":
            data = statcast_pitcher(start_dt=start_str, end_dt=end_str)
            columns_to_return = ["player_name", "pitch_type", "release_speed", "release_spin_rate", "whiff"]
        elif type == "hitter":
            data = statcast_batter(start_dt=start_str, end_dt=end_str)
            columns_to_return = ["player_name", "events", "launch_speed", "launch_angle", "babip"]
        else:
            return {"error": "❌ Invalid type. Use 'pitcher' or 'hitter'."}
    except Exception as e:
        return {"error": f"Error fetching data: {str(e)}"}

    data = data[columns_to_return].dropna()
    data = data.groupby("player_name").mean().reset_index()

    return data.sort_values(by=columns_to_return[1], ascending=False).head(50).to_dict(orient="records")

# Local dev entry point
if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)