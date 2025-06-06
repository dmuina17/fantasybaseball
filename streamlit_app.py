import streamlit as st
import pandas as pd
from pybaseball import pitching_stats, batting_stats, pitching_stats_range, batting_stats_range
from datetime import datetime, timedelta

# --- Season and Timeframe Dropdown ---
season_options = ["2025", "2024", "2023", "Last Week", "Last 2 Weeks", "Last Month"]
selected_season = st.sidebar.selectbox("Select Season or Timeframe:", season_options)

# --- Handle Date Ranges ---
end_date = datetime.today()
if selected_season == "Last Week":
    start_date = end_date - timedelta(days=7)
elif selected_season == "Last 2 Weeks":
    start_date = end_date - timedelta(days=14)
elif selected_season == "Last Month":
    start_date = end_date - timedelta(days=30)
else:
    start_date = None  # For full season

# --- Fetch Data ---
if start_date is None:
    pitcher_dat = pitching_stats(int(selected_season), int(selected_season), qual=0)
    hitter_dat = batting_stats(int(selected_season), int(selected_season), qual=0)
else:
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    pitcher_dat = pitching_stats_range(start_str, end_str)
    hitter_dat = batting_stats_range(start_str, end_str)

pitcher_positions = pd.read_csv('pitcher_positions.csv')
pitcher_positions.rename(columns={'ESPN': 'Pos'}, inplace=True)

# Changes to pitcher_dat
pitcher_dat.loc[(pitcher_dat['Name'] == 'Alexis Diaz') & (pitcher_dat['Team'] == 'CIN'), 'Team'] = 'LAD'
pitcher_dat.loc[(pitcher_dat['Name'] == 'Ian Anderson') & (pitcher_dat['Team'] == 'LAA'), 'Team'] = 'ATL'
pitcher_dat.loc[(pitcher_dat['Name'] == 'Jason Alexander') & (pitcher_dat['Team'] == 'ATH'), 'Team'] = 'HOU'
pitcher_dat.loc[(pitcher_dat['Name'] == 'Josh Walker') & (pitcher_dat['Team'] == 'TOR'), 'Team'] = 'PHI'
pitcher_dat.loc[(pitcher_dat['Name'] == 'Kenta Maeda') & (pitcher_dat['Team'] == 'DET'), 'Team'] = 'CHC'
pitcher_dat.loc[(pitcher_dat['Name'] == 'Kevin Herget') & (pitcher_dat['Team'] == 'NYM'), 'Team'] = 'ATL'
pitcher_dat.loc[(pitcher_dat['Name'] == 'Kyle Gibson') & (pitcher_dat['Team'] == 'BAL'), 'Team'] = 'TBR'
pitcher_dat.loc[(pitcher_dat['Name'] == 'Matt Krook') & (pitcher_dat['Team'] == 'ATH'), 'Team'] = 'CLE'
pitcher_dat.loc[(pitcher_dat['Name'] == 'Noah Murdock') & (pitcher_dat['Team'] == 'ATH'), 'Team'] = 'KCR'
pitcher_dat.loc[(pitcher_dat['Name'] == 'Michael Fulmer') & (pitcher_dat['Team'] == 'BOS'), 'Team'] = 'CHC'#
pitcher_dat.loc[(pitcher_dat['Name'] == 'Tayler Scott') & (pitcher_dat['Team'] == 'HOU'), 'Team'] = 'ARI'
pitcher_dat.loc[(pitcher_dat['Name'] == 'Tyler Matzek') & (pitcher_dat['Team'] == 'NYY'), 'Team'] = 'STL'
pitcher_dat.loc[(pitcher_dat['Name'] == 'Colin Poche') & (pitcher_dat['Team'] == 'WSN'), 'Team'] = 'NYM'
pitcher_dat.loc[(pitcher_dat['Name'] == 'Lucas Sims') & (pitcher_dat['Team'] == 'WSN'), 'Team'] = 'PHI'
pitcher_dat.loc[(pitcher_dat['Name'] == 'Casey Lawrence') & (pitcher_dat['Team'] == '- - -'), 'Team'] = 'SEA'
pitcher_dat.loc[(pitcher_dat['Name'] == 'Hector Neris') & (pitcher_dat['Team'] == '- - -'), 'Team'] = 'LAA'
pitcher_dat.loc[(pitcher_dat['Name'] == 'Jose Castillo') & (pitcher_dat['Team'] == '- - -'), 'Team'] = 'NYM'
pitcher_dat.loc[(pitcher_dat['Name'] == 'Jose Urena') & (pitcher_dat['Team'] == '- - -'), 'Team'] = 'LAD'
pitcher_dat.loc[(pitcher_dat['Name'] == 'Lou Trivino') & (pitcher_dat['Team'] == '- - -'), 'Team'] = 'LAD'
pitcher_dat.loc[(pitcher_dat['Name'] == 'Rafael Montero') & (pitcher_dat['Team'] == '- - -'), 'Team'] = 'ATL'
pitcher_dat.loc[(pitcher_dat['Name'] == 'Scott Blewett') & (pitcher_dat['Team'] == '- - -'), 'Team'] = 'ATL'
pitcher_dat.loc[(pitcher_dat['Name'] == 'Sean Newcomb') & (pitcher_dat['Team'] == '- - -'), 'Team'] = 'ATH'
pitcher_dat.loc[(pitcher_dat['Name'] == 'Yoendrys Gomez') & (pitcher_dat['Team'] == '- - -'), 'Team'] = 'CHW'
pitcher_dat.loc[(pitcher_dat['Name'] == 'Genesis Cabrera') & (pitcher_dat['Team'] == '- - -'), 'Team'] = 'CHC'


# Changes to pitcher_positions
pitcher_positions.loc[(pitcher_positions['Name'] == 'Carl Edwards Jr.') & (pitcher_positions['Team'] == 'FA'), 'Team'] = 'CHC'
pitcher_positions.loc[(pitcher_positions['Name'] == 'Brooks Kriske') & (pitcher_positions['Team'] == 'FA'), 'Team'] = 'LAA'
pitcher_positions.loc[(pitcher_positions['Name'] == 'Cody Bolton') & (pitcher_positions['Team'] == 'FA'), 'Team'] = 'CLE'
pitcher_positions.loc[(pitcher_positions['Name'] == 'Joe Mantiply') & (pitcher_positions['Team'] == 'FA'), 'Team'] = 'ARI'
pitcher_positions.loc[(pitcher_positions['Name'] == 'Jose Ruiz') & (pitcher_positions['Team'] == 'FA'), 'Team'] = 'PHI'
pitcher_positions.loc[(pitcher_positions['Name'] == 'Julian Merryweather') & (pitcher_positions['Team'] == 'FA'), 'Team'] = 'CHC'
pitcher_positions.loc[(pitcher_positions['Name'] == 'Tanner Rainey') & (pitcher_positions['Team'] == 'FA'), 'Team'] = 'PIT'
pitcher_positions.loc[(pitcher_positions['Name'] == 'Triston McKenzie') & (pitcher_positions['Team'] == 'FA'), 'Team'] = 'CLE'
pitcher_positions.loc[(pitcher_positions['Name'] == 'Tyler Alexander') & (pitcher_positions['Team'] == 'FA'), 'Team'] = 'MIL'
pitcher_positions.loc[(pitcher_positions['Name'] == 'Xzavion Curry') & (pitcher_positions['Team'] == 'FA'), 'Team'] = 'MIA'


team_replacements_pitcher = {
    'WSH': 'WSN',#
    'CWS': 'CHW',
    'TB': 'TBR',#
    'SD': 'SDP',#
    'SF': 'SFG',#
    'KC': 'KCR',#
}
pitcher_positions['Team'] = pitcher_positions['Team'].replace(team_replacements_pitcher)

name_replacements_pitcher = {
    'AJ Blubaugh': 'A.J. Blubaugh',
    'Jack Dreyer': 'Jacob Dreyer',
    'Jake Eder': 'Jacob Eder',
    'Jacob Latz': 'Jake Latz',
    'Louis Varland': 'Louie Varland',
    'Pat Monteverde': 'Patrick Monteverde',
    "Riley Oâ€™Brien": "Riley O'Brien",
    'Thomas Harrington': 'Tom Harrington',
    'Yerry De los Santos': 'Yerry De Los Santos',
    'Zach Agnos': 'Zachary Agnos',
    'Brad Lord': 'Bradley Lord',
    'Michael Soroka': 'Mike Soroka'
}

pitcher_positions['Name'] = pitcher_positions['Name'].replace(name_replacements_pitcher)

pitcher_data = pd.merge(pitcher_dat, pitcher_positions[['Name', 'Team', 'Pos']], on = ['Name', 'Team'], how = 'left')


#Replace Teams: WSH with WSN, CWS with CHW, TB with TBR, SD with SDP, SF with SFG, KC with KCR, 
#Replace Ben Williamson with Benjamin Williamson, Bobby Witt with Bobby Witt Jr., CJ Alexander with C.J. Alexander
#Replace DaShawn Keirsey with DaShawn Keirsey Jr., Jackson Chourio with Jazz Chisholm Jr., Leo Rivas with Leonardo Rivas
#Replace Lourdes Gurriel with Lourdes Gurriel Jr., Michael Harris with Michael Harris II, Nick Kurtz with Nicholas Kurtz
#Replace Ronald Acuna with Ronald Acuna Jr., Victor Scott with Victor Scott II, Vladimir Guerrero with Vladimir Guerrero Jr., 
#Replace Vladimir Guerrero with Vladimir Guerrero Jr., Zach Dezenzo with Zachary Dezenzo
hitter_positions = pd.read_csv("/workspaces/fantasybaseball/Player Positions.csv")
name_mistakes_hitter = {
    'Jack WInkler': 'Jack Winkler',

}
hitter_dat['Name'] = hitter_dat['Name'].replace(name_mistakes_hitter)
team_replacements_hitter = {
    'WSH': 'WSN',
    'CWS': 'CHW',
    'TB': 'TBR',
    'SD': 'SDP',
    'SF': 'SFG',
    'KC': 'KCR',
}
hitter_positions['Team'] = hitter_positions['Team'].replace(team_replacements_hitter)
# Replace player names
name_replacements_hitter = {
    'Ben Williamson': 'Benjamin Williamson',
    'Bobby Witt': 'Bobby Witt Jr.',
    'CJ Alexander': 'C.J. Alexander',
    'DaShawn Keirsey': 'DaShawn Keirsey Jr.',
    'Leo Rivas': 'Leonardo Rivas',
    'Lourdes Gurriel': 'Lourdes Gurriel Jr.',
    'Michael Harris': 'Michael Harris II',
    'Nick Kurtz': 'Nicholas Kurtz',
    'Ronald Acuna': 'Ronald Acuna Jr.',
    'Victor Scott': 'Victor Scott II',
    'Vladimir Guerrero': 'Vladimir Guerrero Jr.',
    'Zach Dezenzo': 'Zachary Dezenzo',
    'Fernando Tatis': 'Fernando Tatis Jr.',
    'LaMonte Wade': 'LaMonte Wade Jr.',
    'Luis Garcia': 'Luis Garcia Jr.',
    'Luis Robert': 'Luis Robert Jr.',
    'Michael Taylor': 'Michael A. Taylor',
    'Robert Hassell': 'Robert Hassell III',
    'Jazz Chisholm': 'Jazz Chisholm Jr.',
    'Tim Elko': 'Timothy Elko'
}

hitter_positions['Name'] = hitter_positions['Name'].replace(name_replacements_hitter)

hitter_data = pd.merge(hitter_dat, hitter_positions[['Name', 'Team', 'Pos']], on = ['Name', 'Team'], how = 'left')
hitter_data.rename(columns={'Pos_y': 'Pos'}, inplace=True)

# --- Pitcher Z-Scores ---
pitcher_categories = ['Name','Team', 'Pos', 'W', 'ERA', 'WHIP', 'SO', 'SV', 'HLD','IP']
pitcher_data_categories = pitcher_data[pitcher_categories]
pitcher_data_filtered = pitcher_data_categories[pitcher_data_categories['IP'] > 0]
pitcher_stats_mean = pitcher_data_filtered.drop(columns=['Name','Team', 'Pos',]).mean()
pitcher_stats_std = pitcher_data_filtered.drop(columns=['Name','Team', 'Pos',]).std()
pitcher_numeric_columns = ['W', 'ERA', 'WHIP', 'SO', 'SV', 'HLD']
pitcher_z_scores = (pitcher_data_filtered[pitcher_numeric_columns] - pitcher_stats_mean[pitcher_numeric_columns]) / pitcher_stats_std[pitcher_numeric_columns]
pitcher_data_filtered['Weighted_ERA'] = (pitcher_data_filtered['ERA'] - pitcher_stats_mean['ERA']) / (pitcher_stats_std['ERA'] / (pitcher_data_filtered['IP'] ** 0.5))
pitcher_z_scores['ERA'] = (pitcher_data_filtered['Weighted_ERA'] - pitcher_data_filtered['Weighted_ERA'] .mean()) / pitcher_data_filtered['Weighted_ERA'] .std()
pitcher_data_filtered['Weighted_WHIP'] = (pitcher_data_filtered['WHIP'] - pitcher_stats_mean['WHIP']) / (pitcher_stats_std['WHIP'] / (pitcher_data_filtered['IP'] ** 0.5))
pitcher_z_scores['WHIP'] = (pitcher_data_filtered['Weighted_WHIP'] - pitcher_data_filtered['Weighted_WHIP'] .mean()) / pitcher_data_filtered['Weighted_WHIP'] .std()
pitcher_z_scores['ERA'] *= -1
pitcher_z_scores['WHIP'] *= -1
pitcher_z_scores.insert(0, 'Name', pitcher_data_filtered['Name'])
pitcher_z_scores.insert(1, 'Team', pitcher_data_filtered['Team'])
pitcher_z_scores.insert(2, 'Pos', pitcher_data_filtered['Pos'])

# --- Hitter Z-Scores ---
hitter_categories = ['Name','Team', 'Pos', 'R', 'HR', 'RBI', 'SB', 'AVG', 'PA']
hitter_data_categories = hitter_data[hitter_categories]
hitter_data_filtered = hitter_data_categories[hitter_data_categories['PA'] > 0]
hitter_stats_mean = hitter_data_filtered.drop(columns=['Name','Team', 'Pos']).mean()
hitter_stats_std = hitter_data_filtered.drop(columns=['Name','Team', 'Pos']).std()
hitter_numeric_columns = ['R', 'HR', 'RBI', 'SB', 'AVG']
hitter_z_scores = (hitter_data_filtered[hitter_numeric_columns] - hitter_stats_mean[hitter_numeric_columns]) / hitter_stats_std[hitter_numeric_columns]
hitter_data_filtered['Weighted_AVG'] = (hitter_data_filtered['AVG'] - hitter_stats_mean['AVG']) / (hitter_stats_std['AVG'] / (hitter_data_filtered['PA'] ** 0.5))
hitter_z_scores['AVG'] = (hitter_data_filtered['Weighted_AVG'] - hitter_data_filtered['Weighted_AVG'] .mean()) / hitter_data_filtered['Weighted_AVG'] .std()
hitter_z_scores.insert(0, 'Name', hitter_data_filtered['Name'])
hitter_z_scores.insert(1, 'Team', hitter_data_filtered['Team'])
hitter_z_scores.insert(2, 'Pos', hitter_data_filtered['Pos'])

# --- Total Z-Scores ---
pitcher_z_scores['Total Z-Score'] = pitcher_z_scores[pitcher_numeric_columns].sum(axis=1)
hitter_z_scores['Total Z-Score'] = hitter_z_scores[hitter_numeric_columns].sum(axis=1)

# --- Rank and Save ---
pitcher_z_scores_ranked = pitcher_z_scores.sort_values(by='Total Z-Score', ascending=False)
hitter_z_scores_ranked = hitter_z_scores.sort_values(by='Total Z-Score', ascending=False)
pitcher_z_scores_ranked.to_csv("pitcher_z_scores.csv", index=False)
hitter_z_scores_ranked.to_csv("hitter_z_scores.csv", index=False)
pitcher_z_scores_ranked = pd.read_csv("pitcher_z_scores.csv")
hitter_z_scores_ranked = pd.read_csv("hitter_z_scores.csv")
pitcher_z_scores_ranked.insert(0, "Rank", pitcher_z_scores_ranked.index + 1)
hitter_z_scores_ranked.insert(0, "Rank", hitter_z_scores_ranked.index + 1)
rank_hitter_data = pd.merge(hitter_data, hitter_z_scores_ranked[['Name', 'Team', 'Rank']], on = ['Name', 'Team'], how='left')
sortedrank_hitter_data = rank_hitter_data.sort_values(by='Rank', ascending=True)
rank_pitcher_data = pd.merge(pitcher_data, pitcher_z_scores_ranked[['Name', 'Team', 'Rank']], on = ['Name', 'Team'], how='left')
sortedrank_pitcher_data = rank_pitcher_data.sort_values(by='Rank', ascending=True)

# --- Streamlit UI ---
st.title("Baseball Player Z-Score Rankings")
stat_type = st.sidebar.radio("Select Stat Type", ["Raw Stats", "Z-Scores"])
tab1, tab2, tab3, tab4 = st.tabs(["🔍 Player Lookup", "🏆 Top 100 Players", "📊 Team Overview", "🎮 Fantasy Team"])

import math  # for math.isnan

def color_z_scores(val):      
    try:
        # Handle None or NaN explicitly
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return ""  # no style for None/NaN
        
        val = float(val)
        if val >= 2.5:
            color = "#006400"  # dark green
        elif val >= 1.5:
            color = "#008000"  # green
        elif val >= 0.5:
            color = "#32CD32"  # light green
        elif val >= -0.5:
            color = "#FFFFFF"  # white
        elif val >= -1.5:
            color = "#FFA07A"  # light red
        elif val >= -2.5:
            color = "#FF0000"  # red
        else:
            color = "#8B0000"  # dark red
        return f"background-color: {color}; color: black;"
    except:
        return "" 

with tab1:
    player_type = st.sidebar.radio("Select Player Type", ["Pitcher", "Hitter"])
    if player_type == "Pitcher":
        teams = pitcher_z_scores_ranked["Team"].unique()
        selected_team = st.sidebar.selectbox("Select a Team", teams)
        filtered_players = pitcher_z_scores_ranked[pitcher_z_scores_ranked["Team"] == selected_team]
        selected_player = st.sidebar.selectbox("Select a Player", filtered_players["Name"])
        player_stats = filtered_players[filtered_players["Name"] == selected_player]
        st.write(f"### Z-Scores for {selected_player}")

        if stat_type == "Z-Scores":
            zscore_cols = ['W', 'ERA', 'WHIP', 'SO', 'SV', 'HLD']
            df_to_show = player_stats[['Rank','Name', 'Pos', 'Team'] + zscore_cols + ['Total Z-Score']]
            styled_df = df_to_show.style.applymap(color_z_scores, subset=zscore_cols)
            st.dataframe(styled_df, hide_index=True)
        else:
            raw_player_stats = sortedrank_pitcher_data[sortedrank_pitcher_data['Name'] == selected_player]
            st.dataframe(raw_player_stats[['Rank','Name', 'Pos', 'Team', 'W', 'ERA', 'WHIP', 'SO', 'SV', 'HLD']].style.format({
                "ERA": "{:.2f}",
                "WHIP": "{:.2f}"
            }), 
            hide_index=True)
    else:
        teams = hitter_z_scores_ranked["Team"].unique()
        selected_team = st.sidebar.selectbox("Select a Team", teams)
        filtered_players = hitter_z_scores_ranked[hitter_z_scores_ranked["Team"] == selected_team]
        selected_player = st.sidebar.selectbox("Select a Player", filtered_players["Name"])
        player_stats = filtered_players[filtered_players["Name"] == selected_player]
        st.write(f"### Z-Scores for {selected_player}")
        if stat_type == "Z-Scores":
            zscore_cols = ['R', 'HR', 'RBI', 'SB', 'AVG']
            df_to_show = player_stats[['Rank', 'Name', 'Pos', 'Team'] + zscore_cols + ['Total Z-Score']]
            styled_df = df_to_show.style.applymap(color_z_scores, subset=zscore_cols)
            st.dataframe(styled_df, hide_index=True)
        else:
            raw_player_stats = sortedrank_hitter_data[sortedrank_hitter_data['Name'] == selected_player]
            st.dataframe(raw_player_stats[['Rank', 'Name', 'Pos', 'Team', 'R', 'HR', 'RBI', 'SB', 'AVG']].style.format({
                "Rank": "{:.0f}",
                "AVG": "{:.3f}",
            }), 
            hide_index=True)

with tab2:
    st.subheader("🏆 Top 100 Pitchers (Z-Score Rankings)")

    pitcher_positions = pitcher_z_scores_ranked['Pos'].dropna().unique()
    selected_pitcher_pos = st.selectbox("Filter Pitchers by Position:", ['All'] + sorted(pitcher_positions), key='pitcher_pos_filter')

    if selected_pitcher_pos != 'All':
        filtered_pitchers = pitcher_z_scores_ranked[pitcher_z_scores_ranked['Pos'] == selected_pitcher_pos].head(100)
        filtered_pitchers_raw = sortedrank_pitcher_data[sortedrank_pitcher_data['Pos'] == selected_pitcher_pos].head(100)
    else:
        filtered_pitchers = pitcher_z_scores_ranked.head(100)
        filtered_pitchers_raw = sortedrank_pitcher_data.head(100)

    if stat_type == "Z-Scores":
        zscore_cols = ['W', 'ERA', 'WHIP', 'SO', 'SV', 'HLD']
        df = filtered_pitchers[['Rank', 'Name', 'Pos', 'Team', 'Total Z-Score'] + zscore_cols]
        df = df[[col for col in df.columns if col != 'Total Z-Score'] + ['Total Z-Score']]
        styled_df = df.style.applymap(color_z_scores, subset=zscore_cols)
        st.dataframe(styled_df, hide_index=True)
    else:
        st.dataframe(filtered_pitchers_raw[['Rank', 'Name', 'Pos', 'Team', 'W', 'ERA', 'WHIP', 'SO', 'SV', 'HLD']].style.format({
            "ERA": "{:.2f}",
            "WHIP": "{:.2f}"
        }), hide_index=True)

    st.subheader("🏆 Top 100 Hitters (Z-Score Rankings)")

    hitter_positions = hitter_z_scores_ranked['Pos'].dropna().unique()
    selected_hitter_pos = st.selectbox("Filter Hitters by Position:", ['All'] + sorted(hitter_positions), key='hitter_pos_filter')

    if selected_hitter_pos != 'All':
        filtered_hitters = hitter_z_scores_ranked[hitter_z_scores_ranked['Pos'] == selected_hitter_pos].head(100)
        filtered_hitters_raw = sortedrank_hitter_data[sortedrank_hitter_data['Pos'] == selected_hitter_pos].head(100)
    else:
        filtered_hitters = hitter_z_scores_ranked.head(100)
        filtered_hitters_raw = sortedrank_hitter_data.head(100)

    if stat_type == "Z-Scores":
        zscore_cols = ['R', 'HR', 'RBI', 'SB', 'AVG']
        df = filtered_hitters[['Rank', 'Name', 'Pos', 'Team', 'Total Z-Score'] + zscore_cols]
        df = df[[col for col in df.columns if col != 'Total Z-Score'] + ['Total Z-Score']]
        styled_df = df.style.applymap(color_z_scores, subset=zscore_cols)
        st.dataframe(styled_df, hide_index=True)
    else:
        st.dataframe(filtered_hitters_raw[['Rank', 'Name', 'Pos', 'Team', 'R', 'HR', 'RBI', 'SB', 'AVG']].style.format({
            "Rank": "{:.0f}",
            "AVG": "{:.3f}"
        }), hide_index=True)

with tab3:
    st.subheader("📊 Teams Overview (Pitchers & Hitters)")
    teams = list(set(pitcher_z_scores_ranked["Team"]) | set(hitter_z_scores_ranked["Team"]))
    selected_team = st.selectbox("Select a Team", teams)
    team_pitchers = pitcher_z_scores_ranked[pitcher_z_scores_ranked["Team"] == selected_team]
    team_hitters = hitter_z_scores_ranked[hitter_z_scores_ranked["Team"] == selected_team]

    if not team_pitchers.empty:
        st.write("### Pitchers")
        if stat_type == "Z-Scores":
            zscore_cols = ['W', 'ERA', 'WHIP', 'SO', 'SV', 'HLD']
            display_cols = ['Rank', 'Name', 'Pos'] + zscore_cols + ['Total Z-Score']
            styled_df = team_pitchers[display_cols].style.applymap(color_z_scores, subset=zscore_cols)
            st.dataframe(styled_df, hide_index=True)
        else:
            raw_player_stats = sortedrank_pitcher_data[sortedrank_pitcher_data['Team'] == selected_team]
            st.dataframe(raw_player_stats[['Rank','Name', 'Pos', 'W', 'ERA', 'WHIP', 'SO', 'SV', 'HLD']].style.format({
                "ERA": "{:.2f}",
                "WHIP": "{:.2f}"
            }),
            hide_index=True)

    if not team_hitters.empty:
        st.write("### Hitters")
        if stat_type == "Z-Scores":
            zscore_cols = ['R', 'HR', 'RBI', 'SB', 'AVG']
            display_cols = ['Rank', 'Name', 'Pos'] + zscore_cols + ['Total Z-Score']
            styled_df = team_hitters[display_cols].style.applymap(color_z_scores, subset=zscore_cols) 
            st.dataframe(styled_df, hide_index=True)
        else:
            raw_player_stats = sortedrank_hitter_data[sortedrank_hitter_data['Team'] == selected_team]
            st.dataframe(raw_player_stats[['Rank', 'Name', 'Pos','R', 'HR', 'RBI', 'SB', 'AVG']].style.format({
                "Rank": "{:.0f}",
                "AVG": "{:.3f}",
            }),
            hide_index=True)

with tab4:
    st.subheader("🎮 Create Your Fantasy Team")
    selected_pitchers = st.multiselect("Select Pitchers", pitcher_z_scores_ranked["Name"], key="pitcher_select")
    selected_hitters = st.multiselect("Select Hitters", hitter_z_scores_ranked["Name"], key="hitter_select")
    selected_players = pitcher_z_scores_ranked[pitcher_z_scores_ranked["Name"].isin(selected_pitchers)]
    selected_players = pd.concat([selected_players, hitter_z_scores_ranked[hitter_z_scores_ranked["Name"].isin(selected_hitters)]])
    
    if not selected_players.empty:
        total_row = selected_players.iloc[:, 2:].sum()
        total_row["Name"] = "TOTAL"
        total_row["Team"] = "-"
        selected_players = pd.concat([selected_players, pd.DataFrame([total_row])], ignore_index=True)
        cols = [col for col in selected_players.columns if col != "Total Z-Score"] + ["Total Z-Score"]
        selected_players = selected_players[cols]

        zscore_cols = ['W', 'ERA', 'WHIP', 'SO', 'SV', 'HLD', 'R', 'HR', 'RBI', 'SB', 'AVG']
        zscore_cols = [col for col in zscore_cols if col in selected_players.columns]  # only include existing columns

        mask = selected_players["Name"] != "TOTAL"
        # Apply styling only to non-TOTAL rows, and exclude "Total Z-Score"
        styled_df = selected_players.style \
            .applymap(lambda val: color_z_scores(val), subset=pd.IndexSlice[mask, zscore_cols]) \
            .format({
                "Rank": "{:.0f}",  # No decimals for Rank
            })

        st.dataframe(styled_df, hide_index=True)
