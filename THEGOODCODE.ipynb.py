from pybaseball import pitching_stats
pitcher_data = pitching_stats(2025,2025)
#print(list(pitcher_data.columns))
from pybaseball import batting_stats
hitter_data = batting_stats(2025,2025)
#print(list(hitter_data.columns))
pitcher_categories = ['Name', 'W', 'ERA', 'WHIP', 'SO', 'SV', 'HLD','IP']
pitcher_data_categories = pitcher_data[pitcher_categories]
pitcher_data_filtered = pitcher_data_categories[pitcher_data_categories['IP'] > 0]
#print(pitcher_data_filtered)
pitcher_stats_mean = pitcher_data_filtered.drop(columns=['Name']).mean()
pitcher_stats_std = pitcher_data_filtered.drop(columns=['Name']).std()
#print("Mean values:\n", pitcher_stats_mean)
#print("\nStandard Deviation:\n", pitcher_stats_std)
numeric_columns = ['W', 'ERA', 'WHIP', 'SO', 'SV', 'HLD']
pitcher_z_scores = (pitcher_data_filtered[numeric_columns] - pitcher_stats_mean[numeric_columns]) / pitcher_stats_std[numeric_columns]
pitcher_z_scores['ERA'] *= -1
pitcher_z_scores['WHIP'] *= -1
# Add 'Name' column back
pitcher_z_scores.insert(0, 'Name', pitcher_data_filtered['Name'])

# Print final Z-score table
#print(pitcher_z_scores)

hitter_categories = ['Name', 'R', 'HR', 'RBI', 'SB', 'AVG', 'PA']
hitter_data_categories = hitter_data[hitter_categories]
hitter_data_filtered = hitter_data_categories[hitter_data_categories['PA'] > 0]
#print(hitter_data_filtered)
hitter_stats_mean = hitter_data_filtered.drop(columns=['Name']).mean()
hitter_stats_std = hitter_data_filtered.drop(columns=['Name']).std()
numeric_columns = ['R', 'HR', 'RBI', 'SB', 'AVG']
hitter_z_scores = (hitter_data_filtered[numeric_columns] - hitter_stats_mean[numeric_columns]) / hitter_stats_std[numeric_columns]

# Add 'Name' column back
hitter_z_scores.insert(0, 'Name', hitter_data_filtered['Name'])

# Print final Z-score table
#print(hitter_z_scores)
#print(hitter_data_categories)

# Sum up all Z-scores (excluding 'Name')
pitcher_z_scores['Total Z-Score'] = pitcher_z_scores.drop(columns=['Name']).sum(axis=1)

# Sort the dataset by Total Z-Score in descending order
pitcher_z_scores_ranked = pitcher_z_scores.sort_values(by='Total Z-Score', ascending=False)

# Display the ranked dataset
print(pitcher_z_scores_ranked)

# Sum up all Z-scores (excluding 'Name')
hitter_z_scores['Total Z-Score'] = hitter_z_scores.drop(columns=['Name']).sum(axis=1)

# Sort the dataset by Total Z-Score in descending order
hitter_z_scores_ranked = hitter_z_scores.sort_values(by='Total Z-Score', ascending=False)

# Display the ranked dataset
print(hitter_z_scores_ranked)

import streamlit as st

# Load datasets
pitcher_z_scores_ranked = pd.read_csv("pitcher_z_scores.csv")  # Adjust filename as needed
hitter_z_scores_ranked = pd.read_csv("hitter_z_scores.csv")  # Adjust filename as needed

# Sidebar - Select Pitcher or Hitter
player_type = st.sidebar.radio("Select Player Type", ["Pitcher", "Hitter"])

if player_type == "Pitcher":
    # Sidebar - Select Team
    teams = pitcher_z_scores_ranked["Team"].unique()  # Assuming you have a 'Team' column
    selected_team = st.sidebar.selectbox("Select a Team", teams)

    # Filter players based on the selected team
    filtered_players = pitcher_z_scores_ranked[pitcher_z_scores_ranked["Team"] == selected_team]

    # Sidebar - Select Player
    selected_player = st.sidebar.selectbox("Select a Player", filtered_players["Name"])

    # Show player's Z-scores
    player_stats = filtered_players[filtered_players["Name"] == selected_player]
    st.write(f"### Z-Scores for {selected_player}")
    st.dataframe(player_stats)

else:
    # Sidebar - Select Team
    teams = hitter_z_scores_ranked["Team"].unique()  # Assuming you have a 'Team' column
    selected_team = st.sidebar.selectbox("Select a Team", teams)

    # Filter players based on the selected team
    filtered_players = hitter_z_scores_ranked[hitter_z_scores_ranked["Team"] == selected_team]

    # Sidebar - Select Player
    selected_player = st.sidebar.selectbox("Select a Player", filtered_players["Name"])

    # Show player's Z-scores
    player_stats = filtered_players[filtered_players["Name"] == selected_player]
    st.write(f"### Z-Scores for {selected_player}")
    st.dataframe(player_stats)