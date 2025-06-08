import os
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import streamlit as st
import ast # Import for safely evaluating string representations of lists
from collections import defaultdict
import itertools # For combinations in recommender
import numpy as np # Import numpy to handle numpy arrays

# --- Configuration ---
# Path to your exploitation DuckDB database
# This path is relative to where your Streamlit app will be run (e.g., inside the Docker container)
# Corrected path to match the Docker volume mount: ./data on host is /data in container
EXPLOITATION_DB_PATH = os.path.join("/data", "exploitation_zone", "explotation.duckdb")


# Ensure the directory exists (it should be created by the previous script)
# In a Docker container, ensure the data volume is correctly mounted.
os.makedirs(os.path.dirname(EXPLOITATION_DB_PATH), exist_ok=True)


# --- Helper function for outlier cleaning ---
def remove_outliers_iqr(df, column):
    """
    Removes outliers from a specified column in a DataFrame using the IQR method.
    Returns a new DataFrame with outliers removed.
    """
    if column not in df.columns or df[column].isnull().all() or not pd.api.types.is_numeric_dtype(df[column]):
        return df.copy() # No column, all NaNs, or not numeric, return original copy

    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    # Filter out the outliers
    df_cleaned = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)].copy()
    return df_cleaned


# --- Helper function to generate plots ---
# This function will now return the matplotlib figure directly for Streamlit to display
def generate_plot(df, plot_type, x_col, y_col=None, title="", xlabel="", ylabel="", sort_by=None, n_top=None):
    """
    Generates a matplotlib plot and returns the figure object.

    Args:
        df (pd.DataFrame): The DataFrame to plot.
        plot_type (str): Type of plot ('bar', 'scatter', 'hist').
        x_col (str): Column for the x-axis.
        y_col (str, optional): Column for the y-axis (required for 'bar', 'scatter'). Defaults to None.
        title (str): Title of the plot.
        xlabel (str): Label for the x-axis.
        ylabel (str): Label for the y-label.
        sort_by (str, optional): Column to sort by before selecting top N. Defaults to None.
        n_top (int, optional): Number of top items to display for bar charts. Defaults to None.
    Returns:
        matplotlib.figure.Figure: The generated matplotlib figure.
    """
    fig, ax = plt.subplots(figsize=(10, 6))

    if n_top and sort_by and plot_type == 'bar':
        df = df.sort_values(by=sort_by, ascending=False).head(n_top)
    
    if plot_type == 'bar':
        if x_col and y_col:
            ax.bar(df[x_col], df[y_col])
            ax.set_xticks(df[x_col]) # Ensure all x-ticks are shown
            ax.set_xticklabels(df[x_col], rotation=45, ha='right')
    elif plot_type == 'scatter':
        if x_col and y_col:
            ax.scatter(df[x_col], df[y_col], alpha=0.7)
    elif plot_type == 'hist':
        # Drop NaN values for histogram as plt.hist doesn't handle them
        ax.hist(df[x_col].dropna(), bins=20, edgecolor='black')
    else:
        st.warning(f"Unsupported plot type: {plot_type}")
        return None

    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    fig.tight_layout() # Adjust layout to prevent labels from overlapping
    
    # Ensure consistent scale, avoid scientific notation for y-axis
    ax.ticklabel_format(style='plain', axis='y')

    return fig

# --- Data Loading Functions (with caching for performance and auto-refresh) ---
@st.cache_data(ttl=60) # Data will refresh every 60 seconds (1 minute)
def load_data():
    """Loads data from DuckDB into Pandas DataFrames and creates appid-name mappings."""
    conn = None
    try:
        conn = duckdb.connect(database=EXPLOITATION_DB_PATH, read_only=True)
        df_game_metrics = conn.execute("SELECT * FROM game_metrics").fetchdf()
        df_country_metrics = conn.execute("SELECT * FROM country_metrics").fetchdf()
        df_user_metrics = conn.execute("SELECT * FROM user_metrics").fetchdf()

        # Removed processing for 'genres', 'developers', 'publishers' from df_game_metrics as they are always empty.

        # Ensure 'ListOfOwnedGames' is treated as a list of strings (appids) for user metrics
        if 'ListOfOwnedGames' in df_user_metrics.columns:
            df_user_metrics['ListOfOwnedGames'] = df_user_metrics['ListOfOwnedGames'].apply(
                lambda x: [str(item) for item in ast.literal_eval(x)] if isinstance(x, str) else \
                          ([str(item) for item in x.tolist()] if isinstance(x, np.ndarray) else \
                           ([str(item) for item in x] if isinstance(x, list) else []))
            )
        else:
            df_user_metrics['ListOfOwnedGames'] = [[] for _ in range(len(df_user_metrics))]


        # Create appid <-> game name mappings
        # Ensure 'appid' and 'GameName' exist in df_game_metrics and are not None/NaN
        df_game_metrics_cleaned_for_map = df_game_metrics.dropna(subset=['appid', 'GameName']).copy()
        
        # Check if mappings can be created, otherwise provide empty maps
        if not df_game_metrics_cleaned_for_map.empty:
            appid_to_name_map = pd.Series(df_game_metrics_cleaned_for_map.GameName.values, index=df_game_metrics_cleaned_for_map.appid).to_dict()
            name_to_appid_map = pd.Series(df_game_metrics_cleaned_for_map.appid.values, index=df_game_metrics_cleaned_for_map.GameName).to_dict()
        else:
            appid_to_name_map = {}
            name_to_appid_map = {}
        
        return df_game_metrics, df_country_metrics, df_user_metrics, appid_to_name_map, name_to_appid_map
    except duckdb.Error as e:
        st.error(f"Error connecting to or querying DuckDB: {e}. Make sure the exploitation database is populated and accessible.")
        st.warning("Please ensure the `duckdb_loader_to_explotation.py` script has been run and the `explotation.duckdb` file exists in the correct path (data/exploitation_zone/).")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}, {} # Return empty DFs and maps on error
    except Exception as e:
        st.error(f"An unexpected error occurred during data loading: {e}")
        st.exception(e)
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}, {} # Return empty DFs and maps on error
    finally:
        if conn:
            conn.close()


# --- Streamlit App Layout ---
st.set_page_config(layout="wide", page_title="Project Dashboard")

st.title("Project Service Dashboard")

# Create tabs for navigation
tab1, tab2, tab3 = st.tabs(["Quick Links", "Gaming Data Analytics", "Game Recommender"])

# Load data once and cache it (refreshes every 60 seconds)
df_game_metrics, df_country_metrics, df_user_metrics, appid_to_name_map, name_to_appid_map = load_data()


with tab1:
    st.markdown("### Quick Links")

    services = {
        "MongoDB - Trusted Zone": "http://localhost:8081",
        "MongoDB - Exploitation Zone": "http://localhost:8083",
        "Airflow": "http://localhost:8082",
        "InfluxDB": "http://localhost:8087",
        "Spark": "http://localhost:8080",
    }

    for name, url in services.items():
        st.markdown(f"- [{name}]({url})")

    st.info("Click the links above to open each service in a new tab.")

with tab2:
    st.title("Gaming Data Analytics Dashboard")
    st.markdown("---") # Add a separator

    if not df_game_metrics.empty and not df_country_metrics.empty and not df_user_metrics.empty:
        # --- Display Raw KPI Tables ---
        st.header("Raw KPI Table Content")
        with st.expander("View Game Metrics Table"):
            st.dataframe(df_game_metrics, use_container_width=True)
        with st.expander("View Country Metrics Table"):
            st.dataframe(df_country_metrics, use_container_width=True)
        with st.expander("View User Metrics Table"):
            st.dataframe(df_user_metrics, use_container_width=True)
        st.markdown("---")


        # --- Apply Outlier Cleaning ---
        df_game_metrics_cleaned = df_game_metrics.copy()
        df_game_metrics_cleaned = remove_outliers_iqr(df_game_metrics_cleaned, 'NumberOfOwners')
        df_game_metrics_cleaned = remove_outliers_iqr(df_game_metrics_cleaned, 'TotalPlayedHoursByUsers')
        df_game_metrics_cleaned = remove_outliers_iqr(df_game_metrics_cleaned, 'CurrentPlayerCount')
        df_game_metrics_cleaned = remove_outliers_iqr(df_game_metrics_cleaned, 'Price') # Added price to outlier cleaning

        df_country_metrics_cleaned = df_country_metrics.copy()
        df_country_metrics_cleaned = remove_outliers_iqr(df_country_metrics_cleaned, 'AverageSteamLevel')
        df_country_metrics_cleaned = remove_outliers_iqr(df_country_metrics_cleaned, 'AverageTotalPlaytime')

        df_user_metrics_cleaned = df_user_metrics.copy()
        df_user_metrics_cleaned = remove_outliers_iqr(df_user_metrics_cleaned, 'Steam_Level')
        df_user_metrics_cleaned = remove_outliers_iqr(df_user_metrics_cleaned, 'Owned_Games_Count')
        df_user_metrics_cleaned = remove_outliers_iqr(df_user_metrics_cleaned, 'TotalPlaytime')


        # --- Display Plots ---

        # Game Metrics
        st.header("Game Metrics Analysis")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.subheader("Top Games by Owners")
            fig = generate_plot(
                df_game_metrics_cleaned, 'bar', 'GameName', 'NumberOfOwners',
                title='Top 10 Games by Number of Owners',
                xlabel='Game Name', ylabel='Number of Owners',
                sort_by='NumberOfOwners', n_top=10
            )
            if fig:
                st.pyplot(fig)
                st.caption("Top 10 games with the most owners based on user data.")

        with col2:
            st.subheader("Playtime vs. Price for Games")
            fig = generate_plot(
                df_game_metrics_cleaned, 'scatter', 'Price', 'TotalPlayedHoursByUsers',
                title='Total Played Hours vs. Price for Games',
                xlabel='Price', ylabel='Total Played Hours by Users'
            )
            if fig:
                st.pyplot(fig)
                st.caption("Relationship between game price and total played hours by users.")

        with col3:
            st.subheader("Top Games by Current Player Count")
            fig = generate_plot(
                df_game_metrics_cleaned.sort_values(by='CurrentPlayerCount', ascending=False).head(10), 
                'bar', 'GameName', 'CurrentPlayerCount',
                title='Top 10 Games by Current Player Count',
                xlabel='Game Name', ylabel='Current Player Count'
            )
            if fig:
                st.pyplot(fig)
                st.caption("Top 10 games currently being played the most.")
        st.markdown("---") # Add a separator

        # Country Metrics
        st.header("Country Metrics Analysis")
        col4, col5, col6 = st.columns(3)

        # Filter countries to include only those with more than 5 users for these plots (changed from 10 to 5 for testing)
        df_country_metrics_filtered = df_country_metrics_cleaned[df_country_metrics_cleaned['UserCount'] > 5].copy()

        if df_country_metrics_filtered.empty:
            st.info("No countries found with more than 5 users for plotting. Adjusting filter or data might be needed.")
        else:
            with col4:
                st.subheader("Top Countries by User Count")
                fig = generate_plot(
                    df_country_metrics_filtered.sort_values(by='UserCount', ascending=False).head(10),
                    'bar', 'Country', 'UserCount',
                    title='Top 10 Countries by User Count',
                    xlabel='Country', ylabel='Number of Users'
                )
                if fig:
                    st.pyplot(fig)
                    st.caption("Distribution of user counts across different countries (min 5 users).")
            
            with col5:
                st.subheader("Top Countries by Average Steam Level")
                fig = generate_plot(
                    df_country_metrics_filtered.sort_values(by='AverageSteamLevel', ascending=False).head(10),
                    'bar', 'Country', 'AverageSteamLevel',
                    title='Top 10 Countries by Average Steam Level',
                    xlabel='Country', ylabel='Average Steam Level'
                )
                if fig:
                    st.pyplot(fig)
                    st.caption("Average Steam level of users per country (min 5 users).")

            with col6:
                st.subheader("Top Countries by Average Total Playtime")
                fig = generate_plot(
                    df_country_metrics_filtered.sort_values(by='AverageTotalPlaytime', ascending=False).head(10),
                    'bar', 'Country', 'AverageTotalPlaytime',
                    title='Top 10 Countries by Average Total Playtime (Hours)',
                    xlabel='Country', ylabel='Average Total Playtime (Hours)'
                )
                if fig:
                    st.pyplot(fig)
                    st.caption("Average total playtime (hours) of users per country (min 5 users).")
        st.markdown("---") # Add a separator

        # User Metrics
        st.header("User Metrics Analysis")
        col7, col8, col9 = st.columns(3)
        with col7:
            st.subheader("Distribution of Steam Levels")
            fig = generate_plot(
                df_user_metrics_cleaned, 'hist', 'Steam_Level',
                title='Distribution of Steam Levels',
                xlabel='Steam Level', ylabel='Number of Users'
            )
            if fig:
                st.pyplot(fig)
                st.caption("Histogram showing the distribution of Steam levels among users.")

        with col8:
            st.subheader("Distribution of Owned Games Count")
            fig = generate_plot(
                df_user_metrics_cleaned, 'hist', 'Owned_Games_Count',
                title='Distribution of Owned Games Count',
                xlabel='Owned Games Count', ylabel='Number of Users'
            )
            if fig:
                st.pyplot(fig)
                st.caption("Histogram showing the distribution of the number of owned games per user.")

        with col9:
            st.subheader("Distribution of Total Playtime")
            fig = generate_plot(
                df_user_metrics_cleaned, 'hist', 'TotalPlaytime',
                title='Distribution of Total Playtime (Hours)',
                xlabel='Total Playtime (Hours)', ylabel='Number of Users'
            )
            if fig:
                st.pyplot(fig)
                st.caption("Histogram showing the distribution of total playtime (hours) per user.")
        
        st.markdown("---") # Add a separator
        st.header("Playtime vs. Steam Level")
        col10, col11 = st.columns([2,1]) # Use columns for layout
        with col10:
            fig = generate_plot(
                df_user_metrics_cleaned, 'scatter', 'Steam_Level', 'TotalPlaytime',
                title='Total Playtime vs. Steam Level',
                xlabel='Steam Level', ylabel='Total Playtime (Hours)'
            )
            if fig:
                st.pyplot(fig)
                st.caption("Scatter plot showing the relationship between a user's Steam level and their total playtime.")

    else:
        st.warning("Data could not be loaded for Gaming Data Analytics. Please check the DuckDB path and ensure the database is populated.")


with tab3:
    st.title("Game Recommender")
    st.markdown("---")

    # Check if mappings are available
    if not df_game_metrics.empty and not df_user_metrics.empty and appid_to_name_map and name_to_appid_map:
        st.markdown("Select games you like, and we'll suggest similar ones based on what other users who enjoy these games also own!")

        # Use GameName for display in multiselect (removed max_selections)
        game_names_list = sorted(df_game_metrics['GameName'].dropna().unique().tolist())
        selected_game_names = st.multiselect(
            "Choose games:", 
            game_names_list
        )

        if selected_game_names:
            st.subheader(f"Recommendations for: {', '.join(selected_game_names)}")

            # Convert selected GameNames to appids for internal logic
            selected_game_appids = [name_to_appid_map[name] for name in selected_game_names if name in name_to_appid_map]
            
            found_collaborative_recs = False
            collaborative_recommendations_df = pd.DataFrame() # Initialize empty DataFrame

            if selected_game_appids: # Only proceed if we have valid appids for selected games
                # Try to find users based on combinations of selected game appids
                # Iterate from matching all selected games down to just one
                for num_appids_to_match in range(len(selected_game_appids), 0, -1):
                    if found_collaborative_recs:
                        break # Stop if recommendations were already found
                    
                    for combo_appids in itertools.combinations(selected_game_appids, num_appids_to_match):
                        combo_appids_list = list(combo_appids) # Convert tuple to list for easier checking
                        
                        relevant_users_df = df_user_metrics[
                            df_user_metrics['ListOfOwnedGames'].apply(
                                # Ensure the comparison is robust even if ListOfOwnedGames has empty lists or non-list values
                                lambda owned_appids: all(app_id in owned_appids for app_id in combo_appids_list) if isinstance(owned_appids, list) and owned_appids else False
                            )
                        ]
                        
                        if not relevant_users_df.empty:
                            combo_names = [appid_to_name_map.get(app_id, app_id) for app_id in combo_appids_list]
                            st.info(f"Found users who own: {', '.join(combo_names)}. Generating collaborative recommendations based on these games.")
                            
                            recommended_appids_counts = defaultdict(int)
                            for _, user_row in relevant_users_df.iterrows():
                                # Ensure user_row['ListOfOwnedGames'] is not empty before iterating
                                if isinstance(user_row['ListOfOwnedGames'], list) and user_row['ListOfOwnedGames']:
                                    for app_id in user_row['ListOfOwnedGames']:
                                        if app_id not in selected_game_appids: # Exclude the appids user already likes (from original selection)
                                            recommended_appids_counts[app_id] += 1
                            
                            if recommended_appids_counts:
                                # Convert counts of appids to a DataFrame and sort
                                collaborative_recommendations_df = pd.DataFrame(
                                    recommended_appids_counts.items(), columns=['appid', 'Co_Occurrence_Count']
                                )
                                collaborative_recommendations_df = collaborative_recommendations_df.sort_values(by='Co_Occurrence_Count', ascending=False)

                                # Join with df_game_metrics to get more details and GameName
                                # Removed 'genres', 'developers', 'publishers' from merge
                                final_collaborative_recs = collaborative_recommendations_df.merge(
                                    df_game_metrics[['appid', 'GameName', 'Price']], 
                                    on='appid',
                                    how='left'
                                )
                                
                                # Filter out rows where GameName is NaN (games not found in df_game_metrics)
                                final_collaborative_recs = final_collaborative_recs.dropna(subset=['GameName']).copy()

                                # Format Price for display
                                final_collaborative_recs['Price'] = final_collaborative_recs['Price'].apply(lambda x: f"${x:.2f}" if pd.notna(x) else 'N/A')
                                
                                # Define display columns for simple output
                                display_cols_simple = ['GameName', 'Price']
                                st.write("Here are some collaborative recommendations (based on what users who like your selected games also own):")
                                st.dataframe(final_collaborative_recs[display_cols_simple].head(10), use_container_width=True)
                                found_collaborative_recs = True
                                break # Break from inner combo loop
                
            if not found_collaborative_recs: # If no collaborative recommendations were found at all
                st.info("Could not find sufficient user history for collaborative recommendations. Falling back to content-based recommendations...")
                
                # --- Fallback to Content-Based (Multi-Game Input) ---
                # Removed combined_genres, combined_developers, combined_publishers as they are empty
                # Adjusted similarity score to rely only on NumberOfOwners
                
                content_based_recommendations = []
                for index, row in df_game_metrics.iterrows():
                    # Check against selected GameNames
                    if row['GameName'] in selected_game_names:
                        continue # Don't recommend the selected games

                    # Since genres, developers, publishers are empty, similarity score is simplified
                    score = (row['NumberOfOwners'] / df_game_metrics['NumberOfOwners'].max() if 'NumberOfOwners' in row else 0)

                    # Only add if score is positive, implying some relevance via owners (if any)
                    if score > 0:
                        content_based_recommendations.append({
                            'GameName': row['GameName'],
                            'Price': row['Price'], # Keep as raw number for now
                            'Similarity_Score': score
                        })
                
                if content_based_recommendations:
                    content_based_df = pd.DataFrame(content_based_recommendations)
                    content_based_df = content_based_df.sort_values(by='Similarity_Score', ascending=False).drop(columns=['Similarity_Score'])
                    
                    # Filter out rows where GameName is NaN
                    content_based_df = content_based_df.dropna(subset=['GameName']).copy()

                    # Format Price for display
                    content_based_df['Price'] = content_based_df['Price'].apply(lambda x: f"${x:.2f}" if pd.notna(x) else 'N/A')

                    # Define display columns for simple output
                    display_cols_simple = ['GameName', 'Price']

                    st.write("Here are some content-based recommendations (based on combined attributes of your selected games):")
                    st.dataframe(content_based_df[display_cols_simple].head(10), use_container_width=True)
                else:
                    st.info("No content-based recommendations found either based on your selection. Please try selecting different games.")


        else:
            st.info("Please select at least one game from the dropdown to get recommendations.")
    else:
        st.warning("Game metrics, User metrics data, or appid-name mappings could not be loaded for the recommender. Please check the DuckDB path and ensure the database is populated.")
