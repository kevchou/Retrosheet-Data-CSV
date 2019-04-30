import pandas as pd

# Get table headers
game_headers_df = pd.read_csv("table_info/cwgame_standard.csv")
game_headers_df2 = pd.read_csv("table_info/cwgame_extended.csv")
game_headers = game_headers_df['Header'].tolist() + game_headers_df2['Header'].tolist()

event_headers_df = pd.read_csv("table_info/cwevent_standard.csv")
event_headers_df2 = pd.read_csv("table_info/cwevent_extended.csv")
event_headers = event_headers_df['Header'].tolist() + event_headers_df2['Header'].tolist()

sub_headers_df = pd.read_csv("table_info/cwsub.csv")
sub_headers = sub_headers_df['Header'].tolist()

# import table and rename columns
games_df = spark.read.load("parsed/games*.csv", format="csv", header="false")
games_df = games_df.selectExpr([f"{old} AS {new}" for old, new in zip(games_df.columns, game_headers)])

event_df = spark.read.load("parsed/all*.csv", format="csv", header="false")
event_df = event_df.selectExpr([f"{old} AS {new}" for old, new in zip(event_df.columns, event_headers)])

sub_df = spark.read.load("parsed/sub*.csv", format="csv", header="false")
sub_df = sub_df.selectExpr([f"{old} AS {new}" for old, new in zip(sub_df.columns, sub_headers)])

# Save as parquet
games_df.write.parquet("games.parquet")
event_df.write.parquet("event.parquet")
sub_df.write.parquet("sub.parquet")