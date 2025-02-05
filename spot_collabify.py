import sqlite3 
import networkx as nx # pip install networkx
import pandas as pd # pip install pandas
import community as community_louvain  # pip install python-louvain
import plotly.graph_objects as go # pip install plotly

# Load data from database into a pandas DataFrame
with sqlite3.connect("db/spotify.sqlite") as conn:
    data_frame = pd.read_sql(f"""
    SELECT 
        a1.name AS artist_1, 
        a2.name AS artist_2, 
        COUNT(DISTINCT ta1.track_id) AS collaboration_count
    FROM 
        TrackArtist ta1
    JOIN 
        TrackArtist ta2 ON ta1.track_id = ta2.track_id
    JOIN 
        Track t ON ta1.track_id = t.id
    JOIN 
        Artist a1 ON ta1.artist_id = a1.id
    JOIN 
        Artist a2 ON ta2.artist_id = a2.id
    WHERE 
        a1.name < a2.name  -- Avoid duplicate artist pairs (A,B) and (B,A)
        AND a1.popularity > 50  -- Filter out unpopular artists
        AND a2.popularity > 50
        AND t.popularity >= 10  -- Exclude low-popularity tracks
        AND LOWER(t.name) NOT LIKE '%remix%'
        AND LOWER(t.name) NOT LIKE '%edit%'
        AND LOWER(t.name) NOT LIKE '%version%'
        AND LOWER(t.name) NOT LIKE '%radio%'
        AND LOWER(t.name) NOT LIKE '%extended%'
    GROUP BY 
        a1.name, a2.name
    ORDER BY 
        collaboration_count DESC;
    """, conn)

# Create NetworkX graph
G = nx.Graph()

# Add edges with weights (collaboration count)
for _, row in data_frame.iterrows():
    G.add_edge(row["artist_1"], row["artist_2"], weight=row["collaboration_count"])

# Filter out low-weight edges
G_filtered = nx.Graph((u, v, d) for u, v, d in G.edges(data=True) if d["weight"] > 3)

# Calculate degree centrality
degree_centrality = nx.degree_centrality(G_filtered)
top_degree = sorted(degree_centrality.items(), key=lambda x: x[1], reverse=True)[:10] # Sort by centrality score and get top 10
print("Top 10 Most Connected Artists (Degree Centrality):")
for artist, score in top_degree:
    print(artist, score)

# Calculate betweenness centrality
betweenness_centrality = nx.betweenness_centrality(G_filtered)
top_betweenness = sorted(betweenness_centrality.items(), key=lambda x: x[1], reverse=True)[:10]
print("\nTop 10 Most Influential Artists (Betweenness Centrality):")
for artist, score in top_betweenness:
    print(artist, score)

# Calculate Louvain community detection
partition = community_louvain.best_partition(G_filtered, weight="weight")
nx.set_node_attributes(G_filtered, partition, "community")
num_communities = len(set(partition.values()))
print(f"Detected {num_communities} artist communities. (Louvain community detection)")

# Get positions using a force-directed layout
pos = nx.spring_layout(G_filtered, weight="weight")

# Extract node positions
node_x = [pos[node][0] for node in G_filtered.nodes()]
node_y = [pos[node][1] for node in G_filtered.nodes()]
node_text = [str(node) for node in G_filtered.nodes()]
node_color = [partition[node] for node in G_filtered.nodes()]  # Color by community

# Create edges
edge_x = []
edge_y = []
for edge in G_filtered.edges(data=True):
    x0, y0 = pos[edge[0]]
    x1, y1 = pos[edge[1]]
    edge_x.extend([x0, x1, None])
    edge_y.extend([y0, y1, None])

# Create figure
fig = go.Figure()

# Add edges
fig.add_trace(go.Scatter(x=edge_x, y=edge_y, mode="lines",
                         line=dict(width=0.5, color="#888"), hoverinfo="none"))

# Add nodes
fig.add_trace(go.Scatter(
    x=node_x, y=node_y, mode="markers",
    marker=dict(size=10, color=node_color, colorscale="Viridis"),
    text=node_text, hoverinfo="text"
))

# Layout settings
fig.update_layout(showlegend=False, hovermode="closest", title="🎵 Artist Collaboration Communities")

fig.show()
