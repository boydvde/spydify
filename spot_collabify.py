import sqlite3, random
import networkx as nx # pip install networkx
import pandas as pd # pip install pandas
import community as community_louvain  # pip install python-louvain
import plotly.graph_objects as go # pip install plotly

# Load data from database into a pandas DataFrame
def load(conn):
    data_frame = pd.read_sql("""
SELECT 
    a1.name AS artist_1, 
    a2.name AS artist_2, 
    COUNT(DISTINCT ta1.track_id) AS collaboration_count,
    COALESCE(g1.name, 'unknown') AS genre_1,
    COALESCE(g2.name, 'unknown') AS genre_2
FROM TrackArtist ta1
JOIN TrackArtist ta2 ON ta1.track_id = ta2.track_id AND ta1.artist_id < ta2.artist_id
JOIN Artist a1 ON ta1.artist_id = a1.id
JOIN Artist a2 ON ta2.artist_id = a2.id
LEFT JOIN ArtistGenre ag1 ON a1.id = ag1.artist_id
LEFT JOIN Genre g1 ON ag1.genre_id = g1.id
LEFT JOIN ArtistGenre ag2 ON a2.id = ag2.artist_id
LEFT JOIN Genre g2 ON ag2.genre_id = g2.id
WHERE 
    a1.popularity > 50 
    AND a2.popularity > 50
    AND ta1.track_id IN (
        SELECT id FROM Track 
        WHERE popularity >= 10
    )
GROUP BY a1.name, a2.name, genre_1, genre_2
ORDER BY collaboration_count DESC;
    """, conn)
    return data_frame

def create_graph(data_frame: pd.DataFrame, min_collaborations: int = 3) -> nx.Graph:
    # Create NetworkX graph
    G = nx.Graph()
    
    # Add edges to the graph
    for _, row in data_frame.iterrows():
        artist_1, artist_2 = row["artist_1"], row["artist_2"]
        weight = row["collaboration_count"]
        G.add_edge(artist_1, artist_2, weight=weight)

    # Filter out low-weight edges
    filtered_edges = [(u, v) for u, v, d in G.edges(data=True) if d["weight"] > min_collaborations]
    G_filtered = G.edge_subgraph(filtered_edges).copy()

    return G_filtered

if __name__ == "__main__":
    # Load data from database into a pandas DataFrame
    with sqlite3.connect("db/spotify.sqlite") as conn:
        data_frame = load(conn)

    # Create NetworkX graph
    min_collaborations = 3  # Set this value as needed
    G_filtered = create_graph(data_frame, min_collaborations)

    # Calculate degree centrality
    degree_centrality = nx.degree_centrality(G_filtered)

    # Calculate betweenness centrality
    betweenness_centrality = nx.betweenness_centrality(G_filtered)

    # Calculate Louvain community detection
    partition = community_louvain.best_partition(G_filtered)
    print(f"Detected {len(set(partition.values()))} artist communities. (Louvain community detection)")

    # Compute positions for NetworkX graph
    pos = nx.spring_layout(G_filtered, seed=42, k=0.1, iterations=50, weight="weight")

    # Extract node positions
    node_x = [pos[node][0] for node in G_filtered.nodes()]
    node_y = [pos[node][1] for node in G_filtered.nodes()]

    # Create a dictionary mapping artists to genres
    artist_genre_map = {row["artist_1"]: row["genre_1"] for _, row in data_frame.iterrows()}
    artist_genre_map.update({row["artist_2"]: row["genre_2"] for _, row in data_frame.iterrows()})

    # Create figure
    fig = go.Figure()

    # Add edges (collaborations)
    edge_x, edge_y = [], []
    for edge in G_filtered.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])

    fig.add_trace(go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=0.5, color='#888'),
        hoverinfo='none',
        mode='lines'
    ))

    # Add nodes (artists, colored by community)
    community_colors = {community: f"#{random.randint(0, 0xFFFFFF):06x}" for community in set(partition.values())}
    node_colors = [community_colors[partition[node]] for node in G_filtered.nodes()]
    fig.add_trace(go.Scatter(
        x=node_x, y=node_y, mode="markers",
        marker=dict(color=node_colors, size=10),
        text=[f"{node} ({artist_genre_map.get(node, 'unknown')})" for node in G_filtered.nodes()],
        hoverinfo="text"
    ))

    fig.update_layout(
        showlegend=False,
        hovermode="closest", 
        title="🎵 Artist Collaboration Network"
    )
    fig.show()