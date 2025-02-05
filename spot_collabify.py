import sqlite3, random
import networkx as nx # pip install networkx
import pandas as pd # pip install pandas
import community as louv  # pip install python-louvain
import plotly.graph_objects as go # pip install plotly

# Load data from database into a pandas DataFrame
def load():
    with sqlite3.connect("db/spotify.sqlite") as conn:
        data_frame = pd.read_sql(f"""
WITH GenrePopularity AS (
    SELECT g.name AS genre, COUNT(*) AS total_count
    FROM ArtistGenre ag
    JOIN Genre g ON ag.genre_id = g.id
    GROUP BY g.name
),
MostPopularGenres AS (
    SELECT 
        ag.artist_id, 
        g.name AS genre, 
        gp.total_count,
        ROW_NUMBER() OVER (PARTITION BY ag.artist_id ORDER BY gp.total_count DESC) AS rank
    FROM ArtistGenre ag
    JOIN Genre g ON ag.genre_id = g.id
    JOIN GenrePopularity gp ON g.name = gp.genre
)
SELECT 
    a1.name AS artist_1, 
    a2.name AS artist_2, 
    COUNT(DISTINCT ta1.track_id) AS collaboration_count,
    COALESCE(mpg1.genre, 'unknown') AS genre_1,
    COALESCE(mpg2.genre, 'unknown') AS genre_2
FROM TrackArtist ta1
JOIN TrackArtist ta2 ON ta1.track_id = ta2.track_id AND ta1.artist_id < ta2.artist_id
JOIN Artist a1 ON ta1.artist_id = a1.id
JOIN Artist a2 ON ta2.artist_id = a2.id
LEFT JOIN MostPopularGenres mpg1 ON a1.id = mpg1.artist_id AND mpg1.rank = 1
LEFT JOIN MostPopularGenres mpg2 ON a2.id = mpg2.artist_id AND mpg2.rank = 1
WHERE 
    a1.popularity > 50 
    AND a2.popularity > 50
    AND ta1.track_id IN (
        SELECT id FROM Track 
        WHERE popularity >= 10
        AND LOWER(name) NOT LIKE '%remix%'
        AND LOWER(name) NOT LIKE '%edit%'
        AND LOWER(name) NOT LIKE '%version%'
        AND LOWER(name) NOT LIKE '%radio%'
        AND LOWER(name) NOT LIKE '%extended%'
    )
GROUP BY a1.name, a2.name, genre_1, genre_2
ORDER BY collaboration_count DESC;

                                 """, conn)
    return data_frame

if __name__ == "__main__":
    # Load data
    data_frame = load()
    
    # Create NetworkX graph
    G = nx.Graph()
    
    # Add edges to the graph (ensure missing collaborations are included)
    for _, row in data_frame.iterrows():
        artist_1, artist_2 = row["artist_1"], row["artist_2"]
        weight = row["collaboration_count"]

        if G.has_edge(artist_1, artist_2):
            G[artist_1][artist_2]["weight"] += weight  # Accumulate weight if edge exists
        else:
            G.add_edge(artist_1, artist_2, weight=weight)


    # Filter out low-weight edges
    min_collaborations = 3  # Change this dynamically
    G_filtered = nx.Graph((u, v, d) for u, v, d in G.edges(data=True) if d["weight"] > min_collaborations)

    # # Calculate degree centrality (print top 10)
    # degree_centrality = nx.degree_centrality(G_filtered)
    # print("Top 10 Most Connected Artists (Degree Centrality):")
    # for artist, score in sorted(degree_centrality.items(), key=lambda x: x[1], reverse=True)[:10]:
    #     print(artist, score)

    # # Calculate betweenness centrality (print top 10)
    # betweenness_centrality = nx.betweenness_centrality(G_filtered)
    # print("\nTop 10 Most Influential Artists (Betweenness Centrality):")
    # for artist, score in sorted(betweenness_centrality.items(), key=lambda x: x[1], reverse=True)[:10]:
    #     print(artist, score)

    # Calculate Louvain community detection
    partition = louv.best_partition(G_filtered, weight="weight")
    nx.set_node_attributes(G_filtered, partition, "community")
    num_communities = len(set(partition.values()))
    print(f"Detected {num_communities} artist communities. (Louvain community detection)")

    # Compute positions for NetworkX graph
    pos = nx.spring_layout(G_filtered, seed=42, weight="weight")

    # Extract node positions
    node_x = [pos[node][0] for node in G_filtered.nodes()]
    node_y = [pos[node][1] for node in G_filtered.nodes()]

    # Create a dictionary mapping artists to genres
    artist_genre_map = {row["artist_1"]: row["genre_1"] for _, row in data_frame.iterrows()}
    artist_genre_map.update({row["artist_2"]: row["genre_2"] for _, row in data_frame.iterrows()})

    # Define a large set of unique colors
    color_palette = [
        "blue", "red", "green", "purple", "orange", "yellow",
        "black", "teal", "pink", "brown", "cyan", "magenta",
        "lime", "coral", "navy", "gold", "darkgreen", "indigo"
    ]

    # Ensure colors are unique by shuffling
    random.shuffle(color_palette)

    # Assign colors to genres dynamically, ensuring no duplicates
    genre_to_color = {'unknown': 'gray'}
    unique_genres = list(set(artist_genre_map.values()))  # Get unique genres
    for i, genre in enumerate(unique_genres):
        if genre not in genre_to_color:
            genre_to_color[genre] = color_palette[i % len(color_palette)]  # Rotate colors if needed

    # Assign colors based on genre
    node_colors = [
        genre_to_color.get(artist_genre_map.get(node, "unknown"), "gray")  
        for node in G_filtered.nodes()
    ]

    # Create figure
    fig = go.Figure()

    # Add edges (collaborations)
    edge_x, edge_y = [], []
    for edge in G_filtered.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])

    # Add nodes (artists, colored by genre)
    fig.add_trace(go.Scatter(
        x=node_x, y=node_y, mode="markers",
        marker=dict(size=10, color=node_colors),  # Use genre-based colors
        text=[f"{node} ({artist_genre_map.get(node, 'unknown')})" for node in G_filtered.nodes()],
        hoverinfo="text"
    ))

    fig.update_layout(showlegend=False, hovermode="closest", title="🎵 Artist Collaboration Network")
    fig.show()
