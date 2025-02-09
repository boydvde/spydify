# pip install pandas networkx community scikit-learn pyvis
import sqlite3
import networkx as nx
import pandas as pd
import community.community_louvain as community_louvain
from sklearn.preprocessing import MinMaxScaler
from pyvis.network import Network

# ---- Load the data ----

# Define the filters to apply to the data
artist_popularity = 70 
track_popularity = 50

# Load the data into a DataFrame
with sqlite3.connect("db/spotify.sqlite") as conn:
    cursor = conn.cursor()
    query = """
    SELECT a1.name AS artist_1,
        a2.name AS artist_2,
        COUNT(*) AS collaboration_count
    FROM TrackArtist ta1
    JOIN TrackArtist ta2 ON ta1.track_id = ta2.track_id
    AND ta1.artist_id < ta2.artist_id
    JOIN Artist a1 ON ta1.artist_id = a1.id
    JOIN Artist a2 ON ta2.artist_id = a2.id
    JOIN Track ON ta1.track_id = Track.id
    WHERE a1.name IS NOT NULL
    AND a2.name IS NOT NULL
    AND Track.popularity > :track_popularity
    AND a1.popularity > :artist_popularity
    AND a2.popularity > :artist_popularity
    GROUP BY a1.name, a2.name
    ORDER BY collaboration_count DESC;
    """
    cursor.execute(query, {"artist_popularity": artist_popularity, "track_popularity": track_popularity})
    result = cursor.fetchall()

# Convert the result to a DataFrame
data_frame = pd.DataFrame(result, columns=["artist_1", "artist_2", "collaboration_count"])

# ---- Create a graph from the DataFrame ----

# Define filter for minimum amount of collaborations
min_col = 3

# Create NetworkX graph from the DataFrame, filtering edges based on a minimum number of collaborations
def create_graph(data_frame: pd.DataFrame, min_collaborations: int = 3) -> nx.Graph:
    G = nx.Graph()
    
    # Use itertuples() for better performance
    for row in data_frame.itertuples(index=False):
        G.add_edge(row.artist_1, row.artist_2, weight=row.collaboration_count)

    # Filter low-weight edges
    filtered_edges = [(u, v) for u, v, d in G.edges(data=True) if d["weight"] > min_collaborations]
    return G.edge_subgraph(filtered_edges).copy()

# Create graph with a minimum collaboration threshold
G_filtered = create_graph(data_frame, min_col)
print(f"Nodes: {G_filtered.number_of_nodes()}, Edges: {G_filtered.number_of_edges()}")

# ---- Analyze the graph ----

# Centrality measures
degree_centrality = nx.degree_centrality(G_filtered)
betweenness_centrality = nx.betweenness_centrality(G_filtered)

# Use the Louvain method to detect communities within the graph
partition = community_louvain.best_partition(G_filtered)

# ---- Visualize the graph ----

# Create a Pyvis network visualization
def visualize_graph_pyvis(G: nx.Graph):
    net = Network(notebook=True, width="100%", height="700px", bgcolor="#222222", font_color="white")
    
    for node in G.nodes():
        net.add_node(node, title=node, color="blue")
    
    for edge in G.edges(data=True):
        net.add_edge(edge[0], edge[1], value=edge[2].get('weight', 1))

    net.toggle_physics(True)  # Enable physics for better layout
    
    net.set_options("""
        var options = {
          "nodes": { "shape": "dot", "size": 10 },
          "edges": { "color": "gray", "width": 1 },
          "physics": {
            "barnesHut": { "gravitationalConstant": -30000, "centralGravity": 0.3 }
          }
        }
    """)
    
    net.show("output/artist_network.html")

visualize_graph_pyvis(G_filtered)