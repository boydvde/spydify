import sqlite3
import networkx as nx
import pandas as pd
import community.community_louvain as community_louvain
from pyvis.network import Network
from networkx.algorithms.coloring import greedy_color

# ---- Set the filters ----

a_pop = 70  # Minimum artist popularity threshold
t_pop = 50  # Minimum track popularity threshold
min_col = 3  # Minimum collaborations threshold

# ---- Load the data ----

# Load data with optimized SQL query
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
    HAVING COUNT(*) > :collaboration_count
    ORDER BY collaboration_count DESC;
    """
    cursor.execute(query, {"artist_popularity": a_pop, "track_popularity": t_pop, "collaboration_count": min_col})
    result = cursor.fetchall()

data_frame = pd.DataFrame(result, columns=["artist_1", "artist_2", "collaboration_count"])

# ---- Create and analyze the graph ----

def create_graph(data_frame: pd.DataFrame) -> nx.Graph:
    G = nx.Graph()
    for row in data_frame.itertuples(index=False):
        G.add_edge(row.artist_1, row.artist_2, weight=row.collaboration_count)
    return G

G_filtered = create_graph(data_frame)
print(f"Nodes: {G_filtered.number_of_nodes()}, Edges: {G_filtered.number_of_edges()}")

# Calculate weighted centralities
degree_centrality = nx.degree_centrality(G_filtered)
betweenness_centrality = nx.betweenness_centrality(G_filtered, weight="weight")

# Community detection using Louvain method
partition = community_louvain.best_partition(G_filtered)

# ---- Ensure adjacent communities have different colors ----

# Create a meta-graph where each node represents a community
community_graph = nx.Graph()

# Add community nodes
for community in set(partition.values()):
    community_graph.add_node(community)

# Add edges between communities if there's a connection in the original graph
for node1, node2 in G_filtered.edges():
    comm1, comm2 = partition[node1], partition[node2]
    if comm1 != comm2:
        community_graph.add_edge(comm1, comm2)

# Apply graph coloring to the community graph
community_colors = greedy_color(community_graph, strategy="largest_first")

# Define a color palette
color_map = [
    "#FF5733", "#33FF57", "#3357FF", "#FF33A8", "#A833FF", "#FF8C33",
    "#33FFA8", "#A8FF33", "#5733FF", "#FF338C", "#33A8FF", "#8CFF33",
    "#FF33F5", "#33FFF5", "#FF5733", "#A833FF", "#FF33A8", "#338CFF"
]

# ---- Visualize the graph ----

def visualize_graph_pyvis(G: nx.Graph, partition, degree_centrality, community_colors):
    net = Network(notebook=True, width="100%", height="700px", bgcolor="#222222", font_color="white")
    
    for node in G.nodes():
        community_id = partition[node]
        color = color_map[community_colors[community_id] % len(color_map)]  # Assign distinct community color
        size = 5 + degree_centrality[node] * 20  # Scale node size based on centrality
        net.add_node(node, title=node, color=color, size=size)
    
    for edge in G.edges(data=True):
        net.add_edge(edge[0], edge[1], value=edge[2].get('weight', 1))

    net.toggle_physics(True)
    net.set_options("""
        var options = {
          "nodes": { "shape": "dot" },
          "edges": { "color": "gray", "width": 1 },
          "physics": {
            "barnesHut": { "gravitationalConstant": -30000, "centralGravity": 0.3 }
          }
        }
    """)
    
    net.show("output/artist_network.html")

visualize_graph_pyvis(G_filtered, partition, degree_centrality, community_colors)
