import sqlite3
import networkx as nx
import numpy as np
import pandas as pd
import community.community_louvain as community_louvain
from pyvis.network import Network
from networkx.algorithms.coloring import greedy_color

def load_to_dataframe(file: str, filters: dict[str, int]) -> pd.DataFrame:
    with sqlite3.connect(file) as conn:
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
        params = {
            "artist_popularity": filters['a_pop'],
            "track_popularity": filters['t_pop'],
            "collaboration_count": filters['min_col']
        }
        cursor.execute(query, params)
        result = cursor.fetchall()

    return pd.DataFrame(result, columns=["artist_1", "artist_2", "collaboration_count"])

def create_graph(data_frame: pd.DataFrame) -> nx.Graph:
    G = nx.Graph()
    for row in data_frame.itertuples(index=False):
        G.add_edge(row.artist_1, row.artist_2, weight=row.collaboration_count)
    return G

def analyze_graph(G: nx.Graph) -> dict:
    # 1. Degree Centrality (basic influence measure)
    degree_centrality = nx.degree_centrality(G)
    
    # 2. Betweenness Centrality (measures brokerage potential)
    betweenness_centrality = nx.betweenness_centrality(G)
    
    # 3. Eigenvector Centrality (influence measure)
    eigenvector_centrality = nx.eigenvector_centrality(G, max_iter=1000)
    
    # 4. PageRank (importance based on influence spread)
    pagerank = nx.pagerank(G, alpha=0.85)
    
    # 5. Clustering Coefficient (local density of connections)
    avg_clustering = nx.average_clustering(G)
    
    # 6. Assortativity (do artists collaborate within their community?)
    assortativity = nx.degree_assortativity_coefficient(G)
    
    # 7. Community Detection using Louvain
    partition = community_louvain.best_partition(G)
    
    # 8. Robustness Simulation - Remove top influential nodes
    G_copy = G.copy()
    top_influencers = sorted(eigenvector_centrality, key=eigenvector_centrality.get, reverse=True)[:5]
    G_copy.remove_nodes_from(top_influencers)
    remaining_components = nx.number_connected_components(G_copy)
    
    # 9. Shortest Paths Between Two Random Artists
    all_nodes = list(G.nodes())
    if len(all_nodes) >= 2:
        artist_1, artist_2 = np.random.choice(all_nodes, 2, replace=False)
        try:
            shortest_path = nx.shortest_path(G, source=artist_1, target=artist_2)
        except nx.NetworkXNoPath:
            shortest_path = "No path found"
    else:
        shortest_path = "Not enough nodes"
    
    return {
        'degree_centrality': degree_centrality,
        'betweenness_centrality': betweenness_centrality,
        'eigenvector_centrality': eigenvector_centrality,
        'pagerank': pagerank,
        'avg_clustering': avg_clustering,
        'assortativity': assortativity,
        'partition': partition,
        'remaining_components': remaining_components,
        'shortest_path': shortest_path
    }

def visualize_graph(G: nx.Graph, partition: dict, degree_centrality: dict):
    net = Network(notebook=True, width="100%", height="700px", bgcolor="#222222", font_color="white")
    
    # Create a meta-graph where each node represents a community
    community_graph = nx.Graph()

    # Add community nodes
    for community in set(partition.values()):
        community_graph.add_node(community)

    # Add edges between communities if there's a connection in the original graph
    for node1, node2 in G.edges():
        comm1, comm2 = partition[node1], partition[node2]
        if comm1 != comm2:
            community_graph.add_edge(comm1, comm2)

    # Apply graph coloring to the community graph
    community_colors = greedy_color(community_graph, strategy="DSATUR")

    # Define a color palette for the communities
    color_map = [
        "#FF5733", "#33FF57", "#3357FF", "#FF33A8", "#A833FF", "#FF8C33",
        "#33FFA8", "#A8FF33", "#5733FF", "#FF338C", "#33A8FF", "#8CFF33",
        "#FF33F5", "#33FFF5", "#A85533", "#33A8F5", "#F533FF", "#FFA833",
        "#33FFD5", "#D533FF", "#8CFF33", "#FF5733", "#338CFF", "#FF8C57",
        "#57FF8C", "#8C33FF", "#FF57A8", "#A8FF33", "#FF338C", "#5733FF",
        "#F5A623", "#F523A6", "#23F5A6", "#A623F5", "#F5A623", "#23A6F5",
        "#A6F523", "#F5236A", "#6AF523", "#23F5D5", "#F5D523", "#23D5F5",
        "#A8A623", "#6A23F5", "#23F56A", "#F5A6F5", "#A623A6", "#D5F523"
    ]

    # Map colors directly to community indices to avoid clustering
    unique_communities = sorted(set(partition.values()))
    community_to_color = {community: color_map[i % len(color_map)] for i, community in enumerate(unique_communities)}
    
    for node in G.nodes():
        community_id = partition[node]
        color = community_to_color[community_id]  # Assign color directly
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

if __name__ == "__main__":

    # Filter to apply on the data
    filters = {
        'a_pop': 70,  # Minimum artist popularity threshold
        't_pop': 50,  # Minimum track popularity threshold
        'min_col': 3  # Minimum collaborations threshold
    }

    # Load the data into a DataFrame
    data_frame = load_to_dataframe("data/spotify.db", filters)

    # Create a graph from the DataFrame
    G_filtered = create_graph(data_frame)
    print(f"Nodes: {G_filtered.number_of_nodes()}, Edges: {G_filtered.number_of_edges()}")

    # Run the analysis on the graph
    analysis = analyze_graph(G_filtered)

    # Use PyVis to visualize the graph
    visualize_graph(G_filtered, analysis['partition'], analysis['degree_centrality'])

    # Export the graph to a GraphML file
    nx.set_node_attributes(G_filtered, analysis['partition'], "community")
    nx.write_graphml(G_filtered, "output/graph_export.graphml")
    print("Graph exported to output/graph_export.graphml")