import networkx as nx
import matplotlib.pyplot as plt
import xml.etree.ElementTree as ET
import warnings

# Suppress DeprecationWarnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

def build_dag(root):
    """
    Build a Directed Acyclic Graph (DAG) from the XML root element.
    Parameters:
        root (Element): The root element of the XML tree.
    Returns:
        dict: A dictionary representing the DAG with child nodes and their parents.
    """
    nodes = {}

    for child in root.findall("{http://pegasus.isi.edu/schema/DAX}child"):
        child_ref = child.get("ref")
        nodes[child_ref] = {"parents": []}
        for parent in child.findall("{http://pegasus.isi.edu/schema/DAX}parent"):
            parent_ref = parent.get("ref")
            nodes[child_ref]["parents"].append(parent_ref)
    
    return nodes

def extract_runtime(root):
    """
    Extract runtime information from XML root element.
    Parameters:
        root (Element): The root element of the XML tree.
    Returns:
        dict: A dictionary mapping job IDs to their runtime values.
    """
    nodes = {}

    for job in root.findall("{http://pegasus.isi.edu/schema/DAX}job"):
        nodes[job.get("id")] = float(job.get("runtime"))

    return nodes

def main():
    tree = ET.parse("data\Montage_25.xml")
    root = tree.getroot()
    
    dag = build_dag(root)
    runtime = extract_runtime(root)
    G = nx.DiGraph()

    for child_ref, node_data in dag.items():
        label = f"{child_ref}"
        G.add_node(child_ref, label=label)
        for parent_ref in node_data['parents']:
            G.add_edge(parent_ref, child_ref)

    # # Find one topological order
    plt.figure(figsize=(10, 8))

    # Use "dot" layout algorithm from Graphviz
    pos = nx.nx_pydot.graphviz_layout(G, prog="dot")  

    nx.draw_networkx(G, pos, node_size=250, node_color='skyblue', with_labels=True,
                     arrows=True, font_size=6, font_color='black', font_weight='bold')

    plt.axis('off')  # Turn off axis
    plt.show()
    

if __name__ == "__main__":
    main()
