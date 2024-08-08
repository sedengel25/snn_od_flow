from qgis.core import QgsProject, QgsVectorLayer
import pandas as pd

def get_group_name(layer_id):
    """Findet den Gruppennamen für einen Layer anhand der Layer-ID."""
    layer_tree = QgsProject.instance().layerTreeRoot()
    layer = layer_tree.findLayer(layer_id)
    if layer:
        parent = layer.parent()
        if parent:
            return parent.name()
    return None

def extract_cluster_ids_with_groups():
    # Alle Layer im Projekt durchsuchen
    layers = QgsProject.instance().mapLayers().values()
    
    # Liste für eindeutige cluster_pred Werte und Gruppennamen
    cluster_data = []

    # Alle Layer durchgehen und cluster_pred Werte sammeln
    for layer in layers:
        if isinstance(layer, QgsVectorLayer):  # Nur Vektor-Layer verarbeiten
            if 'cluster_pred' in [field.name() for field in layer.fields()]:
                # Alle einzigartigen Werte für cluster_pred in diesem Layer sammeln
                values = layer.uniqueValues(layer.fields().indexOf('cluster_pred'))
                group_name = get_group_name(layer.id())
                for value in values:
                    cluster_data.append((value, group_name))
    
    # Erstellen eines DataFrame aus den gesammelten Daten
    df = pd.DataFrame(cluster_data, columns=['cluster_pred', 'group_name'])

    # Entfernen von Duplikaten
    df = df.drop_duplicates().sort_values(by='cluster_pred').reset_index(drop=True)
    
    return df

# Funktion ausführen und Ergebnis in ein DataFrame speichern
df_clusters = extract_cluster_ids_with_groups()

