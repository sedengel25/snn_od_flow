from qgis.core import QgsProject, QgsVectorLayer

# Alle Layer im Projekt durchsuchen
layers = QgsProject.instance().mapLayers().values()

# Set für eindeutige cluster_pred Werte
unique_cluster_ids = set()

# Alle Layer durchgehen und cluster_pred Werte sammeln
for layer in layers:
    if isinstance(layer, QgsVectorLayer):  # Nur Vektor-Layer verarbeiten
        if 'cluster_pred' in [field.name() for field in layer.fields()]:
            # Alle einzigartigen Werte für cluster_pred in diesem Layer sammeln
            values = layer.uniqueValues(layer.fields().indexOf('cluster_pred'))
            unique_cluster_ids.update(values)

# Alle möglichen cluster_pred Werte von 1 bis 217
all_cluster_ids = set(range(1, 218))

# Fehlende cluster_pred Werte berechnen
missing_cluster_ids = sorted(all_cluster_ids - unique_cluster_ids)

# Ausgabe der vorhandenen und fehlenden cluster_pred Werte
print("Vorhandene cluster_pred Werte:", sorted(unique_cluster_ids))
print("Fehlende cluster_pred Werte:", missing_cluster_ids)
