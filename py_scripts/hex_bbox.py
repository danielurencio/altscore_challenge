import h3

def get_hexagon_bbox(h3_index: str) -> tuple:
    """
    Obtiene las coordenadas del bounding box (min_lat, min_lng, max_lat, max_lng)
    que rodea a un hexágono H3.
    
    Args:
        h3_index: Índice H3 del hexágono
    
    Returns:
        tuple: (min_lat, min_lng, max_lat, max_lng)
    """
    # Obtener los vértices del hexágono
    boundaries = h3.cell_boundary(h3_index)
    
    # Extraer listas separadas de latitudes y longitudes
    lats = [vertex[0] for vertex in boundaries]
    lngs = [vertex[1] for vertex in boundaries]
    
    # Encontrar los valores mínimos y máximos
    min_lat = min(lats)
    max_lat = max(lats)
    min_lng = min(lngs)
    max_lng = max(lngs)
    
    return (min_lat, min_lng, max_lat, max_lng)

# Ejemplo de uso:
h3_index = "8928308280fffff"  # Reemplaza con tu índice H3
bbox = get_hexagon_bbox(h3_index)
print(f"""
Bounding Box:
Sur-Oeste: ({bbox[0]}, {bbox[1]})
Nor-Este: ({bbox[2]}, {bbox[3]})
""")

# Si necesitas todos los puntos del bounding box:
def get_bbox_corners(bbox: tuple) -> list:
    """
    Obtiene las cuatro esquinas del bounding box.
    """
    min_lat, min_lng, max_lat, max_lng = bbox
    return [
        (min_lat, min_lng),  # Sur-Oeste
        (min_lat, max_lng),  # Sur-Este
        (max_lat, max_lng),  # Nor-Este
        (max_lat, min_lng),  # Nor-Oeste
    ]
