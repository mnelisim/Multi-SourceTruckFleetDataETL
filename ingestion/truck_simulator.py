import random
import time
import json
from datetime import datetime
from utils.logger_factory import LoggerFactory 

class DataExtractor:
    def __init__(self):
        # 1. Initialize Logger for this class
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)

    #This script downloads the road network for a specific area (Johannesburg).
    #finds a route between two points, and "drives" the trucks along that exact path.
    def get_truck_data(self, truck_id):
        import networkx as nx
        import osmnx as ox
        # 1. Download road network (5km radius around Joburg Central)
        start_coords = (-26.2041, 28.0473)
        end_coords = (-26.1265, 28.0321)

        self.logger.info("Downloading map data...")
        graph = ox.graph_from_point(start_coords, dist=5000, network_type="drive")

        # 2. Find route
        orig_node = ox.nearest_nodes(graph, start_coords[1], start_coords[0])
        dest_node = ox.nearest_nodes(graph, end_coords[1], end_coords[0])
        route = nx.shortest_path(graph, orig_node, dest_node, weight='length')


        # 4. Extract real coordinates along the route
        route_coords = []
        for node in route:
            node_data = graph.nodes[node]
            route_coords.append((node_data['y'], node_data['x']))
       
        self.logger.info(f"Starting journey for {truck_id} with {len(route_coords)} waypoints...")

        # 5. Stream Data
        for lat, lon in route_coords:
            # 1. Simulate a realistic speed (between 40 and 80 km/h)
            # small random float
            current_speed = random.uniform(55.0, 75.0) 
    
            # 2. Add "Stop" logic (5% chance the truck is idling)
            current_status = "in-transit"
            if random.random() < 0.05:
                current_speed = 0.0
                current_status = "idling"
            yield {
                "timestamp": datetime.now().isoformat(),
                "truck_id": truck_id,
                "latitude": lat,
                "longitude": lon,
                "speed_kmh": round(current_speed, 2),
                "status": current_status
            }

            # controls the "Real-Time" frequency
            time.sleep(1) 

if __name__ == "__main__":
    try:
        extract_data = DataExtractor()
        # testing the generator
        for data in extract_data.get_truck_data():
            print(data)
    except KeyboardInterrupt:
        print("\nSimulation stopped.")