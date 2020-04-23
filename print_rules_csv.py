import csv

from neo4j import GraphDatabase


def main():
    uri = "bolt://localhost:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "test"))

    with open('rules.csv', 'w', newline='') as csvfile:
        fieldnames = ['sensor_type', 'sensor_band', 'observable_property', 'confFG', 'confGF', 'support']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        with driver.session() as session:
            results = session.run("MATCH (st:SensorType)-[r]->(o:ObservableProperty) RETURN st, r, o")
            for result in results:
                sensor_type = result["st"]
                observes = result["r"]
                obs_prop = result["o"]
                writer.writerow({
                    'sensor_type': sensor_type['type'],
                    'sensor_band': sensor_type['waveband'],
                    'observable_property': obs_prop['name'],
                    'confFG': observes['confTypeImpliesObservation'],
                    'confGF': observes['confObservationImpliesType'],
                    'support': observes['support']
                })


if __name__ == "__main__":
    main()
