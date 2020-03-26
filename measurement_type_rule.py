from neo4j import GraphDatabase


def main():
    uri = "bolt://localhost:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "test"))

    with driver.session() as session:
        # Remove all nodes with SensorType as label
        results = session.run('MATCH (st:SensorType) DETACH DELETE st')
        print(results.summary().counters)
        # Get list of sensor types
        results = session.run('MATCH (s:Sensor) RETURN DISTINCT s.types, count(*)')
        sensor_types_dict = {}
        for sensor_types in results:
            types_list = sensor_types["s.types"]
            types_count = sensor_types[1]
            for sensor_type in types_list:
                if sensor_type not in sensor_types_dict:
                    sensor_types_dict[sensor_type] = types_count
                else:
                    sensor_types_dict[sensor_type] += types_count
        # Get list of sensor technologies
        results = session.run('MATCH (s:Sensor) RETURN DISTINCT s.technology, count(*)')
        sensor_technologies_dict = {}
        for sensor_technology_record in results:
            technology = sensor_technology_record["s.technology"]
            tech_count = sensor_technology_record[1]
            sensor_technologies_dict[technology] = tech_count
        # Get list of observable properties
        results = session.run('MATCH (s:Sensor)-[:OBSERVES]->(o:ObservableProperty) RETURN DISTINCT o.name, count(*)')
        observable_names_dict = {}
        for result in results:
            name = result["o.name"]
            name_count = result[1]
            observable_names_dict[name] = name_count

        # For each instrument type...
        for sensor_type, sensor_type_count in sensor_types_dict.items():
            # Find all measurements the sensors with that type can do
            results = session.run(
                'MATCH (s:Sensor)-[:OBSERVES]->(o:ObservableProperty) '
                'WHERE {sensor_type} in s.types '
                'RETURN o.name',
                sensor_type=sensor_type)
            observable_subset = set()
            for result in results:
                observable_subset.add(result['o.name'])
            # For each pair (type, observable) count the number of sensors
            for observable in observable_subset:
                results = session.run(
                    'MATCH (s:Sensor)-[:OBSERVES]->(o:ObservableProperty) '
                    'WHERE {sensor_type} in s.types AND o.name = {name} '
                    'RETURN count(s)',
                    sensor_type=sensor_type,
                    name=observable
                )
                intersection_count = results.single().value()
                # Add a new relation with the confidences
                result = session.run('MATCH (o:ObservableProperty)'
                                     'WHERE o.name = {name}'
                                     'CREATE (st:SensorType {type: {type}})'
                                     'CREATE (st)-[:OBSERVES {confTypeImpliesObservation: {conf1}, confObservationImpliesType: {conf2}}]->(o)',
                                     type=sensor_type,
                                     name=observable,
                                     conf1=float(intersection_count)/sensor_type_count,
                                     conf2=float(intersection_count)/observable_names_dict[observable])
                print(result.summary().counters)


if __name__ == "__main__":
    main()
