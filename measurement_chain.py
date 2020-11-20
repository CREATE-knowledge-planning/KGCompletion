from neo4j import GraphDatabase


def main():
    uri = "bolt://localhost:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "test"))

    with driver.session() as session:
        result = session.run('MATCH (st:SensorType)'
        'WHERE st.waveband = "TIR" '
        'CREATE (ob:ObservableProperty {name: {rule_name}, type: {type}, waveband: {band}})'
        'CREATE (st)-[:OBSERVES {confTypeImpliesObservation: {conf1}, confObservationImpliesType: {conf2}, support:{support}}]->(o)',)