# Vectorfriends Neo4j Recommendations

Cloud function for retrieveing recommendations from the `neo4j` branch version of [Vectorfriends](https://github.com/itsajchan/vectorfriends)

## Running Locally

```
NEO4J_URI=<uri> \
NEO4J_USER=<username> \
NEO4J_PASSWORD=<password> \
BASIC_AUTH_USER=user \
BASIC_AUTH_PASSWORD=password \
poetry run functions-framework --target=get_recommendations
```

Default port is 8080
To adjust add `--port=<port_number>` to the above
