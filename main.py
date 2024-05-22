from dataclasses import dataclass, asdict
from basicauth import decode
from neo4j import GraphDatabase, basic_auth
import functions_framework
import os
import json

HOST = os.environ.get("NEO4J_URI")
PASSWORD = os.environ.get("NEO4J_PASSWORD")
USER = os.environ.get("NEO4J_USER", "neo4j")
DATABASE = os.environ.get("NEO4J_DATABASE", "neo4j")


@dataclass(frozen=True)
class RecommendationOptions:
    email: str
    max_recommendations: int = 9


@dataclass(frozen=True)
class User:
    email: str
    firstName: str
    directMatches: int
    indirectMatches: int


def query_db(query, params):
    try:
        with GraphDatabase.driver(
            HOST, auth=basic_auth(USER, PASSWORD), database=DATABASE
        ) as driver:
            records, _, _ = driver.execute_query(query, params)
            return records
    except Exception as e:
        print(f"Upload query error: {e}")
        return None


def weighted_multi_hop_recommendations(request: RecommendationOptions):
    query = """
    MATCH (u:User)-[:INTERESTED_IN|LIKES]->(t:Tech)<-[:KNOWS]-(u2:User)
    WHERE u.email = $email
    OPTIONAL MATCH (t)-[:CHILD_OF|USES|IS]->(t2:Tech)<-[:KNOWS]-(u2)
    WITH u2, 
        count(DISTINCT t) AS matching_tech, 
        count(DISTINCT t2) AS related_tech
    ORDER BY (2 * matching_tech) + related_tech DESC
    RETURN u2.email as email,
        u2.firstName as firstName,
        matching_tech AS directMatches, 
        related_tech AS indirectMatches
    LIMIT $max_recommendations;
    """
    params = {
        "email": request.email,
        "max_recommendations": request.max_recommendations,
    }
    records = query_db(query, params)
    return records


def simple_recommendations(request: RecommendationOptions):
    query = """
    MATCH (u)-[r:INTERESTED_IN|LIKES]->(t:Tech)<-[r2:KNOWS]-(u2:User)
    WHERE u.email = $email 
    WITH u, count(t) as matching_tech, u2
    ORDER BY matching_tech DESC
    RETURN DISTINCT u2
    """

    params = {"email": request.email}
    records = query_db(query, params)
    return records


def recommendations(request: RecommendationOptions):
    recommended_users = weighted_multi_hop_recommendations(request)
    print(f"recommended_users: {recommended_users}")
    result = []
    for u in recommended_users:
        try:
            user = User(**u)
            result.append(user)
        except Exception as e:
            print(f"Could not transform data for user {u}. ERROR: {e}")
            continue
    return result


@functions_framework.http
def get_recommendations(request):

    # Optional Basic Auth
    basic_user = os.environ.get("BASIC_AUTH_USER", None)
    basic_password = os.environ.get("BASIC_AUTH_PASSWORD", None)
    if basic_user and basic_password:
        auth_header = request.headers.get("Authorization")
        if auth_header is None:
            return "Missing authorization credentials", 401
        request_username, request_password = decode(auth_header)
        if request_username != basic_user or request_password != basic_password:
            return "Unauthorized", 401

    # Extract Post Data
    try:
        payload = request.get_json(silent=True)
        options = RecommendationOptions(**payload)
    except Exception as e:
        return f"Could not parse payload: {e}", 400

    try:
        r = recommendations(options)
        json_list = [asdict(user) for user in r]
        return json.dumps(json_list), 200
    except Exception as e:
        return f"Processing error: {e}", 500
