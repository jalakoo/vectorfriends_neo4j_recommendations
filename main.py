from pydantic import (
    BaseModel,
    EmailStr,
    Field,
    ValidationError,
    model_validator,
)
from typing import Optional
from basicauth import decode
from neo4j import GraphDatabase, basic_auth
import functions_framework
import os
import json

HOST = os.environ.get("NEO4J_URI")
PASSWORD = os.environ.get("NEO4J_PASSWORD")
USER = os.environ.get("NEO4J_USER", "neo4j")
DATABASE = os.environ.get("NEO4J_DATABASE", "neo4j")


class RecommendationOptions(BaseModel):
    email: EmailStr | None = None
    diffbot_uri: str | None = None
    max_recommendations: int = Field(9, alias="maxRecommendations")
    tenant: str | None = None

    @model_validator(mode="before")
    def check_email_or_diffbot_uri(cls, values):
        if not values.get("email") and not values.get("diffbot_uri"):
            raise ValueError("Either email or diffbot_uri must be provided.")
        return values


class User(BaseModel):
    diffbotUri: Optional[str] = None
    email: Optional[EmailStr] = None
    firstName: str


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


# def weighted_multi_hop_recommendations(request: RecommendationOptions):
#     query = """
#     MATCH (u:User)-[:INTERESTED_IN|LIKES]->(t:Tech)<-[:KNOWS|INTERESTED_IN|LIKES]-(u2:User)
#     WHERE u.email = $email
#     OPTIONAL MATCH (t)-[:CHILD_OF|USES|IS]->(t2:Tech)<-[:KNOWS|INTERESTED_IN|LIKES]-(u2)
#     WITH u2,
#         count(DISTINCT t) AS matching_tech,
#         count(DISTINCT t2) AS related_tech
#     ORDER BY (2 * matching_tech) + related_tech DESC
#     RETURN u2.email as email,
#         u2.firstName as firstName,
#         matching_tech AS directMatches,
#         related_tech AS indirectMatches
#     LIMIT $maxRecommendations;
#     """
#     params = {
#         "email": request.email,
#         "maxRecommendations": request.max_recommendations,
#     }
#     records = query_db(query, params)
#     return records


def simple_email_recommendations(request: RecommendationOptions):

    query = """
    MATCH (u)-[r:INTERESTED_IN|LIKES]->(t:Tech)<-[r2:KNOWS|INTERESTED_IN|LIKES]-(u2:User)
    WITH u, t, u2
    MATCH (u)-[:ATTENDED]->(te:Tenant)<-[:ATTENDED]-(u2)
    WHERE u.email = $email AND te.name = $tenant
    WITH u, count(t) as matching_tech, u2
    ORDER BY matching_tech DESC
    RETURN DISTINCT u2 LIMIT $maxRecommendations;
    """
    params = {
        "email": request.email,
        "maxRecommendations": request.max_recommendations,
        "tenant": request.tenant,
    }

    records = query_db(query, params)
    return records


def simple_diffbot_recommendations(request: RecommendationOptions):
    query = """
    MATCH (u:User)-[:ATTENDED]->(te:Tenant {name: $tenant})
    MATCH (u1:User {diffbotUri: $diffbotUri})
    MATCH path = (u1)-[*1..3]-(n)-[*1..3]-(u2:User)
    WHERE (u2)-[:ATTENDED]->(te)
    AND u1 <> u2
    AND NONE(node IN nodes(path) WHERE node:Tenant)
    AND ANY(label IN labels(n) WHERE label IN ['Tech', 'Role', 'Employer'])
    WITH u2, count(n) AS matching_nodes
    ORDER BY matching_nodes DESC
    RETURN DISTINCT u2 LIMIT $maxRecommendations;
    """
    params = {
        "diffbotUri": request.diffbot_uri,
        "maxRecommendations": request.max_recommendations,
        "tenant": request.tenant,
    }

    records = query_db(query, params)
    return records


def recommendations(request: RecommendationOptions):
    if request.email:
        recommended_users = simple_email_recommendations(request)
    elif request.diffbot_uri:
        recommended_users = simple_diffbot_recommendations(request)
    else:
        raise ValueError("Either email or diffbot_uri must be provided.")

    print(f"recommended_users: {recommended_users}")
    result = []
    for u in recommended_users:
        u_data = u.data()["u2"]
        try:
            user = User(**u_data)
            result.append(user)
        except ValidationError as e:
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
    except ValidationError as e:
        return f"Could not parse payload: {e}", 400

    try:
        r = recommendations(options)
        return json.dumps(r, default=lambda x: x.dict()), 200
    except Exception as e:
        return f"Processing error: {e}", 500
