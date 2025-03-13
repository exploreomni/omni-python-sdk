import requests
import os
import base64
import lkml
import sys
import argparse
import pdb
from omni_python_sdk import OmniAPI


# Fetch the token if available
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

# Set up headers with conditional authentication
headers = {
    "Accept": "application/vnd.github.v3+json"
}
if GITHUB_TOKEN:
    headers["Authorization"] = f"token {GITHUB_TOKEN}"

def base_url(repo: str):
    return f"https://api.github.com/repos/{repo}"

def find_lkml_files_in_repo(repo: str, branch: str = "main"):
    lkml_files = []
    url = f"{base_url(repo)}/git/trees/{branch}?recursive=1"  # Adjust 'main' if you’re using a different branch

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    # Search for .lkml files in the tree response
    files = response.json().get("tree", [])
    for file in files:
        if file["path"].endswith(".lkml"):
            lkml_files.append(file["path"])

    return lkml_files

def get_file_content(repo: str, file_path: str):
    url = f"{base_url(repo)}/contents/{file_path}"
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    content = response.json().get("content")
    if content:
        # Decode the Base64 content
        decoded_content = base64.b64decode(content).decode("utf-8")
        return decoded_content
    return None

def parse_lkml_content(content):
    parsed = lkml.load(content)
    return parsed

def parse_lookml(repo: str, branch: str):
    lkml_files = find_lkml_files_in_repo(repo, branch)
    parsed_objects = []
    
    for file_path in lkml_files:
        content = get_file_content(repo, file_path)
        if content:
            parsed = parse_lkml_content(content)
            parsed_objects.append(parsed)

    # Continue with mapping and creating objects as needed
    return parsed_objects

def map_lookml_model_to_omni(parsed_model):
    # Map LookML model to Omni model
    pass


# explore_source: identifier {
#   bind_all_filters: yes
#   column: identifier {
#     field: field_name
#   }
#   derived_column: identifier {
#     sql: SQL expression ;;
#   }
#   expression_custom_filter: [custom filter expression]
#   filters: [field_name_1: "string", field_name_2: "string", ...]
#   limit: number
#   sorts: [field_name_1: asc | desc, field_name_2: asc | desc, ...]
#   timezone: "string"
# }
def map_lookml_query_to_omni(parsed_query):
    omni_query = {
        "topic": parsed_query["explore_source"]["name"]
    }
    omni_query["fields"]= {}
    for column in parsed_query["columns"]:
        if "field" in column:
            omni_query["fields"][column["field"]] = column["name"]
        else:
            omni_query["fields"][column["name"]] = column["name"]
    if "derived_columns" in parsed_query:
        for derived_column in parsed_query["derived_columns"]:
            # todo map derived column to omni
            omni_query["fields"][derived_column["name"]] = derived_column["sql"]
    if "filters_all" in parsed_query:
        pass
    if "persist_for" in parsed_query:
        pass
    if "bind_filters__all" in parsed_query:
        pass
    if "limit" in parsed_query:
        pass
    if "sorts" in parsed_query:
        pass
    if "timezone" in parsed_query:
        pass


def map_lookml_view_to_omni(parsed_view):
    omni_view = {
        "name": parsed_view["name"]
    }

    if "sql_table_name" in parsed_view:
        # if schema is present
        if parsed_view["sql_table_name"].count(".") > 0:
            omni_view["schema"] = parsed_view["sql_table_name"].split(".")[0]
            omni_view["table"] = parsed_view["sql_table_name"].split(".")[1]
        else:
            omni_view["table"] = parsed_view["sql_table_name"]
    elif "derived_table" in parsed_view:
        if "sql" in parsed_view["derived_table"]:
            omni_view["sql"] = parsed_view["derived_table"]["sql"]
        elif "explore_source" in parsed_view["derived_table"]:
            omni_view["query"] = map_lookml_query_to_omni(parsed_view["derived_table"]["explore_source"])
        else:
            # Handle derived_table: sql_trigger
            pass
    
    # Map LookML view to Omni view
    if "drill_fields" in parsed_view:
        omni_view["drill_fields"] = parsed_view["drill_fields"]
    if "label" in parsed_view:
        omni_view["label"] = parsed_view["label"]
    if "required_access_grants" in parsed_view:
        # TODO map required access grants
        # omni_view["required_access_grants"] = parsed_view["required_access_grants"]
        pass
    
    # Map fields
    if "dimensions" in parsed_view:
        omni_view["dimensions"] = [map_lookml_dimension_to_omni(dim) for dim in parsed_view["dimensions"]]
    
    if "dimension_groups" in parsed_view:
        dim_groups = [map_lookml_dimension_group_to_omni(dim_group) for dim_group in parsed_view["dimension_groups"]]

    if "dimensions" in omni_view:
        omni_view["dimensions"] += dim_groups
    else:
        omni_view["dimensions"] = dim_groups

    if "measures" in parsed_view:
        omni_view["measures"] = [map_lookml_measure_to_omni(measure) for measure in parsed_view["measures"]]
    return omni_view

def map_lookml_dimension_to_omni(parsed_field):
    # Map LookML dimension to Omni dimension
    pass

def map_lookml_measure_to_omni(parsed_field):
    # Map LookML measure to Omni measure
    pass

def map_lookml_dimension_group_to_omni(parsed_field):
    # Map LookML dimension group to Omni date field
    pass

def map_lookml_explore_to_omni(parsed_explore):
    # Map LookML explore to Omni topic
    pass

def migrate_lookml(repo: str, branch: str, client: OmniAPI, connection_id: str):
    parsed_objects = parse_lookml(repo, branch)
    # migrate lookml views

    views = [view for obj in parsed_objects if "views" in obj for view in obj["views"]]
    

if __name__ == "__main__":

  parser = argparse.ArgumentParser(description="LookML Migrator")

  # Define named arguments with the -- prefix
  parser.add_argument("--github_repo", type=str, help="LookML GitHub repository", required=True)
  parser.add_argument("--branch", type=str, help="Branch name", default="main")
  parser.add_argument("--omni_api_key", type=str, help="Omni API key", required=True)
  parser.add_argument("--omni_base_url", type=str, help="Omni base URL", required=True)
  parser.add_argument("--omni_connection_id", type=str, help="Omni Connection ID", required=True)

  args = parser.parse_args()

  # Initialize the OmniAPI client
  client = OmniAPI(args.omni_api_key, args.omni_base_url)

  migrate_lookml(args.github_repo, args.branch, client, args.omni_connection_id,)