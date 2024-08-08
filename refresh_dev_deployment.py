# Based on:
# https://github.com/dagster-io/dagster/issues/3803

import requests
import json

RELOAD_REPOSITORY_LOCATION_MUTATION = """
mutation ($repositoryLocationName: String!) {
   reloadRepositoryLocation(repositoryLocationName: $repositoryLocationName) {
      __typename
      ... on RepositoryLocation {
        name
        repositories {
            name
        }
        isReloadSupported
      }
      ... on RepositoryLocationLoadFailure {
          name
          error {
              message
          }
      }
   }
}
"""

dagit_host = "127.0.0.1"

variables = {
    "repositoryLocationName": "popgetter",
}
reload_res = requests.post(
    "http://{dagit_host}:3000/graphql?query={query_string}&variables={variables}".format(
        dagit_host=dagit_host,
        query_string=RELOAD_REPOSITORY_LOCATION_MUTATION,
        variables=json.dumps(variables),
    )
).json()

did_succeed = False

if reload_res:
    # did_succeed = reload_res["data"]["reloadRepositoryLocation"]["__typename"] == "RepositoryLocation"
    print(reload_res)

print(f"Reload {'succeeded' if did_succeed else 'failed'}")
