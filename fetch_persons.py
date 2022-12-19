import yaml
from github import Github
import os

ORGANIZATION_NAME = 'RoyalAholdDelhaize'
TEAMS_AS_CODE_REPO_NAME = 'sre-teams-configuration'
TOKEN_GITHUB = 'TOKEN_GITHUB'

def update_product_owners():
    github_client = Github(os.getenv(TOKEN_GITHUB))
    teams_as_code = github_client.get_organization(ORGANIZATION_NAME).get_repo(TEAMS_AS_CODE_REPO_NAME)

    persons = teams_as_code.get_contents(path='persons')
    
    person_dict = {}

    for person in persons:
        content = person.decoded_content.decode("utf-8")
        yaml_content = yaml.safe_load(content)
        name = yaml_content['person']['name']
        info = {'teams': yaml_content['person']['teams'], 'roles': yaml_content['person']['roles']}
        person_dict[name] = info
    
    print("PERSON DICT")
    print(person_dict)

    with open('/persons.yml', 'w') as outfile:
        yaml.safe_dump(person_dict, outfile)

if __name__ == "__main__":
    print("main")
    update_product_owners()