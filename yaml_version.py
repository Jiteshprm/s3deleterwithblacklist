import os
import yaml
from git import Repo
from datetime import datetime

def get_git_history(file_path):
    repo = Repo(search_parent_directories=True)
    file_rel_path = os.path.relpath(file_path, repo.working_dir)
    commits = list(repo.iter_commits(paths=file_rel_path))
    return len(commits), commits[-1].hexsha, commits[-1].committed_datetime

def update_yaml_file(file_path, version_info):
    with open(file_path, 'r') as yaml_file:
        data = yaml.safe_load(yaml_file)

    data['version'] = f"{version_info[0]}-{version_info[1]}-{version_info[2]}"

    with open(file_path, 'w') as yaml_file:
        yaml.safe_dump(data, yaml_file, default_flow_style=False)

def process_yaml_files(directory):
    yaml_files = [f for f in os.listdir(directory) if f.endswith('.yaml') or f.endswith('.yml')]

    for yaml_file in yaml_files:
        yaml_path = os.path.join(directory, yaml_file)
        version_info = get_git_history(yaml_path)
        update_yaml_file(yaml_path, version_info)

if __name__ == "__main__":
    git_folder_path = "/path/to/your/git/repository"
    os.chdir(git_folder_path)
    process_yaml_files(git_folder_path)
