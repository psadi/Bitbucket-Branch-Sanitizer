#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import asyncio
import json
import logging
import os
import sys
import warnings
from datetime import date, datetime
from shutil import rmtree

import requests
from jinja2 import Environment

warnings.filterwarnings("ignore")
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
)

json_file_name: str = "results/state.json"


class RequestsApi:
    def __init__(self, base_url, **kwargs) -> None:
        self.base_url = base_url
        self.session = requests.Session()
        for arg in kwargs:
            if isinstance(kwargs[arg], dict):
                kwargs[arg] = self.__deep_merge(getattr(self.session, arg), kwargs[arg])
            setattr(self.session, arg, kwargs[arg])

    def get(self, url, **kwargs) -> requests.Response:
        return self.session.get(self.base_url + url, **kwargs)

    def delete(self, url, **kwargs) -> requests.Response:
        return self.session.delete(self.base_url + url, **kwargs)

    @staticmethod
    def __deep_merge(source, destination):
        for key, value in source.items():
            if isinstance(value, dict):
                node = destination.setdefault(key, {})
                RequestsApi.__deep_merge(value, node)
            else:
                destination[key] = value
        return destination


def argument_parser() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--username", type=str, required=True, metavar="Bitbucket Username"
    )
    parser.add_argument(
        "--password", type=str, required=True, metavar="Bitbucket Password/token"
    )
    parser.add_argument(
        "--project", type=str, required=True, metavar="Bitbucket Project Name"
    )
    parser.add_argument(
        "--config",
        type=str,
        required=True,
        default="./config.ini",
        metavar="Configuration File Path",
    )
    parser.add_argument(
        "--repositories",
        type=str,
        metavar="Bitbucket repositories for the given project [Optional]",
    )
    return parser.parse_args()


def jinja_template():
    return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Bitbucket Branch Purging Summary</title>
</head>
<style>
    body {
        font-family: arial, sans-serif;
    }

    table {
      border-collapse: collapse;
    }

    td, th {
      border: 1px solid #dddddd;
      text-align: center;
      padding: 8px;
    }

    tr:nth-child(even) {
      background-color: #dddddd;
    }

    li {
        padding-bottom: 8px
    }
    </style>
<body>
    <h3>Bitbucket Branch Maintainance</h3>

    <h4>Rule for Purging</h4>
    <p>
    Branches withresults any commits for consequitive days of retention period mentioned below are the candidates for purging except for Master & Develop
    <br/>
    Deprecated repos are excluded
    </p>
    <ol>
        <li>Release & Hotfix - Retention period is 30 days</li>
        <li>All other branches - Rentention period is 45 days</li>
    </ol>
    <h4>Summary</h4>
    <i>For detailed log please refer the attachment</i>
    <table>
        <tr>
        {% for h in header %}
            <th>{{ h }}</th>
        {% endfor %}
        </tr>
        {% for c in config %}
        <tr>
            {% for b in c %}
            <td>{{ b }}</td>
            {% endfor %}
        </tr>
        {% endfor %}
        <tr>
    </table>
</body>
</html>
    """


def load_config(project: str, repository: str, config_file: str) -> str:
    with open(config_file, "r", encoding="utf-8") as file:
        config = json.load(file)
    url = config["url"]
    branches_to_exclude = config["branches_to_exclude"]
    thresholds = config["thresholds"]

    return json.dumps(
        {
            "url": url,
            "endpoints": {
                "get_repositories": f"/rest/api/latest/projects/{project}/repos?limit=1000",
                "get_branch_permissions": f"/rest/branch-permissions/latest/projects/{project}/repos/{repository}/restrictions",
                "get_branches": f"/rest/api/latest/projects/{project}/repos/{repository}/branches?limit=1000",
                "get_commit_stats": f"/rest/api/1.0/projects/{project}/repos/{repository}/commits",
                "delete_branch": f"/rest/branch-utils/latest/projects/{project}/repos/{repository}/branches",
            },
            "branches_to_exclude": ["master", "develop"] + branches_to_exclude,
            "thresholds": thresholds,
        }
    )


def format_date(input_date: str) -> date:
    [year, month, day] = map(int, input_date.split("-"))
    return date(year, month, day)


def format_data(data_list: list) -> list:
    header = ["BRANCH", "LAST COMMIT", "INACTIVE (days)", "STATUS"]
    separator = ["-" * (len(i)) for i in header]
    widths = [len(cell) for cell in header]
    for row in data_list:
        for i, cell in enumerate(row):
            widths[i] = max(len(str(cell)), widths[i])
    formatted_row = " ".join("{:%d}" % width for width in widths)
    data = [formatted_row.format(*header), formatted_row.format(*separator)]
    data.extend(formatted_row.format(*row) for row in data_list)
    return data


def delete_branch(bitbucket, url: str, body: dict) -> str:
    response = bitbucket.delete(
        url, data=body, headers={"content-type": "application/json;charset=UTF-8"}
    )
    if response.status_code != 204:
        raise ValueError(
            f"Error deleting {body['name']} branch, code: {response.status_code}"
        )

    return "DELETED"


def delete_branch_permissions(bitbucket, config: dict, branch_name: dict) -> None:
    branch_permissions = config["endpoints"]["get_branch_permissions"]
    get_branch_permissions = bitbucket.get(branch_permissions, verify=False)
    if get_branch_permissions.status_code != 200:
        raise ValueError(get_branch_permissions.json())

    for i in get_branch_permissions.json()["values"]:
        if branch_name == i["matcher"]["displayId"]:
            response = bitbucket.delete(f"{branch_permissions}/{i['id']}")
            if response.status_code != 204:
                raise ValueError(
                    f"Error deleting {branch_name} branch permissions, code: {response.status_code}"
                )


def get_threshold(branch_name: str, config) -> int:
    branch_prefix = branch_name.split("/")[0]

    return (
        config["thresholds"][branch_prefix]
        if branch_prefix in config["thresholds"].keys()
        else config["thresholds"]["default"]
    )


def read_from_json():
    try:
        if not os.path.exists(json_file_name):
            raise FileNotFoundError
        with open(json_file_name, "r", encoding="utf-8") as json_file:
            return json.loads(json_file.read())
    except FileNotFoundError:
        logging.error("'%s' not found", json_file_name)


def write_to_json(repository: str, results: list):
    if os.path.exists(json_file_name):
        data: dict = read_from_json()

        if (repository not in data) or (len(data[repository]) != len(results)):
            data[repository] = results
    else:
        data: dict = {repository: results}

    with open(json_file_name, "w", encoding="utf-8") as json_file:
        json.dump(data, json_file)


def generate_report(
    project: str, repository: str, results: list, _template_data: dict
) -> None:
    data: list = format_data(results)
    if len(data) > 2:
        total = len(results)
        retained = 0
        deleted = 0
        for i in results:
            if "retained" in i[3].lower():
                retained += 1
            else:
                deleted += 1

        if deleted > 0:
            _template_data["body"].append([repository, total, retained, deleted])

        with open(
            f"results/{project.upper()}-Bitbucket-Branch-Purging-{str(date.today())}.log",
            "a",
            encoding="utf-8",
        ) as logfile:
            logfile.write("\n" + "=" * 100 + "\n")
            logfile.write(f"{project.upper()} - {repository.upper()}" + "\n")
            logfile.write("=" * 100 + "\n\n")
            for line in data:
                logfile.write(line + "\n")
            logfile.write("\n")

        logging.info("<-- RESULTS: %s - %s -->", project, repository)

    for item in data:
        logging.info("%s", item)

    environment = Environment()
    _template = environment.from_string(jinja_template())
    _rendered_template = _template.render(
        header=_template_data["header"], config=_template_data["body"]
    )

    with open("results/index.html", "w", encoding="utf-8") as template:
        template.write(_rendered_template)


def get_last_commit_date(bitbucket, config: dict, commit_id: str) -> str:
    commit_stats = bitbucket.get(
        f"{config['endpoints']['get_commit_stats']}/{commit_id}",
        verify=False,
    ).json()
    commit_epoch_time = commit_stats["committerTimestamp"] / 1000.0
    return datetime.utcfromtimestamp(commit_epoch_time).strftime("%Y-%m-%d")


async def filter_branches(
    bitbucket, config: dict, index: int, total_len: int, branch_info: dict
) -> list:
    exclude = branch_info["displayId"] in config["branches_to_exclude"]
    logging.info(
        "(%s/%s) %s : %s",
        index,
        total_len,
        "Excluding" if exclude else "Processing",
        branch_info["displayId"],
    )

    if not exclude:
        threshold = get_threshold(branch_info["displayId"], config)

        last_commit_date = get_last_commit_date(
            bitbucket, config, branch_info["latestCommit"]
        )
        delta = (format_date(str(date.today())) - format_date(last_commit_date)).days

        status = "RETAINED" if delta <= threshold else "MARKED FOR DELETION"
        return [branch_info["displayId"], branch_info["latestCommit"], delta, status]


async def process_branches_for_deletion(
    bitbucket,
    config: dict,
    branch_info: str,
    index: int,
    total_len: int,
    branches: list,
):
    branch_name, latest_commit, status = (
        branch_info[0],
        branch_info[1],
        branch_info[3],
    )
    logging.info(
        "(%s/%s) Processing : %s",
        index,
        total_len,
        branch_name,
    )

    for _b in branches:
        if _b["displayId"] == branch_name:
            if _b["latestCommit"] == latest_commit:
                delta = branch_info[2]
            else:
                last_commit_date = get_last_commit_date(
                    bitbucket, config, _b["latestCommit"]
                )
                delta = (
                    format_date(str(date.today())) - format_date(last_commit_date)
                ).days

    threshold = get_threshold(branch_name, config)
    if int(delta) > threshold and status == "MARKED FOR DELETION":
        delete_branch_permissions(bitbucket, config, branch_name)
        body: dict = json.dumps(
            {
                "name": branch_name,
                "endPoint": latest_commit,
            }
        )
        status = delete_branch(bitbucket, config["endpoints"]["delete_branch"], body)
    return [branch_name, latest_commit, delta, status]


async def main() -> None:
    args = argument_parser()
    _config = json.loads(load_config(args.project, "", args.config))

    bitbucket = RequestsApi(_config["url"], auth=(args.username, args.password))
    if not args.repositories:
        repo_payload = bitbucket.get(
            _config["endpoints"]["get_repositories"], verify=False
        )
        repositories = [value["name"] for value in repo_payload.json()["values"]]
    else:
        repositories = args.repositories.split(",")

    is_friday = date.today().weekday() == 4

    _branch_report_status = "deleted" if is_friday else "marked for deletion"
    _process_branches_flag = not is_friday
    _delete_branches_flag = is_friday

    if os.path.exists("results") and not _delete_branches_flag:
        rmtree("results")

    if not os.path("results"):
        os.mkdir("results")

    _template_data: dict = {
        "header": [
            "Repository",
            "Total Branches",
            "# of branches retained",
            f"# of branches {_branch_report_status}",
        ],
        "body": [],
    }

    for repository in repositories:
        if "deprecated" in repository:
            logging.info("Deprecated repo %s will be skipped ...", repository)
            continue

        _config = json.loads(load_config(args.project, repository, args.config))
        branches = bitbucket.get(_config["endpoints"]["get_branches"], verify=False)
        logging.info(
            "'%s' branches for '%s' repository in '%s' project",
            len(branches.json()["values"]),
            repository,
            args.project,
        )

        if _process_branches_flag:
            results = await asyncio.gather(
                *[
                    filter_branches(
                        bitbucket,
                        _config,
                        index,
                        len(branches.json()["values"]),
                        branch_info,
                    )
                    for index, branch_info in enumerate(branches.json()["values"], 1)
                ]
            )

            results: list = list(filter(lambda item: item is not None, results))
            write_to_json(repository, results)

        if _delete_branches_flag:
            repo_branch_data = read_from_json()[repository]
            results = await asyncio.gather(
                *[
                    process_branches_for_deletion(
                        bitbucket,
                        _config,
                        branch_info,
                        index,
                        len(repo_branch_data),
                        branches.json()["values"],
                    )
                    for index, branch_info in enumerate(repo_branch_data, 1)
                ]
            )

        generate_report(args.project, repository, results, _template_data)


if __name__ == "__main__":
    try:
        if sys.version_info >= (3, 7):
            asyncio.run(main())
        else:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(main())
    except Exception as err:
        logging.error(err)
        sys.exit(1)
