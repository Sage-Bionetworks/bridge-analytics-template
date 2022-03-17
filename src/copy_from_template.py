"""
This script is used for copying stats wiki of a digital health study
it will prompt user target id (project to build dashboard and folder structure)
and the desired synapseformation template,
health summary table to create table on,
and the wiki id of the dashboard to copy

Author: aryton.tediarjo@sagebase.org
"""
import sys
import pandas as pd
import numpy as np
import logging
import argparse
import yaml

import synapseclient
import synapseutils
from synapseclient import File
import synapseformation.client

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def read_args():
    parser = argparse.ArgumentParser(description = 'copy from template')
    parser.add_argument("--parent-project",
                        help= "Synapse ID of study project")
    parser.add_argument("--bridge-raw-data",
                        help= "Synapse ID of folder containing Bridge exported data")
    parser.add_argument("--template",
                        help= "File path to synapseformation template")
    parser.add_argument("--wiki",
                       help = "Optional. Synapse ID of wiki template for dashboard. "
                       "Defaults to syn26546076.",
                       default = "syn26546076")
    args = parser.parse_args()
    return args

def get_project_id(syn, project_name):
    logger.info(f'Getting Synapse project id for {project_name}')
    response = syn.findEntityId(project_name)
    return "" if response is None else response

def create_project(syn, template_path, project_name):
    logger.info(f'Creating Synapse project {project_name}, ' + f'with template_path {template_path}')
    try:
        response = synapseformation_client.create_synapse_resources(
                syn = syn,
                template_path = template_path)
        logger.debug(f'Project response: {response}')
        if response is not None:
            return response.get('id')
    except Exception as e:
        logger.error(e)
        sys.exit(1)

def get_raw_data_view(created_entities, raw_data_folder):
    # This should be unique
    raw_data_view_finder = [
            i for i in created_entities
            if i["entity"]["concreteType"] == \
                    'org.sagebionetworks.repo.model.table.EntityView'
            and raw_data_folder.replace("syn", "") in \
                    i["entity"]["scopeIds"]
            ]
    if (len(raw_data_view_finder) == 0):
        raise Exception(
                "Did not find created entity view with "
                f"scope {raw_data_folder}")
    elif (len(raw_data_view_finder) > 1):
        raise Exception(
                "Found more than one created entity view with "
                f"scope {raw_data_folder}")
    raw_data_view = raw_data_view_finder.pop()
    return raw_data_view["entity"]

def main():
    # setup
    args = read_args()
    syn = synapseclient.login()
    template_substitutions = {
            "{bridge_raw_data}": args.bridge_raw_data
            }

    # read synapseformation template and create entities
    with open(args.template, "r") as f:
        template = f.read()
        for sub, replacement in template_substitutions.items():
            template = template.replace(sub, replacement)
        config = yaml.safe_load(template)
    creation_cls = synapseformation.SynapseCreation(syn)
    created_entities = synapseformation.client._create_synapse_resources(
            config_list=config,
            creation_cls=creation_cls,
            parentid=args.parent_project)

    # TODO: link parquet folder to external storage location

    # copy wiki dashboard
    raw_data_view = get_raw_data_view(
            created_entities=created_entities,
            raw_data_folder=args.bridge_raw_data)
    synapseutils.copyWiki(
        syn = syn,
        entity = args.wiki,
        destinationId = args.parent_project,
        entityMap = {"source_table":raw_data_view["id"]})


if __name__ == "__main__":
    main()
