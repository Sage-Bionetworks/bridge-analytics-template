"""
This script creates resources over an exporter 3.0
Synapse project created by Bridge. It's assumed that:

    1. The project already exists.
    2. The project-level permissions have been set.
    3. A Bridge Raw Data folder has been created.

The following resources will be created:

    1. A "parquet" and "examples" folder. The parquet folder will
    be configured to use an S3 bucket owned by the Bridge Downstream
    account as an external storage location.
    2. A file view over the Bridge Raw Data folder
    3. A wiki page will be copied from a template to the project
    and will be updated to use the above file view in its graphs.

[1] and [2] in the previous list will be created from
a synapseformation template passed to `--template`. A default
template has been provided in this repo.
"""
import logging
import argparse
import yaml

import boto3
import synapseclient
import synapseutils
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
    parser.add_argument("--app",
                        help= "App identifier associated with --parent-project.")
    parser.add_argument("--study",
                        help= "Study identifier associated with --parent-project.")
    parser.add_argument("--template",
                        help= "File path to synapseformation template")
    parser.add_argument("--owner-txt",
                        help= "File path to owner.txt for S3 bucket external storage location.")
    parser.add_argument("--parquet-bucket",
                        help= "S3 bucket where parquet data will be stored.")
    parser.add_argument("--wiki",
                       help = "Optional. Synapse ID of wiki template for dashboard. "
                       "Defaults to syn26546076.",
                       default = "syn26546076")
    parser.add_argument("--aws-profile",
                        help="Optional. The AWS profile to use. "
                        "defaults to 'default'.")
    parser.add_argument("--aws-region",
                        help="Optional. The AWS region to use. "
                        "defaults to 'us-east-1'.",
                        default="us-east-1")
    parser.add_argument("--ssm-parameter",
                        help=("Optional. The name of the SSM parameter containing "
                              "the Synapse personal access token. "
                              "If not provided, cached credentials are used"))
    args = parser.parse_args()
    return args

def get_synapse_client(ssm_parameter=None, aws_session=None):
    if ssm_parameter is not None:
        ssm_client = aws_session.client("ssm")
        token = ssm_client.get_parameter(
            Name=ssm_parameter,
            WithDecryption=True)
        syn = synapseclient.Synapse()
        syn.login(authToken=token["Parameter"]["Value"])
    else: # try cached credentials
        syn = synapseclient.login()
    return syn

def get_raw_data_view(created_entities, raw_data_folder):
    # This should be unique
    raw_data_view_finder = [
            i for i in created_entities
            if i["entity"]["concreteType"] == \
                    'org.sagebionetworks.repo.model.table.EntityView'
            and raw_data_folder.replace("syn", "") in \
                    i["entity"]["scopeIds"]
            and len(i["entity"]["scopeIds"]) == 1
            ]
    if len(raw_data_view_finder) == 0:
        raise Exception(
                "Did not find created entity view with "
                f"scope {raw_data_folder}")
    elif len(raw_data_view_finder) > 1:
        raise Exception(
                "Found more than one created entity view with "
                f"scope {raw_data_folder}")
    raw_data_view = raw_data_view_finder.pop()
    return raw_data_view["entity"]

def get_folder(created_entities, folder_name):
    # This should be unique
    folder_finder = [
            i for i in created_entities
            if i["entity"]["concreteType"] == \
                    'org.sagebionetworks.repo.model.Folder'
            and i["entity"]["name"] == folder_name]
    if len(folder_finder) == 0:
        raise Exception(
                "Did not find created folder with "
                f"name {folder_name}")
    elif len(folder_finder) > 1:
        raise Exception(
                "Found more than one created folder with "
                f"name {folder_name}")
    folder = folder_finder.pop()
    return folder["entity"]

def main():
    # setup
    args = read_args()
    aws_session = boto3.session.Session(
            profile_name=args.aws_profile,
            region_name=args.aws_region)
    syn = get_synapse_client(
            ssm_parameter=args.ssm_parameter,
            aws_session=aws_session)
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

    parquet_folder = get_folder(
            created_entities=created_entities,
            folder_name="parquet")
    base_key = f"bridge-downstream/{args.app}/{args.study}/parquet/"
    s3_client = aws_session.client("s3")
    with open (args.owner_txt, "rb") as f:
        s3_client.put_object(
                Body=f,
                Bucket=args.parquet_bucket,
                Key=base_key + "owner.txt")
    syn.create_s3_storage_location(
            folder=parquet_folder["id"],
            bucket_name=args.parquet_bucket,
            base_key=base_key,
            sts_enabled=True)

    # Set permissions on parquet folder by copying ACL
    # from Bridge Raw Data folder, excepting BridgeDownstream
    bridge_raw_data_acl = syn._getACL(args.bridge_raw_data)
    bridge_downstream_id = 3432808
    for acl in bridge_raw_data_acl["resourceAccess"]:
        if acl["principalId"] == bridge_downstream_id:
            continue
        else:
            syn.setPermissions(
                    entity=parquet_folder["id"],
                    principalId=acl["principalId"],
                    accessType=acl["accessType"],
                    warn_if_inherits=False,
                    overwrite=True)
    # Grant BridgeDownstream admin permissions on parquet folder
    syn.setPermissions(
            entity=parquet_folder["id"],
            principalId=bridge_downstream_id,
            accessType=[
                "DOWNLOAD", "READ", "UPDATE", "CREATE", "CHANGE_PERMISSIONS",
                "DELETE", "MODERATE", "CHANGE_SETTINGS"],
            warn_if_inherits=False,
            overwrite=True)

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
