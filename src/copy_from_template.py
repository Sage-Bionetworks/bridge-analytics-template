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
import json
from copy import deepcopy

import boto3
import yaml
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
                        help= "Synapse ID of folder called 'Bridge Raw Data' containing Bridge exported data")
    parser.add_argument("--app",
                        help= "App identifier associated with --parent-project.")
    parser.add_argument("--study",
                        help= "Study identifier associated with --parent-project.")
    parser.add_argument("--template",
                        help= "File path to synapseformation template.")
    parser.add_argument("--parquet-wiki",
                        help = "Optional. Synapse ID of the study project containing the parquet wiki template"
                        "to use for your study project's parquet folder's dashboard. "
                        "Defaults to syn26546076.",
                        default = "syn26546076")
    parser.add_argument("--parquet-wiki-sub-page",
                    help = "Optional. Sub page ID of the parquet wiki template"
                    "to use for your study project's parquet folder's dashboard. "
                    "See synapseutils.copy_functions.copyWiki function's entitySubPageId parameter for more info."
                    "Defaults to 620176",
                    default = 620176)
    parser.add_argument("--owner-txt",
                        help= "File path to owner.txt for S3 bucket external storage location.")
    parser.add_argument("--parquet-bucket",
                        help= "Name of S3 bucket where parquet data will be stored. "
                        "Defaults to 'bridge-downstream-dev-parquet'",
                        default = "bridge-downstream-dev-parquet")
    parser.add_argument("--wiki",
                       help = "Optional. Synapse ID of the study project containing the wiki template"
                       "to use for your study project's main wiki's dashboard. "
                       "Defaults to syn26546076.",
                       default = "syn26546076")
    parser.add_argument("--wiki-sub-page",
                help = "Optional. Sub page ID of the wiki template"
                "to use for your study project's main wiki's dashboard. "
                "See synapseutils.copy_functions.copyWiki function's entitySubPageId parameter for more info."
                "Defaults to 620218",
                default = 620218)
    parser.add_argument("--aws-profile",
                        help="Optional. The AWS profile to use. "
                        "Defaults to 'default'.")
    parser.add_argument("--aws-region",
                        help="Optional. The AWS region to use. "
                        "Defaults to 'us-east-1'.",
                        default="us-east-1")
    parser.add_argument("--ssm-parameter",
                        help=("Optional. The name of the SSM parameter containing "
                              "the Synapse personal access token. "
                              "If not provided, cached credentials are used from your .synapseConfig file"))
    args = parser.parse_args()
    return args


def get_synapse_client(ssm_parameter=None, aws_session=None):
    '''Returns a synapse client from credentials stored in SSM'''
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
    '''
    Get the file view entity which has the raw data folder,
    and *only* the raw data folder, in its scope.
    '''
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
    if len(raw_data_view_finder) > 1:
        raise Exception(
                "Found more than one created entity view with "
                f"scope {raw_data_folder}")
    raw_data_view = raw_data_view_finder.pop()
    return raw_data_view["entity"]


def get_folder(created_entities, folder_name):
    ''' Returns the Folder entity associated with folder_name '''
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


def modify_file_view_types(
        syn, file_view_id, default_str_length=128,
        xl_str_fields=["clientInfo"], xl_str_length=512):
    '''
    Some string values in the file view are bound to become longer
    than the default set upon creation of the view. We expand their length
    to a conservative value so that we can be reasonably certain the file
    view won't break at some point in the future. We also correct the types
    of other columns.
    '''
    ignore_cols = ["name", "etag", "type"]
    date_cols = ["exportedOn", "eventTimestamp", "uploadedOn", "scheduleModifiedOn"]
    boolean_cols = ["timeWindowPersistent"]
    int_cols = ["sessionInstanceStartDay", "sessionInstanceEndDay",
            "assessmentRevision", "participantVersion"]
    file_view = syn.get(file_view_id)
    cols = list(syn.getColumns(file_view["columnIds"]))
    col_changes = []
    for c in cols:
        if c["columnType"] == "STRING" and c["name"] not in ignore_cols:
            new_col = deepcopy(c)
            new_col.pop("id")
            if new_col["name"] in date_cols:
                new_col["columnType"] = "DATE"
                new_col["maximumSize"] = None
            elif new_col["name"] in boolean_cols:
                new_col["columnType"] = "BOOLEAN"
                new_col["maximumSize"] = None
            elif new_col["name"] in int_cols:
                new_col["columnType"] = "INTEGER"
                new_col["maximumSize"] = None
            elif new_col["name"] in xl_str_fields:
                new_col["maximumSize"] = xl_str_length
            else:
                new_col["maximumSize"] = default_str_length
            new_col = syn.store(new_col)
            col_changes.append({
                "oldColumnId": c["id"],
                "newColumnId": new_col["id"]})
        else:
            col_changes.append({
                "oldColumnId": c["id"],
                "newColumnId": c["id"]})
    schema_change_request = {
            "concreteType": "org.sagebionetworks.repo.model.table.TableSchemaChangeRequest",
            "entityId": file_view_id,
            "changes": col_changes,
            "orderedColumnIds": [j["newColumnId"] for j in col_changes]
            }
    table_update_request = {
            "concreteType": "org.sagebionetworks.repo.model.table.TableUpdateTransactionRequest",
            "entityId": file_view_id,
            "changes": [schema_change_request]
            }
    syn.restPOST(
            f"/entity/{file_view_id}/table/transaction/async/start",
            body=json.dumps(table_update_request))


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

    # copy parquet wiki dashboard
    parquet_folder = get_folder(
            created_entities=created_entities,
            folder_name="parquet")
    synapseutils.copyWiki(
        syn = syn,
        entity = args.parquet_wiki,
        destinationId = parquet_folder.id,
        entitySubPageId = args.parquet_wiki_sub_page)
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

    # Correct column types in raw data file view
    raw_data_view = get_raw_data_view(
            created_entities=created_entities,
            raw_data_folder=args.bridge_raw_data)
    modify_file_view_types(
        syn=syn,
        file_view_id=raw_data_view["id"])
    # copy main wiki dashboard
    scores_folder = get_folder(
        created_entities=created_entities,
        folder_name="scores")
    synapseutils.copyWiki(
        syn = syn,
        entity = args.wiki,
        entitySubPageId=args.wiki_sub_page,
        destinationId = args.parent_project,
        entityMap = {"source_table":raw_data_view["id"],
                     "score_folder" : scores_folder['id']})


if __name__ == "__main__":
    main()

