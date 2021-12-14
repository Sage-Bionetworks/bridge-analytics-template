#######################################################
# Utility script to query Synapse tables
########################################################


def get_last_update(table_id):
    return(f"select max(uploadDate) as `Last Update` FROM {table_id}")

def get_n_users(table_id):
    return(f"select count(distinct healthCode) as `Participants` FROM {table_id}")

def get_n_android_users(table_id):
    return(f"SELECT COUNT(DISTINCT healthCode) as `Android Users` FROM {table_id} WHERE ((phoneInfo not like '%iPhone%') AND (phoneInfo<>'SmsLogHealthDataBackfill') AND (phoneInfo<>'Bridge Server'))")

def get_n_ios_users(table_id):
    return(f"select count(distinct healthCode) as `IOS Users` FROM {table_id} where phoneInfo not like '%iPhone%'")

def get_n_activities(table_id):
    return(f"SELECT count(distinct recordId) as `Activities` FROM {table_id}")

def get_n_passive_contributors(table_id):
    return(f"SELECT count(distinct healthCode) as `Passive Contributors` FROM {table_id} WHERE dataGroups NOT LIKE '%test%' and originalTable like '%Passive%'")

def get_activity_by_week(table_id):
    return(f"select week(FROM_UNIXTIME(`createdOn`/1000)) as `week`, count(*) as `activities` from {table_id} WHERE dataGroups NOT LIKE '%test%' GROUP BY `week` ORDER BY originalTable")

def get_activity_by_hour(table_id):
    return(f"select hour(FROM_UNIXTIME(`createdOn`/1000)) as `t`, count(recordId) as `activities` from {table_id} WHERE dataGroups NOT LIKE '%test%'GROUP BY `t`")

def get_active_users_per_date(table_id):
    return(f"select `uploadDate`, count(distinct healthCode) as activities from {table_id} WHERE dataGroups NOT LIKE '%test%' GROUP BY uploadDate ORDER BY originalTable")
