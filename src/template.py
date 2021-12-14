import query
import utils
import synapseclient as syn

sc = syn.login()

def create_subpage(project_id, subpage):
    wiki_id = None
    headers = sc.getWikiHeaders(project_id)
    parentWikiId = headers[0]["id"]
    for content in headers:
        if content["title"] == subpage:
            wiki_id = content["id"]
            break
    if wiki_id is not None:
        return(wiki_id)
    else:
        entity = syn.Wiki(title= subpage, 
                          owner= project_id, 
                          markdown = "",
                          parentWikiId = parentWikiId)
        store = sc.store(entity)
        return(store["id"])


def generate_wiki_md(data):
    curr_row = 0
    string_list = []
    row_prefix = """{row}\n%s"""
    row_suffix = """{row}"""
    for col, row in zip(data.col, data.row):
        subset = data[(data["row"] == row) & (data["col"] == col)]

        # get id
        table_id = subset["table_id"].iloc[0]

        # get query
        query_func = subset["query_funs"].iloc[0]
        query_clause = getattr(query, query_func)(table_id)

        # get width
        width = subset["width"].iloc[0]

        # get plot params
        plot_params = subset["plot_params"].iloc[0]

        # check if plot parameter exist:
        if(plot_params != ""):
            md = """{column width=%s}\n${plot?query=%s&%s}\n{column}""" % (
                width, query_clause, plot_params)
        else:
            md = """{column width=%s}\n${synapsetable?query=%s&showquery=False}%s\n{column}""" % (
                width, query_clause, plot_params)

        # row-check:
        if(curr_row == 0):
            md = row_prefix % md
        elif(row == curr_row):
            pass
        else:
            md = row_suffix + "\n" + (row_prefix % md)
        string_list.append(md)
        curr_row = row

    # join markdown
    wiki_md = "\n".join(string_list) + row_suffix
    return(wiki_md)


def build_wiki(yaml_file, project_id, target, store = True):
    data_dict = utils.parse_yaml_to_data_mapping(yaml_file, target)
    wiki_md = generate_wiki_md(data_dict["md_mapping"])
    
    if (data_dict["subpage"] != ""):
        subpage_id = create_subpage(project_id, data_dict["subpage"])
        wiki = sc.getWiki(project_id, subpageId = subpage_id)
    else:
        wiki = sc.getWiki(project_id)
    wiki.markdown = wiki_md
    if(store == True):
        wiki = sc.store(wiki)
    return(wiki)    