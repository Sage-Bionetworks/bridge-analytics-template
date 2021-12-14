from yaml import safe_load
import pandas as pd
import numpy as np


def parse_yaml_to_data_mapping(yaml, target):
    
    # open file
    with open(yaml, 'r') as f:
        normalized_data = pd.json_normalize(safe_load(f))
    
    # massage data
    normalized_data = normalized_data[normalized_data["name"] == target]
    normalized_data["subpage"] = normalized_data["subpage"].fillna('')
    data = pd.DataFrame(normalized_data["content"].iloc[0]).sort_values(["row", "col"])
    data["plot_params"] = data["plot_params"].fillna("")
    data["table_id"] = normalized_data["table_id"].iloc[0]
    
    # fill in dictionary
    data_dict = {}
    data_dict["subpage"] = normalized_data["subpage"].iloc[0]
    data_dict["md_mapping"] = data
    
    return(data_dict)
