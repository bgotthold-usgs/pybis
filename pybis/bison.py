class Bison:
    def __init__(self):
        self.description = "Set of functions for working with the BISON system"


    def get_bison_search_url(queryType,criteria):
        from pybis.bis import Bis as bis
        bisonBaseSearchURL_json = "https://bison.usgs.gov/api/search.json?count=1&"

        if queryType != "TSN":
            return bisonBaseSearchURL_json+"type=scientific_name&species="+bis.string_cleaning(criteria)
        else:
            return bisonBaseSearchURL_json+"tsn="+str(criteria)
