class Bison:
    def __init__(self):
        self.description = "Set of functions for working with the BISON system"


    def get_bison_search_url(queryType,criteria):
        from bis import bis

        _baseURL = "https://bison.usgs.gov/api/search.json?count=1&"

        if queryType != "TSN":
            return _baseURL+"type=scientific_name&species="+bis.stringCleaning(criteria)
        else:
            return _baseURL+"tsn="+str(criteria)
