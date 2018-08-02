class Iucn:
    def __init__(self):
        self.description = 'Set of functions for working with the IUCN API'


    def get_species_search_url(scientificname):
        return "http://apiv3.iucnredlist.org/api/v3/species/"+scientificname