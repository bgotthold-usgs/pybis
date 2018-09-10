class Gap:
    def __init__(self):
        self.description = "Set of functions for working with GAP species and other GAP data"

    def gap_to_tir(sbItem):
        from datetime import datetime
        import requests

        speciesItem = dict()
        speciesItem['Source'] = 'GAP Species'
        speciesItem['Cache Date'] = datetime.utcnow().isoformat()

        itemIdentifierTypes = [identifier['type'] for identifier in sbItem['identifiers']]

        for identifier in sbItem['identifiers']:
            speciesItem[identifier['type']] = identifier['key']

        # Temporary usage of the ScienceBase Vocab to put an appropriate qualifier on ITIS information
        sbVocab = requests.get(
            'https://www.sciencebase.gov/vocab/categories?parentId=59e62074e4b0adbd11e26b12&format=json').json()
        itisIdentifiersFromVocab = [id for id in sbVocab['list'] if id['name'][:4] == 'itis']
        itisIdentifiers = {}
        for i in itisIdentifiersFromVocab:
            itisIdentifiers[i['name']] = i['description']
        itisIdentifierSet = set(itisIdentifiers.keys())
        thisItisIdentifier = next((element for element in itemIdentifierTypes if element in itisIdentifierSet), None)

        if thisItisIdentifier is not None:
            from pybis.itis import Itis as itis
            from pybis.tess import Tess as tess

            itisTSN = speciesItem[thisItisIdentifier]
            itisResponse = requests.get(itis.get_itis_search_url(itisTSN)).json()
            speciesItem['ITIS'] = itis.package_itis_json(itisResponse['response']['docs'][0])
            speciesItem['ITIS']['ITIS TSN Usage Qualifier'] = thisItisIdentifier
            speciesItem['ITIS']['ITIS TSN Usage Qualifier Description'] = itisIdentifiers[thisItisIdentifier]

            speciesItem['TESS'] = tess.tess_query(tess.get_tess_search_url('TSN', itisTSN))

        modelReportFileURL = next(
            (f['url'] for f in sbItem['files'] if f['title'] == 'Machine Readable Habitat Database Parameters'), None)
        if modelReportFileURL is not None:
            speciesItem['GAP Model Report'] = requests.get(modelReportFileURL).json()

        return speciesItem
