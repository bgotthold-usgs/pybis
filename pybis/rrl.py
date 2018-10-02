class ResearchReferenceLibrary:
    def __init__(self):
        self.description = 'Set of functions for working with the Research Reference Library'
    

    def register_citation(registryContainer,citationString,source,url=None):
        import hashlib
        from datetime import datetime
        
        result = {}
        
        hash_id = hashlib.md5(citationString.encode()).hexdigest()
        
        existingRecord = registryContainer.find_one({"_id":hash_id},{"Sources":1})
        
        if existingRecord is None:
            newCitation = {}
            newCitation["_id"] = hash_id
            newCitation["Citation String"] = citationString
            newCitation["Sources"] = [{"source":source,"date":datetime.utcnow().isoformat()}]
            if url is not None:
                newCitation["url"] = url
            registryContainer.insert_one(newCitation)
            result = {"status":"ok","_id":hash_id,"message":"New citation registered."}
        else:
            existingSource = [s for s in existingRecord["Sources"] if s["source"] == source]
            if len(existingSource) > 0:
                result = {"status":"failed","_id":existingRecord["_id"],"message":"Citation string was already registered for this source."}
            else:
                newSources = existingRecord["Sources"]
                newSources.append({"source":source,"date":datetime.utcnow().isoformat()})
                registryContainer.update({"_id":existingSource["_id"]},{"$set":{"Sources":newSources}})
                result = {"status":"ok","_id":existingRecord["_id"],"message":"Citation string already registered; new source added."}

        return result

    
    def ref_link_data(url):
        import requests
        from datetime import datetime
        
        response = {"Date Checked":datetime.utcnow().isoformat(),"Link Checked":url}
        
        try:
            response["Link Response"] = requests.get(url, headers={"Accept":"application/json"}).json()
            response["Success"] = True
        except:
            response["Success"] = False
        
        return response
    
    
    def lookup_crossref(citation,threshold=60):
        """
        Pass a citation or fragments of a citation.  If crossref score > 60 then accept entry as correct citation.
        :param citation: user defined citation or fragments of a citation, this does not need to be in a specific format
        :param threshold: default 60 based on some of Sky's initial exploration, as more extensive testing occurs we may want to adjust this default value
        :return: JSON block of crossref metadata
        """
        
        import requests
        from datetime import datetime

        cross_ref_doc = {"Success":False,"Date Checked":datetime.utcnow().isoformat()}

        cross_ref_work_api = "https://api.crossref.org/works"
        mail_to = "bcb@usgs.gov"

        cross_ref_query = cross_ref_work_api+"?mailto="+mail_to+"&query.bibliographic="+citation
        cross_ref_doc["Query URL"] = cross_ref_query
        cross_ref_results = requests.get(cross_ref_query).json()

        if cross_ref_results["status"] != "failed" and "items" in cross_ref_results["message"].keys() and len(cross_ref_results["message"]["items"]) > 0 and cross_ref_results["message"]["items"][0]["score"] >= threshold:
            cross_ref_doc["Success"] = True
            cross_ref_doc["Score"] = cross_ref_results["message"]["items"][0]["score"]
            cross_ref_doc["Record"] = cross_ref_results["message"]["items"][0]

        return cross_ref_doc
    
   
    def lookup_scopus_by_doi(doi):
        import requests
        import os
        
        result = requests.get("https://api.elsevier.com/content/search/scopus?apiKey="+os.environ["SCOPUSKEY"]+"&query=doi("+doi+")", headers={"Accept":"application/json"}).json()
        return result

    
    def scopus_citations_by_doi(doi):
        import requests
        import os

        result = requests.get("https://api.elsevier.com/content/abstract/citations?apiKey="+os.environ["SCOPUSKEY"]+"&doi="+doi, headers={"Accept":"application/json"}).json()
        return result
