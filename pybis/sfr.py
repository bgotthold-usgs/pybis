import requests
import math
import zipfile
import os
import pysb
from osgeo import ogr, osr
import shutil

"""SFR pipeline tools.

This class contains tools to download a zip file containing spatial files
from ScienceBase, extract those files, create a postgis table from them,
and ping the pg2elastic microservice to index that table.

These OS environment variables must be set if they are something other than
the defaults:

DB_DATABASE
POSTGIS_SERVER
POSTGIS_PORT
DB_USERNAME
PG_TO_ELASTIC
API_TOKEN
"""


class SfrPipeline:
    def __init__(self, item_id, table, srid, zipfile_title, schema="sfr", overwrite_existing_table="No",
                 flip_coordinates=False, custom_encoding=None, spatial_file_type=None, batch_size=5, make_valid=False):
        """
        :param item_id: ScienceBase item ID of item with zipfile containing either a shape or geojson file --required
        :param schema: Name of the schema to add the new table to (must already exist, for now use sfr)
        :param table: Name of table to create -- required
        :param srid: The SRID of the geospatial data -- required
        :param zipfile_title: title of the target zipfile in the ScienceBase item -- required
        :param overwrite_existing_table: Overwrite table if it already exists. "Yes" or "No"
        :param flip_coordinates: Some of the files with point geometry need to have their lat/lng's swapped
        :param custom_encoding: A significant number of the shape files in SB have needed a "LATIN1" encoding
        :param spatial_file_type: ".geojson" or ".shp" -- If nothing is specified, it will grab whatever one is there
        :param batch_size: Number of geometries to be indexed at a time. 5 is safe, can be increases for point geoms
        :param make_valid: Run ST_MakeValid on the geoms before sending the geojson to ElasticSearch
        """
        if item_id is None or table is None or srid is None or zipfile_title is None:
            raise Exception("Missing one of the required params: item_id, table, srid, or zipfile_title")

        self.description = "Set of functions for adding data to the SFR"
        self.item_id = item_id
        self.schema = schema
        self.table = table
        self.srid = srid
        self.zip_file = None
        self.directory = None
        self.overwrite_existing_table = overwrite_existing_table
        self.flip_coordinates = flip_coordinates
        self.custom_encoding = custom_encoding
        self.database = os.getenv("DB_DATABASE", "bis")
        self.postgis_server = os.getenv("POSTGIS_SERVER", "localhost")
        self.postgis_port = os.getenv("POSTGIS_PORT", "5432")
        self.db_user = os.getenv("DB_USERNAME", "postgres")
        self.db_password = os.getenv("DB_PASSWORD", "admin")
        self.spatial_file_type = spatial_file_type
        self.pg2elastic = os.getenv("PG_TO_ELASTIC", "http://localhost:8090")
        self.api_token = os.getenv("API_TOKEN", "token1234")
        self.batch_size = batch_size
        self.make_valid = make_valid
        self.zipfile_title = zipfile_title
        self.spatial_file_list = []

    def get_zip_file(self, item):
        """
        Grab the first zip file from the item. This can be improved moving forward
        :param item: JSON of ScienceBase item
        :return: JSON block of zipfile in SB item, or None
        """
        try:
            for file in item["files"]:
                if file["contentType"] == "application/zip" and file["title"] == self.zipfile_title:
                    return file
        except:
            print("Error getting zip file from ScienceBase item: %s" % (item["id"]))
            raise

        # If it gets here, no zipfile was found
        raise Exception("No zip file found in ScienceBase item with title: %s" % self.zipfile_title)

    def download_file(self, url, size=1):
        """
        Download the zip file as a stream
        :param url: URL of the zipfile download
        :param size: Size of the zipfile (grabbed from the item JSON)
        :return: None
        """
        current = 0
        chunk_size = 1024
        printed = []
        print("Downloading file", flush=True)

        r = requests.get(url, stream=True)

        with open(self.zip_file, 'wb') as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                current = current + chunk_size
                percent = math.floor(current / size * 100)
                if percent not in printed and percent % 10 == 0:
                    printed.append(percent)
                    print("Current: %d%%" % percent, flush=True)
                if chunk:
                    f.write(chunk)

    def extract_zip_file(self):
        """
        Extract the contents of the zip file
        :return: None
        """
        print("Extracting zip file", flush=True)
        self.directory = self.zip_file[0:-4]
        zip_file = zipfile.ZipFile(self.zip_file, 'r')
        zip_file.extractall(self.directory)
        zip_file.close()

    def get_zip_file_and_extract(self):
        """
        Get item JSON, download zipfile, and extract it in current working directory
        :return:
        """
        sb = pysb.SbSession()
        item = sb.get_item(self.item_id)
        zip_file = self.get_zip_file(item)
        download_uri = zip_file["downloadUri"]
        file_size = zip_file["size"]

        if download_uri is not None:
            self.zip_file = self.item_id + zip_file["name"]
            self.download_file(download_uri, file_size)
            self.extract_zip_file()
        else:
            raise Exception("No URI was found for zipfile download")

    def set_spatial_file_shape_file(self):
        """
        Set the spatial file with the name of the first shape file in the extracted directory
        :return: None
        """
        for file in os.listdir(self.directory):
            if file.endswith(".shp"):
                self.spatial_file_list.append(os.path.join(self.directory, file))

    def set_spatial_file_geojson(self):
        """
        Set the spatial file with the name of the first geojson file in the extracted directory
        :return: None
        """
        for file in os.listdir(self.directory):
            if file.endswith(".geojson"):
                self.spatial_file_list.append(os.path.join(self.directory, file))

    def set_spatial_file_type(self, spatial_file_type):
        """
        Set the spatial file with the name of the first specified file type in the extracted directory
        :param spatial_file_type: File type -- ".geojson" or ".shp" for example
        :return: None
        """
        for file in os.listdir(self.directory):
            if file.endswith(spatial_file_type):
                self.spatial_file_list.append(os.path.join(self.directory, file))

    def create_layer_from_definition(self, ogr_db, layer_definition, geom_type):
        """
        Create the postgres table definition from the columns in the shape file
        :param ogr_db: Postgis database connection
        :param layer_definition: Spatial data from file
        :param geom_type: Geometry type (Polygon, Point, etc.) -- Polygons are converted to MultiPolygons below
        :return: Reference to the table created in Postgis
        """
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(self.srid)
        db_layer = ogr_db.CreateLayer(self.schema + '.' + self.table, srs,
                                      geom_type,
                                      ['OVERWRITE=' + self.overwrite_existing_table])
        for i in range(layer_definition.GetFieldCount()):
            db_layer.CreateField(layer_definition.GetFieldDefn(i))
        return db_layer

    def copy_features(self, src_layer, dest_layer, start_count):
        """
        Iterate through each feature, converting polygons to multipolygons if needed then add them to the postgis table
        :param src_layer: Source of spatial data
        :param dest_layer: Table to add geom to
        :return: None
        """
        src_len = len(src_layer)
        total = start_count
        for x in range(src_len):
            total = total + 1
            if total % 100 == 0:
                print(total, flush=True)
            feature = src_layer[x]
            geom = feature.GetGeometryRef()

            if self.flip_coordinates and geom.GetGeometryType() == ogr.wkbPoint:
                x = geom.GetX(0)
                y = geom.GetY(0)
                geom.SetPoint_2D(0, y, x)
            elif geom.GetGeometryType() == ogr.wkbPolygon:
                feature.SetGeometryDirectly(ogr.ForceToMultiPolygon(geom))
            feature.SetFID(total)
            dest_layer.CreateFeature(feature)
        return total

    @staticmethod
    def get_wkb_type(src_layer):
        """
        Get geometry type of spatial data
        :param src_layer: Source spatial file
        :return: Geometry type
        """
        first_obj = src_layer.GetFeature(0)
        return first_obj.GetGeometryRef().GetGeometryType()

    def create_table_from_spatial_file(self):
        """
        Import spatial file into postgis
        :return: None
        """
        if self.custom_encoding is not None:
            os.environ["PGCLIENTENCODING"] = self.custom_encoding
        else:
            os.environ["PGCLIENTENCODING"] = ""

        ogr_sf = None
        ogr_db = None

        try:
            connection_string = "dbname='%s' host='%s' port='%s' user='%s' password='%s'" % (
                self.database,
                self.postgis_server,
                self.postgis_port,
                self.db_user,
                self.db_password
            )

            # Create ogr object for postgis
            ogr_db = ogr.Open("PG:" + connection_string)
            first_layer = True
            db_layer = None
            block = 0
            for spatial_file in self.spatial_file_list:
                # Create ogr object from shape file
                ogr_sf = ogr.Open(spatial_file)
                shape_file_layer = ogr_sf.GetLayer(0)

                if first_layer:
                    first_layer = False
                    layer_definition = shape_file_layer.GetLayerDefn()
                    wkb_type = self.get_wkb_type(shape_file_layer)
                    if wkb_type == ogr.wkbPolygon:
                        wkb_type = ogr.wkbMultiPolygon
                    db_layer = self.create_layer_from_definition(
                        ogr_db,
                        layer_definition,
                        wkb_type
                    )

                block = self.copy_features(shape_file_layer, db_layer, block)
                ogr_db.SyncToDisk()
                ogr_sf.Destroy()
        except:
            # Close connections before raising exception
            if ogr_sf is not None:
                ogr_sf.Destroy()
            if ogr_db is not None:
                ogr_db.Destroy()
            self.clean_up_files()
            print("This is the error")
            raise

        # Close connection
        ogr_db.Destroy()

    def clean_up_files(self):
        """
        Remove zip file and directory from extracted zip
        :return: None
        """
        os.remove(self.zip_file)
        shutil.rmtree(self.directory)

    def check_and_set_spatial_file_type(self):
        """
        Set spatial file type using helper function
        :return: None
        """
        if self.spatial_file_type:
            self.set_spatial_file_type(self.spatial_file_type)
        else:
            self.set_spatial_file_type(".geojson")
            if not self.spatial_file_list:
                self.set_spatial_file_type(".shp")

    def add_index_job_to_queue(self):
        """
        Ping pg2elastic microservice to index postgis table
        :return: JSON of response from microservice
        """
        pg2elastic = self.pg2elastic + "/elastic/reindex"
        pg2elastic += "?database=%s" % self.database
        pg2elastic += "&token=%s" % self.api_token
        pg2elastic += "&schema=%s" % self.schema
        pg2elastic += "&table=%s" % self.table
        pg2elastic += "&docSize=%s" % self.batch_size

        if self.make_valid:
            pg2elastic += "&makeValid=true"

        return requests.request(method='get', url=pg2elastic).json()

    def spatial_file_to_postgis(self):
        """
        Run the full process of adding the SB item's spatial file to Postgis
        :return: None
        """
        self.get_zip_file_and_extract()
        self.check_and_set_spatial_file_type()

        if not self.spatial_file_list:
            self.clean_up_files()
            raise Exception("No spatial file found in extracted zip")

        self.create_table_from_spatial_file()
        self.clean_up_files()

    def run_full_pipeline(self):
        """
        Run through full shape file -> postgis -> elasticsearch pipeline
        :return:
        """
        self.spatial_file_to_postgis()
        self.add_index_job_to_queue()


# This is an example working usage:

# obj = sfr.SfrPipeline(item_id="5b7611bee4b0f5d5787feb66",
#                   table="my_test_sfr_table",
#                   srid=5070,
#                   zipfile_title="Source Data",
#                   overwrite_existing_table="No",
#                   flip_coordinates=False,
#                   custom_encoding=None,
#                   spatial_file_type=".geojson")
#
# obj.spatial_file_to_postgis()
