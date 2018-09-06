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

    default_params = {
        'item_id':None,
        'table':None,
        'srid':None,
        'zipfile_title':None,
        'schema':"sfr",
        'overwrite_existing_table':"No",
        'flip_coordinates':False,
        'custom_encoding':None,
        'spatial_file_type':None,
        'fit_to_bounding_box':False,
        'rounding_precision':None,
        'clean_up_geom':False,
        'spatial_file_list': []
    }

    def __init__(self, *initial_data, **kwargs):
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
        :param fit_to_bounding_box: This is for lat/lng geoms whose lat's fall outside the +-90.0
        :param rounding_precision: Round geom points to this level of precision when fixing
        :param clean_up_geom: Send the geoms through the rigorous cleanup process
        """
        self.description = "Set of functions for adding data to the SFR"
        for key in self.default_params:
            setattr(self, key, self.default_params[key])
        for dictionary in initial_data:
            for key in dictionary:
                setattr(self, key, dictionary[key])
        for key in kwargs:
            setattr(self, key, kwargs[key])
        if self.item_id is None or self.table is None or self.srid is None or self.zipfile_title is None:
            raise Exception("Missing one of the required params: item_id, table, srid, or zipfile_title")

        self.pg2elastic = os.getenv("PG_TO_ELASTIC", "http://localhost:8090")
        self.api_token = os.getenv("API_TOKEN", "token1234")

        self.database = os.getenv("DB_DATABASE", "bis")
        self.postgis_server = os.getenv("POSTGIS_SERVER", "localhost")
        self.postgis_port = os.getenv("POSTGIS_PORT", "5432")
        self.db_user = os.getenv("DB_USERNAME", "postgres")
        self.db_password = os.getenv("DB_PASSWORD", "admin")

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
            if "Src_Date" != layer_definition.GetFieldDefn(i).GetName():
                if layer_definition.GetFieldDefn(i).GetType() == ogr.OFTReal:
                    layer_definition.GetFieldDefn(i).SetPrecision(6)
                db_layer.CreateField(layer_definition.GetFieldDefn(i))
            else:
                print("Got source date")
        return db_layer

    @staticmethod
    def poly_from_line(geom):
        num_geoms = geom.GetGeometryCount()
        poly = ogr.Geometry(ogr.wkbPolygon)
        rings = [None] * num_geoms
        for i in range(num_geoms):
            rings[i] = ogr.Geometry(ogr.wkbLinearRing)
            linestring = geom.GetGeometryRef(i)
            num_points = linestring.GetPointCount()
            for idx in range(num_points):
                point = linestring.GetPoint(idx)
                rings[i].AddPoint(point[0], point[1])
        for ring in rings:
            if ring.Area() < .0001:
                print("Area too small!")
            else:
                poly.AddGeometry(ring)

        poly.CloseRings()
        return poly

    @staticmethod
    def fit_geom_to_bounding_box(geom):
        nbr_rings = geom.GetGeometryCount()
        for i in range(nbr_rings):
            top_geom = geom.GetGeometryRef(i)
            actual_rings = top_geom.GetGeometryCount()
            for idx in range(actual_rings):
                geom_ring = top_geom.GetGeometryRef(idx)
                nbr_points = geom_ring.GetPointCount()
                for pt_idx in range(nbr_points):
                    if geom_ring.GetPoint(pt_idx)[1] < -90.0:
                        print("Too small")
                        geom_ring.SetPoint(pt_idx, geom_ring.GetPoint(pt_idx)[0], -90.0)
                    elif geom_ring.GetPoint(pt_idx)[1] > 90.0:
                        print("Too big")
                        geom_ring.SetPoint(pt_idx, geom_ring.GetPoint(pt_idx)[0], 90.0)
        return geom

    def fix_geometry(self, geom, num):
        num_polies = geom.GetGeometryCount()
        multipolygon = ogr.Geometry(ogr.wkbMultiPolygon)
        for n in range(num_polies):
            geo = geom.GetGeometryRef(n)
            num_rings = geo.GetGeometryCount()
            poly = ogr.Geometry(ogr.wkbPolygon)
            for r_idx in range(num_rings):
                ring = geo.GetGeometryRef(r_idx)

                if ring.Area() > 0.0001:
                    point_map = {}
                    new_ring = ogr.Geometry(ogr.wkbLinearRing)
                    rp_count = ring.GetPointCount()
                    for rp in range(rp_count):
                        next_point = ring.GetPoint(rp)
                        new_point = (next_point[0], next_point[1], 0)
                        if self.rounding_precision:
                            new_point = (
                                round(next_point[0], self.rounding_precision),
                                round(next_point[1], self.rounding_precision),
                                0)
                        if new_point not in point_map:
                            new_ring.AddPoint(new_point[0], new_point[1])
                            point_map[new_point] = True

                    last_point = new_ring.GetPoint(new_ring.GetPointCount() - 1)
                    first_point = new_ring.GetPoint(0)
                    if first_point != last_point:
                        new_ring.AddPoint(first_point[0], first_point[1])
                    poly.AddGeometry(new_ring)

            if poly.IsEmpty():
                print(num, "It's empty!!!!!!!!")
            else:
                multipolygon.AddGeometry(poly)
        return multipolygon

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
            out_layer_defn = dest_layer.GetLayerDefn()
            geom = feature.GetGeometryRef()
            out_feature = ogr.Feature(out_layer_defn)

            for i in range(out_layer_defn.GetFieldCount()):
                field_defn = out_layer_defn.GetFieldDefn(i)
                field_name = field_defn.GetName()
                field_type = field_defn.GetType()
                field = feature.GetField(field_name)

                if field_type == ogr.OFTReal:
                    field = round(field, 6)

                out_feature.SetField(out_layer_defn.GetFieldDefn(i).GetNameRef(), field)

            if self.flip_coordinates and geom.GetGeometryType() == ogr.wkbPoint:
                x = geom.GetX(0)
                y = geom.GetY(0)
                geom.SetPoint_2D(0, y, x)
            else:
                geom = geom.SimplifyPreserveTopology(0)
                geom.FlattenTo2D()
                if geom.GetGeometryType() == ogr.wkbPolygon:
                    geom = ogr.ForceToMultiPolygon(geom)
                if self.clean_up_geom:
                    geom = self.fix_geometry(geom, total)
                if self.fit_to_bounding_box:
                    geom = self.fit_geom_to_bounding_box(geom)
            out_feature.SetGeometryDirectly(geom)
            out_feature.SetFID(total)
            dest_layer.CreateFeature(out_feature)
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

            # This is for testing
            # last = len(self.spatial_file_list)
            # self.spatial_file_list = self.spatial_file_list[last-5:last]

            for spatial_file in self.spatial_file_list:
                # Create ogr object from shape file
                ogr_sf = ogr.Open(spatial_file)
                shape_file_layer = ogr_sf.GetLayer(0)

                print("CRS:", shape_file_layer.GetSpatialRef())

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

    def add_index_job_to_queue(self,
                               schema,
                               table,
                               batch_size=5,
                               make_valid=False,
                               reverse_orientation=False,
                               primary_key=None):
        """
        Ping pg2elastic microservice to index postgis table
        :return: JSON of response from microservice
        """
        pg2elastic = self.pg2elastic + "/elastic/reindex"
        pg2elastic += "?database=%s" % self.database
        pg2elastic += "&token=%s" % self.api_token
        pg2elastic += "&schema=%s" % schema
        pg2elastic += "&table=%s" % table
        pg2elastic += "&docSize=%d" % batch_size

        if make_valid:
            pg2elastic += "&makeValid=true"

        if reverse_orientation:
            pg2elastic += "&reverseOrientation=true"

        if primary_key:
            pg2elastic += "&primaryKey=%s" % primary_key

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
