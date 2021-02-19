# =================================================================
#
# Authors: Tom Kralidis <tomkralidis@gmail.com>
#
# Copyright (c) 2018 Tom Kralidis
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

from collections import OrderedDict
import csv
import itertools
import logging

from h3 import h3

from pygeoapi.provider.base import (
    BaseProvider,
    ProviderQueryError,
    ProviderItemNotFoundError,
)

LOGGER = logging.getLogger(__name__)


class CSVH3Provider(BaseProvider):
    """CSV H3 provider"""

    def __init__(self, provider_def):
        """
        Initialize object

        :param provider_def: provider definition

        :returns: pygeoapi.provider.csv_h3.CSVH3Provider
        """

        BaseProvider.__init__(self, provider_def)
        # self.cell_id = provider_def['geometry']['cell_id']
        # can just use self.id_field ?
        self.fields = self.get_fields()

    def get_fields(self):
        """
         Get provider field information (names, types)

        :returns: dict of fields
        """

        LOGGER.debug("Treating all columns as string types")
        with open(self.data) as ff:
            LOGGER.debug("Serializing DictReader")
            data_ = csv.DictReader(ff)
            fields = {}
            for f in data_.fieldnames:
                fields[f] = "string"
            return fields

    def _load(
        self,
        startindex=0,
        limit=10,
        resulttype="results",
        identifier=None,
        bbox=[],
        datetime_=None,
        properties=[],
        select_properties=[],
        skip_geometry=False,
    ):
        """
        Load CSV data

        :param startindex: starting record to return (default 0)
        :param limit: number of records to return (default 10)
        :param datetime_: temporal (datestamp or extent)
        :param resulttype: return results or hit limit (default results)
        :param properties: list of tuples (name, value)
        :param select_properties: list of property names
        :param skip_geometry: bool of whether to skip geometry (default False)

        :returns: dict of GeoJSON FeatureCollection
        """

        found = False
        result = None
        feature_collection = {"type": "FeatureCollection", "features": []}

        with open(self.data) as ff:
            LOGGER.debug("Serializing DictReader")
            data_ = csv.DictReader(ff)
            if resulttype == "hits":
                LOGGER.debug("Returning hits only")
                feature_collection["numberMatched"] = len(list(data_))
                return feature_collection
            LOGGER.debug("Slicing CSV rows")
            for row in itertools.islice(data_, startindex, startindex + limit):
                feature = {"type": "Feature"}
                feature["id"] = row.pop(self.id_field)
                geom = h3.h3_to_geo_boundary(feature["id"], geo_json=True)
                if not skip_geometry:
                    feature["geometry"] = {"type": "Polygon", "coordinates": [geom]}
                else:
                    feature["geometry"] = None
                if self.properties or select_properties:
                    feature["properties"] = OrderedDict()
                    for p in set(self.properties) | set(select_properties):
                        try:
                            feature["properties"][p] = row[p]
                        except KeyError as err:
                            LOGGER.error(err)
                            raise ProviderQueryError()
                else:
                    feature["properties"] = row

                if identifier is not None and feature["id"] == identifier:
                    found = True
                    result = feature
                feature_collection["features"].append(feature)
                feature_collection["numberMatched"] = len(
                    feature_collection["features"]
                )

        if identifier is not None and not found:
            return None
        elif identifier is not None and found:
            return result

        feature_collection["numberReturned"] = len(feature_collection["features"])

        return feature_collection

    def query(
        self,
        startindex=0,
        limit=10,
        resulttype="results",
        bbox=[],
        datetime_=None,
        properties=[],
        sortby=[],
        select_properties=[],
        skip_geometry=False,
    ):
        """
        CSV query

        :param startindex: starting record to return (default 0)
        :param limit: number of records to return (default 10)
        :param resulttype: return results or hit limit (default results)
        :param bbox: bounding box [minx,miny,maxx,maxy]
        :param datetime_: temporal (datestamp or extent)
        :param properties: list of tuples (name, value)
        :param sortby: list of dicts (property, order)
        :param select_properties: list of property names
        :param skip_geometry: bool of whether to skip geometry (default False)

        :returns: dict of GeoJSON FeatureCollection
        """

        return self._load(
            startindex,
            limit,
            resulttype,
            select_properties=select_properties,
            skip_geometry=skip_geometry,
        )

    def get(self, identifier):
        """
        query CSV id

        :param identifier: feature id

        :returns: dict of single GeoJSON feature
        """
        item = self._load(identifier=identifier)
        if item:
            return item
        else:
            err = "item {} not found".format(identifier)
            LOGGER.error(err)
            raise ProviderItemNotFoundError(err)

    def __repr__(self):
        return "<CSVH3Provider> {}".format(self.data)
