"""
Service for parsing the XML course structure
"""
import base_service
import os
import utils
import json
import xml.etree.ElementTree as ElTree


class CourseStructure(base_service.BaseService):
    """
    Converts the XML course structure to JSON
    """

    inst = None

    def __init__(self):
        CourseStructure.inst = self
        super(CourseStructure, self).__init__()

        #The pretty name of the service
        self.pretty_name = "Course Structure"
        #Whether the service is enabled
        self.enabled = True
        #Whether to run more than once
        self.loop = True
        #The amount of time to sleep in seconds
        self.sleep_time = 60

        #The location to output course structure
        self.outputdir = 'www/course_structure'

        self.initialize()

    pass

    def setup(self):
        """
        Set initial variables before the run loop starts
        """
        pass

    def run(self):
        """
        Runs every X seconds, the main run loop
        """
        ingests = self.get_ingests()
        for ingest in ingests:
            if ingest['type'] == 'file':

                self.start_ingest(ingest['id'])

                path = ingest['meta']

                coursename = os.path.basename(os.path.normpath(path))

                coursesplit = coursename.split("-")
                term = coursesplit[-1]
                #Parse the course
                coursefile = os.path.join(path, 'course', term + '.xml')
                course = self.xml_unpack_file(coursefile)
                course = self.add_linked_file_xml(path, course)
                policyfileurl = os.path.join(path, 'policies', term, 'policy.json')
                policyfile = open(policyfileurl).read()
                policydata = json.loads(policyfile)
                course['policy'] = policydata
                f = open(self.outputdir+'/'+coursename+'.json', 'w+')
                print self.outputdir+'/'+coursename+'.json'
                f.write(json.dumps(course))

                utils.log("Parsed "+coursename)

                self.finish_ingest(ingest['id'])

    def xml_unpack_file(self, filename):
        """
        Unpacks an XML file into a python object
        :param filename: The file of the XML file
        :return: A python object representation of the XML file
        """
        return self.xml_unpack(ElTree.parse(filename))

    def xml_unpack(self, tree):
        """
        Unpacks an XML tree into a python object
        :param tree: The tree to parse
        :return: Unpacked XML Element
        """
        return self.xml_unpackelement(tree.getroot())

    def xml_unpackelement(self, el):
        """
        Recursively turns XML into a nested object
        :param el: The element
        :return: A python object
        """
        obj = {'children': [], 'tag': el.tag}
        for attrib_name in el.attrib:
            obj[attrib_name] = el.attrib[attrib_name]
        for child in el:
            obj['children'].append(self.xml_unpackelement(child))
        return obj

    def add_linked_file_xml(self, basepath, xml_object):
        """
        Find linked XML files
        :param basepath: The base path
        :param xml_object: The currently parsing object
        :return: A parsed object
        """
        if len(xml_object['children']) > 0:
            index = 0
            for child in xml_object['children']:
                if len(child['children']) == 0 and 'url_name' in child:
                    child_path = os.path.join(basepath, child['tag'], child['url_name']+'.xml')
                    if os.path.isfile(child_path):
                        child_obj = (self.xml_unpack_file(child_path))
                        for key in child_obj:
                            child[key] = child_obj[key]
                        xml_object['children'][index] = self.add_linked_file_xml(basepath, child)
                index += 1
        return xml_object


def get_files(path):
    """
    Returns a list of files that the service will ingest
    :param path: The path of the files
    :return: An array of file paths
    """
    required_files = []
    main_path = os.path.realpath(os.path.join(path, 'database_state', 'latest'))
    for subdir in os.listdir(main_path):
        if os.path.isdir(os.path.join(main_path, subdir)):
            required_files.append(os.path.join(main_path, subdir))
    return required_files


def name():
    """
    Returns the name of the service class
    """
    return "CourseStructure"


def service():
    """
    Returns an instance of the service
    """
    return CourseStructure()