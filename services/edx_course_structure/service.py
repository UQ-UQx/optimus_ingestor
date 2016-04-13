"""
Service for copying the json course structure to the pointed place
"""
import base_service
import os
import utils
import shutil

class EdxCourseStructure(base_service.BaseService):
	"""
	Copy the {org}-{course}-{date}-course_structure-{site}-analytics.json files to the directory pointed by self.outputdir
	"""
	inst = None
		
	def __init__(self):
		EdxCourseStructure.inst = self
		super(EdxCourseStructure, self).__init__()
		
		self.pretty_name = "edX Course Structure"
		self.enabled = True
		self.loop = True
		self.sleep_time = 60
		self.outputdir = 'www/edx_course_structure'
		self.initialize()
		
	def setup(self):
		pass
  		
	def run(self):
		ingests = self.get_ingests()
		#print 'run edx_cs'
		#print ingests
		for ingest in ingests:
		    if ingest['type'] == 'file':
		        self.start_ingest(ingest['id'])
		        shutil.copy2(ingest['meta'], self.outputdir)
		        self.finish_ingest(ingest['id'])		        
		pass	
		
		
def get_files(path):
    """
    Returns a list of files that the service will ingest
    :param path: The path of the files
    :return: An array of file paths
    """
    required_files = []
    main_path = os.path.realpath(os.path.join(path, 'database_state', 'latest'))
    #print 'main_path'
    #print main_path
    for file in os.listdir(main_path):
      if os.path.isfile(os.path.join(main_path, file)) and file.endswith("-analytics.json"):
          required_files.append(os.path.join(main_path, file))
            
    #print 'required_files'
    #print required_files        
    return required_files


def name():
    """
    Returns the name of the service class
    """
    return "EdxCourseStructure"	


def service():
    """
    Returns an instance of the service
    """
    return EdxCourseStructure()	
