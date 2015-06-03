"""
A service for generating person course derived datasets
"""
import base_service
import os
import utils
import MySQLdb
import config
import warnings
import dateutil.parser
import math
from models import PCModel
from models import CFModel
import urllib2
import json
import time
import datetime
import csv
from pymongo import MongoClient


class PersonCourse(base_service.BaseService):
    """
    Generates derived datasets for each person and each course
    """

    inst = None

    def __init__(self):
        PersonCourse.inst = self
        super(PersonCourse, self).__init__()

        #The pretty name of the service
        self.pretty_name = "Person Course"
        #Whether the service is enabled
        self.enabled = True
        #Whether to run more than once
        self.loop = True
        #The amount of time to sleep in seconds
        self.sleep_time = 60

        self.pc_table = 'personcourse'
        self.cf_table = 'courseprofile'
        self.pc_db = 'Person_Course'

        self.sql_pc_conn = None
        self.sql_course_conn = None

        #Vars
        self.mongo_client = None
        self.mongo_db = None
        self.mongo_dbname = ""
        self.mongo_collection = None
        self.mongo_collectionname = ""
        self.courses = {}

        self.initialize()

    pass

    def setup(self):
        """
        Set initial variables before the run loop starts
        """
        self.courses = self.get_all_courses()
        self.sql_pc_conn = self.connect_to_sql(self.sql_pc_conn, "Person_Course", True)
        self.sql_course_conn = self.connect_to_sql(self.sql_course_conn, "", True)
        pass

    def run(self):
        """
        Runs every X seconds, the main run loop
        """
        last_run = self.find_last_run_ingest("PersonCourse")
        last_timefinder = self.find_last_run_ingest("TimeFinder")
        last_iptocountry = self.find_last_run_ingest("IpToCountry")
        last_dbstate = self.find_last_run_ingest("DatabaseState")
        if self.finished_ingestion("TimeFinder") and last_run < last_timefinder and self.finished_ingestion("IpToCountry") and last_run < last_iptocountry and self.finished_ingestion("DatabaseState") and last_run < last_dbstate:
            # Create 'cf_table'
            self.create_cf_table()
            # Clean 'pc_table'
            self.clean_pc_db()

            for course_id, course in self.courses.items():

                # Get chapters from course info
                json_file = course['dbname'].replace("_", "-") + '.json'
                courseinfo = self.loadcourseinfo(json_file)
                if courseinfo is None:
                    utils.log("Can not find course info for ." + str(course_id))
                    continue

                cf_item = CFModel(course_id, course['dbname'], course['mongoname'], course['discussiontable'])
                # Set cf_item course_launch_date
                bad_start = False
                if 'start' in courseinfo:
                    try:
                        course_launch_time = dateutil.parser.parse(courseinfo['start'].replace('"', ""))
                        course_launch_date = course_launch_time.date()
                        cf_item.set_course_launch_date(course_launch_date)
                    except Exception:
                        print "ERROR: BAD COURSE CODE START DATE"
                        print courseinfo['start']
                        print type(courseinfo['start'])
                        bad_start = True
                else:
                    utils.log("Course not started for " + course_id)
                    continue
                if bad_start:
                    continue
                # Set cf_item course_close_date
                if 'end' in courseinfo:
                    try:
                        course_close_time = dateutil.parser.parse(courseinfo['end'])
                        course_close_date = course_close_time.date()
                        cf_item.set_course_close_date(course_close_date)
                    except ValueError:
                        pass
                # Set cf_item course_length
                if course_launch_date and course_close_date:
                    date_delta = course_close_date - course_launch_date
                    cf_item.set_course_length(math.ceil(date_delta.days/7.0))

                # Set cf_item nchapters
                chapters = []
                chapters = self.get_chapter(courseinfo, chapters)
                nchapters = len(chapters)
                cf_item.set_nchapters(nchapters)
                half_chapters = math.ceil(float(nchapters) / 2)

                # Set cf_item nvideos, nhtmls, nassessments, nsummative_assessments, nformative_assessments, nincontent_discussions, nactivities
                content = self.analysis_chapters(chapters)
                cf_item.set_nvideos(content['nvideos'])
                cf_item.set_nhtmls(content['nhtmls'])
                cf_item.set_nassessments(content['nassessments'])
                cf_item.set_nsummative_assessments(content['nsummative_assessments'])
                cf_item.set_nformative_assessments(content['nformative_assessments'])
                cf_item.set_nincontent_discussions(content['nincontent_discussions'])
                cf_item.set_nactivities(content['nactivities'])

                # Create 'pc_table'
                self.create_pc_table()

                # Dict of items of personcourse, key is the user id
                pc_dict = {}

                # Select the database
                self.sql_course_conn.select_db(course['dbname'])
                course_cursor = self.sql_course_conn.cursor()

                # course_id for PCModel
                pc_course_id = course['mongoname']

                utils.log("LOADING COURSE {" + pc_course_id + "}")

                # find all user_id
                utils.log("{auth_user}")
                query = "SELECT id, is_staff FROM auth_user"
                course_cursor.execute(query)
                result = course_cursor.fetchall()
                for record in result:
                    pc_dict[record[0]] = PCModel(pc_course_id, record[0])
                    pc_dict[record[0]].set_roles(record[1])

                # The list of user_id
                user_id_list = pc_dict.keys()
                user_id_list.sort()
                #print user_id_list

                # Set LoE, YoB, gender based on the data in {auth_userprofile}
                utils.log("{auth_userprofile}")
                query = "SELECT user_id, year_of_birth, level_of_education, gender FROM auth_userprofile WHERE user_id in (" + ",".join(["%s"] * len(user_id_list)) + ")"
                query = query % tuple(user_id_list)
                course_cursor.execute(query)
                result = course_cursor.fetchall()
                for record in result:
                    user_id = int(record[0])
                    pc_dict[user_id].set_YoB(record[1])
                    pc_dict[user_id].set_LoE(record[2])
                    pc_dict[user_id].set_gender(record[3])

                # Set certified based on the data in {certificates_generatedcertificate}
                utils.log("{certificates_generatedcertificate}")
                query = "SELECT user_id, grade, status FROM certificates_generatedcertificate WHERE user_id in (" + ",".join(["%s"] * len(user_id_list)) + ")"
                query = query % tuple(user_id_list)
                course_cursor.execute(query)
                result = course_cursor.fetchall()
                for record in result:
                    user_id = int(record[0])
                    pc_dict[user_id].set_grade(float(record[1]))
                    pc_dict[user_id].set_certified(record[2])

                # Set start_time based on the data in {student_courseenrollment}
                utils.log("{student_courseenrollment}")
                query = "SELECT user_id, created, mode FROM student_courseenrollment WHERE user_id in (" + ",".join(["%s"] * len(user_id_list)) + ")"
                query = query % tuple(user_id_list)
                course_cursor.execute(query)
                result = course_cursor.fetchall()
                nhonor = 0
                naudit = 0
                nvertified = 0
                registration_open_date = datetime.date.today()
                for record in result:
                    user_id = int(record[0])
                    start_time = record[1]  # dateutil.parser.parse(record[1])
                    start_date = start_time.date()
                    pc_dict[user_id].set_start_time(start_date)
                    pc_dict[user_id].set_mode(record[2])
                    if record[2] == 'honor':
                        nhonor += 1
                    if record[2] == 'audit':
                        naudit += 1
                    if record[2] == 'verified':
                        nvertified += 1
                    if start_date < registration_open_date:
                        registration_open_date = start_date
                # Set cf_item nhonor_students, naudit_students, nvertified_students, registration_open_date
                cf_item.set_nhonor_students(nhonor)
                cf_item.set_naudit_students(naudit)
                cf_item.set_nvertified_students(nvertified)
                cf_item.set_registration_open_date(registration_open_date)

                # Set ndays_act and viewed based on the data in {courseware_studentmodule}
                try:
                    utils.log("{ndays_act: courseware_studentmodule}")
                    query = "SELECT student_id, COUNT(DISTINCT SUBSTRING(created, 1, 10)) FROM courseware_studentmodule GROUP BY student_id"
                    course_cursor.execute(query)
                    result = course_cursor.fetchall()
                    for record in result:
                        user_id = int(record[0])
                        if user_id in pc_dict:
                            pc_dict[user_id].set_ndays_act(record[1])
                            if record[1] > 0:
                                pc_dict[user_id].set_viewed(1)
                        else:
                            utils.log("Student id: %s does not exist in {auth_user}." % user_id)
                except self.sql_pc_conn.ProgrammingError:
                    utils.log("Couldnt find courseware_studentmodule for " + course_id)
                    continue

                # Set attempted problems
                utils.log("{attempted_problems: courseware_studentmodule}")
                query = "SELECT student_id, COUNT(state) FROM courseware_studentmodule WHERE state LIKE '%correct_map%' GROUP BY student_id"
                course_cursor.execute(query)
                result = course_cursor.fetchall()
                for record in result:
                    user_id = int(record[0])
                    if user_id in pc_dict:
                        pc_dict[user_id].set_attempted_problems(record[1])
                    else:
                        utils.log("Student id: %s does not exist in {auth_user}." % user_id)

                # Set nplay_video based on the data in {courseware_studentmodule}
                utils.log("{nplay_video: courseware_studentmodule}")
                query = "SELECT student_id, COUNT(*) FROM courseware_studentmodule WHERE module_type = 'video' GROUP BY student_id"
                course_cursor.execute(query)
                result = course_cursor.fetchall()
                for record in result:
                    user_id = int(record[0])
                    if user_id in pc_dict:
                        pc_dict[user_id].set_nplay_video(record[1])

                # Set nchapters and explored based on the data in {courseware_studentmodule}
                utils.log("{nchapters: courseware_studentmodule}")
                query = "SELECT student_id, COUNT(DISTINCT module_id) FROM courseware_studentmodule WHERE module_type = 'chapter' GROUP BY student_id"
                course_cursor.execute(query)
                result = course_cursor.fetchall()
                for record in result:
                    user_id = int(record[0])
                    if user_id in pc_dict:
                        pc_dict[user_id].set_nchapters(record[1])
                        if record[1] >= half_chapters:
                            pc_dict[user_id].set_explored(1)
                        else:
                            pc_dict[user_id].set_explored(0)

                # Mongo
                # Discussion forum
                utils.log("{discussion_forum}")
                self.mongo_dbname = "discussion_forum"
                self.mongo_collectionname = course['discussiontable']
                self.connect_to_mongo(self.mongo_dbname, self.mongo_collectionname)

                user_posts = self.mongo_collection.aggregate([
                    #{"$match": {"author_id": {"$in": user_id_list}}},
                    {"$group": {"_id": "$author_id", "postSum": {"$sum": 1}}}
                ])['result']

                for item in user_posts:
                    user_id = int(item["_id"])
                    if user_id in pc_dict:
                        pc_dict[user_id].set_nforum_posts(item['postSum'])
                    else:
                        utils.log("Author id: %s does not exist in {auth_user}." % user_id)

                # Tracking logs
                utils.log("{logs}")
                self.mongo_dbname = "logs"
                self.mongo_collectionname = "clickstream"
                #self.mongo_collectionname = "clickstream_hypers_301x_sample"
                self.connect_to_mongo(self.mongo_dbname, self.mongo_collectionname)

                user_events = self.mongo_collection.aggregate([
                    {"$match": {"context.course_id": pc_course_id}},
                    {"$sort": {"time": 1}},
                    {"$group": {"_id": "$context.user_id", "countrySet": {"$addToSet": "$country"}, "eventSum": {"$sum": 1}, "last_event": {"$last": "$time"}}}
                ], allowDiskUse=True)['result']

                for item in user_events:
                    user_id = item["_id"]
                    if user_id in pc_dict:
                        pc_dict[user_id].set_last_event(item["last_event"])
                        pc_dict[user_id].set_nevents(item["eventSum"])
                        pc_dict[user_id].set_final_cc_cname(item["countrySet"])
                    else:
                        utils.log("Context.user_id: %s does not exist in {auth_user}." % user_id)

                # Set cf_item nregistered_students, nviewed_students, nexplored_students, ncertified_students
                nregistered_students = sum(pc_item.registered for pc_item in pc_dict.values())
                nviewed_students = sum(pc_item.viewed for pc_item in pc_dict.values())
                nexplored_students = sum(pc_item.explored for pc_item in pc_dict.values())
                ncertified_students = sum(pc_item.certified for pc_item in pc_dict.values())
                cf_item.set_nregistered_students(nregistered_students)
                cf_item.set_nviewed_students(nviewed_students)
                cf_item.set_nexplored_students(nexplored_students)
                cf_item.set_ncertified_students(ncertified_students)

                pc_cursor = self.sql_pc_conn.cursor()
                #print cf_item
                cf_item.save2db(pc_cursor, self.cf_table)

                # Till now, data preparation for pc_tablex has been finished.
                # Check consistent then write them into the database.
                utils.log("save to {personcourse}")
                tablename = self.pc_table + "_" + course_id
                for user_id, user_data in pc_dict.items():
                    pc_dict[user_id].set_inconsistent_flag()
                    pc_dict[user_id].save2db(pc_cursor, tablename)

                self.sql_pc_conn.commit()

            self.datadump2csv()
            self.save_run_ingest()
            utils.log("Person course completed")

    # Connects to a Mongo Database
    def connect_to_mongo(self, db_name="", collection_name=""):
        """
        Connects to a mongo database
        :param db_name:
        :param collection_name:
        :return:
        """
        db_name = safe_name(db_name)
        try:
            if self.mongo_client is None:
                self.mongo_client = MongoClient('localhost', 27017)
            if db_name != "":
                self.mongo_db = self.mongo_client[db_name]
                if self.mongo_db:
                    self.mongo_dbname = db_name
                    if collection_name != "":
                        self.mongo_collection = self.mongo_db[collection_name]
                        if self.mongo_collection:
                            self.mongo_collectionname = collection_name
            return True
        except Exception, e:
            utils.log("Could not connect to MongoDB: %s" % e)
        return False


    def clean_pc_db(self):
        """
        Deletes the existing person course tables
        """
        pc_cursor = self.sql_pc_conn.cursor()
        warnings.filterwarnings('ignore', category=MySQLdb.Warning)
        for course_id, course in self.courses.items():
            pc_tablename = self.pc_table + "_" + course_id
            query = "DROP TABLE IF EXISTS %s" % pc_tablename
            pc_cursor.execute(query)

            query = "DELETE FROM %s WHERE course = '%s'" % (self.cf_table, course_id)
            pc_cursor.execute(query)
        self.sql_pc_conn.commit()
        warnings.filterwarnings('always', category=MySQLdb.Warning)
        utils.log(self.pc_db + " has been cleaned.")

    def create_cf_table(self):
        """
        Create the course profile table
        """
        tablename = "courseprofile"
        columns = [
            {"col_name": "id", "col_type": "int NOT NULL AUTO_INCREMENT PRIMARY KEY"},
            {"col_name": "course", "col_type": "varchar(255)"},
            {"col_name": "dbname", "col_type": "varchar(255)"},
            {"col_name": "mongoname", "col_type": "varchar(255)"},
            {"col_name": "discussiontable", "col_type": "varchar(255)"},
            {"col_name": "registration_open_date", "col_type": "date"},
            {"col_name": "course_launch_date", "col_type": "date"},
            {"col_name": "course_close_date", "col_type": "date"},
            {"col_name": "nregistered_students", "col_type": "int"},
            {"col_name": "nviewed_students", "col_type": "int"},
            {"col_name": "nexplored_students", "col_type": "int"},
            {"col_name": "ncertified_students", "col_type": "int"},
            {"col_name": "nhonor_students", "col_type": "int"},
            {"col_name": "naudit_students", "col_type": "int"},
            {"col_name": "nvertified_students", "col_type": "int"},
            {"col_name": "course_effort", "col_type": "float"},
            {"col_name": "course_length", "col_type": "int"},
            {"col_name": "nchapters", "col_type": "int"},
            {"col_name": "nvideos", "col_type": "int"},
            {"col_name": "nhtmls", "col_type": "int"},
            {"col_name": "nassessments", "col_type": "int"},
            {"col_name": "nsummative_assessments", "col_type": "int"},
            {"col_name": "nformative_assessments", "col_type": "int"},
            {"col_name": "nincontent_discussions", "col_type": "int"},
            {"col_name": "nactivities", "col_type": "int"},
            {"col_name": "best_assessment", "col_type": "varchar(255)"},
            #{"col_name": "worst_assessment", "col_type": "varchar(255)"},
        ]
        warnings.filterwarnings('ignore', category=MySQLdb.Warning)
        query = "CREATE TABLE IF NOT EXISTS " + tablename
        query += "("
        for column in columns:
            query += column['col_name'] + " " + column['col_type'] + ', '
        query += " worst_assessment varchar(255)"
        query += " );"
        cursor = self.sql_pc_conn.cursor()
        cursor.execute(query)
        warnings.filterwarnings('always', category=MySQLdb.Warning)

    # The function to create the table "personcourse".
    def create_pc_table(self):
        """
        Creates the person course table
        """
        cursor = self.sql_pc_conn.cursor()
        columns = [
            {"col_name": "id", "col_type": "int NOT NULL AUTO_INCREMENT PRIMARY KEY"},
            {"col_name": "course_id", "col_type": "varchar(255)"},
            {"col_name": "user_id", "col_type": "varchar(255)"},
            {"col_name": "registered", "col_type": "TINYINT(1) default 1"},
            {"col_name": "viewed", "col_type": "TINYINT(1)"},
            {"col_name": "explored", "col_type": "TINYINT(1)"},
            {"col_name": "certified", "col_type": "TINYINT(1)"},
            {"col_name": "final_cc_cname", "col_type": "varchar(255)"},
            {"col_name": "LoE", "col_type": "varchar(255)"},
            {"col_name": "YoB", "col_type": "year"},
            {"col_name": "gender", "col_type": "varchar(255)"},
            {"col_name": "mode", "col_type": "varchar(255)"},
            {"col_name": "grade", "col_type": "float"},
            {"col_name": "start_time", "col_type": "date"},
            {"col_name": "last_event", "col_type": "date"},
            {"col_name": "nevents", "col_type": "int"},
            {"col_name": "ndays_act", "col_type": "int"},
            {"col_name": "nplay_video", "col_type": "int"},
            {"col_name": "nchapters", "col_type": "int"},
            {"col_name": "nforum_posts", "col_type": "int"},
            {"col_name": "roles", "col_type": "varchar(255)"},
            {"col_name": "attempted_problems", "col_type": "int"},
            #{"col_name": "inconsistent_flag", "col_type": "TINYINT(1)"}
        ]
        for course_id, course in self.courses.items():
            warnings.filterwarnings('ignore', category=MySQLdb.Warning)
            pc_tablename = self.pc_table + "_" + course_id
            query = "CREATE TABLE IF NOT EXISTS " + pc_tablename
            query += " ("
            for column in columns:
                query += column["col_name"] + " " + column["col_type"] + ", "
            query += " inconsistent_flag TINYINT(1)"
            query += " );"
            #print query
            cursor.execute(query)
            warnings.filterwarnings('always', category=MySQLdb.Warning)

    def loadcourseinfo(self, json_file):
        """
        Loads the course information from JSON course structure file
        :param json_file: the name of the course structure file
        :return the course information
        """
        print self
        courseurl = config.SERVER_URL + '/datasources/course_structure/' + json_file
        print "ATTEMPTING TO LOAD "+courseurl
        courseinfofile = urllib2.urlopen(courseurl)
        if courseinfofile:
            courseinfo = json.load(courseinfofile)
            return courseinfo
        return None

    def get_chapter(self, obj, found=None):
        """
        Gets the chapter for the object (recursive)
        :param obj: the object being added
        :param found: an array of previously found elements
        :return the found object
        """
        if not found:
            found = []
        if obj['tag'] == 'chapter':
            found.append(obj)
        else:
            for child in obj['children']:
                found = self.get_chapter(child, found)
        return found

    def analysis_chapters(self, chapters):
        """
        Analyses chapters within a course
        :param chapters: An array of chapter dictionaries
        :return: A dict containing the analysis
        """
        print self
        nvideos = 0
        nhtmls = 0
        nassessments = 0
        nsummative_assessments = 0
        nformative_assessments = 0
        nincontent_discussions = 0
        nactivities = 0

        for chapter in chapters:
            for sequential in chapter['children']:
                if sequential['tag'] == 'sequential' and 'children' in sequential:
                    for vertical in sequential['children']:
                        if vertical['tag'] == 'vertical' and 'children' in vertical:
                            for child in vertical['children']:
                                if child['tag'] == 'video':
                                    nvideos += 1
                                elif child['tag'] == 'html':
                                    nhtmls += 1
                                elif child['tag'] == 'problem':
                                    nassessments += 1
                                    if 'weight' in child and child['weight'] != "null" and float(child['weight']) > 0:
                                        nsummative_assessments += 1
                                    else:
                                        nformative_assessments += 1
                                elif child['tag'] == 'discussion':
                                    nincontent_discussions += 1
                                else:
                                    nactivities += 1

        return {"nvideos": nvideos, "nhtmls": nhtmls, "nassessments": nassessments,
                "nsummative_assessments": nsummative_assessments, "nformative_assessments": nformative_assessments,
                "nincontent_discussions": nincontent_discussions, "nactivities": nactivities}

    def datadump2csv(self, tablename="personcourse"):
        """
        Generates a CSV file for each course in the derived datasets
        :param tablename: The tablename to use
        """
        print tablename
        if self.sql_pc_conn is None:
            self.sql_pc_conn = self.connect_to_sql(self.sql_pc_conn, "Person_Course", True)
        pc_cursor = self.sql_pc_conn.cursor()

        backup_path = config.EXPORT_PATH
        current_time = time.strftime('%m%d%Y-%H%M%S')

        # export the {personcourse}x tables
        for course_id, course in self.courses.items():

            try:
                pc_tablename = self.pc_table + "_" + course_id

                backup_prefix = pc_tablename + "_" + current_time
                backup_file = os.path.join(backup_path, backup_prefix + ".csv")

                query = "SELECT * FROM %s" % pc_tablename
                pc_cursor.execute(query)
                result = pc_cursor.fetchall()

                with open(backup_file, "w") as csv_file:
                    csv_writer = csv.writer(csv_file)
                    csv_writer.writerow([i[0] for i in pc_cursor.description])  # write headers
                    for record in result:
                        csv_writer.writerow(record)

                utils.log("The personcourse table: %s exported to csv file %s" % (pc_tablename, backup_file))

            except self.sql_pc_conn.ProgrammingError:
                pass

        # export the cf_table
        backup_prefix = self.cf_table + "_" + current_time
        backup_file = os.path.join(backup_path, backup_prefix + ".csv")

        query = "SELECT * FROM %s" % self.cf_table
        pc_cursor.execute(query)
        result = pc_cursor.fetchall()

        with open(backup_file, "w") as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow([i[0] for i in pc_cursor.description])  # write headers
            for record in result:
                csv_writer.writerow(record)
        utils.log("The courseprofile table: %s exported to csv file %s" % (self.cf_table, backup_file))


def safe_name(filename):
    """
    Protection against bad database names
    :param filename: the filename
    :return: a safe fileneame
    """
    return str(filename).replace('.', '_')


def get_files(path):
    """
    Returns a list of files that the service will ingest
    :param path: The path of the files
    :return: An array of file paths
    """
    print path
    required_files = []
    return required_files


def name():
    """
    Returns the name of the service class
    """
    return "PersonCourse"


def service():
    """
    Returns an instance of the service
    """
    return PersonCourse()