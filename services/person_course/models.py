import MySQLdb
import datetime

class CFModel(object):

    def __init__(self, course, dbname, mongoname, discussiontable, registration_open_date="", course_launch_date="",
                 course_close_date="", nregistered_students=0, nviewed_students=0, nexplored_students=0, ncertified_students=0,
                 nhonor_students=0, naudit_students=0, nvertified_students=0, course_effort=0, course_length=0, nchapters=0,
                 nvideos=0, nhtmls=0, nassessments=0, nsummative_assessments=0, nformative_assessments=0, nincontent_discussions=0,
                 nactivities=0, best_assessment="", worst_assessment=""):
        self.course = course
        self.dbname = dbname
        self.mongoname = mongoname
        self.discussiontable = discussiontable
        self.registration_open_date = registration_open_date
        self.course_launch_date = course_launch_date
        self.course_close_date = course_close_date
        self.nregistered_students = nregistered_students
        self.nviewed_students = nviewed_students
        self.nexplored_students = nexplored_students
        self.ncertified_students = ncertified_students
        self.nhonor_students = nhonor_students
        self.naudit_students = naudit_students
        self.nvertified_students = nvertified_students
        self.course_effort = course_effort
        self.course_length = course_length
        self.nchapters = nchapters
        self.nvideos = nvideos
        self.nhtmls = nhtmls
        self.nassessments = nassessments
        self.nsummative_assessments = nsummative_assessments
        self.nformative_assessments = nformative_assessments
        self.nincontent_discussions = nincontent_discussions
        self.nactivities = nactivities
        self.best_assessment = best_assessment
        self.worst_assessment = worst_assessment
        pass

    def set_course_launch_date(self, start):
        self.course_launch_date = start

    def set_course_close_date(self, end):
        self.course_close_date = end

    def set_nregistered_students(self, nregistered_students):
        self.nregistered_students = nregistered_students

    def set_nviewed_students(self, nviewed_students):
        self.nviewed_students = nviewed_students

    def set_nexplored_students(self, nexplored_students):
        self.nexplored_students = nexplored_students

    def set_ncertified_students(self, ncertified_students):
        self.ncertified_students = ncertified_students

    def set_nhonor_students(self, nhonor):
        self.nhonor_students = nhonor

    def set_naudit_students(self, naudit):
        self.naudit_students = naudit

    def set_nvertified_students(self, nvertified):
        self.nvertified_students = nvertified

    def set_registration_open_date(self, registration_open_date):
        self.registration_open_date = registration_open_date

    def set_course_length(self, course_length):
        self.course_length = int(course_length)

    def set_nchapters(self, nchapters):
        self.nchapters = nchapters

    def set_nvideos(self, nvideos):
        self.nvideos = nvideos

    def set_nhtmls(self, nhtmls):
        self.nhtmls = nhtmls

    def set_nassessments(self, nassessments):
        self.nassessments = nassessments

    def set_nsummative_assessments(self, nsummative_assessments):
        self.nsummative_assessments = nsummative_assessments

    def set_nformative_assessments(self, nformative_assessments):
        self.nformative_assessments = nformative_assessments

    def set_nincontent_discussions(self, nincontent_discussions):
        self.nincontent_discussions = nincontent_discussions

    def set_nactivities(self, nactivities):
        self.nactivities = nactivities

    def save2db(self, cursor, table):
        parameters = table, self.course, self.dbname, self.mongoname, self.discussiontable, self.registration_open_date, self.course_launch_date, self.course_close_date, self.nregistered_students, self.nviewed_students, self.nexplored_students, self.ncertified_students, self.nhonor_students, self.naudit_students, self.nvertified_students, self.course_effort, self.course_length, self.nchapters, self.nvideos, self.nhtmls, self.nassessments, self.nsummative_assessments, self.nformative_assessments, self.nincontent_discussions, self.nactivities, self.best_assessment, self.worst_assessment

        query = "INSERT INTO %s (course, dbname, mongoname, discussiontable, registration_open_date, course_launch_date, course_close_date, nregistered_students, nviewed_students, nexplored_students, ncertified_students, nhonor_students, naudit_students, nvertified_students, course_effort, course_length, nchapters, nvideos, nhtmls, nassessments, nsummative_assessments, nformative_assessments, nincontent_discussions, nactivities, best_assessment, worst_assessment) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', %d, %d, %d, %d, %d, %d, %d, %f, %d, %d, %d, %d, %d, %d, %d, %d, %d, '%s', '%s')" % parameters
        #print query
        cursor.execute(query)

    def __repr__(self):
        result = ""
        result += "course: " + str(self.course) + ", "
        result += "dbname: " + str(self.dbname) + ", "
        result += "mongoname: " + str(self.mongoname) + ", "
        result += "discussiontable: " + str(self.discussiontable) + ", "
        result += "registration_open_date: " + str(self.registration_open_date) + ", "
        result += "course_launch_date: " + str(self.course_launch_date) + ", "
        result += "course_close_date: " + str(self.course_close_date) + ", "
        result += "nregistered_students: " + str(self.nregistered_students) + ", "
        result += "nviewed_students: " + str(self.nviewed_students) + ", "
        result += "nexplored_students: " + str(self.nexplored_students) + ", "
        result += "ncertified_students: " + str(self.ncertified_students) + ", "
        result += "nhonor_students: " + str(self.nhonor_students) + ", "
        result += "naudit_students: " + str(self.naudit_students) + ", "
        result += "nvertified_students: " + str(self.nvertified_students) + ", "
        result += "course_effort: " + str(self.course_effort) + ", "
        result += "course_length: " + str(self.course_length) + ", "
        result += "nchapters: " + str(self.nchapters) + ", "
        result += "nvideos: " + str(self.nvideos) + ", "
        result += "nhtmls: " + str(self.nhtmls) + ", "
        result += "nassessments: " + str(self.nassessments) + ", "
        result += "nsummative_assessments: " + str(self.nsummative_assessments) + ", "
        result += "nformative_assessments: " + str(self.nformative_assessments) + ", "
        result += "nincontent_discussions: " + str(self.nincontent_discussions) + ", "
        result += "nactivities: " + str(self.nactivities) + ", "
        result += "best_assessment: " + str(self.best_assessment) + ", "
        result += "worst_assessment: " + str(self.worst_assessment) + ", "
        return result


class PCModel(object):

    def __init__(self, course_id, user_id, registered=1, viewed=0, explored=0, certified=0, final_cc_cname="",
                 LoE="", YoB=None, gender="", mode="", grade=0, start_time="", last_event="", nevents=0, ndays_act=0,
                 nplay_video=0, nchapters=0, nforum_posts=0, roles="", attempted_problems=0, inconsistent_flag=0):
        self.course_id = course_id
        self.user_id = user_id
        self.registered = registered
        self.viewed = viewed
        self.explored = explored
        self.certified = certified
        self.final_cc_cname = final_cc_cname
        self.LoE = LoE
        self.YoB = YoB
        self.gender = gender
        self.mode = mode
        self.grade = grade
        self.start_time = start_time
        self.last_event = last_event
        self.nevents = nevents
        self.ndays_act = ndays_act
        self.nplay_video = nplay_video
        self.nchapters = nchapters
        self.nforum_posts = nforum_posts
        self.roles = roles
        self.attempted_problems = attempted_problems
        self.inconsistent_flag = inconsistent_flag
        pass

    def set_viewed(self, viewed):
        self.viewed = viewed

    def set_explored(self, explored):
        self.explored = explored

    def set_certified(self, status):
        if status == 'downloadable':
            self.certified = 1
        else:
            self.certified = 0

    def set_final_cc_cname(self, final_cc_cname):
        countries = [x for x in final_cc_cname if x is not None]
        self.final_cc_cname = ",".join(countries)

    def set_LoE(self, LoE):
        if LoE == "NULL":
            self.LoE = ""
        else:
            self.LoE = LoE

    def set_YoB(self, YoB):
        if YoB == "NULL":
            self.YoB = ""
        else:
            self.YoB = YoB

    def set_gender(self, gender):
        if gender == "NULL":
            self.gender = ""
        else:
            self.gender = gender

    def set_mode(self, mode):
        self.mode = mode

    def set_grade(self, grade):
        self.grade = grade

    def set_start_time(self, start_time):
        self.start_time = start_time

    def set_last_event(self, last_event):
        self.last_event = last_event

    def set_nevents(self, nevents):
        self.nevents = nevents

    def set_ndays_act(self, ndays_act):
        self.ndays_act = ndays_act

    def set_nplay_video(self, nplay_video):
        self.nplay_video = nplay_video

    def set_nchapters(self, nchapters):
        self.nchapters = nchapters

    def set_nforum_posts(self, nforum_posts):
        self.nforum_posts = nforum_posts

    def set_roles(self, roles):
        self.roles = roles

    def set_attempted_problems(self, attempted_problems):
        self.attempted_problems = attempted_problems

    def set_inconsistent_flag(self):
        if self.nevents == 0 and (self.ndays_act + self.nplay_video + self.nchapters + self.nforum_posts) > 0:
            self.inconsistent_flag = 1
        else:
            self.inconsistent_flag = 0

    def save2db(self, cursor, table):
        parameters = table, self.course_id, self.user_id, self.registered, self.viewed, self.explored, self.certified, self.final_cc_cname, self.LoE, self.YoB, self.gender, self.mode, self.grade, self.start_time, self.last_event, self.nevents, self.ndays_act, self.nplay_video, self.nchapters, self.nforum_posts, self.roles, self.attempted_problems, self.inconsistent_flag
        #print parameters

        query = "INSERT INTO %s (course_id, user_id, registered, viewed, explored, certified, final_cc_cname, LoE, YoB, gender, mode, grade, start_time, last_event, nevents, ndays_act, nplay_video, nchapters, nforum_posts, roles, attempted_problems, inconsistent_flag) VALUES ('%s', '%s', %d, %d, %d, %d, '%s', '%s', '%s', '%s', '%s', %f, '%s', '%s', %d, %d, %d, %d, %d, '%s', %d, %d)" % parameters
        cursor.execute(query)

    def __repr__(self):
        result = ""
        result += "course_id: " + str(self.course_id) + ", "
        result += "user_id: " + str(self.user_id) + ", "
        result += "registered: " + str(self.registered) + ", "
        result += "viewed: " + str(self.viewed) + ", "
        result += "explored: " + str(self.explored) + ", "
        result += "certified: " + str(self.certified) + ", "
        result += "final_cc_cname: " + str(self.final_cc_cname) + ", "
        result += "LoE: " + str(self.LoE) + ", "
        result += "YoB: " + str(self.YoB) + ", "
        result += "gender: " + str(self.gender) + ", "
        result += "mode: " + str(self.mode) + ", "
        result += "grade: " + str(self.grade) + ", "
        result += "start_time: " + str(self.start_time) + ", "
        result += "last_event: " + str(self.last_event) + ", "
        result += "nevents: " + str(self.nevents) + ", "
        result += "ndays_act: " + str(self.ndays_act) + ", "
        result += "nplay_video: " + str(self.nplay_video) + ", "
        result += "nchapters: " + str(self.nchapters) + ", "
        result += "nforum_posts: " + str(self.nforum_posts) + ", "
        result += "roles: " + str(self.roles) + ", "
        result += "attempted_problems: " + str(self.attempted_problems) + ", "
        result += "inconsistent_flag: " + str(self.inconsistent_flag) + ", "
        #result += " " + str(self.) + ", "
        return result




