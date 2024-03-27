import json
import pandas
from datetime import datetime
from datetime import date
from dateutil.relativedelta import relativedelta
import logging
import dotenv
import os


class PatronDataTransformer:
    def __init__(self, config_name, time):
        logging.info("Initializing patron data converter...")
        self.config_file_name = config_name

        # Read staff file
        try:
            logging.info('Reading staff file... \"%s\"...',
                         os.getenv('staffFileName'))
            self.staff_CSV = pandas.read_csv(
                os.getenv('staffFileName'), delimiter="|", dtype="string")
        except FileNotFoundError as exc:
            logging.critical('Staff load file, \"%s\", not found',
                             os.getenv('staffFileName'))
            raise FileNotFoundError from exc
        finally:
            logging.info("Sorting staff...")
            self.staff_CSV.sort_values("EMPLID")
            self.staff_CSV.fillna("", inplace=True)

        # Read student file
        try:
            logging.info('Reading student file... \"%s\"...',
                         os.getenv('studentFileName'))
            with open(os.getenv('studentFileName'), 'r', encoding='utf-8') as file:
                headers = file.readline().strip().split('|')
                self.student_CSV = pandas.read_csv(
                    file, names=headers, delimiter='|', dtype='string')
        except FileNotFoundError as exc:
            logging.critical('Student load file, \"%s\", not found',
                             os.getenv('studentFileName'))
            raise FileNotFoundError from exc
        finally:
            logging.info("Sorting students...")
            self.student_CSV.sort_values("EMPLID")
            self.student_CSV.fillna("", inplace=True)

        if os.getenv('fullLoad').lower() in ('true', '1', 't'):
            self.full_load = True
        elif os.getenv('fullLoad').lower() in ('false', '0', 'f'):
            self.full_load = False
        else:
            logging.critical('Invalid fullLoad value. Use True or False')
            raise ValueError('Invalid fullLoad value. Use True or False')

        if not self.full_load:
            # Read previous staff file
            try:
                logging.info('Reading previous staff file... \"%s\"...',
                             os.getenv('previousStaffCondense'))
                self.previous_staff_CSV = pandas.read_csv(
                    os.getenv('previousStaffCondense'), delimiter="|", dtype="string")
            except FileNotFoundError as exc:
                logging.critical('Previous staff load file, \"%s\", not found', os.getenv(
                    'previousStaffCondense'))
                raise FileNotFoundError from exc
            finally:
                logging.info("Sorting previous staff...")
                self.previous_staff_CSV.sort_values("EMPLID")
                self.previous_staff_CSV.fillna("", inplace=True)

            # Read previous student file
            try:
                logging.info('Reading previous student file... \"%s\"...', os.getenv(
                    'previousStudentCondense'))
                self.previous_student_CSV = pandas.read_csv(
                    os.getenv('previousStudentCondense'), delimiter="|", dtype="string")
            except FileNotFoundError as exc:
                logging.critical('Previous student load file, \"%s\", not found', os.getenv(
                    'previousStudentCondense'))
                raise FileNotFoundError from exc
            finally:
                logging.info("Sorting previous students...")
                self.previous_student_CSV.sort_values("EMPLID")
                self.previous_student_CSV.fillna("", inplace=True)

        logging.info("Files read and sorted")

        # Initializes blank Output file dicts
        self.student_out = []
        self.staff_out = []

        self.time = time
        if os.getenv('destinationFolder') == '':
            self.patron_out_file_name = 'umpatrons.json'
        elif os.getenv('destinationFolder')[-1:] == '/':
            self.patron_out_file_name = f'{os.getenv("destinationFolder")}umpatrons.json'
        else:
            self.patron_out_file_name = f'{os.getenv("destinationFolder")}/umpatrons.json'

        logging.info('Prepared load file will be saved as: %s',
                     self.patron_out_file_name)
        self._logElapsedTime()
        logging.info("Patron data converter initialized\n")

    # Logs time elapsed since the time passed into the object on initialization  
    def _logElapsedTime(self):
        time_now = datetime.now()
        elapsed_time = time_now - self.time
        logging.info('Total elapsed time (seconds): %s', elapsed_time.seconds)

    # Update Config File with changed
    def _updateConfig(self, config_field, data):
        try:
            os.environ[config_field] = data
            dotenv.set_key(self.config_file_name, config_field, data)
        except PermissionError:
            return -1

    # Executes all steps involved in an INCREMENTAL data load
    def _prepareIncrementalLoad(self):
        self.staffCondense()
        self.studentCondense()
        self.recordComparisons()
        self.staffChanges()
        self.studentChanges()
        self.transformStudentRecords()
        self.transformStaffRecords()
        self.saveLoadData()

    # Executes all steps involved in a FULL data load
    def _prepareFullLoad(self):
        self.staffCondense()
        self.studentCondense()
        self.recordComparisons()
        self.transformStudentRecords()
        self.transformStaffRecords()
        self.saveLoadData()

    # Calls the load function indicated by the config
    def preparePatronLoad(self):
        if self.full_load:
            self._prepareFullLoad()
        else:
            self._prepareIncrementalLoad()
        self._logElapsedTime()

    # Removes Staff outside of the desired Staff Classes as well as those without barcodes
    def staffCondense(self):
        logging.info("Condensing Staff Records...")

        skipped = 0
        condensed_list = []
        allowed_classes = ["0", "1", "2", "3", "4", "5", "7", "S", "B", "#"]

        for row in self.staff_CSV.itertuples():
            patron = row._asdict()

            # Selects only records from allowed classes
            if patron["EmplClass"] in allowed_classes:
                if not patron["barcode"] == '':
                    if not ((patron["EmplClass"] == "#") and (patron["um_nens_cat_code"] == "CNTEM")):
                        condensed_list.append(patron)
                    else:
                        # print("Class: " + patron["EmplClass"] + " Status: " + patron["EmplStatus"] + " barcode: " + patron["barcode"])
                        skipped += 1
                else:
                    # print("Class: " + patron["EmplClass"] + " Status: " + patron["EmplStatus"] + " barcode: " + patron["barcode"])
                    skipped += 1
            else:
                # print("Class: " + patron["EmplClass"] + " Status: " + patron["EmplStatus"] + " barcode: " + patron["barcode"])
                skipped += 1

        # Removes records with duplicate EMPLID
        deduped_condensed_list = self.staffDeDupe(condensed_list)

        self.staff_CSV = pandas.DataFrame(deduped_condensed_list)
        logging.info('Saved Records: %s', len(deduped_condensed_list))
        logging.info('Skipped Records: %s', skipped)

        # Saves condensed staff data for later comparison
        logging.info("Saving Condensed Staff data...")
        self.saveCurrentStaffData("Condensed", update_config=True)
        logging.info("Condensed Staff Data Saved")

        self._logElapsedTime()
        logging.info("Staff Records Condensed\n")

    # Uses logic based on EmplStatus to select a record to load
    def staffDeDupe(self, records_in):
        logging.info("De-duping Condensed Staff Records...")
        status_remapping = {"P": "L",
                            "Q": "R",
                            "X": "R",
                            "U": "T",
                            "V": "T",
                            "": "A",
                            "B": "A"
                            }

        records_in = sorted(records_in, key=lambda x: x["EMPLID"])
        records_out = []
        last_emplid = ""
        current_id_rows = []

        for row in records_in:
            if last_emplid == "":
                last_emplid = row["EMPLID"]
            elif last_emplid == row["EMPLID"]:
                current_id_rows.append(row)
            else:
                if row["EmplStatus"] in status_remapping.keys():
                    row["EmplStatus"] = status_remapping[row["EmplStatus"]]
                if len(current_id_rows) == 1:
                    records_out.append(current_id_rows[0])
                elif len(current_id_rows) != 0:
                    records_out.append(
                        self.staffEMPLIDselector(current_id_rows))
                last_emplid = row["EMPLID"]
                current_id_rows = [row]

        logging.info("Condensed Staff Records De-duped")
        return records_out

    # Selects a record using logic
    def staffEMPLIDselector(self, records_with_shared_ids):
        a_index = -1
        d_index = -1
        l_index = -1
        r_index = -1
        s_index = -1
        t_index = -1
        w_index = -1
        for i, id_row in enumerate(records_with_shared_ids):
            if id_row["EmplStatus"] == "A":
                a_index = i
            elif id_row["EmplStatus"] == "D":
                d_index = i
            elif id_row["EmplStatus"] == "L":
                l_index = i
            elif id_row["EmplStatus"] == "R":
                r_index = i
            elif id_row["EmplStatus"] == "S":
                s_index = i
            elif id_row["EmplStatus"] == "T":
                t_index = i
            elif id_row["EmplStatus"] == "W":
                w_index = i

        if d_index != -1:
            selected_index = d_index
        elif a_index != -1:
            selected_index = a_index
        elif r_index != -1:
            selected_index = r_index
        elif w_index != -1:
            selected_index = w_index
        elif l_index != -1:
            selected_index = l_index
        elif s_index != -1:
            selected_index = s_index
        elif t_index != -1:
            selected_index = t_index
        else:
            for id_row in records_with_shared_ids:
                logging.warning('Duplicate records unresolved EMPLID: %s Status: %s',
                                id_row["EMPLID"], id_row["EmplStatus"])
        return records_with_shared_ids[selected_index]

    # Removes Staff records without barcodes
    def studentCondense(self):
        logging.info("Condensing Student Records...")
        logging.info('Starting Record Count: %s', len(self.student_CSV))
        skipped = 0
        condensed_list = []

        for row in self.student_CSV.itertuples():
            patron = row._asdict()
            if not patron["barcode"] == '':
                condensed_list.append(patron)
            else:
                skipped += 1

        self.student_CSV = pandas.DataFrame(condensed_list)

        logging.info('Saved Records: %s', str(len(condensed_list)))
        logging.info('Skipped Records: %s', str(skipped))

        logging.info("Saving Condensed Student data")
        self.saveCurrentStudentData("Condensed", update_config=True)
        logging.info("Condensed Student Data Saved")
        self._logElapsedTime()
        logging.info("Student Records Condensed\n")

    # Includes only records that differ from the previous Staff load
    def staffChanges(self):
        logging.info("Comparing Old and New Staff Files...")
        if not self.full_load:
            old_staff = self.previous_staff_CSV.itertuples()
            old_record = next(old_staff)._asdict()
            new_staff = self.staff_CSV.itertuples()
            new_record = next(new_staff)._asdict()

            staff_changes = []
            file_ends = {"old": False, "new": False}
            updated = 0
            new = 0
            compared_fields = ["EmplClass", "EmplStatus", "LastName", "FirstName", "MiddleName", "Email_Address",
                               "MailCountry", "MailCity", "MailState", "MailZip", "MailAdd1", "MailAdd2", "MailAdd3",
                               "MailAdd4", "PermCountry", "PermCity", "PermState", "PermZip", "PermAdd1", "PermAdd2",
                               "PermAdd3", "PermAdd4", "WorkPhone", "barcode"]

            while (not file_ends["old"]) and (not file_ends["new"]):
                next_old = False
                next_new = False

                if old_record["EMPLID"] < new_record["EMPLID"]:
                    next_old = True
                elif old_record["EMPLID"] > new_record["EMPLID"]:
                    staff_changes.append(new_record)
                    new += 1
                    next_new = True
                else:
                    change = False
                    for compared_field in compared_fields:
                        if str(old_record[compared_field]) != (new_record[compared_field]):
                            logging.info('Modified field: %s', compared_field)
                            change = True
                            break
                    if change:
                        logging.info(
                            'Modified field - Old Record: %s', old_record)
                        logging.info(
                            'Modified field - New Record: %s', new_record)
                        staff_changes.append(new_record)
                        updated += 1
                    next_old = True
                    next_new = True

                if next_old and (not file_ends["old"]):
                    try:
                        old_record = next(old_staff)._asdict()
                    except StopIteration:
                        file_ends["old"] = True
                if next_new and (not file_ends["new"]):
                    try:
                        new_record = next(new_staff)._asdict()
                    except StopIteration:
                        file_ends["new"] = True

            self.staff_CSV = pandas.DataFrame(staff_changes)

            self.saveCurrentStaffData("Old-New-Compare")
            logging.info('Updated: %s', updated)
            logging.info('New: %s', new)
            logging.info('Total Staff Changes Found: %s', len(staff_changes))
            self._logElapsedTime()
            logging.info("Old/New Staff comparison complete\n")
        else:
            logging.warning(
                "\nIncremental load selected, staff change comparison should not be performed\n")
            return -1

    # Includes only records that differ from the previous Student load
    def studentChanges(self):
        logging.info("Comparing Old and New Student Files...")
        if not self.full_load:
            old_student = self.previous_student_CSV.itertuples()
            old_record = next(old_student)._asdict()
            new_student = self.student_CSV.itertuples()
            new_record = next(new_student)._asdict()

            student_changes = []
            file_ends = {"old": False, "new": False}
            updated = 0
            new = 0
            compared_fields = ["AcadCareer1", "AcadCareer2", "AcadCareer3", "AcadProg1", "AcadProg2", "AcadProg3",
                               "LastName", "FirstName", "MiddleName", "Email_Address", "MailCountry", "MailCity",
                               "MailState", "MailZip", "MailAdd1", "MailAdd2", "MailAdd3", "MailAdd4", "PermCountry",
                               "PermCity", "PermState", "PermZip", "PermAdd1", "PermAdd2", "PermAdd3", "PermAdd4",
                               "TermDescr1", "TermDescr2", "TermDescr3", "LoclPhone", "barcode"]

            while (not file_ends["old"]) and (not file_ends["new"]):
                next_old = False
                next_new = False

                if old_record["EMPLID"] < new_record["EMPLID"]:
                    next_old = True
                elif old_record["EMPLID"] > new_record["EMPLID"]:
                    student_changes.append(new_record)
                    new += 1
                    next_new = True
                else:
                    change = False
                    for compared_field in compared_fields:
                        if str(old_record[compared_field]) != (new_record[compared_field]):
                            logging.info('Modified field: %s', compared_field)
                            change = True
                            break
                    if change:
                        logging.info(
                            'Modified field - Old Record: %s', old_record)
                        logging.info(
                            'Modified field - New Record: %s', new_record)
                        student_changes.append(new_record)
                        updated += 1
                    next_old = True
                    next_new = True

                if next_old and (not file_ends["old"]):
                    try:
                        old_record = next(old_student)._asdict()
                    except StopIteration:
                        file_ends["old"] = True
                if next_new and (not file_ends["new"]):
                    try:
                        new_record = next(new_student)._asdict()
                    except StopIteration:
                        file_ends["new"] = True

            self.student_CSV = pandas.DataFrame(student_changes)
            self.saveCurrentStudentData("Old-New-Compare")
            logging.info('Updated: %s', updated)
            logging.info('New: %s', new)
            logging.info('Total Student Changes Found: %s',
                         str(len(student_changes)))
            self._logElapsedTime()
            logging.info("Old/New Student comparison complete\n")
        else:
            logging.warning(
                "\nIncremental load selected, student change comparison should not be performed\n")
            return -1

    # Compares both sets of patron records and appropriately removes records
    def recordComparisons(self):
        if (self.staff_CSV.keys().tolist() == []) or (self.student_CSV.keys().tolist() == []):
            logging.warning(
                "No Record Comparison Necessary, one of both of the load files contain no records\n")
            return -1

        logging.info("Beginning Student/Staff record comparison...\n")

        staff_removed = 0
        staff_remaining = []
        student_ids = self.student_CSV["EMPLID"].tolist()

        # Removes Terminated (T) and Suspended (S) Staff who appear in the student load
        for record in self.staff_CSV.itertuples():
            staff = record._asdict()
            if staff["EmplStatus"] != "T" and staff["EmplStatus"] != "S":
                staff_remaining.append(staff)
            else:
                if staff["EMPLID"] in student_ids:
                    staff_removed += 1
                else:
                    staff_remaining.append(staff)
        self.staff_CSV = pandas.DataFrame(staff_remaining)

        logging.info(
            f"Starting Staff Record Count: {str(staff_removed + len(staff_remaining))}")
        logging.info(f"Staff Records Removed: {str(staff_removed)}")
        logging.info(f"Staff Records Remaining: {str(len(staff_remaining))}")

        students_removed = 0
        students_remaining = []
        staff_ids = self.staff_CSV["EMPLID"].tolist()

        # Removes Students who appear in the Staff load
        for record in self.student_CSV.itertuples():
            student = record._asdict()

            if not student["EMPLID"] in staff_ids:
                students_remaining.append(student)
            else:
                students_removed += 1
        self.student_CSV = pandas.DataFrame(students_remaining)

        logging.info(
            f"Starting Student Records: {str(students_removed + len(students_remaining))}")
        logging.info(f"Student Records Removed: {str(students_removed)}")
        logging.info(
            f"Student Records Remaining: {str(len(students_remaining))}\n")

        logging.info("Saving Compared Files...")
        self.saveCurrentStaffData("Intra-File-Compared")
        self.saveCurrentStudentData("Intra-File-Compared")
        logging.info("Compared Files Saved")

        self._logElapsedTime()
        logging.info("Student/Staff record comparison complete\n")

    # Converts Student records to FOLIO's json format and saves it in the output file
    def transformStudentRecords(self):
        if self.student_CSV.keys().tolist() == []:
            logging.warning(
                "Student file contains no records, output file will contain no student data\n")
            return -1

        logging.info("Converting Student records to json...\n")

        defaulted = 0

        # Iterates through all patron records
        for row in self.student_CSV.itertuples():
            student = row._asdict()

            # Logic for determining Patron Group, prioritizes highest level programs of study then latest graduation date.
            default_patron_group = "Undergraduate"
            grad_date = "UNKNOWN"
            if (student['AcadProg1'] == '' and student['AcadProg2'] == '' and student['AcadProg3'] == ''):
                active = False
                patron_group = default_patron_group
                expire_date= f"{datetime.now().year}-{datetime.now().month}-{datetime.now().day}"
            else:
                active = True
                academic_career = [student["AcadCareer1"],
                                student["AcadCareer2"],
                                student["AcadCareer3"]]
                academic_programs = [student["AcadProg1"],
                                    student["AcadProg2"],
                                    student["AcadProg3"]]
                grad_terms = [student["TermDescr1"],
                            student["TermDescr2"],
                            student["TermDescr3"]]
                graduate_options = []
                undergraduate_options = []
                for index, option in enumerate(academic_career):
                    if option == "GRAD":
                        graduate_options.append(grad_terms[index])
                    if option == "ND":
                        program = academic_programs[index]
                        if program == "ND-ST":
                            undergraduate_options.append(grad_terms[index])
                        elif program == "ND-UG":
                            undergraduate_options.append(grad_terms[index])
                        elif program == "ND-CE":
                            undergraduate_options.append(grad_terms[index])
                        elif program == "ND-GR":
                            graduate_options.append(grad_terms[index])          
                    if option == "UGRD":
                        undergraduate_options.append(grad_terms[index])
                    if option == "NC":
                        if academic_programs[index] == "NC-LL":
                            undergraduate_options.append(grad_terms[index])
                years = []
                semesters = []
                if graduate_options and graduate_options != ['']:
                    patron_group = "Graduate"
                    for option in graduate_options:
                        if option != '':
                            years.append(option[-4:])
                            if option[:-5] == 'Sprng':
                                semesters.append(4)
                            elif option[:-5] == 'Summr':
                                semesters.append(3)
                            elif option[:-5] == 'Fall':
                                semesters.append(2)
                            elif option[:-5] == 'Wintr':
                                semesters.append(1)
                            else:
                                logging.warning(
                                    'Malformed Graduation Date: %s', option)
                    max_year = max(years)
                    semester = max([semesters[index] for index,
                                year in enumerate(years) if year == max_year])
                elif undergraduate_options and undergraduate_options != ['']:
                    patron_group = "Undergraduate"
                    for option in undergraduate_options:
                        if option != '':
                            years.append(option[-4:])
                            if option[:-5] == 'Sprng':
                                semesters.append(4)
                            elif option[:-5] == 'Summr':
                                semesters.append(3)
                            elif option[:-5] == 'Fall':
                                semesters.append(2)
                            elif option[:-5] == 'Wintr':
                                semesters.append(1)
                            else:
                                logging.warning(
                                    'Malformed Graduation Date: %s', option)
                    max_year = max(years)
                    semester = max([semesters[index] for index,
                                year in enumerate(years) if year == max_year])
                else:
                    defaulted += 1
                    patron_group = default_patron_group
                    max_year = datetime.now().year + 1
                    semester = 0

                if semester == 0:
                    expiration_day = datetime.today() + relativedelta(years=2)
                    expire_date = f'{expiration_day.year:04}-{expiration_day.month:02}-{expiration_day.day:02}'
                elif semester == 1:
                    grad_date = f'Winter {max_year}'
                    expire_date = f'{int(max_year)+1}-02-15'
                elif semester == 2:
                    grad_date = f'Fall {max_year}'
                    expire_date = f'{int(max_year)+1}-01-15'
                elif semester == 3:
                    grad_date = f'Summer {max_year}'
                    expire_date = f'{max_year}-09-15'
                elif semester == 4:
                    grad_date = f'Spring {max_year}'
                    expire_date = f'{max_year}-06-05'
                

            # Determines the student's preferred phone number if a preference exists.
            try:
                if student['Phone_Pref'][0:4] == 'LOCL':
                    phone = student['LoclPhone']
                elif student['Phone_Pref'][0:4] == 'PERM':
                    phone = student['PermPhone']
                else:
                    phone = ''
            except KeyError:
                phone = student['LoclPhone']

            # Maps each patron's data into a list to be added to the output file
            patron_json = {
                "username": student["Email_Address"],
                "externalSystemId": str(student["EMPLID"]) + "@umass.edu",
                "barcode": student["barcode"],
                "active": active,
                "patronGroup": patron_group,
                "departments": [],
                "personal":
                    {
                        "lastName": student["LastName"],
                        "firstName": student["FirstName"],
                        "middleName": student["MiddleName"],
                        "email": student["Email_Address"],
                        "phone": phone,
                        "addresses": [
                            {
                                "countryId": student["MailCountry"],
                                "addressLine1": student["MailAdd1"],
                                "addressLine2": str(student["MailAdd2"]) + " "
                                + str(student["MailAdd3"]) + " "
                                + str(student["MailAdd4"]),
                                "city": student["MailCity"],
                                "region": student["MailState"],
                                "postalCode": student["MailZip"],
                                "addressTypeId": "Primary",
                                "primaryAddress": True
                            },
                            {
                                "countryId": student["PermCountry"],
                                "addressLine1": student["PermAdd1"],
                                "addressLine2": str(student["PermAdd2"]) + " "
                                + str(student["PermAdd3"]) + " "
                                + str(student["PermAdd4"]),
                                "city": student["PermCity"],
                                "region": student["PermState"],
                                "postalCode": student["PermZip"],
                                "addressTypeId": "Secondary",
                                "primaryAddress": False
                            }
                        ],
                        "preferredContactTypeId": "Email"
                },
                "enrollmentDate": "",
                "expirationDate": expire_date,
                "customFields": {
                    "institution": "UMass Amherst",
                    "graduationDate": grad_date
                }
            }

            self.student_out.append(patron_json)

        # Logs Statistics and Saves data to output file
        logging.info(
            'Students defaulted to the \'Undergraduate\' patron group: %s', str(defaulted))
        logging.info('%s Student Records Converted', len(self.student_out))
        self._logElapsedTime()
        logging.info("Student Records converted successfully\n")

    # Converts Staff records to FOLIO's json format and saves it in the output file
    def transformStaffRecords(self):
        if not self.staff_CSV.keys().tolist():
            logging.warning(
                "Staff file contains no records, output file will contain no staff data\n")
            return -1

        logging.info("Converting Staff records to json...\n")
        default_patron_group = "Staff"
        defaulted = 0
        no_barcode = 0
        for row in self.staff_CSV.itertuples():
            staff = row._asdict()

            # Assigns Patron Group
            if staff["EmplClass"] in ["0", "1"]:
                patron_group = "Faculty"
            elif staff["EmplClass"] in ["S", "2", "3", "4", "5", "7"]:
                patron_group = "Staff"
            elif staff["EmplClass"] == "#":
                patron_group = default_patron_group
                defaulted += 1
            else:
                patron_group = default_patron_group
                defaulted += 1
            today = datetime.today()
            try:
                expiration_day = today.replace(year=today.year + 2)
            except ValueError:
                expiration_day = today + \
                    (date(today.year + 2, 1, 1) - date(today.year, 1, 1))
            expiration_date = f'{expiration_day.year:04}-{expiration_day.month:02}-{expiration_day.day:02}'

            # Checks Patron Status and existence of a Barcode
            if staff["EmplStatus"] == "T" or staff["EmplStatus"] == "D":
                if self.full_load:
                    continue
                active = False
            else:
                active = True
            if staff["barcode"] == "":
                no_barcode += 1

            if staff["Email_Address"] == "":
                email = str(staff["EMPLID"]) + "@umass.edu"
            else:
                email = staff["Email_Address"]

            patron_json = {
                "username": email,
                "externalSystemId": str(staff["EMPLID"]) + "@umass.edu",
                "barcode": staff["barcode"],
                "active": active,
                "patronGroup": patron_group,
                "departments": [],
                "personal":
                    {
                        "lastName": staff["LastName"],
                        "firstName": staff["FirstName"],
                        "middleName": staff["MiddleName"],
                        "email": email,
                        "phone": staff["WorkPhone"],
                        "addresses": [
                            {
                                "countryId": staff["MailCountry"],
                                "addressLine1": staff["MailAdd1"],
                                "addressLine2": str(staff["MailAdd2"]) + " "
                                + str(staff["MailAdd3"]) + " "
                                + str(staff["MailAdd4"]),
                                "city": staff["MailCity"],
                                "region": staff["MailState"],
                                "postalCode": staff["MailZip"],
                                "addressTypeId": "Primary",
                                "primaryAddress": True
                            },
                            {
                                "countryId": staff["PermCountry"],
                                "addressLine1": staff["PermAdd1"],
                                "addressLine2": str(staff["PermAdd2"]) + " "
                                + str(staff["PermAdd3"]) + " "
                                + str(staff["PermAdd4"]),
                                "city": staff["PermCity"],
                                "region": staff["PermState"],
                                "postalCode": staff["PermZip"],
                                "addressTypeId": "Secondary",
                                "primaryAddress": False
                            }
                        ],
                        "preferredContactTypeId": "Email"
                },
                "expirationDate": expiration_date,
                "customFields": {
                    "institution": "UMass Amherst"
                }
            }

            self.staff_out.append(patron_json)

        logging.info('Staff with no barcodes: %s', no_barcode)
        logging.info(
            'Staff defaulted to the \'Staff\' patron group: %s', defaulted)
        logging.info('%s Staff Records Converted', len(self.staff_out))
        self._logElapsedTime()
        logging.info("Staff Records converted successfully\n")

    # Saves Current Staff Data as a csv and triggers a config update if indicated for condensed files
    def saveCurrentStaffData(self, load_step, update_config=False):
        file = f"{os.getenv('loadProcessDirectory')}/Staff-{load_step}.csv"
        logging.info('Saving Staff %s to: %s', load_step, file)
        self.staff_CSV.to_csv(file, index=False, sep="|")
        if update_config and load_step == "Condensed":
            self._updateConfig("previousStaffCondense", file)

    # Saves Current student data as a csv and triggers a config update if indicated for condensed files
    def saveCurrentStudentData(self, load_step, update_config=False):
        file = f"{os.getenv('loadProcessDirectory')}/Student-{load_step}.csv"
        logging.info('Saving Student %s to: %s', load_step, file)
        self.student_CSV.to_csv(file, index=False, sep="|")
        if update_config and load_step == "Condensed":
            self._updateConfig("previousStudentCondense", file)

    # Saves Current Staff and Student data together in a json file that is ready-to-load
    def saveLoadData(self):
        with open(self.patron_out_file_name, 'w', encoding='utf-8') as outfile:
            for staff in self.staff_out:
                outfile.write(f"{json.dumps(staff)}\n")
            for student in self.student_out:
                outfile.write(f"{json.dumps(student)}\n")
        logging.info('Patron Records saved to: %s', self.patron_out_file_name)


if __name__ == "__main__":
    config = '.env'
    dotenv.load_dotenv(config)
    start_time = datetime.now()
    logFile = f'{os.getenv("logFileDirectory")}/{start_time.year}-{start_time.month}-{start_time.day}--{start_time.hour}-{start_time.minute}-{start_time.second}.log'
    logging.basicConfig(filename=logFile, encoding='utf-8', level=logging.DEBUG,
                        format='%(asctime)s | %(levelname)s | %(message)s', datefmt='%m/%d/%Y %H:%M:%S')
    print(f"Saving log to: {logFile}")
    logging.info('Beginning Log')

    # Logs Current Configuration and Raises an exception if a required configuration field is missing
    for field in ['staffFileName', 'studentFileName', 'destinationFolder', 'fullLoad',
                  'previousStudentCondense', 'previousStaffCondense',
                  'loadProcessDirectory', 'logFileDirectory']:
        try:
            logging.info('Config - %s = %s', field, os.getenv(field))
        except ValueError as exc:
            logging.critical('.env file must contain a value for %s', field)
            raise ValueError from exc

    # Actually uses the object to convert data
    converter = PatronDataTransformer(config, start_time)
    converter.preparePatronLoad()
