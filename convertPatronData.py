import json
import pandas
from datetime import datetime
from datetime import date
from dateutil.relativedelta import relativedelta
import sys


class patronDataConverter:
    def __init__(self, configFile, time):
        print(
            "**********************************\n\nInitializing patron data converter...\n")

        # Opens Config File
        self.configFileName = configFile
        try:
            with open(self.configFileName, "r") as readConf:
                config = json.load(readConf)
        except FileNotFoundError:
            print("Config File Not Found")
            raise FileNotFoundError

        # Reads Config File
        try:
            self.studentOutFileName = config["studentDestinationFolder"]
            self.staffOutFileName = config["staffDestinationFolder"]
            self.full = config["fullLoad"]
            self.loadProcessDirectory = config["loadProcessDirectory"]
            staff_file_name = config["staffFileName"]
            student_file_name = config["studentFileName"]
            previous_staff_file_name = config["previousStaffCondense"]
            previous_student_file_name = config["previousStudentCondense"]
        except KeyError:
            print("Config file is missing required keys, required keys are:\n"
                  "-studentDestinationFolder\n"
                  "-staffDestinationFolder\n"
                  "-fullLoad (True/False)\n"
                  "-staffFileName\n"
                  "-studentFileName\n"
                  "-previousStaffCondense\n"
                  "-previousStudentCondense")
            raise KeyError

        # Read staff file
        try:
            print(f"Reading staff file... \"{staff_file_name}\"...")
            self.staffCSV = pandas.read_csv(
                staff_file_name, delimiter="|", dtype="string")
        except FileNotFoundError:
            print(f"Staff load file, \"{staff_file_name}\", not found")
            raise FileNotFoundError
        finally:
            print("Sorting staff...")
            self.staffCSV.sort_values("EMPLID")
            self.staffCSV.fillna("", inplace=True)

        # Read student file
        try:
            print(f"Reading student file... \"{student_file_name}\"...")
            self.studentCSV = pandas.read_csv(
                student_file_name, delimiter="|", dtype="string")
        except FileNotFoundError:
            print(f"Student load file, \"{student_file_name}\", not found")
            raise FileNotFoundError
        finally:
            print("Sorting students...")
            self.studentCSV.sort_values("EMPLID")
            self.studentCSV.fillna("", inplace=True)

        if not self.full:
            # Read previous staff file
            try:
                print(
                    f"Reading previous staff file... \"{previous_staff_file_name}\"...")
                self.previousStaffCSV = pandas.read_csv(
                    previous_staff_file_name, delimiter="|", dtype="string")
            except FileNotFoundError:
                print(
                    f"Previous staff load file, \"{previous_staff_file_name}\", not found")
                raise FileNotFoundError
            finally:
                print("Sorting previous staff...")
                self.previousStaffCSV.sort_values("EMPLID")
                self.previousStaffCSV.fillna("", inplace=True)

            # Read previous student file
            try:
                print(
                    f"Reading previous student file... \"{previous_student_file_name}\"...")
                self.previousStudentCSV = pandas.read_csv(
                    previous_student_file_name, delimiter="|", dtype="string")
            except FileNotFoundError:
                print(
                    f"Previous student load file, \"{previous_student_file_name}\", not found")
                raise FileNotFoundError
            finally:
                print("Sorting previous students...")
                self.previousStudentCSV.sort_values("EMPLID")
                self.previousStudentCSV.fillna("", inplace=True)

        print("\nFiles read and sorted\n")

        # Initializes blank Output file dicts
        self.studentOut = []
        self.staffOut = []

        self.time = time
        self.studentOutFileName += f"umstudents--{self.time.year}-{self.time.month}-{self.time.day}--" \
                                   f"{self.time.hour}-{self.time.minute}-{self.time.second}.json"
        self.staffOutFileName += f"umstaff--{self.time.year}-{self.time.month}-{self.time.day}--" \
                                 f"{self.time.hour}-{self.time.minute}-{self.time.second}.json"

        print("Prepared load files will be saved as: ")
        print("\t" + self.staffOutFileName)
        print("\t" + self.studentOutFileName)
        self.printElapsedTime()
        print("Patron data converter initialized\n\n**********************************\n")

    # Prints time elapsed since the time passed into the object on initialization
    def printElapsedTime(self):
        time_now = datetime.now()
        elapsed_time = time_now - self.time
        print(f"\nTotal elapsed time (seconds): {elapsed_time.seconds}")

    # Update Config File with changed
    def updateConfig(self, field, data):
        try:
            with open(self.configFileName, "r") as readConf:
                config = json.load(readConf)
                config[field] = data
        except FileNotFoundError:
            return -1
        try:
            with open(self.configFileName, "w") as writeConf:
                writeConf.write(json.dumps(config, indent=4))
                return 0
        except PermissionError:
            return -1

    # Executes all steps involved in an INCREMENTAL data load
    def prepareIncrementalLoad(self):
        self.staffCondense()
        self.studentCondense()
        self.staffChanges()
        self.studentChanges()
        self.recordComparisons()
        self.convertStudentFile()
        self.convertStaffFile()

    # Executes all steps involved in a FULL data load
    def prepareFullLoad(self):
        self.staffCondense()
        self.studentCondense()
        self.recordComparisons()
        self.convertStudentFile()
        self.convertStaffFile()

    # Calls the load function indicated by the config
    def preparePatronLoad(self):
        if self.full:
            self.prepareFullLoad()
        else:
            self.prepareIncrementalLoad()

    # Removes Staff outside of the desired Staff Classes as well as those without barcodes
    def staffCondense(self):
        print("Condensing Staff Records...\n")

        skipped = 0
        condensed_list = []
        allowed_classes = ["0", "1", "2", "3", "4", "5", "7", "S", "B", "#"]

        for row in self.staffCSV.itertuples():
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

        self.staffCSV = pandas.DataFrame(deduped_condensed_list)
        print("Saved Records: " + str(len(deduped_condensed_list)))
        print("Skipped Records: " + str(skipped))

        # Saves condensed staff data for later comparison
        print("\nSaving Condensed Staff data...")
        self.saveCurrentStaffData("Condensed", updateConfig=True)
        print("Condensed Staff Data Saved")

        self.printElapsedTime()
        print("Staff Records Condensed\n\n**********************************\n")

    # Uses logic based on EmplStatus to select a record to load
    def staffDeDupe(self, records_in):
        print("De-duping Condensed Staff Records...")
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

        print("Condensed Staff Records De-duped\n")
        return records_out

    # Selects a record using logic
    def staffEMPLIDselector(self, recordsWithSharedIDs):
        a_index = -1
        d_index = -1
        l_index = -1
        r_index = -1
        s_index = -1
        t_index = -1
        w_index = -1
        for i, id_row in enumerate(recordsWithSharedIDs):
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
            for id_row in recordsWithSharedIDs:
                print("EMPLID: " + id_row["EMPLID"] +
                      " Status: " + id_row["EmplStatus"])

        return recordsWithSharedIDs[selected_index]

    # Removes Staff records without barcodes
    def studentCondense(self):
        print("Condensing Student Records...\n")
        print("Starting Record Count: " + str(len(self.studentCSV)))
        skipped = 0
        condensed_list = []

        for row in self.studentCSV.itertuples():
            patron = row._asdict()
            if not patron["barcode"] == '':
                condensed_list.append(patron)
            else:
                skipped += 1

        self.studentCSV = pandas.DataFrame(condensed_list)

        print("Saved Records: " + str(len(condensed_list)))
        print("Skipped Records: " + str(skipped))

        print("\nSaving Condensed Student data")
        self.saveCurrentStudentData("Condensed", updateConfig=True)
        print("Condensed Student Data Saved")
        self.printElapsedTime()
        print("Student Records Condensed\n\n**********************************\n")

    # Includes only records that differ from the previous Staff load
    def staffChanges(self):
        print("Comparing Old and New Staff Files...\n")
        if not self.full:
            old_staff = self.previousStaffCSV.itertuples()
            old_record = next(old_staff)._asdict()
            new_staff = self.staffCSV.itertuples()
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
                    for field in compared_fields:
                        if str(old_record[field]) != (new_record[field]):
                            print(f"Mismatched field: {field}")
                            change = True
                            break
                    if change:
                        print(f"Old Record: {old_record}")
                        print(f"New Record: {new_record}")
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

            self.staffCSV = pandas.DataFrame(staff_changes)

            self.saveCurrentStaffData("Old-New-Compare")
            print(f"Updated: {updated}")
            print(f"New: {new}")
            print(f"Total Staff Changes Found: {str(len(staff_changes))}")
            self.printElapsedTime()
            print("Old/New Staff comparison complete\n"
                  "\n**********************************\n")
        else:
            print("\nIncremental load selected, staff change comparison should not be performed\n"
                  "\n**********************************\n")
            return -1

    # Includes only records that differ from the previous Student load
    def studentChanges(self):
        print("Comparing Old and New Student Files...\n")
        if not self.full:
            old_student = self.previousStudentCSV.itertuples()
            old_record = next(old_student)._asdict()
            new_student = self.studentCSV.itertuples()
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
                    for field in compared_fields:
                        if str(old_record[field]) != (new_record[field]):
                            print(f"Mismatched field: {field}")
                            change = True
                            break
                    if change:
                        print(f"Old Record: {old_record}")
                        print(f"New Record: {new_record}")
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

            self.studentCSV = pandas.DataFrame(student_changes)
            self.saveCurrentStudentData("Old-New-Compare")
            print(f"Updated: {updated}")
            print(f"New: {new}")
            print(f"Total Student Changes Found: {str(len(student_changes))}")
            self.printElapsedTime()
            print("Old/New Student comparison complete\n"
                  "\n**********************************\n")
        else:
            print("\nIncremental load selected, student change comparison should not be performed\n"
                  "\n**********************************\n")
            return -1

    # Compares both sets of patron records and appropriately removes records
    def recordComparisons(self):
        if (self.staffCSV.keys().tolist() == []) or (self.studentCSV.keys().tolist() == []):
            print("No Record Comparison Necessary, one of both of the load files contain no records\n"
                  "\n**********************************\n")
            return -1

        print("Beginning Student/Staff record comparison...\n")

        staff_removed = 0
        staff_remaining = []
        student_ids = self.studentCSV["EMPLID"].tolist()

        # Removes Terminated (T) and Suspended (S) Staff who appear in the student load
        for record in self.staffCSV.itertuples():
            staff = record._asdict()
            if staff["EmplStatus"] != "T" and staff["EmplStatus"] != "S":
                staff_remaining.append(staff)
            else:
                if staff["EMPLID"] in student_ids:
                    staff_removed += 1
                else:
                    staff_remaining.append(staff)
        self.staffCSV = pandas.DataFrame(staff_remaining)

        print(
            f"Starting Staff Record Count: {str(staff_removed + len(staff_remaining))}")
        print(f"Staff Records Removed: {str(staff_removed)}")
        print(f"Staff Records Remaining: {str(len(staff_remaining))}\n")

        students_removed = 0
        students_remaining = []
        staff_ids = self.staffCSV["EMPLID"].tolist()

        # Removes Students who appear in the Staff load
        for record in self.studentCSV.itertuples():
            student = record._asdict()

            if not student["EMPLID"] in staff_ids:
                students_remaining.append(student)
            else:
                students_removed += 1
        self.studentCSV = pandas.DataFrame(students_remaining)

        print(
            f"Starting Student Records: {str(students_removed + len(students_remaining))}")
        print(f"Student Records Removed: {str(students_removed)}")
        print(f"Student Records Remaining: {str(len(students_remaining))}\n")

        print("Saving Compared Files...")
        self.saveCurrentStaffData("Intra-File-Compared")
        self.saveCurrentStudentData("Intra-File-Compared")
        print("Compared Files Saved")

        self.printElapsedTime()
        print("Student/Staff record comparison complete\n\n**********************************\n")

    # Converts Student records to FOLIO's json format and saves it in the output file
    def convertStudentFile(self):
        if self.studentCSV.keys().tolist() == []:
            print("Student file contains no records, output file will contain no data\n"
                  "\n**********************************\n")
            with open(self.studentOutFileName, 'w') as out:
                for student in self.studentOut:
                    out.write(json.dumps(student))
            return -1

        print("Converting Student records to json...\n")

        defaulted = 0

        # Iterates through all patron records
        for row in self.studentCSV.itertuples():
            student = row._asdict()

            # Logic for determining Patron Group, prioritizes highest level programs of study then latest graduation date.

            academic_career = [student["AcadCareer1"],
                               student["AcadCareer2"],
                               student["AcadCareer3"]]
            academic_programs = [student["AcadProg1"],
                                 student["AcadProg2"],
                                 student["AcadProg3"]]
            grad_terms = [student["TermDescr1"],
                          student["TermDescr2"],
                          student["TermDescr3"]]
            default_patron_group = "Undergraduate"
            grad_date = "UNKNOWN"
            graduate_options = []
            undergraduate_options = []
            if "GRAD" in academic_career:
                term = grad_terms[academic_career.index("GRAD")]
                graduate_options.append(term)
            if "ND" in academic_career:
                program = academic_programs[academic_career.index("ND")]
                if program == "ND-ST":
                    term = grad_terms[academic_programs.index(program)]
                    undergraduate_options.append(term)
                elif program == "ND-UG":
                    term = grad_terms[academic_programs.index(program)]
                    undergraduate_options.append(term)
                elif program == "ND-CE":
                    term = grad_terms[academic_programs.index(program)]
                    undergraduate_options.append(term)
                elif program == "ND-GR":
                    term = grad_terms[academic_programs.index(program)]
                    graduate_options.append(term)
            if "UGRD" in academic_career:
                term = grad_terms[academic_career.index("UGRD")]
                undergraduate_options.append(term)
            if "NC" in academic_career:
                program = academic_programs[academic_career.index("NC")]
                if program == "NC-LL":
                    term = grad_terms[academic_programs.index(program)]
                    undergraduate_options.append(term)
            years = []
            semesters = []
            if graduate_options != [] and graduate_options != ['']:
                patron_group = "Graduate"
                for option in graduate_options:
                    if option != '':
                        years.append(option[-4:])
                        match option[:-5]:
                            case "Sprng":
                                semesters.append(4)
                            case "Summr":
                                semesters.append(3)
                            case "Fall":
                                semesters.append(2)
                            case "Wintr":
                                semesters.append(1)
                            case _:
                                print(option[:-4])
                max_year = max(years)
                semester = max([semesters[index] for index, year in enumerate(years) if year == max_year])
            elif undergraduate_options != [] and undergraduate_options != ['']:
                patron_group = "Undergraduate"
                for option in undergraduate_options:
                    if option != '':
                        years.append(option[-4:])
                        match option[:-5]:
                            case "Sprng":
                                semesters.append(4)
                            case "Summr":
                                semesters.append(3)
                            case "Fall":
                                semesters.append(2)
                            case "Wintr":
                                semesters.append(1)
                            case _:
                                print(option[:-4])
                max_year = max(years)
                semester = max([semesters[index] for index, year in enumerate(years) if year == max_year])
            else:
                defaulted += 1
                patron_group = default_patron_group
                max_year = datetime.now().year + 1
                semester = 0

            match semester:
                case 0:
                    expiration_day = datetime.today() + relativedelta(years=2)
                    expire_date = f'{expiration_day.year:04}-{expiration_day.month:02}-{expiration_day.day:02}'
                case 1:
                    grad_date = f'Winter {max_year}'
                    expire_date = f'{int(max_year)+1}-02-15'
                case 2:
                    grad_date = f'Fall {max_year}'
                    expire_date = f'{int(max_year)+1}-01-15'
                case 3:
                    grad_date = f'Summer {max_year}'
                    expire_date = f'{max_year}-09-15'
                case 4:
                    grad_date = f'Spring {max_year}'
                    expire_date = f'{max_year}-06-05'

            # Determines the student's preferred phone number if a preference exists.
            try:
                if student['Phone_Pref'] == 'LOCL':
                    phone = student['LoclPhone']
                elif student['Phone_Pref'] == 'PERM':
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
                "active": True,
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

            self.studentOut.append(patron_json)

        # Prints Statistics and Saves data to output file
        print(
            f"Students defaulted to the 'Undergraduate' patron group: {str(defaulted)}")
        print(f"{len(self.studentOut)} Student Records Converted")
        with open(self.studentOutFileName, 'w') as out:
            for student in self.studentOut:
                out.write(json.dumps(student) + '\n')
        print(f"Student Records saved to: {self.studentOutFileName}")
        self.printElapsedTime()
        print(
            "Student Records converted successfully\n\n**********************************\n")

    # Converts Staff records to FOLIO's json format and saves it in the output file
    def convertStaffFile(self):
        if not self.staffCSV.keys().tolist():
            print("Staff file contains no records, output file will contain no data\n"
                  "\n**********************************\n")
            with open(self.staffOutFileName, 'w') as out:
                for staff in self.staffOut:
                    out.write(json.dumps(staff))
            return -1

        print("Converting Staff records to json...\n")
        default_patron_group = "Staff"
        defaulted = 0
        no_barcode = 0
        for row in self.staffCSV.itertuples():
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
                if self.full:
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

            self.staffOut.append(patron_json)

        print(f"Staff with no barcodes: {no_barcode}")
        print(f"Staff defaulted to the 'Staff' patron group: {defaulted}")
        print(f"{len(self.staffOut)} Staff Records Converted")
        with open(self.staffOutFileName, 'w') as out:
            for staff in self.staffOut:
                out.write(json.dumps(staff) + '\n')
        print(f"Staff Records saved to: {self.staffOutFileName}")
        self.printElapsedTime()
        print(
            "Staff Records converted successfully\n\n**********************************\n")

    # Saves Current Staff Data as a csv and triggers a config update if indicated for condensed files
    def saveCurrentStaffData(self, loadStep, updateConfig=False):
        file = f"{self.loadProcessDirectory}/Staff-{loadStep}--{self.time.year}-{self.time.month}-{self.time.day}-" \
               f"-{self.time.hour}-{self.time.minute}-{self.time.second}.csv"
        print("Saving Staff " + loadStep + " to: \n\t" + file)
        self.staffCSV.to_csv(file, index=False, sep="|")
        if updateConfig and loadStep == "Condensed":
            self.updateConfig("previousStaffCondense", file)

    # Saves Current student data as a csv and triggers a config update if indicated for condensed files
    def saveCurrentStudentData(self, loadStep, updateConfig=False):
        file = f"{self.loadProcessDirectory}/Student-{loadStep}--{self.time.year}-{self.time.month}-{self.time.day}-" \
               f"-{self.time.hour}-{self.time.minute}-{self.time.second}.csv"
        print("Saving Student " + loadStep + " to: \n\t" + file)
        self.studentCSV.to_csv(file, index=False, sep="|")
        if updateConfig and loadStep == "Condensed":
            self.updateConfig("previousStudentCondense", file)


def generateLog(filepath):
    start = datetime.now()
    logfile = f"{filepath}/{start.year}-{start.month}-{start.day}--{start.hour}-{start.minute}-{start.second}.log"
    print("Saving Log to: " + logfile)
    sys.stdout = open(logfile, "w")
    print("Log Start time: " + str(start) + "\n")
    return start


if __name__ == "__main__":
    config_file_name = 'config.json'

    try:
        with open(config_file_name, "r") as readConf:
            config = json.load(readConf)
    except FileNotFoundError:
        print("Config File Not Found")
        raise FileNotFoundError

    # Begins writing to a log file and notes start time

    start_time = generateLog(config["logFileDirectory"])

    # Actually uses the object to convert data
    converter = patronDataConverter(config_file_name, start_time)
    converter.preparePatronLoad()
    converter.printElapsedTime()
