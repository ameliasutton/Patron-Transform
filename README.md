

# Patron Import

Script to prepare UMass patron files for import into FOLIO using the FOLIO FSE Migration tools.

## Requirements


* Python 3.x
* json
* pandas
* datetime
* sys


## Instructions

* Create json config file in the following format:
>{  
    "staffFileName": "",  
  "studentFileName": "",  
  "staffDestinationFolder": "",  
  "studentDestinationFolder": "",  
  "fullLoad": true,  
  "previousStudentCondense": "",  
  "previousStaffCondense": "",
  "loadProcessDirectory": "",
  "logFileDirectory": ""
}
* Place Student and Staff data files in the program's directory.
* Ensure that the fullLoad variable is set to False unless a full load is being run.
* Run the script!


## Contributors


* Amelia Sutton


## Version History

* 0.1
    * Initial Release
    
## Known Issues
* 
## Planned Features
* Improved UI - potentially using [Gooey](https://github.com/chriskiehl/Gooey)
* Replace placeholder data in xml with actual data

