

# Patron Import

Script to prepare a single UMass patron file for import into FOLIO using the FOLIO FSE Migration tools.

## Requirements


* Python 3.x
* pandas
* dotenv

## Usage Instructions

* Create .env config file in the following format:
>staffFileName = <br />
studentFileName = <br />
destinationFolder = <br />
fullLoad = <br />
previousStudentCondense=<br />
previousStaffCondense=<br />
loadProcessDirectory = <br />
logFileDirectory = <br />

* Place Student and Staff data files in the program's directory.
* For the initial run of the script a full load must be run in order to generate condensed files for comparison in future loads.
* Run the script!


## Contributors


* Amelia Sutton


## Version History

* 0.1
    * Initial Release
* 0.2
    * Refactored configuration to use dotenv
    * Refactored logging to use the python logging library 
    
## Known Issues
* 
## Planned Features
*