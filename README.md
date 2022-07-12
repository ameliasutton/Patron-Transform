

# FOLIO Voucher Export

Simple program with a few functions:
* Trigger Batch Voucher Exports in FOLIO
* Download Batched Vouchers in JSON format
* Convert JSON formatted Batched Vouchers into the XML format required for import into Jaggaer

## Requirements


* Python 3.x
* json
* pandas
* datetime
* winsound


## Instructions

* Create json config file in the following format:
>{  
    "staffFileName": "umpeople2.csv",  
  "studentFileName": "umstdnt2.csv",  
  "staffDestinationFolder": "Output/Staff load/",  
  "studentDestinationFolder": "Output/Student load/",  
  "fullLoad": true,  
  "previousStudentCondense": "Output/Load Process Files/Student-Condensed--2022-3-11--9-34-36.csv",  
  "previousStaffCondense": "Output/Load Process Files/Staff-Condensed--2022-3-11--9-34-36.csv"  
}
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

