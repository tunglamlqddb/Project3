# Project3
Co-author recommender system
1. In order to use the system, make sure to have the following versions of:
- Flask==1.1.2
- Flask-Cors==3.0.9
2. Create a folder named "Data_Project3" in the directory containing the cloned folder, and place file db.sqlite3 there. Also note that sub-databases will be created and stored in this folder too
3. Run file 'query_code.py' in folder BE through terminal or GUI and open file 'front_end_1.html' in folder FE on a web browser (Google Chrome). 
4. When finishing the last step - calculate scores, result files will be created and stored in folder Result with the format: Data_NumRecords_JournalID_FromDate_ToDate_WeightType_LabelType.csv in which:
	NumRecords: the total number of papers of the selected Journals
	JournalID: list of ID(s) of paper journal chosen (available IDs: 21, 22, 23, 24)
	FromDate: year chosen to start selecting papers 
	ToDate: year chosen to end selecting papers
	WeightType: weighted or unweighted (considered when calculating scores)
	LabelType: static or dynamic (2 labeling schemes)
For example: Data_1028_21_22_2016_2017_unweighted_dynamic.csv
