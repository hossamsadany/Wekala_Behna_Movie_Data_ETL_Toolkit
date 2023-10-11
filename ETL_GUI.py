import sys
from PyQt5.QtWidgets import QApplication, QWidget, QVBoxLayout, QPushButton, QLabel, QFileDialog, QDialog, QCheckBox, QScrollArea, QGridLayout
from ETL_Functions import extract_data, transform_data, upload_to_database
import pandas as pd

class TransformDialog(QDialog):
    def __init__(self, dataframe, df_name, transformed_dataframes, parent=None):
        super().__init__(parent)
        self.transform_data_params = [
            "col_single", "col_list", "col_dict", "col_int", "replace_x", "col_numbers",
            "col_locations", "col_to_date", "col_durations", "col_sets_to_join", "link_col",
            "format_col", "digital_file_col"]
        self.dataframe = dataframe
        self.df_name = df_name
        self.transformed_dataframes = transformed_dataframes
        self.selected_columns = {param: [] for param in self.transform_data_params}
        self.initUI()

    def initUI(self):
        self.setFixedWidth(600)  # Set the desired width

        layout = QVBoxLayout()

        df_label = QLabel(f'<b><font size="7">DataFrame: {self.df_name}</font></b>')
        layout.addWidget(df_label)

        scroll_area = QScrollArea(self)
        scroll_area.setWidgetResizable(True)
        scroll_content = QWidget(scroll_area)
        scroll_content_layout = QGridLayout(scroll_content)

        row = 0
        col = 0
         # Define default selected columns based on df_name
        default_selected_columns_Catalog = {
            "col_single": ["Film_ID", "Film_Name_in_Arabic", "Film_Name_in_latin", "Film_Name_in_French",
                        "Film_Name_in_English", "Film_Name_in_Greek", "Film_Distributer_In_Arabic",
                        "Film_Distributer_In_Latin", "Country_of_origin", "Film_Type", "Color",
                        "Rights_advisory", "Film_Director_In_English", "Season_of_Distribution",
                        "Summary_Arabic", "Summary_English"],
            "col_list": ["Film_Producer_In_Arabic", "Cast_In_Arabic", "Cast_In_Latin", "Stars_In_Arabic",
                        "Stars_In_Latin", "Access_advisory_to_film_Material", "Keywords", "Genre_English",
                        "Genre_Arabic", "Notes_English", "Notes_Arabic", "Film_Producer_In_Latin",
                        "Film_Producer_in_English", "Production_Company_In_Arabic", "Production_Company_In_English",
                        "Production_Company_In_Latin", "Film_Director_In_Arabic", "Film_Director_In_Latin",
                        "Film_Writer_In_Arabic", "Film_Writer_In_Latin"],
            "col_dict" : ["Crew_In_Arabic", "Crew_In_Latin"],
            "col_int" : ["Distribution_Year_By_Behna", "Posters", "Lobby_Cards", "Photos", "Contracts", "Scripts", "Brochures", "Bordereaux",
                        "Correspondences", "Total_No_of_Documents", "Production_Year"],
            "replace_x" : ["Production_Year"],
            "col_numbers" : ["Season_of_Distribution"],
            "col_locations" : ['Filimig_location'],
            "col_to_date" : ["Initial_release"],
            "col_durations": ["Duration_In_s"]
        }
        default_selected_columns_Lobby_card = {
            "col_single": ["Call_No","Section", "Title_in_Arabic", "Film_Id",
                            "Title_in_English", "Title_in_latin", "Title_in_French", "Published_Created",
                            "Links_to_digital_copy", "Physical_Description", "Rights_advisory",
                            "Access_advisory", "Summary", "Keywords", "Acquisition_source",
                            "Repository", "Scanned_File_Format", "Film_Information_Link",
                            "color", "Path"
                                                 ],
            "col_list" : ["Type_of_material", "Notes", "Material_status", "Material_Language", "Keywords",
                        "People_in_photo"], 
            "col_int" : ["Number_of_copies"],
            "col_to_date": ["Published_Created"],
            "col_sets_to_join":['Notes', 'Notes2',"Notes3", "Notes4"],
            "link_col" : ["Internal_Item_link_in_the_Repository"],
            "format_col" : ["Digital_Copy_File_Format"],
            "digital_file_col" : ["Digital_file_name"]
        }
        default_selected_columns_Correspondences = {
        "col_single" : ["Call_No","invaild_calssifactionBasile_No", "Film_Name_In_Arabic",
                                                  "Film_Name_in_English", "Film_Name_In_Latin", "Film_Name_in_French", "Published_Created",
                                                 "Links_to_digital_copy", "Physical_Description", "Summary",
                                                "Acquisition_source","Repository", "Scanned_File_Format",
                                                "Film_Information_Link", "Path", "Film_Id"],
        "col_list" : ["Type_of_material", "Notes", "Material_status", "Material_Language","Section",
                                        "Keywords"], 
        "col_int" : ["Number_of_copies"],
        "col_to_date" : ["Published_Created"],
        "col_sets_to_join":['Notes', 'Notes2',"Notes3", "Notes4"],
        "link_col" : ["Internal_Item_link_in_the_Repository"],
        "format_col" : ["Digital_Copy_File_Format"],
        "digital_file_col" : ["Digital_file_name"]    
        }
        default_selected_columns_Postersphotos = {
        "col_single" : ["Call_No", "Type_of_material", "Film_Name_In_Arabic", "Film_Name_In_English",
                        "Film_Name_In_Latin", "Film_Name_In_French", "Physical_Description",
                        "Summary", "Acquisition_source", "Repository", "Path",
                        "Scanned_File_Format","Material_status", "Material_Language",
                        "People_items_in_photo", "Film_Information_Link",
                        "color", "Film_ID", "Published_Created_Poster",
                    "Links_to_digital_copy", "Title_in_Arabic", "Title_in_latin",
                    "Title_in_French", "Title_in_English"],
        "col_list":["Section", "Subject", "Notes", "People_items_in_photo"],
        "col_int": ["Number_of_copies"],
        "col_to_date" : ["Published_Created_Poster"],
        "col_sets_to_join": ['Notes', 'Notes2',"Notes3", "Notes4"],
        "link_col" : ["Internal_Item_link_in_the_Repository"],
        "format_col" : ["Digital_Copy_File_Format"],
        "digital_file_col" : ["Digital_file_name"]

        }
        default_selected_columns_Contracts = {
            "col_single" : ["Call_No", "Section", "Type_of_material", "Title_in_Arabic",
                            "Title_in_English", "Title_in_latin", "Title_in_French",
                            "Links_to_digital_copy", "Physical_Description", "Summary",
                            "Form_Genre", "Contract_duration", "Acquisition_source",
                            "Repository", "Scanned_File_Format", "Film_Information_Link",
                            "invaild_calssifactionBasile_No", "Film_Name_In_Arabic",
                            "Film_Name_In_Latin", "Film_Name_In_French"],
            "col_list": ["Keywords", "Contract_Parties_in_Arabic", "Contract_Parties_in_Latin",
                        "Contract_Parties_in_English", "Notes", "Material_status", "Material_Language"],
            "col_int":["Number_of_copies"],
            "col_to_date" : ["Published_Created"],
            "col_sets_to_join":['Notes', 'Notes2',"Notes3"],
            "link_col" : ["Internal_Item_link_in_the_Repository"],
            "format_col" : ["Digital_Copy_File_Format"],
            "digital_file_col" : ["Digital_file_name"]

        }
        for param in self.transform_data_params:
            label = QLabel(f'<b><font size="5">{param}</font></b>')
            scroll_content_layout.addWidget(label, row, 0, 1, 3)  # Span label across 3 columns
            row += 1

            for column in self.dataframe.columns:
                checkbox = QCheckBox(column)
                # Check if the checkbox should be initially checked based on df_name
                if self.df_name == 'Film_full_Cataloging' and column in default_selected_columns_Catalog.get(param, []):
                    checkbox.setChecked(True)
                    for key, value in default_selected_columns_Catalog.items():
                        self.selected_columns[key] = value
                if self.df_name == 'Lobby_card' and column in default_selected_columns_Lobby_card.get(param, []):
                    checkbox.setChecked(True)
                    for key, value in default_selected_columns_Lobby_card.items():
                        self.selected_columns[key] = value
                if self.df_name == 'Correspondences' and column in default_selected_columns_Correspondences.get(param, []):
                    checkbox.setChecked(True)
                    for key, value in default_selected_columns_Correspondences.items():
                        self.selected_columns[key] = value
                if self.df_name == 'Postersphotos' and column in default_selected_columns_Postersphotos.get(param, []):
                    checkbox.setChecked(True)
                    for key, value in default_selected_columns_Postersphotos.items():
                        self.selected_columns[key] = value
                if self.df_name == 'Contracts' and column in default_selected_columns_Contracts.get(param, []):
                    checkbox.setChecked(True)
                    for key, value in default_selected_columns_Contracts.items():
                        self.selected_columns[key] = value 
                checkbox.stateChanged.connect(lambda state, col=column, param=param: self.on_checkbox_change(state, col, param))
                scroll_content_layout.addWidget(checkbox, row, col)
                col = (col + 1) % 3
                if col == 0:
                    row += 1

            # Add an empty row for spacing after each parameter
            row += 1
      
        
        scroll_area.setWidget(scroll_content)
        layout.addWidget(scroll_area)

        self.btn_transform = QPushButton('Transform', self)
        self.btn_transform.clicked.connect(self.transform)
        layout.addWidget(self.btn_transform)

        self.setLayout(layout)
        self.setWindowTitle(f'Customize Columns for {self.df_name}')

    def on_checkbox_change(self, state, column, param):
        if state == 2:  # Checked
            self.selected_columns[param].append(column)
        elif state == 0:  # Unchecked
            self.selected_columns[param].remove(column)
        
    def transform(self):
        print(self.selected_columns)
        transformed_data = transform_data(self.dataframe, **self.selected_columns)
        if transformed_data is not None:
            self.transformed_dataframes[self.df_name] = transformed_data
            self.accept()
        else:
            self.lbl_status.setText('Transformation failed. Please select at least one column.')

class ETLGUI(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()

    def initUI(self):
        self.setWindowTitle('ETL Process GUI')
        self.setGeometry(300, 300, 400, 200)

        self.lbl_status = QLabel('Status: Ready', self)
        self.btn_select_file = QPushButton('Select Excel File', self)
        self.btn_extract = QPushButton('Extract Data', self)
        self.btn_transform = QPushButton('Transform Data', self)
        self.btn_save_transformed = QPushButton('Save Transformed Data', self)
        self.btn_upload = QPushButton('Upload to Database', self)

        self.btn_select_file.clicked.connect(self.select_file)
        self.btn_extract.clicked.connect(self.extract_data)
        self.btn_transform.clicked.connect(self.transform_data)
        self.btn_save_transformed.clicked.connect(self.save_transformed_data)
        self.btn_upload.clicked.connect(self.upload_to_db)

        self.file_path = None  # Store the file path
        self.transformed_dataframes = {}  # Dictionary to store transformed dataframes

        vbox = QVBoxLayout()
        vbox.addWidget(self.lbl_status)
        vbox.addWidget(self.btn_select_file)
        vbox.addWidget(self.btn_extract)
        vbox.addWidget(self.btn_transform)
        vbox.addWidget(self.btn_save_transformed)
        vbox.addWidget(self.btn_upload)

        self.setLayout(vbox)

    def select_file(self):
        options = QFileDialog.Options()
        options |= QFileDialog.DontUseNativeDialog
        file_path, _ = QFileDialog.getOpenFileName(self, "Select Excel File for Extraction", "", "Excel Files (*.xlsx);;All Files (*)", options=options)

        if file_path:
            self.file_path = file_path
            self.lbl_status.setText(f'Selected file for extraction: {file_path}')
        else:
            self.lbl_status.setText('Extraction canceled')

    def extract_data(self):
        if self.file_path:
            self.extracted_data = extract_data(self.file_path)
            self.lbl_status.setText('Data extracted successfully.')
        else:
            self.lbl_status.setText('Please select an Excel file first.')

    def transform_data(self):
        if self.file_path:
            
            df_names = list(self.extracted_data.keys())

            for df_name in df_names:
                selected_dataframe = self.extracted_data.get(df_name)
                if selected_dataframe is not None:
                    dialog = TransformDialog(selected_dataframe, df_name, self.transformed_dataframes)
                    if dialog.exec_() == QDialog.Accepted:
                        self.lbl_status.setText(f'Transformation for {df_name} completed.')
                    else:
                        self.lbl_status.setText('Transformation canceled.')
                else:
                    self.lbl_status.setText(f'No valid DataFrame to transform for {df_name}.')

        else:
            self.lbl_status.setText('Please select an Excel file first.')

    def save_transformed_data(self):
        if self.transformed_dataframes:
            output_file_name, _ = QFileDialog.getSaveFileName(self, "Save Excel File", "", "Excel Files (*.xlsx)")
            if output_file_name:
                with pd.ExcelWriter(output_file_name) as writer:
                    for df_name, df in self.transformed_dataframes.items():
                        df.to_excel(writer, sheet_name=df_name, index=False)
                self.lbl_status.setText('Transformed data saved to Excel file.')
            else:
                self.lbl_status.setText('Save canceled.')
        else:
            self.lbl_status.setText('No transformed data to save.')

    def upload_to_db(self):
        # Assuming transformed_data is available (you can modify this accordingly)
        if self.transformed_dataframes:
            df_names = list(self.transformed_dataframes.keys())
            for df_name in df_names:
                selected_dataframe = self.transformed_dataframes.get(df_name)
                if selected_dataframe is not None and not selected_dataframe.empty:
                    try:
                        upload_to_database(df_name,selected_dataframe)  # Implement your database upload logic here
                        self.lbl_status.setText(f'Table {df_name} uploaded to the database.')
                    except Exception as E:
                        self.lbl_status.setText(f'An Error Occured During Uploading Dataframe {selected_dataframe} : {E}')
                        
                else:
                    self.lbl_status.setText(f'No valid DataFrame to transform for {df_name}.')
        else:
            self.lbl_status.setText('Please transform the data first.')

if __name__ == '__main__':
    app = QApplication(sys.argv)
    gui = ETLGUI()
    gui.show()
    sys.exit(app.exec_())
