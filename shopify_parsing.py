import pandas as pd
import sys
import numpy as np
import csv
import gspread
from oauth2client.service_account import ServiceAccountCredentials

class ShopifyParser:

    def __init__(self):
        self.masterfile = pd.read_csv("product_masterfile.csv")
        self.credentials = "credentials.json"

        self.glowcare_sheet = "1F9JwYgSKO0wY9qPPMuJNsiX2eCz4inUIljOKK45I0A8"
        self.glowcare_input = "orders_export (9).csv"

        self.fleximart_sheet = "1XuLCEPS3F7fiz3UtDWgUF6pUOq4BmNFrUhdMXHNu_Uw"
        self.fleximart_input = ""

        self.glownest_sheet = "1K2tvS41V16-GShLlWxeRUE5L61llkVEp2GnGgLuwSYE"
        self.glownest_input = "orders_export (2).csv"

        self.herbiotics_sheet = "1_AoyPt2tqMZxYRc8YMLP58TglnQl7wgrOemAOHtTHZ0"
        self.herbiotics_input = "orders_export (3).csv"

        self.bw_sheet = ""
        self.bw_input = ""

        self.stacks_sheet = "1yxO303uIsU6GMcwXbLB0FihYvSfYuBWETAMNlTR07nw"
        self.stacks_input = "orders_export.csv"

        self.nutria_sheet = ""
        self.nutria_input = "orders_export (1).csv"

        self.gravien_sheet = "1Abs6tPDIXDtFQWVYzA6_jdCMGOX2N8200dc6DfS_rc4"
        self.gravien_input = ""

        self.primep_sheet = ""
        self.primep_input = ""

        self.glamore_sheet = ""
        self.glamore_input = "orders_export (5).csv"

        self.wandw_sheet = ""
        self.wandw_input = ""

        self.flash_sheet = ""
        self.flash_input = ""

        self.ML_sheet = ""
        self.ML_input = "orders_export (21).csv"

    def csv_parser(self, csv_file, alt=False):

        # read in csv file and extract relevant columns
        df = pd.read_csv(csv_file)
        if(alt):
            self.input_data = df[["Name", "Created at", "Shipping Name", "Shipping Phone", "Total", "Lineitem name", "Lineitem quantity"]]
        else:
            self.input_data = df[["Name", "Created at", "Shipping Phone", "Total", "Lineitem name", "Lineitem quantity"]]
        self.data_length = len(self.input_data["Name"])

        # initialize output dataframe
        self.output_data = {"Date": [], 
                       "Order Number": [], 
                       "Phone Number": [], 
                       "Revenue": []
                       }
        if(alt):
            self.output_data["Customer"] = []

        # loop through all rows
        prev_name = self.input_data["Name"][0]
        for i in range(self.data_length):

            # parse date into readable string
            if(self.input_data["Name"][i] == prev_name and i > 0):
                continue
            else:
                prev_name = self.input_data["Name"][i]
            date_str = self.input_data["Created at"][i]
            date_obj = pd.to_datetime(date_str)
            self.output_data["Date"].append(date_obj.strftime("%B %-d, %Y"))

            # parse order number
            if(isinstance(self.input_data["Name"][0], str)):
                self.output_data["Order Number"].append(self.input_data["Name"][i])
            else:
                self.output_data["Order Number"].append("{:.0f}".format(self.input_data["Name"][i]))

            # parse phone number
            phone_str = "{:.0f}".format(self.input_data["Shipping Phone"][i])
            if(phone_str.startswith("880")):
                phone_str = phone_str[3:]
            elif(phone_str.startswith("92")):
                phone_str = phone_str[2:]
            elif(phone_str.startswith("966")):
                phone_str = phone_str[4:]

            self.output_data["Phone Number"].append(phone_str)

            # parse revenue
            self.output_data["Revenue"].append("{:.2f}".format(self.input_data["Total"][i]))

            # parse customer name if needed
            if(alt):
                self.output_data["Customer"].append(self.input_data["Shipping Name"][i])


    def parse_GC(self):
        
        # add GC products to output dataframe
        self.output_data["Undereye serum PCS"] = ['']*len(self.output_data["Date"])
        self.output_data["Hairline powder"] = ['']*len(self.output_data["Date"])
        self.output_data["Hair Wax Stick"] = ['']*len(self.output_data["Date"])
        self.output_data["Hair Donut Scrunchie"] = ['']*len(self.output_data["Date"])
        self.output_data["Night Shred"] = ['']*len(self.output_data["Date"])
        self.output_data["Seamless Straight Hair Strand"] = ['']*len(self.output_data["Date"])
        self.output_data["Seapuri Scalpy Hair System"] = ['']*len(self.output_data["Date"])
        self.output_data["DHT Blocker"] = ['']*len(self.output_data["Date"])
        self.output_data["Feg Hair Growth Spray"] = ['']*len(self.output_data["Date"])
        self.output_data["Moroccan Blue Nila Skin Whitening Powder"] = ['']*len(self.output_data["Date"])
        self.output_data["Zephta H-Regrow 2.0"] = ['']*len(self.output_data["Date"])
        self.output_data["Shampoo"] = ['']*len(self.output_data["Date"])
        self.output_data["Duo Curl"] = ['']*len(self.output_data["Date"])
        self.output_data["Effeclar Duo Acne Spot Treatment"] = ['']*len(self.output_data["Date"])
        self.output_data["Facial Hair Removal Kit"] = ['']*len(self.output_data["Date"])
        self.output_data["SleepSlime Gummies"] = ['']*len(self.output_data["Date"])
        self.output_data["The Real Beef Tallow Balm"] = ['']*len(self.output_data["Date"])
        self.output_data["Wax"] = ['']*len(self.output_data["Date"])
        self.output_data["Hair Revival Duo"] = ['']*len(self.output_data["Date"])
        self.output_data["Hair Volume Duo"] = ['']*len(self.output_data["Date"])

        # loop through all line items and mark their quantities
        prev_name = self.input_data["Name"][0]
        marker = 0
        group_size = 0
        for i in range(self.data_length):

            input_lineitem = self.input_data["Lineitem name"][i]

            # check if product is known
            lookup_idx = np.where(np.asarray(self.masterfile["Lineitem name"]) == input_lineitem)[0]
            if(len(lookup_idx) == 0):

                # if product is not known, add to product masterfile
                print("Product '{}' is not known, product needs to be added to the masterfile.".format(input_lineitem))
                print("0 - Undereye serum PCS")
                print("1 - Hairline powder")
                print("2 - Hair Wax Stick")
                print("3 - Hair Donut Scrunchie")
                print("4 - Night Shred")
                print("5 - Seamless Straight Hair Strand")
                print("6 - Seapuri Scalpy Hair System")
                print("7 - DHT Blocker")
                print("8 - Feg Hair Growth Spray")
                print("9 - Moroccan Blue Nila Skin Whitening Powder")
                print("10 - Zephta H-Regrow 2.0")
                print("11 - Shampoo")
                print("12 - Duo Curl")
                print("13 - Effeclar Duo Acne Spot Treatment")
                print("14 - Facial Hair Removal Kit")
                print("15 - SleepSlime Gummies")
                print("16 - The Real Beef Tallow Balm")
                print("17 - Wax")
                print("18 - Hair Revival Duo")
                print("19 - Hair Volume Duo")
                print("20 - test product, omit")
                product = input("Please enter the number (0-20) that corresponds with the correct product for '{}': ".format(input_lineitem))

                # Check that the input is a number and within the specified range
                while(not product.isnumeric()):
                    print("Input was not valid! Try again...")
                    product = input("Please enter the number (0-20) that corresponds with the correct product for '{}': ".format(input_lineitem))
                    if(product.isnumeric() and int(product) > 20):
                        product = "Nan"
                while(int(product) > 20):
                    print("Input was not valid! Try again...")
                    product = input("Please enter the number (0-2) that corresponds with the correct product for '{}': ".format(input_lineitem))
                
                # convert product index to product name
                product_name = "Undereye serum PCS"
                if(product == "1"):
                    product_name = "Hairline powder"
                elif(product == "2"):
                    product_name = "Hair Wax Stick"
                elif(product == "3"):
                    product_name = "Hair Donut Scrunchie"
                elif(product == "4"):
                    product_name = "Night Shred"
                elif(product == "5"):
                    product_name = "Seamless Straight Hair Strand"
                elif(product == "6"):
                    product_name = "Seapuri Scalpy Hair System"
                elif(product == "7"):
                    product_name = "DHT Blocker"
                elif(product == "8"):
                    product_name = "Feg Hair Growth Spray"
                elif(product == "9"):
                    product_name = "Moroccan Blue Nila Skin Whitening Powder"
                elif(product == "10"):
                    product_name = "Zephta H-Regrow 2.0"
                elif(product == "11"):
                    product_name = "Shampoo"
                elif(product == "12"):
                    product_name = "Duo Curl"
                elif(product == "13"):
                    product_name = "Effeclar Duo Acne Spot Treatment"
                elif(product == "14"):
                    product_name = "Facial Hair Removal Kit"
                elif(product == "15"):
                    product_name = "SleepSlime Gummies"
                elif(product == "16"):
                    product_name = "The Real Beef Tallow Balm"
                elif(product == "17"):
                    product_name = "Wax"
                elif(product == "18"):
                    product_name = "Hair Revival Duo"
                elif(product == "19"):
                    product_name = "Hair Volume Duo"
                else:
                    product_name = "TEST"

                # ask for quantity and check that input is numeric
                quantity = input("Please enter the quantity for product '{}': ".format(input_lineitem))
                while(not product.isnumeric()):
                    print("Input was not valid! Try again...")
                    quantity = input("Please enter the quantity for product '{}: '".format(input_lineitem))

                # write new line to masterfile
                with open("product_masterfile.csv", "a", newline="") as f:
                    csv.writer(f).writerow([input_lineitem, product_name, quantity])
                
                # read the new masterfile
                self.masterfile = pd.read_csv("product_masterfile.csv")

                # account for duplicate order numbers
                if(i > 0 and self.input_data["Name"][i] == prev_name):
                    marker -= 1
                    group_size += 1
                else:
                    marker = i - group_size
                    prev_name = self.input_data["Name"][i]

                # write data to output dataframe
                if(product_name == "Hair Revival Duo"):
                    self.output_data["DHT Blocker"][marker] = "{:.0f}".format(int(self.input_data["Lineitem quantity"][i]))
                    self.output_data["Feg Hair Growth Spray"][marker] = "{:.0f}".format(int(self.input_data["Lineitem quantity"][i]))
                    self.output_data["Hair Revival Duo"][marker] = "1"
                elif(product_name == "Hair Volume Duo"):
                    self.output_data["Hairline Powder"][marker] = "{:.0f}".format(int(self.input_data["Lineitem quantity"][i]))
                    self.output_data["Seamless Straight Hair Strand"][marker] = "{:.0f}".format(int(self.input_data["Lineitem quantity"][i]))
                    self.output_data["Hair Volume Duo"][marker] = "1"
                elif(product_name == "TEST"):
                    marker += 1
                    continue
                else:
                    total = int(quantity) * int(self.input_data["Lineitem quantity"][i])
                    self.output_data[product_name][i] = "{:.0f}".format(total)
            else:

                # extract data from masterfile
                product_name = self.masterfile["Product"][lookup_idx[0]]

                # account for duplicate order numbers
                if(i > 0 and self.input_data["Name"][i] == prev_name):
                    marker -= 1
                    group_size += 1
                else:
                    marker = i - group_size
                    prev_name = self.input_data["Name"][i]


                # write data to output dataframe
                if(product_name == "Hair Revival Duo"):
                    self.output_data["DHT Blocker"][marker] = "{:.0f}".format(int(self.input_data["Lineitem quantity"][i]))
                    self.output_data["Feg Hair Growth Spray"][marker] = "{:.0f}".format(int(self.input_data["Lineitem quantity"][i]))
                    self.output_data["Hair Revival Duo"][marker] = "1"
                elif(product_name == "Hair Volume Duo"):
                    self.output_data["Hairline Powder"][marker] = "{:.0f}".format(int(self.input_data["Lineitem quantity"][i]))
                    self.output_data["Seamless Straight Hair Strand"][marker] = "{:.0f}".format(int(self.input_data["Lineitem quantity"][i]))
                    self.output_data["Hair Volume Duo"][marker] = "1"
                elif(product_name == "TEST"):
                    marker += 1
                    continue
                else:
                    total = int(self.input_data["Lineitem quantity"][i]) * int(self.masterfile["Quantity"][lookup_idx[0]])
                    self.output_data[product_name][marker] = "{:.0f}".format(total)

            marker += 1
        
        self.append_to_sheet_GC()


    def append_to_sheet_GC(self):
        
        # authenticate credentials to google drive and open sheet
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(self.credentials, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(self.glowcare_sheet).worksheet("P/L")

        start_row = len(sheet.col_values(2)) + 1

        info_array = []
        item_array = []

        # loop through and process rows
        for i in range(len(self.output_data["Date"])):

            # create array of order info and items
            info_array.append([self.output_data["Date"][i], 
                                self.output_data["Order Number"][i], 
                                self.output_data["Phone Number"][i], 
                                self.output_data["Revenue"][i]])
            
            item_array.append([self.output_data["Undereye serum PCS"][i], 
                                self.output_data["Hairline powder"][i], 
                                self.output_data["Hair Wax Stick"][i], 
                                self.output_data["Hair Donut Scrunchie"][i], 
                                self.output_data["Night Shred"][i], 
                                self.output_data["Seamless Straight Hair Strand"][i], 
                                self.output_data["Seapuri Scalpy Hair System"][i], 
                                self.output_data["DHT Blocker"][i], 
                                self.output_data["Feg Hair Growth Spray"][i], 
                                self.output_data["Moroccan Blue Nila Skin Whitening Powder"][i], 
                                self.output_data["Zephta H-Regrow 2.0"][i], 
                                self.output_data["Shampoo"][i], 
                                self.output_data["Duo Curl"][i], 
                                self.output_data["Effeclar Duo Acne Spot Treatment"][i], 
                                self.output_data["Facial Hair Removal Kit"][i], 
                                self.output_data["SleepSlime Gummies"][i], 
                                self.output_data["The Real Beef Tallow Balm"][i], 
                                self.output_data["Wax"][i]])
            
            # handle checkboxes separately
            if(self.output_data["Hair Revival Duo"][i] == "1"):
                sheet.update_acell("P/L!AA{}".format(start_row + i), True)
            if(self.output_data["Hair Volume Duo"][i] == "1"):
                sheet.update_acell("P/L!AB{}".format(start_row + i), True)

        # paste arrays into spreadsheet
        sheet.spreadsheet.values_update("P/L!B{}:E".format(start_row), 
                                        params={'valueInputOption': 'USER_ENTERED'}, 
                                        body={'values': info_array})
        sheet.spreadsheet.values_update("P/L!I{}:Z".format(start_row), 
                                        params={'valueInputOption': 'USER_ENTERED'},
                                        body={'values': item_array})
    

    def parse_glownest(self):
        
        # add glownest products to output dataframe
        self.output_data["SFD"] = ['']*len(self.output_data["Date"])
        self.output_data["TKAP"] = ['']*len(self.output_data["Date"])
        self.output_data["KAHI"] = ['']*len(self.output_data["Date"])
        self.output_data["DVCCC"] = ['']*len(self.output_data["Date"])
        self.output_data["345RC"] = ['']*len(self.output_data["Date"])
        self.output_data["SCS"] = ['']*len(self.output_data["Date"])
        self.output_data["SFD 2.0"] = ['']*len(self.output_data["Date"])
        self.output_data["HD"] = ['']*len(self.output_data["Date"])
        self.output_data["GUMMIES"] = ['']*len(self.output_data["Date"])
        self.output_data["LullaBites"] = ['']*len(self.output_data["Date"])
        self.output_data["URO"] = ['']*len(self.output_data["Date"])
        self.output_data["WAX"] = ['']*len(self.output_data["Date"])

        # loop through all line items and mark their quantities
        prev_name = self.input_data["Name"][0]
        marker = 0
        group_size = 0
        for i in range(self.data_length):

            input_lineitem = self.input_data["Lineitem name"][i]

            # check if product is known
            lookup_idx = np.where(np.asarray(self.masterfile["Lineitem name"]) == input_lineitem)[0]
            if(len(lookup_idx) == 0):

                # if product is not known, add to product masterfile
                print("Product '{}' is not known, product needs to be added to the masterfile.".format(input_lineitem))
                print("0 - SFD")
                print("1 - TKAP")
                print("2 - KAHI")
                print("3 - DVCCC")
                print("4 - 345RC")
                print("5 - SCS")
                print("6 - SFD 2.0")
                print("7 - HD")
                print("8 - GUMMIES")
                print("9 - LullaBites")
                print("10 - URO")
                print("11 - WAX")
                print("12 - test product, omit")
                product = input("Please enter the number (0-12) that corresponds with the correct product for '{}': ".format(input_lineitem))

                # Check that the input is a number and within the specified range
                while(not product.isnumeric()):
                    print("Input was not valid! Try again...")
                    product = input("Please enter the number (0-12) that corresponds with the correct product for '{}': ".format(input_lineitem))
                    if(product.isnumeric() and int(product) > 12):
                        product = "Nan"
                while(int(product) > 12):
                    print("Input was not valid! Try again...")
                    product = input("Please enter the number (0-2) that corresponds with the correct product for '{}': ".format(input_lineitem))
                
                # convert product index to product name
                product_name = "SFD"
                if(product == "1"):
                    product_name = "TKAP"
                elif(product == "2"):
                    product_name = "KAHI"
                elif(product == "3"):
                    product_name = "DVCCC"
                elif(product == "4"):
                    product_name = "345RC"
                elif(product == "5"):
                    product_name = "SCS"
                elif(product == "6"):
                    product_name = "SFD 2.0"
                elif(product == "7"):
                    product_name = "HD"
                elif(product == "8"):
                    product_name = "GUMMIES"
                elif(product == "9"):
                    product_name = "LullaBites"
                elif(product == "10"):
                    product_name = "URO"
                elif(product == "11"):
                    product_name = "WAX"
                else:
                    product_name = "TEST"
                
                # ask for quantity and check that input is numeric
                quantity = input("Please enter the quantity for product '{}': ".format(input_lineitem))
                while(not product.isnumeric()):
                    print("Input was not valid! Try again...")
                    quantity = input("Please enter the quantity for product '{}: '".format(input_lineitem))
                
                # write new line to masterfile
                with open("product_masterfile.csv", "a", newline="") as f:
                    csv.writer(f).writerow([input_lineitem, product_name, quantity])
                
                # read the new masterfile
                self.masterfile = pd.read_csv("product_masterfile.csv")

                # account for duplicate order numbers
                if(i > 0 and self.input_data["Name"][i] == prev_name):
                    marker -= 1
                    group_size += 1
                else:
                    marker = i - group_size
                    prev_name = self.input_data["Name"][i]

                if(product_name == "TEST"):
                    marker += 1
                    continue
                else:
                    total = int(quantity) * int(self.input_data["Lineitem quantity"][i])
                    self.output_data[product_name][marker] = "{:.0f}".format(total)
            else:

                # extract data from masterfile
                product_name = self.masterfile["Product"][lookup_idx[0]]

                # account for duplicate order numbers
                if(i > 0 and self.input_data["Name"][i] == prev_name):
                    marker -= 1
                    group_size += 1
                else:
                    marker = i - group_size
                    prev_name = self.input_data["Name"][i]

                if(product_name == "TEST"):
                    marker += 1
                    continue
                else:
                    total = int(self.input_data["Lineitem quantity"][i]) * int(self.masterfile["Quantity"][lookup_idx[0]])
                    self.output_data[product_name][marker] = "{:.0f}".format(total)

            marker += 1
    
        self.append_to_sheet_glownest()


    def append_to_sheet_glownest(self):
        
        # authenticate credentials to google drive and open sheet
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(self.credentials, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(self.glownest_sheet).worksheet("P/L")

        start_row = len(sheet.col_values(2)) + 1

        info_array = []
        item_array = []

        # loop through and process rows
        for i in range(len(self.output_data["Date"])):

            # create array of order info and items
            info_array.append([self.output_data["Date"][i], 
                                self.output_data["Order Number"][i], 
                                self.output_data["Phone Number"][i], 
                                self.output_data["Revenue"][i]])
            
            item_array.append([self.output_data["SFD"][i], 
                                self.output_data["TKAP"][i], 
                                self.output_data["KAHI"][i], 
                                self.output_data["DVCCC"][i], 
                                self.output_data["345RC"][i], 
                                self.output_data["SCS"][i], 
                                self.output_data["SFD 2.0"][i], 
                                self.output_data["HD"][i], 
                                self.output_data["GUMMIES"][i], 
                                self.output_data["LullaBites"][i], 
                                self.output_data["URO"][i], 
                                self.output_data["WAX"][i]])

        # paste arrays into spreadsheet
        sheet.spreadsheet.values_update("P/L!A{}:D".format(start_row), 
                                        params={'valueInputOption': 'USER_ENTERED'}, 
                                        body={'values': info_array})
        sheet.spreadsheet.values_update("P/L!H{}:S".format(start_row), 
                                        params={'valueInputOption': 'USER_ENTERED'},
                                        body={'values': item_array})


    def parse_herbiotics(self):
        
        # add herbiotics products to output dataframe
        self.output_data["E&F MINTS"] = ['']*len(self.output_data["Date"])
        self.output_data["Metadetox"] = ['']*len(self.output_data["Date"])
        self.output_data["Mg"] = ['']*len(self.output_data["Date"])
        self.output_data["Ca Balm"] = ['']*len(self.output_data["Date"])
        self.output_data["Mg Powder"] = ['']*len(self.output_data["Date"])
        self.output_data["Toothpaste"] = ['']*len(self.output_data["Date"])
        self.output_data["Roller"] = ['']*len(self.output_data["Date"])
        self.output_data["PDA"] = ['']*len(self.output_data["Date"])
        self.output_data["Oil"] = ['']*len(self.output_data["Date"])
        self.output_data["ACVE"] = ['']*len(self.output_data["Date"])

        # loop through all line items and mark their quantities
        prev_name = self.input_data["Name"][0]
        marker = 0
        group_size = 0
        for i in range(self.data_length):

            input_lineitem = self.input_data["Lineitem name"][i]

            # check if product is known
            lookup_idx = np.where(np.asarray(self.masterfile["Lineitem name"]) == input_lineitem)[0]
            if(len(lookup_idx) == 0):

                # if product is not known, add to product masterfile
                print("Product '{}' is not known, product needs to be added to the masterfile.".format(input_lineitem))
                print("0 - E&F MINTS")
                print("1 - Metadetox")
                print("2 - Mg")
                print("3 - Ca Balm")
                print("4 - Mg Powder")
                print("5 - Toothpaste")
                print("6 - Roller")
                print("7 - PDA")
                print("8 - Oil")
                print("9 - ACVE")
                print("10 - test product, omit")
                product = input("Please enter the number (0-10) that corresponds with the correct product for '{}': ".format(input_lineitem))

                # Check that the input is a number and within the specified range
                while(not product.isnumeric()):
                    print("Input was not valid! Try again...")
                    product = input("Please enter the number (0-10) that corresponds with the correct product for '{}': ".format(input_lineitem))
                    if(product.isnumeric() and int(product) > 10):
                        product = "Nan"
                while(int(product) > 10):
                    print("Input was not valid! Try again...")
                    product = input("Please enter the number (0-2) that corresponds with the correct product for '{}': ".format(input_lineitem))
                
                # convert product index to product name
                product_name = "E&F MINTS"
                if(product == "1"):
                    product_name = "Metadetox"
                elif(product == "2"):
                    product_name = "Mg"
                elif(product == "3"):
                    product_name = "Ca Balm"
                elif(product == "4"):
                    product_name = "Mg Powder"
                elif(product == "5"):
                    product_name = "Toothpaste"
                elif(product == "6"):
                    product_name = "Roller"
                elif(product == "7"):
                    product_name = "PDA"
                elif(product == "8"):
                    product_name = "Oil"
                elif(product == "9"):
                    product_name = "ACVE"
                else:
                    product_name = "TEST"

                # ask for quantity and check that input is numeric
                quantity = input("Please enter the quantity for product '{}': ".format(input_lineitem))
                while(not product.isnumeric()):
                    print("Input was not valid! Try again...")
                    quantity = input("Please enter the quantity for product '{}: '".format(input_lineitem))
                
                # write new line to masterfile
                with open("product_masterfile.csv", "a", newline="") as f:
                    csv.writer(f).writerow([input_lineitem, product_name, quantity])
                
                # read the new masterfile
                self.masterfile = pd.read_csv("product_masterfile.csv")

                # account for duplicate order numbers
                if(i > 0 and self.input_data["Name"][i] == prev_name):
                    marker -= 1
                    group_size += 1
                else:
                    marker = i - group_size
                    prev_name = self.input_data["Name"][i]

                if(product_name == "TEST"):
                    marker += 1
                    continue
                else:
                    total = int(quantity) * int(self.input_data["Lineitem quantity"][i])
                    self.output_data[product_name][marker] = "{:.0f}".format(total)
            else:

                # extract data from masterfile
                product_name = self.masterfile["Product"][lookup_idx[0]]

                # account for duplicate order numbers
                if(i > 0 and self.input_data["Name"][i] == prev_name):
                    marker -= 1
                    group_size += 1
                else:
                    marker = i - group_size
                    prev_name = self.input_data["Name"][i]

                if(product_name == "TEST"):
                    marker += 1
                    continue
                else:
                    total = int(self.input_data["Lineitem quantity"][i]) * int(self.masterfile["Quantity"][lookup_idx[0]])
                    self.output_data[product_name][marker] = "{:.0f}".format(total)
            
            marker += 1
    
        self.append_to_sheet_herbiotics()


    def append_to_sheet_herbiotics(self):
        
        # authenticate credentials to google drive and open sheet
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(self.credentials, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(self.herbiotics_sheet).worksheet("P/L")

        start_row = len(sheet.col_values(2)) + 1

        info_array = []
        item_array = []

        # loop through and process rows
        for i in range(len(self.output_data["Date"])):

            # create array of order info and items
            info_array.append([self.output_data["Date"][i], 
                                self.output_data["Order Number"][i], 
                                self.output_data["Phone Number"][i], 
                                self.output_data["Revenue"][i]])

            item_array.append([self.output_data["E&F MINTS"][i], 
                                self.output_data["Metadetox"][i], 
                                self.output_data["Mg"][i], 
                                self.output_data["Ca Balm"][i], 
                                self.output_data["Mg Powder"][i], 
                                self.output_data["Toothpaste"][i], 
                                self.output_data["Roller"][i], 
                                self.output_data["PDA"][i], 
                                self.output_data["Oil"][i], 
                                self.output_data["ACVE"][i]])
            
        #paste arrays into spreadsheet
        sheet.spreadsheet.values_update("P/L!A{}:D".format(start_row), 
                                        params={'valueInputOption': 'USER_ENTERED'}, 
                                        body={'values': info_array})
        sheet.spreadsheet.values_update("P/L!I{}:R".format(start_row), 
                                        params={'valueInputOption': 'USER_ENTERED'},
                                        body={'values': item_array})


    def parse_stacks(self):
        
        # add stacks products to output dataframe
        self.output_data["Ark Drops"] = ['']*len(self.output_data["Date"])
        self.output_data["ACVE"] = ['']*len(self.output_data["Date"])

        # loop through all line items and mark their quantities
        prev_name = self.input_data["Name"][0]
        marker = 0
        group_size = 0
        for i in range(self.data_length):

            input_lineitem = self.input_data["Lineitem name"][i]

            # check if product is known
            lookup_idx = np.where(np.asarray(self.masterfile["Lineitem name"]) == input_lineitem)[0]
            if(len(lookup_idx) == 0):

                # if product is not known, add to product masterfile
                print("Product '{}' is not known, product needs to be added to the masterfile.".format(input_lineitem))
                print("0 - Ark Drops")
                print("1 - ACVE")
                print("2 - test product, omit")
                product = input("Please enter the number (0-2) that corresponds with the correct product for '{}': ".format(input_lineitem))

                # Check that the input is a number and within the specified range
                while(not product.isnumeric()):
                    print("Input was not valid! Try again...")
                    product = input("Please enter the number (0-2) that corresponds with the correct product for '{}': ".format(input_lineitem))
                    if(product.isnumeric() and int(product) > 2):
                        product = "Nan"
                while(int(product) > 2):
                    print("Input was not valid! Try again...")
                    product = input("Please enter the number (0-2) that corresponds with the correct product for '{}': ".format(input_lineitem))
                
                # convert product index to product name
                product_name = "Ark Drops"
                if(product == "1"):
                    product_name = "ACVE"
                else:
                    product_name = "TEST"

                # ask for quantity and check that input is numeric
                quantity = input("Please enter the quantity for product '{}': ".format(input_lineitem))
                while(not product.isnumeric()):
                    print("Input was not valid! Try again...")
                    quantity = input("Please enter the quantity for product '{}: '".format(input_lineitem))
                
                # write new line to masterfile
                with open("product_masterfile.csv", "a", newline="") as f:
                    csv.writer(f).writerow([input_lineitem, product_name, quantity])
                
                # read the new masterfile
                self.masterfile = pd.read_csv("product_masterfile.csv")

                # account for duplicate order numbers
                if(i > 0 and self.input_data["Name"][i] == prev_name):
                    marker -= 1
                    group_size += 1
                else:
                    marker = i - group_size
                    prev_name = self.input_data["Name"][i]

                if(product_name == "TEST"):
                    marker += 1
                    continue
                else:
                    total = int(quantity) * int(self.input_data["Lineitem quantity"][i])
                    self.output_data[product_name][marker] = "{:.0f}".format(total)
            else:

                # extract data from masterfile
                product_name = self.masterfile["Product"][lookup_idx[0]]

                # account for duplicate order numbers
                if(i > 0 and self.input_data["Name"][i] == prev_name):
                    marker -= 1
                    group_size += 1
                else:
                    marker = i - group_size
                    prev_name = self.input_data["Name"][i]

                if(product_name == "TEST"):
                    marker += 1
                    continue
                else:
                    total = int(self.input_data["Lineitem quantity"][i]) * int(self.masterfile["Quantity"][lookup_idx[0]])
                    self.output_data[product_name][marker] = "{:.0f}".format(total)
            
            marker += 1

        self.append_to_sheet_stacks()


    def append_to_sheet_stacks(self):
        
        # authenticate credentials to google drive and open sheet
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(self.credentials, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(self.stacks_sheet).worksheet("P/L")

        start_row = len(sheet.col_values(2)) + 1

        info_array = []
        item_array = []

        # loop through and process rows
        for i in range(len(self.output_data["Date"])):

            # create array of order info and items
            info_array.append([self.output_data["Order Number"][i], 
                                self.output_data["Date"][i], 
                                self.output_data["Customer"][i], 
                                self.output_data["Phone Number"][i], 
                                self.output_data["Revenue"][i]])

            item_array.append([self.output_data["Ark Drops"][i], 
                                self.output_data["ACVE"][i]])
        
        #paste arrays into spreadsheet
        sheet.spreadsheet.values_update("P/L!A{}:E".format(start_row), 
                                        params={'valueInputOption': 'USER_ENTERED'}, 
                                        body={'values': info_array})
        sheet.spreadsheet.values_update("P/L!J{}:K".format(start_row), 
                                        params={'valueInputOption': 'USER_ENTERED'},
                                        body={'values': item_array})


def main(argv=None):
    
    if argv is None:
        argv = sys.argv
    
    sp = ShopifyParser()
    sp.csv_parser("orders_export.csv", alt=True)
    sp.parse_stacks()

if __name__ == "__main__":
    sys.exit(main())