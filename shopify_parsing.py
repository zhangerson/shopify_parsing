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

        self.gc_sheet = "1F9JwYgSKO0wY9qPPMuJNsiX2eCz4inUIljOKK45I0A8"
        self.gc_input = "gc.csv"
        self.run_gc = True

        self.fleximart_sheet = "1XuLCEPS3F7fiz3UtDWgUF6pUOq4BmNFrUhdMXHNu_Uw"
        self.fleximart_input = "orders_export (15).csv"
        self.run_fleximart = True

        self.glownest_sheet = "1K2tvS41V16-GShLlWxeRUE5L61llkVEp2GnGgLuwSYE"
        self.glownest_input = "orders_export (2).csv"
        self.run_glownest = True

        self.herbiotics_sheet = "1_AoyPt2tqMZxYRc8YMLP58TglnQl7wgrOemAOHtTHZ0"
        self.herbiotics_input = "orders_export (3).csv"
        self.run_herbiotics = True

        self.Bw_sheet = "1qSc4rJpFiu6QQagHU9ZlxFQ_jcciz-xDveDyX8Sndf4"
        self.Bw_input = "orders_export (10).csv"
        self.run_Bw = True

        self.stacks_sheet = "1yxO303uIsU6GMcwXbLB0FihYvSfYuBWETAMNlTR07nw"
        self.stacks_input = "orders_export.csv"
        self.run_stacks = True

        self.nutria_sheet = "1uQJwL4GbNv-hqf_6r1iDU9CwhMLRXG1V-u4Y24ss1qk"
        self.nutria_input = "orders_export (1).csv"
        self.run_nutria = True

        self.gravien_sheet = "1Abs6tPDIXDtFQWVYzA6_jdCMGOX2N8200dc6DfS_rc4"
        self.gravien_input = "orders_export (11).csv"
        self.run_gravien = True

        self.primep_sheet = "1qxfb15xDWcWy5VouaPHwVirp2JP4YhJv5wZOZg1kWZc"
        self.primep_input = "orders_export (12).csv"
        self.run_primep = True

        self.glamore_sheet = "1l_VC0XsnUupaVdqDal2-IUuopFw4nRZav8TkWG8zDzc"
        self.glamore_input = "orders_export (5).csv"
        self.run_glamore = True

        self.wandw_sheet = "1vlO_iB61qeCVukmP7Stso3_dfps3Z_nAQ5m3xgKN1_8"
        self.wandw_input = "orders_export (13).csv"
        self.run_wandw = True

        self.flash_sheet = "1OQnhFekWrCeEILvn7Z3ZsX40UoYnWDKOiIJuO3D70GA"
        self.flash_input = "orders_export (14).csv"
        self.run_flash = True

        self.ML_sheet = "1jq4umlamappOYcugHmFzRVkFALzcPkU9hShk2oxSUeU"
        self.ML_input = "orders_export (21).csv"
        self.run_ML = True

        self.NB_sheet = "1qWAezLY9_nQuf5RN8idI8A-R6vm3lf_v3UXPoeyQ3lU"
        self.NB_input = "orders_export (8).csv"
        self.run_NB = True

        self.Rg_sheet = "1pGLbyAwuVtId4UzvsyOCX_pZB18DCEnIyBT9-mbBs8c"
        self.Rg_input = "orders_export (6).csv"
        self.run_Rg = True

        self.Fs_sheet = "16SES7pkE2P6oQXVtvDRTIXmnTvbRpSk-B410RUowbwY"
        self.Fs_input = "orders_export (7).csv"
        self.run_Fs = True

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

        print("Running parser for GC!")
        
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
                while(not product.isnumeric() or (product.isnumeric() and int(product) > 20)):
                    print("Input was not valid! Try again...")
                    product = input("Please enter the number (0-20) that corresponds with the correct product for '{}': ".format(input_lineitem))
                
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
                elif(product == "20"):
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
        sheet = client.open_by_key(self.gc_sheet).worksheet("P/L")

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
                sheet.update_acell("AA{}".format(start_row + i), True)
            if(self.output_data["Hair Volume Duo"][i] == "1"):
                sheet.update_acell("AB{}".format(start_row + i), True)

        # paste arrays into spreadsheet
        sheet.spreadsheet.values_update("P/L!B{}:E".format(start_row), 
                                        params={'valueInputOption': 'USER_ENTERED'}, 
                                        body={'values': info_array})
        sheet.spreadsheet.values_update("P/L!I{}:Z".format(start_row), 
                                        params={'valueInputOption': 'USER_ENTERED'},
                                        body={'values': item_array})
    

    def parse_glownest(self):

        print("Running parser for glownest!")
        
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
                while(not product.isnumeric() or (product.isnumeric() and int(product) > 12)):
                    print("Input was not valid! Try again...")
                    product = input("Please enter the number (0-12) that corresponds with the correct product for '{}': ".format(input_lineitem))
                
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
                elif(product == "12"):
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

        print("Running parser for herbiotics!")
        
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
                while(not product.isnumeric() or (product.isnumeric() and int(product) > 10)):
                    print("Input was not valid! Try again...")
                    product = input("Please enter the number (0-10) that corresponds with the correct product for '{}': ".format(input_lineitem))
                
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
                elif(prouct == "10"):
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

        print("Running parser for stacks!")
        
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
                while(not product.isnumeric() or (product.isnumeric() and int(product) > 2)):
                    print("Input was not valid! Try again...")
                    product = input("Please enter the number (0-2) that corresponds with the correct product for '{}': ".format(input_lineitem))
                
                # convert product index to product name
                product_name = "Ark Drops"
                if(product == "1"):
                    product_name = "ACVE"
                elif(product == "2"):
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


    def parse_nutria(self):

        print("Running parser for nutria!")

        # add nutria products to output dataframe
        self.output_data["Shilajit-30"] = ['']*len(self.output_data["Date"])
        self.output_data["Shilajit-120"] = ['']*len(self.output_data["Date"])
        self.output_data["Glutathione"] = ['']*len(self.output_data["Date"])

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
                print("0 - Shilajit-30")
                print("1 - Shilajit-120")
                print("2 - Glutathione")
                print("3 - test product, omit")
                product = input("Please enter the number (0-3) that corresponds with the correct product for '{}': ".format(input_lineitem))

                # Check that the input is a number and within the specified range
                while(not product.isnumeric() or (product.isnumeric() and int(product) > 3)):
                    print("Input was not valid! Try again...")
                    product = input("Please enter the number (0-3) that corresponds with the correct product for '{}': ".format(input_lineitem))
                
                # convert product index to product name
                product_name = "Shilajit-30"
                if(product == "1"):
                    product_name = "Shilajit-120"
                elif(product == "2"):
                    product_name = "Glutathione"
                elif(product == "3"):
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

        self.append_to_sheet_nutria()
    

    def append_to_sheet_nutria(self):
        
        # authenticate credentials to google drive and open sheet
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(self.credentials, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(self.nutria_sheet).worksheet("P/L")

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
            
            item_array.append([self.output_data["Shilajit-30"][i], 
                                self.output_data["Shilajit-120"][i], 
                                self.output_data["Glutathione"][i]])
        
        #paste arrays into spreadsheet
        sheet.spreadsheet.values_update("P/L!A{}:E".format(start_row), 
                                        params={'valueInputOption': 'USER_ENTERED'}, 
                                        body={'values': info_array})
        sheet.spreadsheet.values_update("P/L!J{}:L".format(start_row), 
                                        params={'valueInputOption': 'USER_ENTERED'},
                                        body={'values': item_array})
                

    def parse_glamore(self):

        print("Running parser for glamore!")

        # add glamore products to output dataframe
        self.output_data["MLE"] = ['']*len(self.output_data["Date"])
        self.output_data["BT"] = ['']*len(self.output_data["Date"])
        self.output_data["CNM"] = ['']*len(self.output_data["Date"])
        self.output_data["NGS"] = ['']*len(self.output_data["Date"])
        self.output_data["MB"] = ['']*len(self.output_data["Date"])
        self.output_data["MORINGA"] = ['']*len(self.output_data["Date"])
        self.output_data["GUMMIES"] = ['']*len(self.output_data["Date"])
        self.output_data["BESQUE"] = ['']*len(self.output_data["Date"])
        self.output_data["DIFFUSER"] = ['']*len(self.output_data["Date"])

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
                print("0 - MLE")
                print("1 - BT")
                print("2 - CNM")
                print("3 - NGS")
                print("4 - MB")
                print("5 - MORINGA")
                print("6 - GUMMIES")
                print("7 - BESQUE")
                print("8 - DIFFUSER")
                print("9 - test product, omit")
                product = input("Please enter the number (0-9) that corresponds with the correct product for '{}': ".format(input_lineitem))

                # Check that the input is a number and within the specified range
                while(not product.isnumeric() or (product.isnumeric() and int(product) > 9)):
                    print("Input was not valid! Try again...")
                    product = input("Please enter the number (0-9) that corresponds with the correct product for '{}': ".format(input_lineitem))

                # convert product index to product name
                product_name = "MLE"
                if(product == "1"):
                    product_name = "BT"
                elif(product == "2"):
                    product_name = "CNM"
                elif(product == "3"):
                    product_name = "NGS"
                elif(product == "4"):
                    product_name = "MB"
                elif(product == "5"):
                    product_name = "MORINGA"
                elif(product == "6"):
                    product_name = "GUMMIES"
                elif(product == "7"):
                    product_name = "BESQUE"
                elif(product == "8"):
                    product_name = "DIFFUSER"
                elif(product == "9"):
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

        self.append_to_sheet_glamore()


    def append_to_sheet_glamore(self):
        
        # authenticate credentials to google drive and open sheet
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(self.credentials, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(self.glamore_sheet).worksheet("P/L")

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

            item_array.append([self.output_data["MLE"][i], 
                                self.output_data["BT"][i], 
                                self.output_data["CNM"][i], 
                                self.output_data["NGS"][i], 
                                self.output_data["MB"][i], 
                                self.output_data["MORINGA"][i], 
                                self.output_data["GUMMIES"][i], 
                                self.output_data["BESQUE"][i], 
                                self.output_data["DIFFUSER"][i]])
            
        #paste arrays into spreadsheet
        sheet.spreadsheet.values_update("P/L!A{}:D".format(start_row), 
                                        params={'valueInputOption': 'USER_ENTERED'}, 
                                        body={'values': info_array})
        sheet.spreadsheet.values_update("P/L!H{}:P".format(start_row), 
                                        params={'valueInputOption': 'USER_ENTERED'},
                                        body={'values': item_array})


    def parse_ML(self):

        print("Running parsrer for ML!")
        
        # add ML products to output dataframe
        self.output_data["BED PAD"] = ['']*len(self.output_data["Date"])

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
                print("0 - BED PAD")
                print("1 - test product, omit")
                product = input("Please enter the number (0-1) that corresponds with the correct product for '{}': ".format(input_lineitem))

                # Check that the input is a number and within the specified range
                while(not product.isnumeric() or (product.isnumeric() and int(product) > 1)):
                    print("Input was not valid! Try again...")
                    product = input("Please enter the number (0-1) that corresponds with the correct product for '{}': ".format(input_lineitem))

                # convert product index to product name
                product_name = "BED PAD"
                if(product == "1"):
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

        self.append_to_sheet_ML()


    def append_to_sheet_ML(self):
        
        # authenticate credentials to google drive and open sheet
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(self.credentials, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(self.ML_sheet).worksheet("P/L")

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

            item_array.append([self.output_data["BED PAD"][i]])
            
        #paste arrays into spreadsheet
        sheet.spreadsheet.values_update("P/L!A{}:D".format(start_row), 
                                        params={'valueInputOption': 'USER_ENTERED'}, 
                                        body={'values': info_array})
        sheet.spreadsheet.values_update("P/L!H{}:H".format(start_row), 
                                        params={'valueInputOption': 'USER_ENTERED'},
                                        body={'values': item_array})


    def parse_NB(self):

        print("Running parser for NB!")
        
        # add NB products to output dataframe
        self.output_data["PATCHES"] = ['']*len(self.output_data["Date"])
        self.output_data["PLUMPER"] = ['']*len(self.output_data["Date"])
        self.output_data["BALM"] = ['']*len(self.output_data["Date"])
        self.output_data["OIL"] = ['']*len(self.output_data["Date"])
        self.output_data["HIJAB"] = ['']*len(self.output_data["Date"])
        self.output_data["EYE CREAM"] = ['']*len(self.output_data["Date"])

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
                print("0 - PATCHES")
                print("1 - PLUMPER")
                print("2 - BALM")
                print("3 - OIL")
                print("4 - HIJAB")
                print("5 - EYE CREAM")
                print("6 - test product, omit")
                product = input("Please enter the number (0-6) that corresponds with the correct product for '{}': ".format(input_lineitem))

                # Check that the input is a number and within the specified range
                while(not product.isnumeric() or (product.isnumeric() and int(product) > 6)):
                    print("Input was not valid! Try again...")
                    product = input("Please enter the number (0-6) that corresponds with the correct product for '{}': ".format(input_lineitem))

                # convert product index to product name
                product_name = "PATCHES"
                if(product == "1"):
                    product_name = "PLUMPER"
                elif(product == "2"):
                    product_name = "BALM"
                elif(product == "3"):
                    product_name = "OIL"
                elif(product == "4"):
                    product_name = "HIJAB"
                elif(product == "5"):
                    product_name = "EYE CREAM"
                elif(product == "6"):
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

        self.append_to_sheet_NB()


    def append_to_sheet_NB(self):
        
        # authenticate credentials to google drive and open sheet
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(self.credentials, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(self.NB_sheet).worksheet("P/L")

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

            item_array.append([self.output_data["PATCHES"][i], 
                                self.output_data["PLUMPER"][i], 
                                self.output_data["BALM"][i], 
                                self.output_data["OIL"][i], 
                                self.output_data["HIJAB"][i], 
                                self.output_data["EYE CREAM"][i]])
            
        #paste arrays into spreadsheet
        sheet.spreadsheet.values_update("P/L!A{}:D".format(start_row), 
                                        params={'valueInputOption': 'USER_ENTERED'}, 
                                        body={'values': info_array})
        sheet.spreadsheet.values_update("P/L!H{}:M".format(start_row), 
                                        params={'valueInputOption': 'USER_ENTERED'},
                                        body={'values': item_array})


    def parse_Rg(self):

        print("Running parser for Rg!")
        
        # add Rg products to output dataframe
        self.output_data["RUST CONVERTER"] = ['']*len(self.output_data["Date"])

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
                print("0 - RUST CONVERTER")
                print("1 - test product, omit")
                product = input("Please enter the number (0-1) that corresponds with the correct product for '{}': ".format(input_lineitem))

                # Check that the input is a number and within the specified range
                while(not product.isnumeric() or (product.isnumeric() and int(product) > 1)):
                    print("Input was not valid! Try again...")
                    product = input("Please enter the number (0-1) that corresponds with the correct product for '{}': ".format(input_lineitem))

                # convert product index to product name
                product_name = "RUST CONVERTER"
                if(product == "1"):
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

        self.append_to_sheet_Rg()
    

    def append_to_sheet_Rg(self):
        
        # authenticate credentials to google drive and open sheet
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(self.credentials, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(self.Rg_sheet).worksheet("P/L")

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

            item_array.append([self.output_data["RUST CONVERTER"][i]])
            
        #paste arrays into spreadsheet
        sheet.spreadsheet.values_update("P/L!A{}:D".format(start_row), 
                                        params={'valueInputOption': 'USER_ENTERED'}, 
                                        body={'values': info_array})
        sheet.spreadsheet.values_update("P/L!H{}:H".format(start_row), 
                                        params={'valueInputOption': 'USER_ENTERED'},
                                        body={'values': item_array})


    def parse_Fs(self):

        print("Running parser for Fs!")
        
        # add Fs products to output dataframe
        self.output_data["SPINNER"] = ['']*len(self.output_data["Date"])

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
                print("0 - SPINNER")
                print("1 - test product, omit")
                product = input("Please enter the number (0-1) that corresponds with the correct product for '{}': ".format(input_lineitem))

                # Check that the input is a number and within the specified range
                while(not product.isnumeric() or (product.isnumeric() and int(product) > 1)):
                    print("Input was not valid! Try again...")
                    product = input("Please enter the number (0-1) that corresponds with the correct product for '{}': ".format(input_lineitem))

                # convert product index to product name
                product_name = "SPINNER"
                if(product == "1"):
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

        self.append_to_sheet_Fs()

    
    def append_to_sheet_Fs(self):
        
        # authenticate credentials to google drive and open sheet
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(self.credentials, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(self.Fs_sheet).worksheet("P/L")

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

            item_array.append([self.output_data["SPINNER"][i]])
            
        #paste arrays into spreadsheet
        sheet.spreadsheet.values_update("P/L!A{}:D".format(start_row), 
                                        params={'valueInputOption': 'USER_ENTERED'}, 
                                        body={'values': info_array})
        sheet.spreadsheet.values_update("P/L!H{}:H".format(start_row), 
                                        params={'valueInputOption': 'USER_ENTERED'},
                                        body={'values': item_array})


    def parse_Bw(self):

        print("Running parser for Bw!")

        # add Bw products to output dataframe
        self.output_data["Serum"] = ['']*len(self.output_data["Date"])

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
                print("0 - Serum")
                print("1 - test product, omit")
                product = input("Please enter the number (0-1) that corresponds with the correct product for '{}': ".format(input_lineitem))

                # Check that the input is a number and within the specified range
                while(not product.isnumeric() or (product.isnumeric() and int(product) > 1)):
                    print("Input was not valid! Try again...")
                    product = input("Please enter the number (0-1) that corresponds with the correct product for '{}': ".format(input_lineitem))

                # convert product index to product name
                product_name = "Serum"
                if(product == "1"):
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

        self.append_to_sheet_Bw()
    

    def append_to_sheet_Bw(self):
        
        # authenticate credentials to google drive and open sheet
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(self.credentials, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(self.Bw_sheet).worksheet("P/L")

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

            item_array.append([self.output_data["Serum"][i]])
            
        #paste arrays into spreadsheet
        sheet.spreadsheet.values_update("P/L!A{}:E".format(start_row), 
                                        params={'valueInputOption': 'USER_ENTERED'}, 
                                        body={'values': info_array})
        sheet.spreadsheet.values_update("P/L!J{}:J".format(start_row), 
                                        params={'valueInputOption': 'USER_ENTERED'},
                                        body={'values': item_array})
    

    def parse_fleximart(self):

        print("Running parser for fleximart!")
        
        # add fleximart products to output dataframe
        self.output_data["GEL"] = ['']*len(self.output_data["Date"])
        self.output_data["DERMA"] = ['']*len(self.output_data["Date"])

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
                print("0 - GEL")
                print("1 - DERMA")
                print("2 - test product, omit")
                product = input("Please enter the number (0-2) that corresponds with the correct product for '{}': ".format(input_lineitem))

                # Check that the input is a number and within the specified range
                while(not product.isnumeric() or (product.isnumeric() and int(product) > 2)):
                    print("Input was not valid! Try again...")
                    product = input("Please enter the number (0-2) that corresponds with the correct product for '{}': ".format(input_lineitem))
                
                # convert product index to product name
                product_name = "GEL"
                if(product == "1"):
                    product_name = "DERMA"
                elif(product == "2"):
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

        self.append_to_sheet_fleximart()
    

    def append_to_sheet_fleximart(self):
        
        # authenticate credentials to google drive and open sheet
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(self.credentials, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(self.fleximart_sheet).worksheet("P/L")

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

            item_array.append([self.output_data["GEL"][i], 
                                self.output_data["DERMA"][i]])
            
        #paste arrays into spreadsheet
        sheet.spreadsheet.values_update("P/L!A{}:D".format(start_row), 
                                        params={'valueInputOption': 'USER_ENTERED'}, 
                                        body={'values': info_array})
        sheet.spreadsheet.values_update("P/L!H{}:I".format(start_row), 
                                        params={'valueInputOption': 'USER_ENTERED'},
                                        body={'values': item_array})


    def parse_gravien(self):

        print("Running parser for gravien!")
        
        # add gravien products to output dataframe
        self.output_data["LEAKSHIELD"] = ['']*len(self.output_data["Date"])
        self.output_data["12-in-1 Mg"] = ['']*len(self.output_data["Date"])
        self.output_data["Mg Complex"] = ['']*len(self.output_data["Date"])

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
                print("0 - LEAKSHIELD")
                print("1 - 12-in-1 Mg")
                print("2 - Mg Complex")
                print("3 - test product, omit")
                product = input("Please enter the number (0-3) that corresponds with the correct product for '{}': ".format(input_lineitem))

                # Check that the input is a number and within the specified range
                while(not product.isnumeric() or (product.isnumeric() and int(product) > 3)):
                    print("Input was not valid! Try again...")
                    product = input("Please enter the number (0-3) that corresponds with the correct product for '{}': ".format(input_lineitem))
                
                # convert product index to product name
                product_name = "LEAKSHIELD"
                if(product == "1"):
                    product_name = "12-in-1 Mg"
                elif(product == "2"):
                    product_name = "Mg Complex"
                elif(product == "3"):
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

        self.append_to_sheet_gravien()

    
    def append_to_sheet_gravien(self):
        
        # authenticate credentials to google drive and open sheet
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(self.credentials, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(self.gravien_sheet).worksheet("P/L")

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

            item_array.append([self.output_data["LEAKSHIELD"][i], 
                                self.output_data["12-in-1 Mg"][i], 
                                self.output_data["Mg Complex"][i]])
            
        #paste arrays into spreadsheet
        sheet.spreadsheet.values_update("P/L!A{}:D".format(start_row), 
                                        params={'valueInputOption': 'USER_ENTERED'}, 
                                        body={'values': info_array})
        sheet.spreadsheet.values_update("P/L!I{}:K".format(start_row), 
                                        params={'valueInputOption': 'USER_ENTERED'},
                                        body={'values': item_array})


    def parse_primep(self):

        print("Running parser for primep!")
        
        # add primep products to output dataframe
        self.output_data["MISWAK"] = ['']*len(self.output_data["Date"])

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
                print("0 - MISWAK")
                print("1 - test product, omit")
                product = input("Please enter the number (0-1) that corresponds with the correct product for '{}': ".format(input_lineitem))

                # Check that the input is a number and within the specified range
                while(not product.isnumeric() or (product.isnumeric() and int(product) > 1)):
                    print("Input was not valid! Try again...")
                    product = input("Please enter the number (0-1) that corresponds with the correct product for '{}': ".format(input_lineitem))
                
                # convert product index to product name
                product_name = "MISWAK"
                if(product == "1"):
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

        self.append_to_sheet_primep()


    def append_to_sheet_primep(self):
        
        # authenticate credentials to google drive and open sheet
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(self.credentials, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(self.primep_sheet).worksheet("P/L")

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

            item_array.append([self.output_data["MISWAK"][i]])
            
        #paste arrays into spreadsheet
        sheet.spreadsheet.values_update("P/L!A{}:D".format(start_row), 
                                        params={'valueInputOption': 'USER_ENTERED'}, 
                                        body={'values': info_array})
        sheet.spreadsheet.values_update("P/L!H{}:H".format(start_row), 
                                        params={'valueInputOption': 'USER_ENTERED'},
                                        body={'values': item_array})


    def parse_wandw(self):

        print("Running parser for wandw!")
        
        # add wandw products to output dataframe
        self.output_data["CAR WASH"] = ['']*len(self.output_data["Date"])
        self.output_data["IC TRAY"] = ['']*len(self.output_data["Date"])
        self.output_data["FLOSSER"] = ['']*len(self.output_data["Date"])

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
                print("0 - CAR WASH")
                print("1 - IC TRAY")
                print("2 - FLOSSER")
                print("3 - test product, omit")
                product = input("Please enter the number (0-3) that corresponds with the correct product for '{}': ".format(input_lineitem))

                # Check that the input is a number and within the specified range
                while(not product.isnumeric() or (product.isnumeric() and int(product) > 3)):
                    print("Input was not valid! Try again...")
                    product = input("Please enter the number (0-3) that corresponds with the correct product for '{}': ".format(input_lineitem))
                
                # convert product index to product name
                product_name = "CAR WASH"
                if(product == "1"):
                    product_name = "IC TRAY"
                elif(product == "2"):
                    product_name = "FLOSSER"
                elif(product == "3"):
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

        self.append_to_sheet_wandw()

    
    def append_to_sheet_wandw(self):
        
        # authenticate credentials to google drive and open sheet
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(self.credentials, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(self.wandw_sheet).worksheet("P/L")

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

            item_array.append([self.output_data["CAR WASH"][i], 
                                self.output_data["IC TRAY"][i], 
                                self.output_data["FLOSSER"][i]])
            
        #paste arrays into spreadsheet
        sheet.spreadsheet.values_update("P/L!A{}:D".format(start_row), 
                                        params={'valueInputOption': 'USER_ENTERED'}, 
                                        body={'values': info_array})
        sheet.spreadsheet.values_update("P/L!H{}:J".format(start_row), 
                                        params={'valueInputOption': 'USER_ENTERED'},
                                        body={'values': item_array})


    def parse_flash(self):

        print("Running parser for flash!")
        
        # add primep products to output dataframe
        self.output_data["TOWEL"] = ['']*len(self.output_data["Date"])

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
                print("0 - TOWEL")
                print("1 - test product, omit")
                product = input("Please enter the number (0-1) that corresponds with the correct product for '{}': ".format(input_lineitem))

                # Check that the input is a number and within the specified range
                while(not product.isnumeric() or (product.isnumeric() and int(product) > 1)):
                    print("Input was not valid! Try again...")
                    product = input("Please enter the number (0-1) that corresponds with the correct product for '{}': ".format(input_lineitem))
                
                # convert product index to product name
                product_name = "TOWEL"
                if(product == "1"):
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

        self.append_to_sheet_flash()


    def append_to_sheet_flash(self):
        
        # authenticate credentials to google drive and open sheet
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(self.credentials, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(self.flash_sheet).worksheet("P/L")

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

            item_array.append([self.output_data["TOWEL"][i]])
            
        #paste arrays into spreadsheet
        sheet.spreadsheet.values_update("P/L!A{}:D".format(start_row), 
                                        params={'valueInputOption': 'USER_ENTERED'}, 
                                        body={'values': info_array})
        sheet.spreadsheet.values_update("P/L!H{}:H".format(start_row), 
                                        params={'valueInputOption': 'USER_ENTERED'},
                                        body={'values': item_array})

    
    def parse_all_stores(self):

        # run script for all stores that are selected in the __init__ function
        if(self.run_gc):
            self.csv_parser(self.gc_input, alt=False)
            self.parse_GC()
            del self.output_data
        if(self.run_fleximart):
            self.csv_parser(self.fleximart_input, alt=False)
            self.parse_fleximart()
            del self.output_data
        if(self.run_glownest):
            self.csv_parser(self.glownest_input, alt=False)
            self.parse_glownest()
            del self.output_data
        if(self.run_herbiotics):
            self.csv_parser(self.herbiotics_input, alt=False)
            self.parse_herbiotics()
            del self.output_data
        if(self.run_Bw):
            self.csv_parser(self.Bw_input, alt=True)
            self.parse_Bw()
            del self.output_data
        if(self.run_stacks):
            self.csv_parser(self.stacks_input, alt=True)
            self.parse_stacks()
            del self.output_data
        if(self.run_nutria):
            self.csv_parser(self.nutria_input, alt=True)
            self.parse_nutria()
            del self.output_data
        if(self.run_gravien):
            self.csv_parser(self.gravien_input, alt=False)
            self.parse_gravien()
            del self.output_data
        if(self.run_primep):
            self.csv_parser(self.primep_input, alt=False)
            self.parse_primep()
            del self.output_data
        if(self.run_glamore):
            self.csv_parser(self.glamore_input, alt=False)
            self.parse_glamore()
            del self.output_data
        if(self.run_wandw):
            self.csv_parser(self.wandw_input, alt=False)
            self.parse_wandw()
            del self.output_data
        if(self.run_flash):
            self.csv_parser(self.flash_input, alt=False)
            self.parse_flash()
            del self.output_data
        if(self.run_ML):
            self.csv_parser(self.ML_input, alt=False)
            self.parse_ML()
            del self.output_data
        if(self.run_NB):
            self.csv_parser(self.NB_input, alt=False)
            self.parse_NB()
            del self.output_data
        if(self.run_Rg):
            self.csv_parser(self.Rg_input, alt=False)
            self.parse_Rg()
            del self.output_data
        if(self.run_Fs):
            self.csv_parser(self.Fs_input, alt=False)
            self.parse_Fs()
            del self.output_data
        print("Parsing complete!")
            

def main(argv=None):
    
    if argv is None:
        argv = sys.argv
    
    sp = ShopifyParser()
    sp.csv_parser(sp.gc_input)
    sp.parse_GC()

if __name__ == "__main__":
    sys.exit(main())