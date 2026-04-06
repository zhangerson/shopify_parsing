import pandas as pd
import sys
import numpy as np
import csv
import gspread
from oauth2client.service_account import ServiceAccountCredentials

class ShopifyParser:

    def __init__(self):
        self.masterfile = pd.read_csv("product_masterfile.csv")

    def csv_parser(self, csv_file):

        # read in csv file and extract relevant columns
        df = pd.read_csv(csv_file)
        self.input_data = df[["Name", "Created at", "Shipping Phone", "Total", "Lineitem name", "Lineitem quantity"]]
        data_length = len(self.input_data["Name"])

        # initialize output dataframe
        self.output_data = {"Date": [], 
                       "Order Number": [], 
                       "Phone Number": [], 
                       "Revenue": []
                       }

        # loop through all rows
        for i in range(data_length):

            # parse date into readable string
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

        # parse item
        self.parse_GC(data_length)

    def parse_GC(self, data_length):
        
        # add GC products to output dataframe
        self.output_data["Undereye serum PCS"] = ['']*data_length
        self.output_data["Hairline powder"] = ['']*data_length
        self.output_data["Hair Donut Scrunchie"] = ['']*data_length
        self.output_data["Night Shred"] = ['']*data_length
        self.output_data["Seamless Straight Hair Strand"] = ['']*data_length
        self.output_data["Seapuri Scalpy Hair System"] = ['']*data_length
        self.output_data["DHT Blocker"] = ['']*data_length
        self.output_data["Feg Hair Growth Spray"] = ['']*data_length
        self.output_data["Moroccan Blue Nila Skin Whitening Powder"] = ['']*data_length
        self.output_data["Zephta H-Regrow 2.0"] = ['']*data_length
        self.output_data["Shampoo"] = ['']*data_length
        self.output_data["Effeclar Duo Acne Spot Treatment"] = ['']*data_length
        self.output_data["Facial Hair Removal Kit"] = ['']*data_length
        self.output_data["SleepSlime Gummies"] = ['']*data_length
        self.output_data["The Real Beef Tallow Balm"] = ['']*data_length

        # loop through all line items and mark their quantities
        for i in range(data_length):

            input_lineitem = self.input_data["Lineitem name"][i]

            # check if product is known
            lookup_idx = np.where(np.asarray(self.masterfile["Lineitem name"]) == input_lineitem)[0]
            if(len(lookup_idx) == 0):

                # if product is not known, add to product masterfile
                print("Product '{}' is not known, product needs to be added to the masterfile.".format(input_lineitem))
                print("0 - Undereye serum PCS")
                print("1 - Hairline powder")
                print("2 - Hair Donut Scrunchie")
                print("3 - Night Shred")
                print("4 - Seamless Straight Hair Strand")
                print("5 - Seapuri Scalpy Hair System")
                print("6 - DHT Blocker")
                print("7 - Feg Hair Growth Spray")
                print("8 - Moroccan Blue Nila Skin Whitening Powder")
                print("9 - Zephta H-Regrow 2.0")
                print("10 - Shampoo")
                print("11 - Effeclar Duo Acne Spot Treatment")
                print("12 - Facial Hair Removal Kit")
                print("13 - SleepSlime Gummies")
                print("14 - The Real Beef Tallow Balm")
                product = input("Please enter the number (0-14) that corresponds with the correct product for '{}': ".format(input_lineitem))

                # Check that the input is a number and within the specified range
                while(not product.isnumeric()):
                    print("Input was not valid! Try again...")
                    product = input("Please enter the number (0-14) that corresponds with the correct product for '{}': ".format(input_lineitem))
                    if(product.isnumeric() and int(product) > 14):
                        product = "Nan"
                while(int(product) > 14):
                    print("Input was not valid! Try again...")
                    product = input("Please enter the number (0-14) that corresponds with the correct product for '{}': ".format(input_lineitem))
                
                # convert product index to product name
                product_name = "Undereye serum PCS"
                if(product == "1"):
                    product_name = "Hairline powder"
                elif(product == "2"):
                    product_name = "Hair Donut Scrunchie"
                elif(product == "3"):
                    product_name = "Night Shred"
                elif(product == "4"):
                    product_name = "Seamless Straight Hair Strand"
                elif(product == "5"):
                    product_name = "Seapuri Scalpy Hair System"
                elif(product == "6"):
                    product_name = "DHT Blocker"
                elif(product == "7"):
                    product_name = "Feg Hair Growth Spray"
                elif(product == "8"):
                    product_name = "Moroccan Blue Nila Skin Whitening Powder"
                elif(product == "9"):
                    product_name = "Zephta H-Regrow 2.0"
                elif(product == "10"):
                    product_name = "Shampoo"
                elif(product == "11"):
                    product_name = "Effeclar Duo Acne Spot Treatment"
                elif(product == "12"):
                    product_name = "Facial Hair Removal Kit"
                elif(product == "13"):
                    product_name = "SleepSlime Gummies"
                elif(product == "14"):
                    product_name = "The Real Beef Tallow Balm"

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

                # write data to output dataframe
                total = int(quantity) * int(self.input_data["Lineitem quantity"][i])
                self.output_data[product_name][i] = "{:.0f}".format(total)
            else:

                # extract data from masterfile
                product_name = self.masterfile["Product"][lookup_idx[0]]
                total = int(self.input_data["Lineitem quantity"][i]) * int(self.masterfile["Quantity"][lookup_idx[0]])

                # write data to output dataframe
                self.output_data[product_name][i] = "{:.0f}".format(total)
        
        self.append_to_sheet(data_length)

    def append_to_sheet(self, data_length):
        
        # authenticate credentials to google drive and open sheet
        sheet_id = "1F9JwYgSKO0wY9qPPMuJNsiX2eCz4inUIljOKK45I0A8"
        tab_name = "P/L"
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(sheet_id).worksheet(tab_name)
        existing_data = sheet.get_all_values()

        start_row = len(sheet.col_values(2))

        for i in range(data_length):

            sheet.

def main(argv=None):
    
    if argv is None:
        argv = sys.argv
    
    sp = ShopifyParser()
    sp.csv_parser("orders_export (4).csv")

    df = pd.DataFrame(sp.output_data)
    df.to_csv("test.csv", index=False)

if __name__ == "__main__":
    sys.exit(main())