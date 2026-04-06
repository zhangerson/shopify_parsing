import pandas as pd

class ShopifyParser:

    def __init__(self):
        self.masterfile = pd.read_csv("product_masterfile.csv")

    def csv_parser(self, csv_file):

        # read in csv file and extract relevant columns
        df = pd.read_csv(csv_file)
        input_data = df["Name", "Created at", "Shipping Phone", "Total", "Lineitem Name", "Lineitem Quantity"]
        data_length = len(input_data["Name"])

        # initialize output dataframe
        output_data = {"Date": [], 
                       "Order Number": [], 
                       "Phone Number": [], 
                       "Revenue": [],
                       "Undereye serum PCS": [0]*data_length,
                       "Hairline powder ": [0]*data_length,
                       "Hair Donut Scrunchie": [0]*data_length,
                       "Night Shred": [0]*data_length,
                       "Seamless Straight Hair Strand": [0]*data_length,
                       "Seapuri Scalpy Hair Serum": [0]*data_length,
                       "DHT Blocker": [0]*data_length,
                       "Feg Hair Growth Spray": [0]*data_length,
                       "Moroccan Blue Nila Skin Whitening Powder": [0]*data_length,
                       "Zephta H-Regrow 2.0": [0]*data_length,
                       "Shampoo": [0]*data_length,
                       "Effeclar Duo Acne Spot Treatment": [0]*data_length,
                       "Facial Hair Removal Kit": [0]*data_length,
                       "SleepSlime Gummies": [0]*data_length,
                       "The Real Beef Tallow Balm": [0]*data_length
                       }

        # loop through all rows
        for i in range(data_length):

            # parse date into readable string
            date_str = input_data["Created at"][i]
            date_obj = pd.to_datetime(date_str)
            output_data["Date"].append(date_obj.strftime("%B %-d, %Y"))

            # parse order number

            # how does order no work

            # parse phone number
            phone_str = "{:.0f}".format(input_data["Shipping Phone"][i])
            if(phone_str.startswith("880")):
                phone_str = phone_str[3:]
            elif(phone_str.startswith("92")):
                phone_str = phone_str[2:]
            elif(phone_str.startswith("966")):
                phone_str = phone_str[4:] # why 4th index

            # parse revenue
            output_data["Revenue"].append("{:.2f}".format(input_data["Total"][i]))

            # parse item
            
            # what are the shop names